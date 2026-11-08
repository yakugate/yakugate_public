// Package session manages an outbound SMPP bind to a vendor SMSC for the vendor
// sender (P3): dial + bind + framed I/O. Writes are serialized; the caller runs
// a single read loop and a keepalive ticker. Reconnect is the caller's job. [SCOPE P3]
package session

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/yakugate/yakugate/lib/smpp/pdu"
)

// Client is a bound SMPP client connection.
type Client struct {
	conn net.Conn
	r    *bufio.Reader
	wmu  sync.Mutex
}

// Dial connects to addr and binds with the given mode
// (pdu.BindTransmitter/BindReceiver/BindTransceiver), returning once the SMSC
// answers bind_resp ESME_ROK. tlsConfig nil means a plain TCP connection
// (unchanged behavior); non-nil dials TLS (or mutual TLS, when tlsConfig
// carries a client certificate) instead.
func Dial(addr, systemID, password string, mode uint32, timeout time.Duration, tlsConfig *tls.Config, bindOpt pdu.BindOptions) (*Client, error) {
	return DialContext(context.Background(), addr, systemID, password, mode, timeout, tlsConfig, bindOpt)
}

// DialContext is Dial that gives up the moment ctx ends: while connecting,
// and while waiting for the vendor's answer to the bind. A session that is
// told to stop must not sit in a connection attempt for another twenty
// seconds — the vendor counts that half-open bind as a session of ours.
func DialContext(ctx context.Context, addr, systemID, password string, mode uint32, timeout time.Duration, tlsConfig *tls.Config, bindOpt pdu.BindOptions) (*Client, error) {
	dialer := &net.Dialer{Timeout: timeout}
	var conn net.Conn
	var err error
	if tlsConfig != nil {
		conn, err = (&tls.Dialer{NetDialer: dialer, Config: tlsConfig}).DialContext(ctx, "tcp", addr)
	} else {
		conn, err = dialer.DialContext(ctx, "tcp", addr)
	}
	if err != nil {
		return nil, err
	}
	// Closing the connection is what ends the wait for bind_resp below.
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()
	c := &Client{conn: conn, r: bufio.NewReader(conn)}
	if err := c.Write(pdu.BindPDU(mode, systemID, password, 1, bindOpt)); err != nil {
		conn.Close()
		return nil, err
	}
	_ = conn.SetReadDeadline(time.Now().Add(timeout))
	p, err := pdu.Read(c.r)
	_ = conn.SetReadDeadline(time.Time{})
	if err != nil {
		conn.Close()
		return nil, err
	}
	if p.Header.CommandStatus != pdu.ESME_ROK {
		conn.Close()
		return nil, fmt.Errorf("bind rejected: status 0x%08X", p.Header.CommandStatus)
	}
	if ctx.Err() != nil {
		// Stopped in the instant the vendor answered: the bind is not ours to keep.
		conn.Close()
		return nil, ctx.Err()
	}
	return c, nil
}

// Write sends one framed PDU (serialized against concurrent writers).
func (c *Client) Write(b []byte) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	_, err := c.conn.Write(b)
	return err
}

// Read blocks for the next inbound PDU; the caller runs this in its read loop.
func (c *Client) Read() (*pdu.PDU, error) { return pdu.Read(c.r) }

// SetReadDeadline bounds the next Read (keepalive liveness).
func (c *Client) SetReadDeadline(t time.Time) error { return c.conn.SetReadDeadline(t) }

// Close tears down the bind connection.
func (c *Client) Close() error { return c.conn.Close() }
