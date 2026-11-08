package provision

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"github.com/yakugate/yakugate/internal/dal"
)

// Where a CLIENT connects to a deployment (config/0147, D-137, Saleem
// 2026-10-15: "no ip no port defined in portal, we need one, so that operator
// may know"). The engine never reads these: it listens where its settings say
// (YAKU_API_ADDR, YAKU_SMPP_ADDR, YAKU_SMPP_TLS_ADDR). They are what the
// public side of a load balancer or a firewall's NAT makes of those, which
// only an operator knows, and the Admin API builds each account's "How to
// connect" sheet from them. The rules are deploymentRoutes.js
// cleanClientAddresses and assertSmppPorts — change together.

// DeploymentClient is one deployment of the register and what its clients
// are told. A zero port or an empty string = not said yet (NULL).
type DeploymentClient struct {
	ID           int64
	DeploymentID string
	Name         string
	Disabled     bool
	APIURL       string // client_api_url
	SMPPHost     string // client_smpp_host
	SMPPPort     int    // client_smpp_port
	SMPPTLSPort  int    // client_smpp_tls_port
}

// ListDeploymentClients is the register, by deployment id.
func (s Store) ListDeploymentClients(ctx context.Context) ([]DeploymentClient, error) {
	list, err := s.DB.DeploymentClientList(ctx)
	if err != nil {
		var re dal.RowsError
		if errors.As(err, &re) {
			return nil, fmt.Errorf("read deployment: %w", re.Err)
		}
		return nil, fmt.Errorf("list deployments: %w", err)
	}
	var out []DeploymentClient
	for _, d := range list {
		out = append(out, DeploymentClient(d))
	}
	return out, nil
}

// GetDeploymentClient reads one deployment.
func (s Store) GetDeploymentClient(ctx context.Context, id int64) (DeploymentClient, error) {
	row, err := s.DB.DeploymentClient(ctx, id)
	d := DeploymentClient(row)
	if errors.Is(err, dal.ErrNoRows) {
		return d, refusef("there is no deployment #%d", id)
	}
	if err != nil {
		return d, fmt.Errorf("read deployment %d: %w", id, err)
	}
	return d, nil
}

// DeploymentClientFields are the four addresses, and what each means.
func DeploymentClientFields() []Field {
	return []Field{
		{Key: "client_api_url", Label: "Client API address", Kind: "text",
			Hint: "the base address HTTP clients send to, like https://sms.example.com — each account's own path is added to it. A pasted /sms/v4/send is cut back. Type " + MasterNumberNone + " to clear it"},
		{Key: "client_smpp_host", Label: "SMPP host", Kind: "text",
			Hint: "the host name or IP address SMPP clients bind to, like smpp.example.com — no smpp:// and no port. Type " + MasterNumberNone + " to clear it"},
		{Key: "client_smpp_port", Label: "SMPP port", Kind: "text",
			Hint: "the plain SMPP port clients bind to, like 2775. Type " + MasterNumberNone + " to clear it"},
		{Key: "client_smpp_tls_port", Label: "SMPP TLS port", Kind: "text",
			Hint: "the SMPP over TLS port clients bind to, like 3550; not the plain port. Type " + MasterNumberNone + " for none offered"},
	}
}

// DeploymentClientValue is a setting as stored, "" for none.
func DeploymentClientValue(d DeploymentClient, key string) string {
	port := func(n int) string {
		if n == 0 {
			return ""
		}
		return strconv.Itoa(n)
	}
	switch key {
	case "client_api_url":
		return d.APIURL
	case "client_smpp_host":
		return d.SMPPHost
	case "client_smpp_port":
		return port(d.SMPPPort)
	case "client_smpp_tls_port":
		return port(d.SMPPTLSPort)
	}
	return ""
}

// UpdateDeploymentClient changes one address. "" or "none" clears it.
// changed is false when the value was already so; a refusal changes nothing.
func (s Store) UpdateDeploymentClient(ctx context.Context, id int64, key, value string) (changed bool, err error) {
	cur, err := s.GetDeploymentClient(ctx, id)
	if err != nil {
		return false, err
	}
	value = strings.TrimSpace(value)
	if strings.EqualFold(value, MasterNumberNone) {
		value = ""
	}
	var after any // nil = NULL
	switch key {
	case "client_api_url":
		u, err := CleanClientAPIURL(value)
		if err != nil {
			return false, err
		}
		if u == cur.APIURL {
			return false, nil
		}
		after = nullStr(u)
	case "client_smpp_host":
		h, err := CleanClientSMPPHost(value)
		if err != nil {
			return false, err
		}
		if h == cur.SMPPHost {
			return false, nil
		}
		after = nullStr(h)
	case "client_smpp_port", "client_smpp_tls_port":
		what, now, other := "SMPP port", cur.SMPPPort, cur.SMPPTLSPort
		if key == "client_smpp_tls_port" {
			what, now, other = "SMPP TLS port", cur.SMPPTLSPort, cur.SMPPPort
		}
		n, err := CleanClientPort(what, value)
		if err != nil {
			return false, err
		}
		if n == now {
			return false, nil
		}
		if n != 0 && n == other {
			return false, refusef("the SMPP port and the SMPP TLS port cannot be the same port")
		}
		if n != 0 {
			after = n
		}
	default:
		return false, refusef("%q is not a setting on a deployment", key)
	}
	before := DeploymentClientValue(cur, key)
	// key is one of the four column names above, never typed by anyone.
	if err := s.DB.SetDeploymentClientColumn(ctx, id, key, after); err != nil {
		return false, fmt.Errorf("update deployment %d: %w", id, err)
	}
	return true, s.audit(ctx, auditEntry{
		Action: "deployments.update", Entity: "deployment", Feature: "deployments", EntityID: id,
		Label:   cur.DeploymentID,
		Summary: "Updated deployment " + cur.DeploymentID + " from the console",
		Detail:  map[string]any{"deployment_id": cur.DeploymentID, "fields": []string{key}},
		Before:  map[string]any{key: nullStr(before)},
		After:   map[string]any{key: after},
	})
}

// CleanClientAPIURL is the client API address as it is kept: http(s), no
// user, password, query or fragment, a pasted /sms/v4/send cut back to its
// base, at most 255 characters. "" = none.
func CleanClientAPIURL(v string) (string, error) {
	v = strings.TrimSpace(v)
	if v == "" {
		return "", nil
	}
	u, err := url.Parse(v)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return "", refusef("the client API address must be a full address, like https://sms.example.com")
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "http" && scheme != "https" {
		return "", refusef("the client API address must start with http:// or https://")
	}
	if u.User != nil {
		return "", refusef("the client API address must not carry a user or password")
	}
	if u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || strings.HasSuffix(v, "#") {
		return "", refusef("the client API address must not carry a query or a #fragment")
	}
	// The host as a browser's URL reads it: lower case, the scheme's own
	// port dropped.
	host := strings.ToLower(u.Host)
	if p := u.Port(); (scheme == "http" && p == "80") || (scheme == "https" && p == "443") {
		host = strings.TrimSuffix(host, ":"+p)
	}
	path := strings.TrimRight(u.EscapedPath(), "/")
	path = strings.TrimSuffix(path, "/sms/v4/send")
	out := scheme + "://" + host + path
	if len(out) > 255 {
		return "", refusef("the client API address must be 255 characters or fewer")
	}
	return out, nil
}

var (
	hostLabels = regexp.MustCompile(`^([A-Za-z0-9]([A-Za-z0-9-]{0,61}[A-Za-z0-9])?)(\.[A-Za-z0-9]([A-Za-z0-9-]{0,61}[A-Za-z0-9])?)*$`)
	hostIPv6   = regexp.MustCompile(`^[0-9A-Fa-f:]+$`)
	hostScheme = regexp.MustCompile(`(?i)^[a-z]+://`)
	hostPort   = regexp.MustCompile(`^[A-Za-z0-9.-]+:\d+$`)
)

// CleanClientSMPPHost is the SMPP host as it is kept: a host name or an IP
// address, no scheme and no port, brackets round an IPv6 address dropped, at
// most 255 characters. "" = none.
func CleanClientSMPPHost(v string) (string, error) {
	v = strings.TrimSpace(v)
	switch {
	case v == "":
		return "", nil
	case hostScheme.MatchString(v):
		return "", refusef("the SMPP host is a name or an address only, like smpp.example.com — no smpp:// in front")
	case hostPort.MatchString(v):
		return "", refusef("put the SMPP port in its own setting, not after the host")
	case !hostLabels.MatchString(v) && !hostIPv6.MatchString(strings.TrimSuffix(strings.TrimPrefix(v, "["), "]")):
		return "", refusef("the SMPP host must be a host name or an IP address, like smpp.example.com or 203.0.113.7")
	case len(v) > 255:
		return "", refusef("the SMPP host must be 255 characters or fewer")
	}
	return strings.TrimSuffix(strings.TrimPrefix(v, "["), "]"), nil
}

// CleanClientPort is a port from 1 to 65535; "" = none (0).
func CleanClientPort(what, v string) (int, error) {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 || n > 65535 {
		return 0, refusef("the %s must be a whole number from 1 to 65535", what)
	}
	return n, nil
}
