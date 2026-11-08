package crypto

// Stream sealing with the GATEWAY key. [SCOPE P13-T4]
//
// Encrypt/Decrypt seal short strings (a message body). A packet capture is a
// file of any size, so it is sealed in chunks instead, as it is produced —
// the plain bytes never need to be on disk at all.
//
// The envelope rule is the same one the rest of this package follows: the data
// is sealed by a DATA key (one per month, "dek:gw:<purpose>:<YYYYMM>"), and the
// data key is stored only wrapped by the gateway key (YAKU_CRYPTO_KEK_REF).
// It is always the gateway key, never a company's KMS: what is sealed here
// belongs to the operator, not to one tenant.
//
// FAIL CLOSED: with no gateway key SealStream returns ErrNoKEK before reading
// a byte. There is no plain fallback.
//
// File layout:
//
//	"YKS1" | u16 key-ref length | key ref | 8 random nonce-prefix bytes
//	then chunks: u32 sealed length | AES-256-GCM(chunk)
//
// Chunk nonce = prefix || u32 counter. Chunk AAD = key ref || 0x00 (more
// follow) or 0x01 (last chunk). The last-chunk flag is authenticated, so a
// file cut short — or with chunks dropped or reordered — fails to open rather
// than reading as a shorter capture.

import (
	"context"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	streamMagic     = "YKS1"
	streamChunk     = 64 * 1024
	streamMaxSealed = streamChunk + 64 // chunk + GCM tag, with slack
)

// ErrStreamCorrupt is returned by OpenStream for a file that is not a sealed
// stream, was cut short, or fails authentication.
var ErrStreamCorrupt = errors.New("crypto: sealed stream is damaged or incomplete")

// gatewayKeyRef names the monthly data key for one purpose ("pcap").
func (s *Service) gatewayKeyRef(purpose string) string {
	return "dek:gw:" + purpose + ":" + s.now().UTC().Format("200601")
}

// SealStream reads src to the end and writes it to dst sealed under the
// gateway key. It returns how many PLAIN bytes were read. purpose names the
// data key family (letters only, e.g. "pcap").
func (s *Service) SealStream(ctx context.Context, purpose string, dst io.Writer, src io.Reader) (int64, error) {
	if !s.HasDefaultKey() {
		return 0, fmt.Errorf("%w: no gateway key (YAKU_CRYPTO_KEK_REF)", ErrNoKEK)
	}
	keyRef := s.gatewayKeyRef(purpose)
	dek, err := s.dek(ctx, keyRef, s.kekRef)
	if err != nil {
		return 0, err
	}
	gcm, err := gcmFor(dek)
	if err != nil {
		return 0, err
	}
	head := make([]byte, 0, len(streamMagic)+2+len(keyRef)+8)
	head = append(head, streamMagic...)
	head = binary.BigEndian.AppendUint16(head, uint16(len(keyRef)))
	head = append(head, keyRef...)
	noncePrefix := make([]byte, 8)
	if _, err := rand.Read(noncePrefix); err != nil {
		return 0, err
	}
	head = append(head, noncePrefix...)
	if _, err := dst.Write(head); err != nil {
		return 0, err
	}

	var (
		total   int64
		counter uint32
		cur     = make([]byte, streamChunk)
		next    = make([]byte, streamChunk)
	)
	// One chunk is always held back, so that when src ends the chunk in hand
	// can be marked as the last one.
	n, rerr := io.ReadFull(src, cur)
	for {
		last := rerr != nil
		var m int
		var nerr error
		if !last {
			m, nerr = io.ReadFull(src, next)
			if nerr != nil && m == 0 {
				last, rerr = true, nerr
			}
		}
		if err := writeChunk(dst, gcm, noncePrefix, counter, keyRef, cur[:n], last); err != nil {
			return total, err
		}
		total += int64(n)
		counter++
		if last {
			break
		}
		cur, next = next, cur
		n, rerr = m, nerr
	}
	if rerr != nil && rerr != io.EOF && rerr != io.ErrUnexpectedEOF {
		// The source broke rather than ended. What was read is sealed and
		// closed properly; the caller still learns the read failed.
		return total, rerr
	}
	return total, nil
}

func chunkNonce(prefix []byte, counter uint32) []byte {
	nonce := make([]byte, 0, nonceBytes)
	nonce = append(nonce, prefix...)
	return binary.BigEndian.AppendUint32(nonce, counter)
}

func chunkAAD(keyRef string, last bool) []byte {
	aad := append([]byte(keyRef), 0)
	if last {
		aad[len(aad)-1] = 1
	}
	return aad
}

func writeChunk(dst io.Writer, gcm cipher.AEAD, prefix []byte, counter uint32, keyRef string, plain []byte, last bool) error {
	sealed := gcm.Seal(nil, chunkNonce(prefix, counter), plain, chunkAAD(keyRef, last))
	var l [4]byte
	binary.BigEndian.PutUint32(l[:], uint32(len(sealed)))
	if _, err := dst.Write(l[:]); err != nil {
		return err
	}
	_, err := dst.Write(sealed)
	return err
}

// OpenStream reads a stream written by SealStream and writes the plain bytes
// to dst. Bytes are written chunk by chunk as each one authenticates; a
// damaged or cut-short file ends with ErrStreamCorrupt, so a caller that
// streams to a client must treat an error as "the download is not complete".
func (s *Service) OpenStream(ctx context.Context, dst io.Writer, src io.Reader) error {
	if s == nil {
		return fmt.Errorf("%w: no gateway key", ErrNoKEK)
	}
	head := make([]byte, len(streamMagic)+2)
	if _, err := io.ReadFull(src, head); err != nil || string(head[:len(streamMagic)]) != streamMagic {
		return ErrStreamCorrupt
	}
	refLen := int(binary.BigEndian.Uint16(head[len(streamMagic):]))
	rest := make([]byte, refLen+8)
	if _, err := io.ReadFull(src, rest); err != nil {
		return ErrStreamCorrupt
	}
	keyRef, noncePrefix := string(rest[:refLen]), rest[refLen:]
	dek, err := s.dek(ctx, keyRef, "")
	if err != nil {
		return err
	}
	gcm, err := gcmFor(dek)
	if err != nil {
		return err
	}
	var counter uint32
	var l [4]byte
	for {
		if _, err := io.ReadFull(src, l[:]); err != nil {
			return ErrStreamCorrupt // ended with no last chunk seen
		}
		size := binary.BigEndian.Uint32(l[:])
		if size > streamMaxSealed {
			return ErrStreamCorrupt
		}
		sealed := make([]byte, size)
		if _, err := io.ReadFull(src, sealed); err != nil {
			return ErrStreamCorrupt
		}
		nonce := chunkNonce(noncePrefix, counter)
		plain, err := gcm.Open(nil, nonce, sealed, chunkAAD(keyRef, false))
		last := false
		if err != nil {
			if plain, err = gcm.Open(nil, nonce, sealed, chunkAAD(keyRef, true)); err != nil {
				return ErrStreamCorrupt
			}
			last = true
		}
		if _, err := dst.Write(plain); err != nil {
			return err
		}
		if last {
			return nil
		}
		counter++
	}
}
