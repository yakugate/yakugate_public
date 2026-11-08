# lib/smpp

The SMPP 3.4 code of the [Yakugate](../../README.md) SMS gateway, as a Go module of its own:

- `pdu` — encodes and decodes SMPP 3.4 PDUs (both sides of a bind), GSM-7 packed and unpacked, the text reader.
- `session` — dials an SMSC and binds (plain or TLS), then reads and writes PDUs on the bind.
- `window` — the async send window: how many `submit_sm` may be unanswered, and which message each answer belongs to.

It uses only the Go standard library and imports nothing from the gateway.

## Use it

```go
import (
	"github.com/yakugate/yakugate/lib/smpp/pdu"
	"github.com/yakugate/yakugate/lib/smpp/window"
)

win := window.New[myMessage](100) // window.Window holds the caller's own message type
```

```
go get github.com/yakugate/yakugate/lib/smpp@latest
```

Versions are tags `lib/smpp/vX.Y.Z`. Inside the Yakugate repository the root `go.mod` takes this folder by a `replace` line, so no tag is needed there.

## License

MIT, as the rest of Yakugate ([LICENSE](../../LICENSE)).
