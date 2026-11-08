# lib/encoding

How the [Yakugate](../../README.md) SMS gateway counts and sends SMS text, as a Go module of its own:

- `Plan` — the encoding (GSM-7, UCS-2 or Latin-1) and the number of parts of a text: GSM-7 160/153 (extension characters count 2), UCS-2 70/67.
- `SplitByCost` — splits a text into parts, never cutting an extension character.
- `ParseWireRules`, `WireRules` — what each encoding is sent to one vendor as, with which `data_coding`, and how many parts that makes.

It uses only the Go standard library and imports nothing from the gateway.

## Use it

```go
import "github.com/yakugate/yakugate/lib/encoding"
```

```
go get github.com/yakugate/yakugate/lib/encoding@latest
```

Versions are tags `lib/encoding/vX.Y.Z`. Inside the Yakugate repository the root `go.mod` takes this folder by a `replace` line, so no tag is needed there.

## License

MIT, as the rest of Yakugate ([LICENSE](../../LICENSE)).
