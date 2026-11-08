#!/usr/bin/env bash
# Builds the downloadable release (2026-10-02): one archive per platform the
# website's Download page lists, plus checksums.txt, into
# dist/release/<version>/. `make release` runs it; `make site-downloads` then
# copies the result into the website (www/public/downloads/<version>/).
#
# Each archive holds the binary (yakugate, or yakugate.exe on Windows) with
# README.md, LICENSE and yakugate.config.example: tar.gz for Linux and macOS, zip
# for Windows, named yakugate_<version>_<os>_<arch>.<ext> — the names the
# website links to (www/src/config.js releaseAsset).
#
# Reproducible: CGO_ENABLED=0 -trimpath, stamped with the commit and the
# commit's own time (never the build time), and archived with fixed owners,
# order and timestamps, so two builds of one commit are byte-identical.
#
# Trusted by Windows and macOS (2026-10-02): an UNSIGNED program downloaded
# from the internet is blocked by SmartScreen/Defender and by Gatekeeper,
# whatever it does. Only signing fixes that, and signing needs certificates
# in the company's name (products_docs/engineering/guides/release-signing.md).
# This script signs when it is given the commands, and says when it is not:
#
#   WINDOWS_SIGN_CMD     run with the path of yakugate.exe appended, e.g.
#                        jsign --storetype TRUSTEDSIGNING ... (Azure Trusted Signing)
#   MACOS_SIGN_CMD       run with the path of each macOS binary appended, e.g.
#                        rcodesign sign --p12-file id.p12 --p12-password-file pw --code-signature-flags runtime
#   MACOS_NOTARIZE_CMD   run with the path of a zip holding the signed binary, e.g.
#                        rcodesign notary-submit --api-key-file key.json --wait
#
# Windows builds also carry version information (product, version,
# description, icon) and keep their symbols: an anonymous, stripped Go
# executable is a common false positive for Defender's heuristics.
set -euo pipefail
cd "$(dirname "$0")/.."
# Built from go.mod alone, as the public tree builds: its replace lines take
# the lib/ modules from their folders, so go.work is not needed.
export GOWORK=off

VERSION=$(sed -n 's/^[[:space:]]*Version[[:space:]]*=[[:space:]]*"\([^"]*\)".*/\1/p' internal/platform/version/version.go)
[ -n "$VERSION" ] || { echo "cannot read the version from internal/platform/version/version.go" >&2; exit 1; }
COMMIT=$(git rev-parse --short HEAD)
EPOCH=$(git log -1 --format=%ct)
DATE=$(TZ=UTC git log -1 --date=format-local:%Y-%m-%dT%H:%M:%SZ --format=%cd)
if [ -n "$(git status --porcelain -- cmd internal lib migrations go.mod go.sum)" ]; then
  echo "warning: uncommitted engine changes — the binaries will not match commit $COMMIT" >&2
fi

PKG=github.com/yakugate/yakugate/internal/platform/version
OUT=dist/release/$VERSION
TARGETS="linux/amd64 linux/arm64 darwin/arm64 darwin/amd64 windows/amd64"
WORK=$(mktemp -d)
trap 'rm -rf "$WORK" cmd/yakugate/rsrc_windows_*.syso' EXIT

# sign_one signs one binary when the platform's command is given, and records
# which platforms went out unsigned.
UNSIGNED=""
sign_one() {
  local os=$1 bin=$2
  case "$os" in
    windows)
      if [ -n "${WINDOWS_SIGN_CMD:-}" ]; then
        echo "  signing $(basename "$bin") (Windows)"; eval "$WINDOWS_SIGN_CMD \"\$bin\""
      else UNSIGNED="$UNSIGNED windows"; fi ;;
    darwin)
      if [ -n "${MACOS_SIGN_CMD:-}" ]; then
        echo "  signing $(basename "$bin") (macOS)"; eval "$MACOS_SIGN_CMD \"\$bin\""
        if [ -n "${MACOS_NOTARIZE_CMD:-}" ]; then
          local z="$WORK/notarize-$$.zip"
          (cd "$(dirname "$bin")" && zip -q -X "$z" "$(basename "$bin")")
          echo "  notarizing $(basename "$bin") with Apple"; eval "$MACOS_NOTARIZE_CMD \"\$z\""
          rm -f "$z"
        else UNSIGNED="$UNSIGNED macos-not-notarized"; fi
      else UNSIGNED="$UNSIGNED macos"; fi ;;
  esac
}
rm -rf "$OUT"
mkdir -p "$OUT"

for t in $TARGETS; do
  os=${t%/*} arch=${t#*/}
  name=yakugate_${VERSION}_${os}_${arch}
  exe=yakugate; [ "$os" = windows ] && exe=yakugate.exe
  mkdir -p "$WORK/$name"
  echo "building $name"
  strip="-s -w"
  syso=""
  if [ "$os" = windows ]; then
    strip="" # keep symbols on Windows (see above)
    syso=cmd/yakugate/rsrc_windows_${arch}.syso
    numeric=$(printf '%s' "$VERSION" | sed 's/[^0-9.].*//')
    (cd "$WORK" && go run github.com/tc-hib/go-winres@v0.3.3 simply --arch "$arch" --out rsrc \
      --manifest cli --product-name "Yakugate SMS Gateway" --product-version "$VERSION" \
      --file-version "${numeric}.0" --file-description "Yakugate - carrier-grade SMS gateway" \
      --copyright "Copyright (c) 2026 Yakugate - MIT License" --original-filename yakugate.exe \
      --icon "$OLDPWD/scripts/release/icon.png")
    mv "$WORK/rsrc_windows_${arch}.syso" "$syso"
  fi
  CGO_ENABLED=0 GOOS=$os GOARCH=$arch go build -trimpath \
    -ldflags "$strip -X $PKG.Commit=$COMMIT -X $PKG.Date=$DATE" \
    -o "$WORK/$name/$exe" ./cmd/yakugate
  [ -n "$syso" ] && rm -f "$syso"
  sign_one "$os" "$WORK/$name/$exe"
  cp README.md LICENSE yakugate.config.example "$WORK/$name/"
  find "$WORK/$name" -exec touch -h -d "@$EPOCH" {} +
  if [ "$os" = windows ]; then
    (cd "$WORK" && find "$name" | LC_ALL=C sort | zip -q -X -D "$OLDPWD/$OUT/$name.zip" -@)
  else
    tar -C "$WORK" --sort=name --mtime="@$EPOCH" --owner=0 --group=0 --numeric-owner -cf - "$name" \
      | gzip -n -9 > "$OUT/$name.tar.gz"
  fi
done

(cd "$OUT" && sha256sum yakugate_* > checksums.txt)
if [ -n "$UNSIGNED" ]; then
  echo "warning: built UNSIGNED for:$(printf '%s\n' $UNSIGNED | sort -u | tr '\n' ' ')— Windows and macOS will block these downloads" >&2
  echo "         until they are signed (products_docs/engineering/guides/release-signing.md)." >&2
fi
echo "done: $OUT"
ls -l "$OUT"
