#!/usr/bin/env bash
# test-all.sh: the full suite. Builds, vets and tests every module of go.work
# (the engine and each library module under lib/), each step timed.
#
# The engine's tests are ./internal/... and ./cmd/... (never ./..., which
# holds the sqa/ harnesses that need live stores), one package at a time:
# packages share the test Redis and flush it. go test ./... from the root
# does not reach lib/: each library is its own module.
#
# Tests reach the local test containers only: Redis 127.0.0.1:16399, and
# nothing on 192.168.x.x or the dev MySQL on 3306.
set -euo pipefail
cd "$(dirname "$0")/.."
root=$(pwd)

# Local test stores only.
for v in $(env | sed -n 's/^\([A-Za-z_][A-Za-z0-9_]*\)=.*\(192\.168\.\|127\.0\.0\.1:3306\|localhost:3306\).*/\1/p'); do
	unset "$v"
done
unset YAKU_LIVE
export YAKU_TEST_REDIS_ADDR=127.0.0.1:16399

# The modules: go.work's, else the root and every lib/*/go.mod.
if [ -f go.work ] && [ "${GOWORK:-}" != off ]; then
	mods=$(go list -m -f '{{.Dir}}')
else
	mods="$root $(for m in lib/*/go.mod; do echo "$root/$(dirname "$m")"; done)"
fi

now() { perl -MTime::HiRes=time -e 'printf "%.1f", time'; }
start=$(now)
step() {
	local name=$1 t0
	shift
	t0=$(now)
	rc=0
	"$@" || rc=$?
	printf '  %-6s %5.1fs%s\n' "$name" "$(echo "$(now) - $t0" | bc)" "$([ $rc = 0 ] || echo "  FAILED")"
	if [ $rc != 0 ]; then
		printf 'test-all FAILED in %s at %s after %.1fs\n' "$mod" "$name" "$(echo "$(now) - $start" | bc)"
		exit $rc
	fi
}

for dir in $mods; do
	mod=${dir#"$root"}
	mod=${mod#/}
	mod=${mod:-.}
	echo "module $mod ($(sed -n "s/^module //p" "$dir/go.mod"))"
	cd "$dir"
	step build go build ./...
	step vet go vet ./...
	if [ "$mod" = . ]; then
		step test go test -count=1 -p 1 ./internal/... ./cmd/...
	else
		step test go test -count=1 -p 1 ./...
	fi
	cd "$root"
done
printf 'test-all passed in %.1fs\n' "$(echo "$(now) - $start" | bc)"
