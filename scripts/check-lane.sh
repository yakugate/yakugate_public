#!/usr/bin/env bash
# check-lane.sh <lane>: go build ./... and every library module, then go vet
# and go test -count=1 on one lane's packages only, then the lanes rule
# (internal/lanes). Each step is timed.
#
# Lanes (internal/lanes/lanes.go): client-edge vendor-edge receipts-records
# data config-ops core shared. With no lane it lists them. A lane's packages
# include its library modules (vendor-edge: lib/smpp; shared: lib/encoding).
#
# Tests reach the local test containers only: Redis 127.0.0.1:16399, and
# nothing on 192.168.x.x or the dev MySQL on 3306.
set -euo pipefail
cd "$(dirname "$0")/.."

lane="${1:-}"
if [ -z "$lane" ]; then
	echo "usage: scripts/check-lane.sh <lane>"
	go run ./internal/lanes/cmd/lanelist
	exit 2
fi

# Local test stores only.
for v in $(env | sed -n 's/^\([A-Za-z_][A-Za-z0-9_]*\)=.*\(192\.168\.\|127\.0\.0\.1:3306\|localhost:3306\).*/\1/p'); do
	unset "$v"
done
unset YAKU_LIVE
export YAKU_TEST_REDIS_ADDR=127.0.0.1:16399

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
		printf 'lane %s FAILED at %s after %.1fs\n' "$lane" "$name" "$(echo "$(now) - $start" | bc)"
		exit $rc
	fi
}

pkgs=$(go run ./internal/lanes/cmd/lanelist "$lane")
# ./... is the root module only; each lib/ module is built by its path.
libs=$(for m in lib/*/go.mod; do echo "github.com/yakugate/yakugate/${m%/go.mod}/..."; done)
echo "lane $lane: $(echo "$pkgs" | wc -l | tr -d ' ') packages"
# shellcheck disable=SC2086
{
	step build go build ./... $libs
	step vet go vet $pkgs
	# One package at a time: packages share the test Redis and flush it.
	step test go test -count=1 -p 1 $pkgs
	step lanes go test -count=1 ./internal/lanes/...
}
printf 'lane %s passed in %.1fs\n' "$lane" "$(echo "$(now) - $start" | bc)"
