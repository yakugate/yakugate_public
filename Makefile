BINARY=yakugate

GO            ?= go
COVERAGE_MIN  ?= 70
COVERAGE_FILE := coverage.out
VERSION       := $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS       := -s -w -X main.version=$(VERSION)

.PHONY: help build run yakugate tidy vet test check fmt lint test-race cover cover-html integration vuln bench dist release site-downloads ci hooks test-api test-hierarchy test-dialect test-custompath test-p1 test-coldstart test-p3 test-db-scratch test-go-scratch test-p3-redis test-p7 test-p7-redis test-p8 test-p9 clean

help: ## List all targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

# Build stamp (VERSIONS.md §1.3): the binary reports version + commit + date.
VERSION_PKG := github.com/yakugate/yakugate/internal/platform/version
# The engine's own version (version.go), which names the release folder.
SITE_VERSION := $(shell sed -n 's/^[[:space:]]*Version[[:space:]]*=[[:space:]]*"\([^"]*\)".*/\1/p' internal/platform/version/version.go)
LDFLAGS := -X $(VERSION_PKG).Commit=$(shell git rev-parse --short HEAD 2>/dev/null || echo unknown) -X $(VERSION_PKG).Date=$(shell date -u +%Y-%m-%dT%H:%M:%SZ)
# The library modules of go.work (lib/*): ./... is the root module only, so
# each is named by its path, which works with or without the workspace.
LIBS := $(foreach m,$(wildcard lib/*/go.mod),github.com/yakugate/yakugate/$(patsubst %/go.mod,%,$(m))/...)
build: ## Compile the engine and the lib/ modules
	$(GO) build -ldflags "$(LDFLAGS)" ./... $(LIBS)
yakugate: ## Build bin/yakugate with the version stamp
	$(GO) build -ldflags "$(LDFLAGS)" -o bin/yakugate ./cmd/yakugate
run: ## Run a role: make run ROLE=http-server [CONFIG=path/to/yakugate.config]
	$(GO) run ./cmd/yakugate $(if $(CONFIG),--config $(CONFIG)) $(ROLE)
tidy: ## Sync and verify module dependencies
	$(GO) mod tidy
	$(GO) mod verify
vet: ## go vet only (fast), engine and lib/ modules
	$(GO) vet ./... $(LIBS)
test: ## Unit tests (plain), engine and lib/ modules
	$(GO) test ./... $(LIBS)

# P1 register TC-199 (go build clean) + TC-200 (go test passes), PO 2026-09-17.
# Unit tests are the engine's own packages; sqa/e2e/go/* need live stores and
# have their own targets below (test-p1, test-p3, ...).
check: ## Gate 0: build + vet + engine unit tests (TC-199 / TC-200)
	$(GO) build ./... $(LIBS)
	$(GO) vet ./... $(LIBS)
	$(GO) test -count=1 ./cmd/... ./internal/... $(LIBS)

# The full suite (before a push): every module of go.work built, vetted and
# tested, one package at a time, on the local test stores only.
.PHONY: test-all
test-all: ## Full suite: build, vet and test every module of go.work (scripts/test-all.sh)
	scripts/test-all.sh

# Lanes (internal/lanes/lanes.go): one lane's packages only — build, vet,
# unit tests, then the lanes rule. Local test stores only (scripts/check-lane.sh).
.PHONY: check-lanes check-client-edge check-vendor-edge check-receipts-records check-data check-config-ops check-core check-shared
check-lanes: ## Lanes rule: no import crosses lanes beyond those allowed today
	$(GO) test -count=1 -v ./internal/lanes/...
check-client-edge: ## Lane check: client-edge (Salman)
	scripts/check-lane.sh client-edge
check-vendor-edge: ## Lane check: vendor-edge, with lib/smpp (Saleem)
	scripts/check-lane.sh vendor-edge
check-receipts-records: ## Lane check: receipts-records (Raza)
	scripts/check-lane.sh receipts-records
check-data: ## Lane check: data — every DAL, migrations (Saleem)
	scripts/check-lane.sh data
check-config-ops: ## Lane check: config-ops (Saleem)
	scripts/check-lane.sh config-ops
check-core: ## Lane check: core contracts (Saleem)
	scripts/check-lane.sh core
check-shared: ## Lane check: shared libraries, with lib/encoding (Saleem as lead)
	scripts/check-lane.sh shared

# ---------------------------------------------------------------------------
# SQA gates (framework: sqa/ — see sqa/README.md; local authority = `make ci`)
# ---------------------------------------------------------------------------
fmt: ## Auto-format (gofumpt + goimports via golangci-lint)
	golangci-lint fmt ./...

lint: ## Gate 2: static analysis (vet + golangci-lint, .golangci.yml)
	$(GO) vet ./...
	golangci-lint run ./...

test-race: ## Gate 3: unit tests (race + shuffle + coverage profile)
	$(GO) test -race -shuffle=on -covermode=atomic -coverprofile=$(COVERAGE_FILE) ./...

cover: test-race ## Gate 3: enforce the coverage threshold
	@total=$$($(GO) tool cover -func=$(COVERAGE_FILE) | awk '/^total:/ {sub("%","",$$3); print $$3}'); \
	echo "coverage: $$total% (gate: $(COVERAGE_MIN)%)"; \
	awk -v t=$$total -v m=$(COVERAGE_MIN) 'BEGIN { exit (t+0 < m+0) ? 1 : 0 }' || \
	  { echo "FAIL: coverage $$total% is below the $(COVERAGE_MIN)% gate"; exit 1; }

cover-html: test-race ## Open the HTML coverage report
	$(GO) tool cover -html=$(COVERAGE_FILE)

integration: ## Gate 4: integration tests (//go:build integration; needs store DSNs)
	$(GO) test -tags=integration -race -count=1 ./...

vuln: ## Gate 5: reachability-aware CVE scan
	govulncheck ./...

bench: ## Gate 7: benchmarks with memory stats
	$(GO) test -run=^$$ -bench=. -benchmem ./...

dist: ## Gate 6: reproducible, version-stamped build into dist/
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags '$(LDFLAGS)' -o dist/ ./...

release: ## The downloads: every platform's archive + checksums.txt into dist/release/<version>/
	./scripts/build-release.sh

# Only this version's files are written: every earlier release's downloads
# stay on the site (Saleem, 6 Nov 2026: 1.2.0-beta "should not be removed, it
# should be kept"). Nothing is deleted; a rebuild of the same version
# overwrites its own files.
site-downloads: release ## Copy the release into the website (www/public/downloads/<version>/, not in git); earlier versions stay
	mkdir -p www/public/downloads/$(SITE_VERSION)
	cp dist/release/$(SITE_VERSION)/* www/public/downloads/$(SITE_VERSION)/
	@echo "website downloads in www/public/downloads — rebuild and deploy the site to publish them"

ci: tidy lint cover integration vuln dist ## Run every automatable gate locally, in order

hooks: ## Install the git pre-commit hook (one-time setup)
	git config core.hooksPath .githooks
	chmod +x .githooks/pre-commit
	@echo "pre-commit hook active (.githooks/pre-commit)"

# Local dev only — the SQA workspace (sqa/) is not in the public repo.
# Gate 7 black-box API tests against a running Submit API (make run ROLE=http-server).
BASE?=http://localhost:8080
KEY?=testkey123
test-api: ## Gate 7: black-box Submit-API suite (sqa/e2e/api)
	@test -f sqa/e2e/api/run.py || (echo "SQA harness not present (local dev only)" && exit 1)
	python sqa/e2e/api/run.py --base $(BASE) --key $(KEY)

test-hierarchy: ## Gate 8: tenant hierarchy / reject-log / rate-versioning e2e
	@test -f sqa/e2e/hierarchy_reject_e2e.py || (echo "SQA harness not present (local dev only)" && exit 1)
	python sqa/e2e/hierarchy_reject_e2e.py

test-dialect: ## Gate 9: per-account API dialect e2e (mapping, GET, formats, auth)
	@test -f sqa/e2e/dialect_e2e.py || (echo "SQA harness not present (local dev only)" && exit 1)
	python sqa/e2e/dialect_e2e.py

test-custompath: ## Gate 10: per-account custom submit path e2e
	@test -f sqa/e2e/custompath_e2e.py || (echo "SQA harness not present (local dev only)" && exit 1)
	python sqa/e2e/custompath_e2e.py

test-p1: ## Gate 10b: P1 blocker Go e2e — cold start, 100-way prepaid atomicity, TPS benchmark (YAKU_P1_BENCH=1); dev MySQL from yakugate.config
	@test -d sqa/e2e/go/p1h || (echo "SQA harness not present (local dev only)" && exit 1)
	$(GO) test ./sqa/e2e/go/p1h -count=1 -v -timeout 900s

test-p3: ## Gate 11: vendor-egress Go e2e suites — P3 smpp-vendor + P4 http-vendor (in-process, Go mocks; dev MySQL from yakugate.config or YAKU_TEST_MYSQL)
	@test -d sqa/e2e/go/p3h || (echo "SQA harness not present (local dev only)" && exit 1)
	$(GO) test ./sqa/e2e/go/... -count=1 -timeout 900s

test-coldstart: ## Gate 10c: a gateway must build its own stores from a bare MySQL and run WITHOUT the Admin API (PO 2026-09-18) — TC-001, then engine + Admin API from empty in one pass each
	@test -d sqa/e2e/go/p1h || (echo "SQA harness not present (local dev only)" && exit 1)
	$(GO) test ./sqa/e2e/go/p1h -run TestTC001_ColdStartOnBareMySQL -count=1 -timeout 900s
	$(GO) build -o bin/yakugate ./cmd/yakugate
	bash sqa/e2e/go/scratch-db.sh t_ --drop

test-db-scratch: ## Build a scratch copy of the three MySQL stores for the Go harnesses (prefix t_); add DROP=1 to start empty
	@test -f sqa/e2e/go/scratch-db.sh || (echo "SQA harness not present (local dev only)" && exit 1)
	bash sqa/e2e/go/scratch-db.sh t_ $(if $(DROP),--drop,)

test-go-scratch: ## Gate 11c: every Go e2e suite against the scratch stores — no live role shares them (run test-db-scratch first)
	@test -d sqa/e2e/go/p3h || (echo "SQA harness not present (local dev only)" && exit 1)
	YAKU_TEST_DB_PREFIX=t_ $(GO) test ./sqa/e2e/go/... -count=1 -timeout 1800s

test-p3-redis: ## Gate 11b: same suites under the Redis queue gate (needs dev Redis)
	@test -d sqa/e2e/go/p3h || (echo "SQA harness not present (local dev only)" && exit 1)
	YAKU_P3TEST_REDIS=1 $(GO) test ./sqa/e2e/go/... -count=1 -timeout 900s

test-p7: ## Gate 12: P7 reliability (retry+expiry) Go e2e — full TC-P7-01..10 register (needs dev MySQL)
	@test -d sqa/e2e/go/p7h || (echo "SQA harness not present (local dev only)" && exit 1)
	$(GO) test ./sqa/e2e/go/p7h -count=1 -v -timeout 600s

test-p7-redis: ## Gate 12b: same under the Redis queue gate (isolated Redis DB 9)
	@test -d sqa/e2e/go/p7h || (echo "SQA harness not present (local dev only)" && exit 1)
	YAKU_P3TEST_REDIS=1 $(GO) test ./sqa/e2e/go/p7h -count=1 -v -timeout 600s

test-p8: ## Gate 13: P8 storage-lifecycle Go e2e — retention families + backup/encryption (MySQL-only by design)
	@test -d sqa/e2e/go/p8h || (echo "SQA harness not present (local dev only)" && exit 1)
	$(GO) test ./sqa/e2e/go/p8h -count=1 -v -timeout 600s

test-p9: ## Gate 14: P9 rating & billing Go e2e — rate cards, FX/providers, prepaid, MO, summaries (MySQL-only by design)
	@test -d sqa/e2e/go/p9h || (echo "SQA harness not present (local dev only)" && exit 1)
	$(GO) test ./sqa/e2e/go/p9h -count=1 -v -timeout 600s

clean: ## Remove build and test artifacts
	rm -rf dist $(COVERAGE_FILE)
