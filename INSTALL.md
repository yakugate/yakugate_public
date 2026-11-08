# Installing Yakugate

Yakugate is a single Go binary. It self-provisions its MySQL databases on first start (creates them, applies migrations, seeds a demo account), so you can send your first SMS within a couple of minutes.

## Prerequisites

- **Go 1.22+** — https://go.dev/dl/
- **MySQL 8.x** running locally or reachable over the network (the default backend). MSSQL, MongoDB, and Redis are also supported per store, but MySQL is the quickest start.

## Quick install (2 commands)

```sh
go install github.com/yakugate/yakugate/cmd/yakugate@latest
yakugate http-server
```

That's it for a first look — with no `yakugate.config` the engine starts with a no-op queue and an empty account cache, so you can see it boot. To actually accept and queue messages, give it MySQL credentials (next section).

## Full setup (clone + configure)

```sh
git clone https://github.com/yakugate/yakugate.git
cd yakugate
cp yakugate.config.example yakugate.config   # then edit the DSN passwords
chmod 600 yakugate.config
go run ./cmd/yakugate http-server
```

The minimum `yakugate.config` for a local MySQL on the default port:

```ini
YAKU_API_ADDR=:8080
YAKU_QUEUE_BACKEND=mysql
YAKU_QUEUE_DSN=root:yourpassword@tcp(127.0.0.1:3306)/yakugate_queue?parseTime=true
YAKU_CONFIG_BACKEND=mysql
YAKU_CONFIG_DSN=root:yourpassword@tcp(127.0.0.1:3306)/yakugate_config?parseTime=true
YAKU_HOT_BACKEND=mysql
YAKU_HOT_DSN=root:yourpassword@tcp(127.0.0.1:3306)/yakugate_hot?parseTime=true
YAKU_COLD_BACKEND=fs
YAKU_COLD_DSN=./cold
```

### Where the settings file can live

The gateway reads one settings file, `yakugate.config`. Name it on the command line,
or put it in one of the default places; the first one found is used:

```sh
yakugate --config /etc/yakugate/yakugate.config http-server   # a file you name (must exist)
```

1. `./yakugate.config`, the folder the gateway is started in
2. `yakugate.config` next to the gateway binary
3. `~/.config/yakugate/yakugate.config`, the user's config folder
4. `/etc/yakugate/yakugate.config` (`%ProgramData%\yakugate\yakugate.config` on Windows)

`yakugate --help` prints these folders for your machine. An environment variable
that is already set wins over the same line in the file, so a container can run
with environment variables alone and no file. Relative paths in the file are
relative to the start folder. A line that is not `NAME=value` or a `#` comment
stops the start and the error names the line. Each role logs which file it read.
The file was called `sms.config` until 2026-10-02. That name is still read, in each of those places, when there is no `yakugate.config` there, and the start-up log asks you to rename it. An older `.env` in the start folder is still read when no settings file exists.

On first start the engine **creates the three databases, applies all migrations, and seeds fixture accounts** automatically (disable with `YAKU_AUTO_MIGRATE=false` / `YAKU_AUTO_SEED=false`). You never run SQL by hand.

## Send your first message

The seed includes a demo account with API key `testkey123`:

```sh
curl -X POST http://localhost:8080/sms/v4/send \
  -H "x-api-key: testkey123" \
  -H "Content-Type: application/json" \
  -d '{"to":"923001234567","from":"DEMO","text":"hello from yakugate"}'
```

A successful response returns `"status":"accepted"` with a `messageId` (a ULID). Multiple recipients: comma-separate `to` — you get one result per recipient. Basic auth (`username:password`) works as an alternative to `x-api-key`.

## Running the other roles

One binary, one role per instance — scale by running more instances:

```sh
yakugate <role>
```

| Role | What it does |
|------|--------------|
| `http-server` | HTTP Submit API (the ingress above) |
| `smpp-server` | SMPP ingress for client binds |
| `http-vendor` / `smpp-vendor` | Delivery to upstream vendors over HTTP / SMPP |
| `router` | Routing off the accept path (optional thin-ingress mode) |
| `retry` / `expiry` | Retry with backoff; expire past-validity messages |
| `finalizer` / `retention` / `backup` | Records to hot store; hot→cold archival; backups |
| `importer` | Bulk HLR / reference-data import |
| `abuse-monitor` | Async abuse/fraud detection |

Run `yakugate` with no arguments to list all roles. **Status:** the project is pre-alpha — `http-server` is fully implemented; the other roles are scaffolds under active development, being built in the order 3 → 4 → 5 (SMPP vendor egress, HTTP vendor egress, delivery receipts).

## Choosing storage backends

Each of the four stores is independently configurable via `YAKU_<STORE>_BACKEND` / `_DSN` (see `yakugate.config.example` for DSN formats):

| Store | Purpose | Allowed backends |
|-------|---------|------------------|
| Queue / HLR | per-vendor queues, HLR, API tokens | `mysql`, `mssql`, `redis` |
| Configurations | accounts, vendors, routes, templates | `mysql`, `mssql`, `mongo` |
| Hot | recent message records | `mysql`, `mssql`, `mongo` |
| Cold | long-term archive | `fs` (a directory path) |

Only MySQL is auto-provisioned; for MSSQL/Mongo apply the scripts in `migrations/` manually. If you use Redis as the queue, run it with `maxmemory-policy noeviction` — an evicting queue silently drops accepted messages.

## Build from source / develop

```sh
make build     # go build ./...
make run ROLE=http-server                        # uses ./yakugate.config
make run ROLE=http-server CONFIG=/path/yakugate.config
make vet
```

## Troubleshooting

- **`bootstrap FAILED: ...` at startup** — the engine can't reach MySQL or the user lacks `CREATE DATABASE` rights. Check the DSN in `yakugate.config`; the format is `user:pass@tcp(HOST:3306)/DBNAME?parseTime=true`.
- **`queue backend "mysql" unavailable; using no-op queue`** — same cause; messages will be accepted but not durably queued until the DSN works.
- **401 on `/sms/v4/send`** — seeding may be disabled (`YAKU_AUTO_SEED=false`) or you're using a different key; the demo key is `testkey123`.
- **Port already in use** — change `YAKU_API_ADDR` (e.g. `:8081`).
