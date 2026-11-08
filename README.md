<div align="center">

# Yakugate 約

**The modern, open-source SMS core.**
Built for high throughput, designed for the cloud, and backed by a promise.

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Status: Pre-Alpha](https://img.shields.io/badge/status-pre--alpha-orange.svg)](#project-status)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](CONTRIBUTING.md)

</div>

---

## What is Yakugate?

For decades, the telecom industry has relied on legacy gateways like Kannel to route SMS traffic. While foundational, these systems were built for bare-metal servers, struggle with modern cloud-native deployments, and lack native support for modern REST APIs.

**Yakugate is the successor.** It is a highly concurrent, carrier-grade SMS gateway that turns durable, store-and-forward messaging into a single, flawlessly routed pipeline. Both edges are symmetric: a **client** sends to Yakugate over **REST API** or **SMPP**, Yakugate routes to a **vendor** over **SMPP** or **API**, and inbound messages (MO) and delivery receipts (DLR) flow back the same way — over whichever transport each side uses.

### The meaning of "Yaku" (約)

In Japanese, *Yaku* means **"The Promise"** or **"The Pact."** In an industry where dropped messages cost millions, our software's name is our architectural philosophy: when an enterprise hands a message payload to Yakugate, its delivery is a strict promise.

---

## Why Yakugate?

- **Built for SMS at scale.** A focused, store-and-forward pipeline that does one thing exceptionally well.
- **Symmetric REST/SMPP on both edges.** Clients connect by HTTP or SMPP; vendors are reached by SMPP or HTTP API. Any combination routes cleanly through the queue.
- **Tiered storage.** Redis for real-time work, MySQL for hot relational data and recent records, and the filesystem for long-term cold archival.
- **Cloud-native first.** Built from day one to run in Docker and Kubernetes, scaling horizontally with ease — no bare-metal assumptions baked in.
- **SLA-ready reliability.** Completely stateless worker nodes target 99.99% uptime. If a node dies, another instantly picks up the queue.
- **Developer-friendly.** Modern JSON REST APIs and reliable webhooks replace the need to understand complex telecom protocols.
- **Zero vendor lock-in.** 100% free and open source. No feature gating, no artificial throughput limits.

---

## Architecture at a glance

Yakugate uses a decoupled, hybrid architecture that pairs raw telecom execution speed with a contributor-friendly web ecosystem.

| Layer | Technology | Responsibility |
|-------|-----------|----------------|
| **SMS Engine** | Go (Golang) | The entire engine — HTTP + SMPP ingress, routing, HLR, anti-abuse, vendor egress (SMPP & HTTP), DLR, and the async workers. Goroutines handle tens of thousands of connections without memory bloat. |
| **Admin API** | Node.js (JavaScript) | Backs the React portal — management/reporting endpoints (users, templates, routes, vendors, anti-abuse config). Not in the message hot path. |
| **Admin UI** | React (JavaScript) | Browser dashboard for accounts, vendors, routing rules, and observability. |
| **Queue + Real-Time State** | Redis | Durable store-and-forward queues (Redis Streams) for MT, MO, and DLR — zero accepted-message loss — plus real-time rate-limit counters. See [products_docs/engineering/data-stores/redis.md](products_docs/engineering/data-stores/redis.md). |
| **Config + Hot Store** | MySQL (supported today); MSSQL / MongoDB are Phase 2 | Accounts, vendors, routes, templates, HLR, and recent MDRs. The MSSQL and MongoDB backends are kept in the code but cannot run the gateway yet. See [products_docs/engineering/data-stores/mysql.md](products_docs/engineering/data-stores/mysql.md), [products_docs/engineering/data-stores/mongo.md](products_docs/engineering/data-stores/mongo.md), [products_docs/engineering/architecture/data-model.md](products_docs/engineering/architecture/data-model.md). |
| **Cold Store** | Filesystem | Older MDRs archived out of MySQL as immutable, date-partitioned files for long-term retention. |

For the full design, see **[ARCHITECTURE.md](ARCHITECTURE.md)**.

```
   CLIENTS                                                           VENDORS
   ───────                                                           ───────

  REST / Webhooks ──►┌──────────────────────────┐  HTTP API ──────►
  ◄── MO · DLR       │ API & Management (Node.js)│  ◄── MO · DLR (webhook/poll)
   (HTTP)            │ REST · webhooks · vendor  │   (HTTP)
                     │     API connector · UI    │
                     └─────────────┬────────────┘
                                   │ enqueue / consume
                          ┌────────▼─────────┐
                          │      Redis       │  Streams = durable queues
                          │  queues + state  │  (MT · MO · DLR) + rate limits
                          └────────▲─────────┘
                                   │ enqueue / consume
  SMPP submit_sm ───►┌────────────┴─────────────┐  submit_sm ──────►
  ◄── deliver_sm     │     Core Engine (Go)      │  ◄── deliver_sm   SMPP
   (MO · DLR)        │ SMPP server · SMPP client │   (MO · DLR)
                     │  routing · rate limiting  │
                     └─────────────┬────────────┘
                                   ▼
                  MySQL (hot: accounts, vendors, routes, recent MDRs)
                                   ▼
                  Filesystem (cold: archived MDRs, date-partitioned)
```

Either side can be HTTP or SMPP. Clients submit MT over REST (Node) or SMPP (Go); the router picks a vendor and sends over that vendor's transport — SMPP (Go) or HTTP API (Node). MO and DLR arrive from vendors over either transport, get queued in Redis, and return to each client over its configured channel — webhook or SMPP `deliver_sm`. Records land hot in MySQL and age out to the filesystem cold store.

---

## Quick start

> **Note:** Yakugate is pre-alpha. You need Go 1.25+ and MySQL 8; Redis is optional.

```bash
# Clone and build
git clone https://github.com/yakugate/yakugate.git
cd yakugate
make yakugate                   # → bin/yakugate

# Settings: copy the example and set your MySQL DSNs and admin key
cp yakugate.config.example yakugate.config

# Run the whole gateway: every role as its own process, restarted if one exits.
# The gateway creates and migrates its own databases on first start.
./bin/yakugate                  # or: ./bin/yakugate --config /etc/yakugate/yakugate.config

# Or run a single role
./bin/yakugate http-server

# Configure it with no portal: companies, accounts, vendors, routing, rates,
# and start/stop/restart roles (Gateway menu). Default password: YakuAdmin@123 —
# change it at first login.
./bin/yakugate console

# Send your first message
curl -X POST http://localhost:8080/sms/v4/send \
  -H "yaku-sms-key: $YAKUGATE_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"to": "923001234567", "from": "BRAND", "text": "Hello from Yakugate."}'
```

`yakugate` with no role listens on a control port (`YAKU_CONTROL_ADDR`,
default `127.0.0.1:8099`): `GET /roles`, `POST /roles/<role|all>/start|stop|restart`,
`POST /shutdown`. `yakugate --idle` starts it with every role stopped.

Full API details live in **[products_docs/engineering/api/gateway-api-reference.md](products_docs/engineering/api/gateway-api-reference.md)**.

---

## Processes

The engine is a single Go binary that runs one **process** per role (`yakugate <role>`, or `yakugate` alone to run them all under a supervisor). Each process is small, stateless, independently deployable, and coordinates only through the shared stores/queue. Work is organized **per process**, not per phase — the former "Phase 1" **is Process 1**. Per-process documentation (scope, architecture, flows, API spec, test cases, tasks) lives under **[products_docs/engineering/processes/](products_docs/engineering/processes/)**; see that folder's README for the naming convention.

> **Confirmed process list.** Numbering is by logical grouping; **build order differs** — after Process 1 the next work is the vendor senders (Processes 3–4) and DLR (Process 5), which complete the Phase-1 end-to-end path. Several fine-grained units from [products_docs/engineering/processes/processes-catalog.md](products_docs/engineering/processes/processes-catalog.md) are folded into the deployable processes below.

| # | Process | Role | What it does |
|---|---------|------|--------------|
| 1 | **HTTP Submit API** | `http-server` | Accepts client submissions over HTTP REST; runs the accept-time pipeline (auth, validation, anti-abuse, routing, HLR) and durably enqueues. **Documented.** |
| 2 | **SMPP Submit Server** | `smpp-server` | Accepts client submissions over SMPP (clients bind and `submit_sm`); same canonical pipeline as Process 1. *(Phase 2 ingress.)* |
| 3 | **SMPP Vendor Sender** | `smpp-vendor` | Claims a vendor's pending queue and delivers via `submit_sm` over SMPP binds; manages binds, windows, keepalive, reconnect. |
| 4 | **HTTP Vendor Sender** | `http-vendor` | Claims a vendor's pending queue and delivers via the vendor's HTTP/REST API; maps request/response and retries. |
| 5 | **DLR Receiver** | `dlr` | Ingests delivery receipts from vendors (SMPP `deliver_sm` and HTTP webhook/poll), correlates to messages, updates final status. |
| 6 | **MO Receiver** | `mo` | Ingests mobile-originated (inbound) messages from vendors and routes them back to clients. *(Phase 2.)* |
| 7 | **Reliability Worker** | `retry`, `expiry` | Retry/backoff, dead-letter after max attempts, validity/TTL expiry sweep, and stuck-message reconciliation. |
| 8 | **Storage Lifecycle** | `retention`, `backup` | Finalizer (pending queue → hot store), retention mover (hot → cold archive), and scheduled backups. |
| 9 | **Rating & Billing** | `billing` | Rates finalized records per account/vendor, maintains usage counters, and produces invoices/usage reports. |
| 10 | **Abuse Monitor** | `abuse-monitor` | Async analytical detection (AIT, spam-scoring, spend/volume anomaly) that throttles, quarantines, or suspends accounts and feeds the ingress gates. |
| 11 | **Reference Data & Config** | — (a shared cache in every role; the Admin API writes config) | Loads/refreshes the in-memory caches, imports HLR/number data, and propagates config changes (cache invalidation) across the fleet. |
| 12 | **Admin API & Portal** | `admin` (Node) · `portal` (React) | Management/reporting API and the browser dashboard (accounts, vendors, routes, templates, observability). Off the message hot path. |

Each process gets the same artifact set once confirmed — see [products_docs/engineering/processes/README.md](products_docs/engineering/processes/README.md).

---

## Documentation

| Document | What's inside |
|----------|---------------|
| [products_docs/engineering/guides/getting-started.md](products_docs/engineering/guides/getting-started.md) | Local setup, prerequisites, first message |
| [products_docs/engineering/api/api.md](products_docs/engineering/api/api.md) | REST ingress, webhook egress, message lifecycle |
| [BUILD.md](BUILD.md) | Install Go, build, run a process, config, migrations, tests |
| [products_docs/engineering/guides/developer-guide.md](products_docs/engineering/guides/developer-guide.md) | Complete picture: folders, processes, shared code, libraries, diagram |
| [products_docs/engineering/processes/processes-catalog.md](products_docs/engineering/processes/processes-catalog.md) | Catalogue of gateway processes (input · logic · output) |
| [products_docs/engineering/data-stores/redis.md](products_docs/engineering/data-stores/redis.md) | Redis as queue + real-time state: streams, persistence, ops |
| [products_docs/engineering/data-stores/mysql.md](products_docs/engineering/data-stores/mysql.md) | MySQL hot store, schema, retention, cold archival |
| [ARCHITECTURE.md](ARCHITECTURE.md) | The hybrid design, data flow, scaling model |
| [roadmap/README.md](roadmap/README.md) | Planning master index — single source of truth |
| [roadmap/ROADMAP.md](roadmap/ROADMAP.md) | Phased plan — scope & acceptance criteria |
| [roadmap/Phase-1-Tasks.xlsx](roadmap/Phase-1-Tasks.xlsx) | Live Phase 1 task tracker with dashboard |
| [CONTRIBUTING.md](CONTRIBUTING.md) | How to set up a dev environment and submit changes |
| [SECURITY.md](SECURITY.md) | How to report a vulnerability |
| [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) | Community standards |

---

## The open-source business model

Yakugate is **100% free and open source**. We do not restrict features, and we do not artificially limit throughput. We believe in zero vendor lock-in.

Our revenue model is built entirely on trust and expertise. For telecom carriers, banks, and large-scale messaging platforms who require absolute peace of mind, we offer paid plans built around:

- **Service Level Agreements (SLAs)** — guaranteed response and uptime commitments backed in writing.
- **Customized support** — engineering support tailored to your deployment, traffic profile, and routing requirements.
- **Customer portal** — a dedicated portal for managing your account, raising and tracking support tickets, and monitoring your SLA.

These paid offerings sit alongside the open-source core — never gating it.

---

## Supporting the project

Yakugate is free under the MIT License, with no strings attached — you are never *required* to credit us. That said, the project lives and grows on visibility, so if Yakugate powers your platform we'd genuinely appreciate (entirely optionally):

- A **link back** to [https://yakugate.com](https://yakugate.com) from your site, docs, or repo.
- Keeping the optional **"Powered by Yakugate"** line in the Admin UI footer (shown by default, removable at any time).
- A ⭐ on the [GitHub repository](https://github.com/yakugate/yakugate).

Every link and star helps more teams discover the project. Thank you.

---

## Project status

Yakugate is **pre-alpha**. APIs, schemas, and commands are subject to change. We welcome early contributors, design feedback, and real-world routing requirements — see [CONTRIBUTING.md](CONTRIBUTING.md) to get involved.

## License

Yakugate is released under the [MIT License](LICENSE).
