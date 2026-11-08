// Package lanes is the one table of who owns which engine package (phase 2
// of the engine split; lanes and owners named by the PO, 30 Oct 2026).
//
// Every package under internal/ (and migrations, and the library modules
// under lib/) sits in one lane, in the core contracts, or in the shared
// group. A package may import its own lane,
// the core contracts, the shared group, the standard library and third-party
// modules. A lane reaches another lane only through the core contracts, and a
// role (internal/process/*) never imports another role. The layering of
// source-layout.md (process -> services -> model) still holds inside a lane.
//
// AllowedToday is every crossing the code had when the rule came in. It only
// gets shorter: lanes_test.go fails on a new crossing and on an entry that is
// no longer needed. Only the packages' own imports count, not their tests'.
// Database clients are a separate rule (internal/dal/imports_test.go).
//
// A library module (Libs, phase 3) never imports the engine: it is its own
// Go module, and an import of the engine would make a module cycle.
//
// scripts/check-lane.sh <lane> builds, vets and tests one lane's packages.
package lanes

import (
	"fmt"
	"os/exec"
	"sort"
	"strings"
)

// Module is the module path; every path in this file is relative to it. A
// library module's path is under it too (lib/smpp is the module
// github.com/yakugate/yakugate/lib/smpp).
const Module = "github.com/yakugate/yakugate/"

// Libs is the library modules of go.work, each its own Go module (phase 3,
// 31 Oct 2026). A new one is added here and in go.work.
var Libs = []string{"lib/smpp", "lib/encoding"}

// Lane is a group of packages one owner looks after.
type Lane string

const (
	ClientEdge      Lane = "client-edge"
	VendorEdge      Lane = "vendor-edge"
	ReceiptsRecords Lane = "receipts-records"
	Data            Lane = "data"
	ConfigOps       Lane = "config-ops"
	Core            Lane = "core"   // the contracts lanes talk through
	Shared          Lane = "shared" // small leaf libraries any lane may use
)

// Owner of each lane (PO, 30 Oct 2026).
var Owner = map[Lane]string{
	ClientEdge:      "Salman",
	VendorEdge:      "Saleem",
	ReceiptsRecords: "Raza",
	Data:            "Saleem",
	ConfigOps:       "Saleem",
	Core:            "Saleem (lead)",
	Shared:          "Saleem (lead)",
}

// MayImport is what a lane may import besides itself, the standard library
// and third-party modules.
var MayImport = map[Lane][]Lane{
	ClientEdge:      {Core, Shared},
	VendorEdge:      {Core, Shared},
	ReceiptsRecords: {Core, Shared},
	Data:            {Core, Shared},
	ConfigOps:       {Core, Shared},
	Core:            {Shared},
	Shared:          {Core},
}

// Packages places every package (the lead's table, 30 Oct 2026). A key is
// a package path, or a path ending in "/..." for that package and all below
// it. An exact path wins over a "/..." one, and a longer "/..." over a
// shorter. "(nearest)" marks a package the table did not name.
var Packages = map[string]Lane{
	// client-edge: the client doors and the accept pipeline.
	"internal/process/smppserver": ClientEdge,
	"internal/process/httpserver": ClientEdge,
	"internal/pipeline":           ClientEdge,
	"internal/inboundauth":        ClientEdge,
	"internal/dialect":            ClientEdge,
	"internal/ratelimit":          ClientEdge,
	"internal/dedupe":             ClientEdge,
	"internal/auth":               ClientEdge,
	"internal/lockout":            ClientEdge,
	"internal/hlr":                ClientEdge,
	"internal/senderid":           ClientEdge,
	"internal/sendplan":           ClientEdge,
	"internal/callbackmap":        ClientEdge, // (nearest) the DLR/MO side of dialect
	"internal/webhookauth":        ClientEdge, // (nearest) inbound callback credentials, beside inboundauth
	"internal/audit":              ClientEdge, // (nearest) http-server's admin endpoints

	// vendor-edge: the vendor binds and calls, and the queues they read.
	"internal/process/smppvendor": VendorEdge,
	"internal/process/httpvendor": VendorEdge,
	"internal/process/blackhole":  VendorEdge,
	"internal/vendors/...":        VendorEdge,
	"lib/smpp/...":                VendorEdge, // its own module
	"internal/reroute":            VendorEdge,
	"internal/vendorquality":      VendorEdge,
	"internal/vendorhealth":       VendorEdge,
	"internal/vendorstatus":       VendorEdge,
	"internal/vendorgroup":        VendorEdge,
	"internal/queues":             VendorEdge,
	"internal/queuecheck":         VendorEdge,
	"internal/queueflow":          VendorEdge,
	"internal/ackkeep":            VendorEdge, // (nearest) vendor acks kept by both vendor roles

	// receipts-records: receipts, MO, records and billing.
	"internal/dlr":              ReceiptsRecords,
	"internal/process/dlr":      ReceiptsRecords,
	"internal/mo":               ReceiptsRecords,
	"internal/process/mo":       ReceiptsRecords,
	"internal/recordq":          ReceiptsRecords,
	"internal/process/dbworker": ReceiptsRecords,
	"internal/receiptpolicy":    ReceiptsRecords,
	"internal/concat":           ReceiptsRecords,
	"internal/billing":          ReceiptsRecords,
	"internal/attemptcap":       ReceiptsRecords,
	"internal/webhookalert":     ReceiptsRecords,
	"internal/pushcall":         ReceiptsRecords,
	"internal/pushpool":         ReceiptsRecords,
	"internal/pushretry":        ReceiptsRecords,
	"internal/giveup":           ReceiptsRecords,
	"internal/receiptfacts":     ReceiptsRecords,
	"internal/rejectlog":        ReceiptsRecords,
	"internal/billingq":         ReceiptsRecords, // (nearest) the billing queue
	"internal/billingstore/...": ReceiptsRecords, // (nearest) billing's hot-store types
	"internal/handoff":          ReceiptsRecords, // (nearest) receipts and MO for another gateway

	// data: every DAL, the stores' openers and the migrations.
	"internal/dal/...":            Data,
	"internal/store/...":          Data, // (nearest) the old store interfaces, store/mongo (Phase 2)
	"internal/platform/bootstrap": Data, // (nearest) builds the stores, runs migrations
	"internal/platform/dbpool":    Data, // (nearest)
	"migrations":                  Data,

	// config-ops: provisioning, reference data, monitoring, abuse, keys,
	// lifecycle.
	"internal/provision":            ConfigOps,
	"internal/process/console":      ConfigOps,
	"internal/refdata":              ConfigOps,
	"internal/monitor":              ConfigOps,
	"internal/process/backup":       ConfigOps,
	"internal/process/retention":    ConfigOps,
	"internal/abuse":                ConfigOps,
	"internal/abusecount":           ConfigOps,
	"internal/process/abusemonitor": ConfigOps,
	"internal/campaignwin":          ConfigOps,
	"internal/kms":                  ConfigOps,
	"internal/crypto":               ConfigOps,
	"internal/secrets":              ConfigOps,
	"internal/pcap":                 ConfigOps,
	"internal/seclog":               ConfigOps,
	"internal/supervisor":           ConfigOps,
	"internal/storewatch":           ConfigOps,
	"internal/singleton":            ConfigOps,
	"internal/reloadgate":           ConfigOps,
	"internal/logcfg":               ConfigOps,
	"internal/deployment":           ConfigOps,

	// core contracts.
	"internal/model":            Core,
	"internal/dal":              Core, // the interface package, not its subpackages
	"internal/platform/config":  Core,
	"internal/platform/logging": Core,
	"internal/platform/worker":  Core,

	// shared: leaf libraries any lane may import.
	"lib/encoding":              Shared, // its own module
	"internal/msisdn":           Shared,
	"internal/netcode":          Shared,
	"internal/httpx":            Shared,
	"internal/tlsconf":          Shared,
	"internal/mask":             Shared,
	"internal/ratematch":        Shared,
	"internal/testenv":          Shared, // test settings: skip unless a local store is named
	"internal/platform/version": Shared, // (nearest) the build stamp
	"internal/platform/outhttp": Shared, // (nearest) the outbound HTTP client
	"internal/lanes/...":        Shared, // (nearest) this table
}

// Crossing is one import from a package into another lane's package.
type Crossing struct {
	From, To, Why string
}

// AllowedToday is every crossing the code had on 30 Oct 2026 (08cc293f).
// Grouped by lane pair, most first. Take an entry off when its crossing
// goes; never add one.
var AllowedToday = []Crossing{
	// client-edge -> config-ops (28)
	{"internal/auth", "internal/refdata", "refdata.Account, Mem, JWTConfig: account lookup in the reference cache"},
	{"internal/auth", "internal/seclog", "writes security events (seclog)"},
	{"internal/auth", "internal/secrets", "secrets.Registry"},
	{"internal/inboundauth", "internal/refdata", "refdata.JWTConfig"},
	{"internal/inboundauth", "internal/seclog", "writes security events (seclog)"},
	{"internal/pipeline", "internal/abuse", "the campaign signature (abuse.Normalize, CampaignHash)"},
	{"internal/pipeline", "internal/abusecount", "counts traffic for the abuse rules (abusecount)"},
	{"internal/pipeline", "internal/deployment", "which deployment owns the traffic (deployment.ID, Owner)"},
	{"internal/pipeline", "internal/monitor", "monitor.Routes for the route readings"},
	{"internal/pipeline", "internal/refdata", "account and HLR-step types of the reference cache"},
	{"internal/pipeline", "internal/seclog", "writes security events (seclog)"},
	{"internal/process/httpserver", "internal/abuse", "the campaign signature (abuse.Normalize, CampaignHash)"},
	{"internal/process/httpserver", "internal/abusecount", "counts traffic for the abuse rules (abusecount)"},
	{"internal/process/httpserver", "internal/campaignwin", "the live campaign window (campaignwin)"},
	{"internal/process/httpserver", "internal/crypto", "encrypts and decrypts message bodies (crypto.FromEnv, Service)"},
	{"internal/process/httpserver", "internal/deployment", "which deployment owns the traffic (deployment.ID, Owner)"},
	{"internal/process/httpserver", "internal/kms", "kms.HTTPKeyRef on the key endpoints"},
	{"internal/process/httpserver", "internal/monitor", "serves the monitoring endpoints (monitor.New, ClusterStatus)"},
	{"internal/process/httpserver", "internal/pcap", "serves the packet capture endpoints (pcap)"},
	{"internal/process/httpserver", "internal/refdata", "opens and syncs the reference cache (refdata.New, NewSyncer)"},
	{"internal/process/httpserver", "internal/seclog", "writes security events (seclog)"},
	{"internal/process/smppserver", "internal/abusecount", "counts traffic for the abuse rules (abusecount)"},
	{"internal/process/smppserver", "internal/campaignwin", "the live campaign window (campaignwin)"},
	{"internal/process/smppserver", "internal/crypto", "encrypts and decrypts message bodies (crypto.FromEnv, Service)"},
	{"internal/process/smppserver", "internal/deployment", "which deployment owns the traffic (deployment.ID, Owner)"},
	{"internal/process/smppserver", "internal/monitor", "reports its flows and states to monitor directly"},
	{"internal/process/smppserver", "internal/refdata", "opens and syncs the reference cache (refdata.New, NewSyncer)"},
	{"internal/process/smppserver", "internal/seclog", "writes security events (seclog)"},
	// vendor-edge -> receipts-records (26)
	{"internal/process/blackhole", "internal/attemptcap", "caps a message's attempts (attemptcap.Apply)"},
	{"internal/process/blackhole", "internal/billing", "books money moments where it decides them (billing.Moments)"},
	{"internal/process/blackhole", "internal/dlr", "pushes the simulated receipt (dlr.ClientPusher)"},
	{"internal/process/blackhole", "internal/giveup", "refuses a message whose validity or tries ran out (giveup)"},
	{"internal/process/blackhole", "internal/recordq", "queues its records for the db-worker (recordq.ForRole)"},
	{"internal/process/httpvendor", "internal/attemptcap", "caps a message's attempts (attemptcap.Apply)"},
	{"internal/process/httpvendor", "internal/billing", "books money moments where it decides them (billing.Moments)"},
	{"internal/process/httpvendor", "internal/dlr", "reads and matches vendor receipts (dlr)"},
	{"internal/process/httpvendor", "internal/giveup", "refuses a message whose validity or tries ran out (giveup)"},
	{"internal/process/httpvendor", "internal/handoff", "hands receipts and MO to the gateway that serves the client (handoff)"},
	{"internal/process/httpvendor", "internal/receiptfacts", "records what a receipt said (receiptfacts)"},
	{"internal/process/httpvendor", "internal/receiptpolicy", "decides a receipt outcome (receiptpolicy.Decide)"},
	{"internal/process/httpvendor", "internal/recordq", "queues its records for the db-worker (recordq.ForRole)"},
	{"internal/process/smppvendor", "internal/attemptcap", "caps a message's attempts (attemptcap.Apply)"},
	{"internal/process/smppvendor", "internal/billing", "books money moments where it decides them (billing.Moments)"},
	{"internal/process/smppvendor", "internal/concat", "assembles multi-part messages (concat.Buffer)"},
	{"internal/process/smppvendor", "internal/dlr", "reads and matches vendor receipts (dlr)"},
	{"internal/process/smppvendor", "internal/giveup", "refuses a message whose validity or tries ran out (giveup)"},
	{"internal/process/smppvendor", "internal/handoff", "hands receipts and MO to the gateway that serves the client (handoff)"},
	{"internal/process/smppvendor", "internal/mo", "takes MO from the vendor bind (mo.Inbound)"},
	{"internal/process/smppvendor", "internal/receiptfacts", "records what a receipt said (receiptfacts)"},
	{"internal/process/smppvendor", "internal/receiptpolicy", "decides a receipt outcome (receiptpolicy.Decide)"},
	{"internal/process/smppvendor", "internal/recordq", "queues its records for the db-worker (recordq.ForRole)"},
	{"internal/reroute", "internal/billing", "books money moments where it decides them (billing.Moments)"},
	{"internal/reroute", "internal/dlr", "tells the client our own outcome (dlr.GatewayAnswer)"},
	{"internal/reroute", "internal/receiptpolicy", "our own outcome codes (receiptpolicy.CodeMaxAttempts)"},
	// receipts-records -> config-ops (21)
	{"internal/billing", "internal/deployment", "which deployment owns the traffic (deployment.ID, Owner)"},
	{"internal/billing", "internal/monitor", "registers its monitoring section (monitor.AddSection)"},
	{"internal/billingq", "internal/monitor", "counts with monitor.Counter"},
	{"internal/dlr", "internal/deployment", "which deployment owns the traffic (deployment.ID, Owner)"},
	{"internal/dlr", "internal/monitor", "monitor.Verdicts and monitor.ULIDTime"},
	{"internal/handoff", "internal/deployment", "which deployment owns the traffic (deployment.ID, Owner)"},
	{"internal/handoff", "internal/monitor", "registers its monitoring section (monitor.AddSection)"},
	{"internal/mo", "internal/deployment", "which deployment owns the traffic (deployment.ID, Owner)"},
	{"internal/process/dbworker", "internal/deployment", "which deployment owns the traffic (deployment.ID, Owner)"},
	{"internal/process/dlr", "internal/crypto", "encrypts and decrypts message bodies (crypto.FromEnv, Service)"},
	{"internal/process/dlr", "internal/deployment", "which deployment owns the traffic (deployment.ID, Owner)"},
	{"internal/process/dlr", "internal/monitor", "reports its flows and states to monitor directly"},
	{"internal/process/dlr", "internal/reloadgate", "when to read webhook settings again (reloadgate)"},
	{"internal/process/dlr", "internal/seclog", "writes security events (seclog)"},
	{"internal/process/mo", "internal/crypto", "encrypts and decrypts message bodies (crypto.FromEnv, Service)"},
	{"internal/process/mo", "internal/deployment", "which deployment owns the traffic (deployment.ID, Owner)"},
	{"internal/process/mo", "internal/monitor", "reports its flows and states to monitor directly"},
	{"internal/process/mo", "internal/reloadgate", "when to read webhook settings again (reloadgate)"},
	{"internal/process/mo", "internal/seclog", "writes security events (seclog)"},
	{"internal/recordq", "internal/deployment", "which deployment owns the traffic (deployment.ID, Owner)"},
	{"internal/recordq", "internal/monitor", "counts with monitor.Counter"},
	// receipts-records -> client-edge (17)
	{"internal/billing", "internal/sendplan", "send plan levels and methods (sendplan)"},
	{"internal/dlr", "internal/dialect", "dialect.Renamed for pushed field names"},
	{"internal/mo", "internal/dialect", "dialect.Renamed for pushed field names"},
	{"internal/process/dlr", "internal/auth", "auth.NewIPResolver for the caller's address"},
	{"internal/process/dlr", "internal/callbackmap", "reads a vendor callback in its dialect (callbackmap)"},
	{"internal/process/dlr", "internal/dialect", "dialect.PushNamesJSON for pushed field names"},
	{"internal/process/dlr", "internal/inboundauth", "guards its inbound door (inboundauth)"},
	{"internal/process/dlr", "internal/lockout", "the spray guard on its inbound door (lockout.NewSprayGuard)"},
	{"internal/process/dlr", "internal/ratelimit", "uses the shared rate granter (ratelimit.NewShared)"},
	{"internal/process/dlr", "internal/webhookauth", "checks inbound callback credentials (webhookauth)"},
	{"internal/process/mo", "internal/auth", "auth.NewIPResolver for the caller's address"},
	{"internal/process/mo", "internal/callbackmap", "reads a vendor callback in its dialect (callbackmap)"},
	{"internal/process/mo", "internal/dialect", "dialect.PushNamesJSON for pushed field names"},
	{"internal/process/mo", "internal/inboundauth", "guards its inbound door (inboundauth)"},
	{"internal/process/mo", "internal/lockout", "the spray guard on its inbound door (lockout.NewSprayGuard)"},
	{"internal/process/mo", "internal/ratelimit", "uses the shared rate granter (ratelimit.NewShared)"},
	{"internal/process/mo", "internal/webhookauth", "checks inbound callback credentials (webhookauth)"},
	// receipts-records -> vendor-edge (17)
	{"internal/billing", "internal/vendors/httpadapter", "calls the FX provider with the vendor adapter's HTTP call engine"},
	{"internal/concat", "lib/smpp/pdu", "pdu.MendSplitEscapes for GSM-7 escapes split between parts"},
	{"internal/dlr", "internal/vendorquality", "notes or reads vendor delivery quality (vendorquality)"},
	{"internal/dlr", "internal/vendorstatus", "vendor status-code rules (vendorstatus)"},
	{"internal/giveup", "internal/vendorquality", "notes or reads vendor delivery quality (vendorquality)"},
	{"internal/handoff", "internal/queuecheck", "how often its queues are checked (queuecheck.For)"},
	{"internal/process/dbworker", "internal/queuecheck", "how often its queues are checked (queuecheck.For)"},
	{"internal/process/dlr", "internal/queuecheck", "how often its queues are checked (queuecheck.For)"},
	{"internal/process/dlr", "internal/vendorgroup", "vendor groups (vendorgroup)"},
	{"internal/process/dlr", "internal/vendorquality", "notes or reads vendor delivery quality (vendorquality)"},
	{"internal/process/dlr", "internal/vendors/httpadapter", "httpadapter.ErrPermanent for a client push that cannot succeed"},
	{"internal/process/dlr", "internal/vendorstatus", "vendor status-code rules (vendorstatus)"},
	{"internal/process/mo", "internal/queuecheck", "how often its queues are checked (queuecheck.For)"},
	{"internal/process/mo", "internal/vendors/httpadapter", "httpadapter.ErrPermanent for a client push that cannot succeed"},
	{"internal/pushcall", "internal/vendors/httpadapter", "builds client webhook calls with the vendor adapter's call engine"},
	{"internal/receiptfacts", "internal/queuecheck", "queuecheck.Check"},
	{"internal/recordq", "internal/queueflow", "queue flow names (queueflow.MT, DLR, MO)"},
	// client-edge -> receipts-records (16)
	{"internal/pipeline", "internal/billing", "reserves and rates at acceptance (billing reservers, Rater, events)"},
	{"internal/process/httpserver", "internal/billing", "wires the acceptance reservers, rater and FX (billing)"},
	{"internal/process/httpserver", "internal/dlr", "client receipt push, parse and webhook test (dlr)"},
	{"internal/process/httpserver", "internal/mo", "mo.NewWebhookRequest for the webhook test"},
	{"internal/process/httpserver", "internal/pushcall", "builds a webhook call for the webhook test"},
	{"internal/process/httpserver", "internal/pushpool", "pushpool.DefaultTimeout for the webhook test"},
	{"internal/process/httpserver", "internal/receiptpolicy", "builds the receipt policy cache (receiptpolicy.New)"},
	{"internal/process/httpserver", "internal/recordq", "queues its records for the db-worker (recordq.ForRole)"},
	{"internal/process/httpserver", "internal/rejectlog", "queues what the door refused (rejectlog)"},
	{"internal/process/smppserver", "internal/billing", "wires the acceptance reservers, rater and FX (billing)"},
	{"internal/process/smppserver", "internal/concat", "assembles multi-part messages (concat.Buffer)"},
	{"internal/process/smppserver", "internal/dlr", "receipt text and ack keeping for bound clients (dlr)"},
	{"internal/process/smppserver", "internal/handoff", "hands receipts and MO to the gateway that serves the client (handoff)"},
	{"internal/process/smppserver", "internal/mo", "delivers MO to bound clients (mo.Finalize, HotWriter)"},
	{"internal/process/smppserver", "internal/recordq", "queues its records for the db-worker (recordq.ForRole)"},
	{"internal/process/smppserver", "internal/rejectlog", "queues what the door refused (rejectlog)"},
	// vendor-edge -> config-ops (12)
	{"internal/process/blackhole", "internal/deployment", "which deployment owns the traffic (deployment.ID, Owner)"},
	{"internal/process/blackhole", "internal/monitor", "reports its flows and states to monitor directly"},
	{"internal/process/httpvendor", "internal/crypto", "encrypts and decrypts message bodies (crypto.FromEnv, Service)"},
	{"internal/process/httpvendor", "internal/deployment", "which deployment owns the traffic (deployment.ID, Owner)"},
	{"internal/process/httpvendor", "internal/monitor", "reports its flows and states to monitor directly"},
	{"internal/process/smppvendor", "internal/crypto", "encrypts and decrypts message bodies (crypto.FromEnv, Service)"},
	{"internal/process/smppvendor", "internal/deployment", "which deployment owns the traffic (deployment.ID, Owner)"},
	{"internal/process/smppvendor", "internal/monitor", "reports its flows and states to monitor directly"},
	{"internal/queuecheck", "internal/monitor", "registers the queue-check section (monitor.AddSection)"},
	{"internal/reroute", "internal/crypto", "crypto.IsCiphertext"},
	{"internal/reroute", "internal/refdata", "reads the reference cache (refdata.Mem)"},
	{"internal/vendorhealth", "internal/deployment", "which deployment owns the traffic (deployment.ID, Owner)"},
	// client-edge -> vendor-edge (11)
	{"internal/hlr", "internal/vendors/httpadapter", "calls the live HLR with the vendor adapter's HTTP call engine"},
	{"internal/pipeline", "internal/queues", "queue names (queues.Common, Sub)"},
	{"internal/process/httpserver", "internal/reroute", "serves the re-route inbox (reroute)"},
	{"internal/process/httpserver", "internal/vendorhealth", "reads vendor health for routing (vendorhealth.View)"},
	{"internal/process/httpserver", "internal/vendorquality", "notes or reads vendor delivery quality (vendorquality)"},
	{"internal/process/httpserver", "internal/vendors/httpadapter", "httpadapter.ErrPermanent for a client push that cannot succeed"},
	{"internal/process/smppserver", "internal/queuecheck", "how often its queues are checked (queuecheck.For)"},
	{"internal/process/smppserver", "internal/queueflow", "queue flow names (queueflow.MT, DLR, MO)"},
	{"internal/process/smppserver", "lib/smpp/pdu", "the SMPP codec serves both edges"},
	{"internal/process/smppserver", "internal/vendorhealth", "reads vendor health for routing (vendorhealth.View)"},
	{"internal/process/smppserver", "internal/vendorquality", "notes or reads vendor delivery quality (vendorquality)"},
	// client-edge -> data (8)
	{"internal/process/httpserver", "internal/dal/redis", "opens Redis itself (dal/redis.Store), not through the dal interface"},
	{"internal/process/httpserver", "internal/dal/runtimequeue", "opens the runtime queue (runtimequeue.HandOver), not through the dal interface"},
	{"internal/process/httpserver", "internal/platform/bootstrap", "builds the stores at start (bootstrap.Ensure)"},
	{"internal/process/httpserver", "internal/store", "queue depth and pending stats types of the old store interfaces"},
	{"internal/process/smppserver", "internal/dal/redis", "opens Redis itself (dal/redis.Store), not through the dal interface"},
	{"internal/process/smppserver", "internal/dal/runtimequeue", "opens the runtime queue (runtimequeue.HandOver), not through the dal interface"},
	{"internal/process/smppserver", "internal/platform/bootstrap", "builds the stores at start (bootstrap.Ensure)"},
	{"internal/process/smppserver", "internal/store", "queue depth and pending stats types of the old store interfaces"},
	// config-ops -> client-edge (8)
	{"internal/process/console", "internal/hlr", "the HLR ranges screen (hlr.NewRanges)"},
	{"internal/process/console", "internal/sendplan", "shows and edits send plans (sendplan)"},
	{"internal/provision", "internal/inboundauth", "checks door and dialect paths (one rulebook, two doors)"},
	{"internal/provision", "internal/senderid", "checks sender ids (one rulebook, two doors)"},
	{"internal/provision", "internal/sendplan", "checks send plans (one rulebook, two doors)"},
	{"internal/refdata", "internal/dialect", "parses account dialects into the cache"},
	{"internal/refdata", "internal/hlr", "loads HLR entries into the cache"},
	{"internal/refdata", "internal/sendplan", "send plan levels and methods (sendplan)"},
	// config-ops -> receipts-records (8)
	{"internal/process/abusemonitor", "internal/billing", "prices spend in the base currency (billing.FXResolver)"},
	{"internal/process/abusemonitor", "internal/billingstore", "the record kinds and table names (billingstore)"},
	{"internal/process/console", "internal/dlr", "dlr.GatewayAnswer on the receipt policy screen"},
	{"internal/process/console", "internal/receiptpolicy", "shows and edits receipt policy settings"},
	{"internal/process/retention", "internal/billingstore", "billingstore.MonthStart"},
	{"internal/provision", "internal/billing", "the vendor charge point rule (billing.CleanVendorPoint, MicroFactor)"},
	{"internal/provision", "internal/pushretry", "pushretry.Defaults for account settings"},
	{"internal/provision", "internal/receiptpolicy", "checks receipt policy settings (one rulebook, two doors)"},
	// data -> config-ops (7)
	{"internal/dal/mssql", "internal/crypto", "encrypted columns (crypto.IsCiphertext, TenantConfig)"},
	{"internal/dal/mssql", "internal/deployment", "which deployment owns the traffic (deployment.ID, Owner)"},
	{"internal/dal/mssql", "internal/kms", "the KMS endpoint the store reads (kms.Endpoint, HTTPProvider)"},
	{"internal/dal/mysql", "internal/crypto", "encrypted columns (crypto.IsCiphertext, TenantConfig)"},
	{"internal/dal/mysql", "internal/deployment", "which deployment owns the traffic (deployment.ID, Owner)"},
	{"internal/dal/mysql", "internal/kms", "the KMS endpoint the store reads (kms.Endpoint, HTTPProvider)"},
	{"internal/dal/redis", "internal/deployment", "which deployment owns the traffic (deployment.ID, Owner)"},
	// receipts-records -> data (7)
	{"internal/process/dbworker", "internal/dal/redis", "opens the record lanes on Redis (dal/redis.NewRecordLanes)"},
	{"internal/process/dlr", "internal/dal/redis", "opens Redis itself (dal/redis.Store), not through the dal interface"},
	{"internal/process/dlr", "internal/dal/runtimequeue", "opens the runtime queue (runtimequeue.HandOver), not through the dal interface"},
	{"internal/process/dlr", "internal/store", "queue depth and pending stats types of the old store interfaces"},
	{"internal/process/mo", "internal/dal/runtimequeue", "opens the runtime queue (runtimequeue.HandOver), not through the dal interface"},
	{"internal/process/mo", "internal/store", "queue depth and pending stats types of the old store interfaces"},
	{"internal/recordq", "internal/dal/redis", "the record queue on Redis (dal/redis.Store)"},
	// data -> vendor-edge (6)
	{"internal/dal/mssql", "internal/queueflow", "queue flow names (queueflow.MT, DLR, MO)"},
	{"internal/dal/mssql", "internal/queues", "queue names and the waiting item (queues)"},
	{"internal/dal/mysql", "internal/queueflow", "queue flow names (queueflow.MT, DLR, MO)"},
	{"internal/dal/mysql", "internal/queues", "queue names and the waiting item (queues)"},
	{"internal/dal/redis", "internal/queueflow", "queue flow names (queueflow.MT, DLR, MO)"},
	{"internal/dal/redis", "internal/queues", "queue names and the waiting item (queues)"},
	// config-ops -> data (5)
	{"internal/monitor", "internal/platform/dbpool", "reads pool stats (dbpool.Stats)"},
	{"internal/monitor", "internal/store", "queue depth and pending stats types of the old store interfaces"},
	{"internal/process/console", "internal/platform/bootstrap", "builds the stores at start (bootstrap.Ensure)"},
	{"internal/refdata", "internal/platform/dbpool", "dbpool.Fresh: opens its own pool"},
	{"internal/refdata", "internal/store", "queue depth and pending stats types of the old store interfaces"},
	// config-ops -> vendor-edge (5)
	{"internal/kms", "internal/vendors/httpadapter", "calls the KMS with the vendor adapter's HTTP call engine"},
	{"internal/monitor", "internal/queueflow", "queue flow names (queueflow.MT, DLR, MO)"},
	{"internal/provision", "internal/queues", "queues.Common"},
	{"internal/provision", "internal/vendors/httpadapter", "checks HTTP endpoint profiles (httpadapter.ProfileFromEndpoints)"},
	{"internal/refdata", "internal/vendorquality", "vendor quality counts in the cache (vendorquality.Counts)"},
	// data -> receipts-records (5)
	{"internal/dal/mssql", "internal/billing", "billing.RatesOf, RecordRates: rate rules the DAL applies"},
	{"internal/dal/mssql", "internal/billingstore", "the record kinds and table names (billingstore)"},
	{"internal/dal/mysql", "internal/billing", "billing.RatesOf, RecordRates: rate rules the DAL applies"},
	{"internal/dal/mysql", "internal/billingstore", "the record kinds and table names (billingstore)"},
	{"internal/store/mongo", "internal/billingstore", "the record kinds and table names (billingstore)"},
	// vendor-edge -> client-edge (5)
	{"internal/process/blackhole", "internal/ratelimit", "uses the shared rate granter (ratelimit.NewShared)"},
	{"internal/process/httpvendor", "internal/ratelimit", "uses the shared rate granter (ratelimit.NewShared)"},
	{"internal/process/smppvendor", "internal/ratelimit", "uses the shared rate granter (ratelimit.NewShared)"},
	{"internal/reroute", "internal/pipeline", "pipeline.Rerouter"},
	{"internal/reroute", "internal/sendplan", "send plan levels and methods (sendplan)"},
	// vendor-edge -> data (5)
	{"internal/process/blackhole", "internal/dal/runtimequeue", "opens the runtime queue (runtimequeue.HandOver), not through the dal interface"},
	{"internal/process/httpvendor", "internal/dal/redis", "opens Redis itself (dal/redis.Store), not through the dal interface"},
	{"internal/process/httpvendor", "internal/dal/runtimequeue", "opens the runtime queue (runtimequeue.HandOver), not through the dal interface"},
	{"internal/process/smppvendor", "internal/dal/redis", "opens Redis itself (dal/redis.Store), not through the dal interface"},
	{"internal/process/smppvendor", "internal/dal/runtimequeue", "opens the runtime queue (runtimequeue.HandOver), not through the dal interface"},
	// core -> config-ops (3)
	{"internal/dal", "internal/crypto", "core carries crypto.TenantConfig for encrypted columns"},
	{"internal/dal", "internal/kms", "core carries kms.Endpoint"},
	{"internal/platform/config", "internal/secrets", "core resolves secret references in settings (secrets)"},
	// core -> data (1)
	{"internal/dal", "internal/store", "core carries store.VendorQueueStat"},
	// core -> receipts-records (1)
	{"internal/dal", "internal/billingstore", "core carries billing record table names (billingstore.TableOf)"},
	// core -> vendor-edge (1)
	{"internal/dal", "internal/queues", "core carries queues.Mode"},
	// data -> client-edge (1)
	{"internal/dal/mssql", "internal/hlr", "hlr.Shard names the HLR table"},
}

// Of is the lane of a package path (relative to Module).
func Of(pkg string) (Lane, bool) {
	if l, ok := Packages[pkg]; ok {
		return l, true
	}
	best, lane := -1, Lane("")
	for k, l := range Packages {
		root, ok := strings.CutSuffix(k, "/...")
		if !ok || (pkg != root && !strings.HasPrefix(pkg, root+"/")) {
			continue
		}
		if len(root) > best {
			best, lane = len(root), l
		}
	}
	return lane, best >= 0
}

// role is the role name of a package under internal/process/, or "".
func role(pkg string) string {
	rest, ok := strings.CutPrefix(pkg, "internal/process/")
	if !ok {
		return ""
	}
	r, _, _ := strings.Cut(rest, "/")
	return r
}

// Fits says whether the import from -> to keeps the rule.
func Fits(from, to string) bool {
	if rf, rt := role(from), role(to); rf != "" && rt != "" && rf != rt {
		return false
	}
	lf, ok1 := Of(from)
	lt, ok2 := Of(to)
	if !ok1 || !ok2 {
		return false
	}
	if lf == lt {
		return true
	}
	for _, l := range MayImport[lf] {
		if l == lt {
			return true
		}
	}
	return false
}

// Graph is each engine and library package's imports of other engine and
// library packages, all relative to Module, as go list reports them (test
// files not counted). -e: a package that does not build still lists its
// imports.
func Graph() (map[string][]string, error) {
	args := []string{"list", "-e", "-f", "{{.ImportPath}} {{join .Imports \" \"}}",
		Module + "internal/...", Module + "migrations"}
	for _, l := range Libs {
		args = append(args, Module+l+"/...")
	}
	out, err := exec.Command("go", args...).Output()
	if err != nil {
		return nil, fmt.Errorf("go list: %w", err)
	}
	g := map[string][]string{}
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		f := strings.Fields(line)
		if len(f) == 0 {
			continue
		}
		pkg := strings.TrimPrefix(f[0], Module)
		g[pkg] = nil
		for _, imp := range f[1:] {
			// a library's every import of this repository counts
			if p, ok := strings.CutPrefix(imp, Module); ok && (engine(p) || lib(pkg) != "") {
				g[pkg] = append(g[pkg], p)
			}
		}
	}
	return g, nil
}

func engine(p string) bool {
	return p == "migrations" || strings.HasPrefix(p, "internal/") || lib(p) != ""
}

// lib is the library module a package belongs to, or "".
func lib(p string) string {
	for _, l := range Libs {
		if p == l || strings.HasPrefix(p, l+"/") {
			return l
		}
	}
	return ""
}

// In is the packages of one lane, sorted.
func In(g map[string][]string, lane Lane) []string {
	var out []string
	for p := range g {
		if l, ok := Of(p); ok && l == lane {
			out = append(out, p)
		}
	}
	sort.Strings(out)
	return out
}

// Report is what Check found wrong.
type Report struct {
	Unplaced   []string   // packages with no lane
	Unused     []string   // Packages keys that match no package
	RoleToRole []Crossing // a role importing another role (never allowed)
	LibEngine  []Crossing // a library module importing the engine (never allowed)
	New        []Crossing // crossings not on AllowedToday
	Gone       []Crossing // AllowedToday entries no longer needed
	Today      []Crossing // every crossing found, allowed or not
}

// Check holds the graph against the table and allowed.
func Check(g map[string][]string, allowed []Crossing) Report {
	var r Report
	used := map[string]bool{}
	ok := map[[2]string]bool{}
	for _, c := range allowed {
		ok[[2]string{c.From, c.To}] = true
	}
	seen := map[[2]string]bool{}
	for p := range g {
		if _, placed := Of(p); !placed {
			r.Unplaced = append(r.Unplaced, p)
		}
		for k := range Packages {
			root, wild := strings.CutSuffix(k, "/...")
			if k == p || wild && (p == root || strings.HasPrefix(p, root+"/")) {
				used[k] = true
			}
		}
	}
	for p, imps := range g {
		for _, imp := range imps {
			if lib(p) != "" && lib(imp) == "" {
				r.LibEngine = append(r.LibEngine, Crossing{From: p, To: imp})
				continue
			}
			if Fits(p, imp) {
				continue
			}
			c := Crossing{From: p, To: imp}
			if rf, rt := role(p), role(imp); rf != "" && rt != "" && rf != rt {
				r.RoleToRole = append(r.RoleToRole, c)
				continue
			}
			r.Today = append(r.Today, c)
			seen[[2]string{p, imp}] = true
			if !ok[[2]string{p, imp}] {
				r.New = append(r.New, c)
			}
		}
	}
	for _, c := range allowed {
		if !seen[[2]string{c.From, c.To}] {
			r.Gone = append(r.Gone, c)
		}
	}
	for k := range Packages {
		if !used[k] {
			r.Unused = append(r.Unused, k)
		}
	}
	sort.Strings(r.Unplaced)
	sort.Strings(r.Unused)
	for _, l := range [][]Crossing{r.RoleToRole, r.LibEngine, r.New, r.Gone, r.Today} {
		sort.Slice(l, func(i, j int) bool {
			if l[i].From != l[j].From {
				return l[i].From < l[j].From
			}
			return l[i].To < l[j].To
		})
	}
	return r
}
