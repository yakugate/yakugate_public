// Package httpadapter is the config-driven vendor HTTP egress framework
// (products_docs/engineering/processes/process-4/): a JSON profile per vendor
// (templated requests, regex extraction, session auth, poll/webhook status),
// the shared request builder every http_endpoints row runs through, and the
// classification of vendor answers (accepted / temporary / permanent).
package httpadapter
