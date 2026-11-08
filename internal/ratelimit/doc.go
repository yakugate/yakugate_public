// Package ratelimit provides Yakugate's shared per-account request rate limiting.
//
// It is used by the ingress processes (the HTTP Submit API and the SMPP server)
// to enforce each account's per-second throughput cap with an identical, tested
// implementation instead of a per-process copy.
package ratelimit
