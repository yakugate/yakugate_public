// Package window enforces the SMPP async window on the vendor-facing client
// (P3). Over-window sends are silently dropped by peers; enforce strictly.
package window
