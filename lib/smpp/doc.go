// Package smpp is the SMPP 3.4 core shared by the SMPP ingress (smppserver, P2)
// and the SMPP vendor sender (smppvendor, P3). Subpackages: pdu (codecs),
// session (bind lifecycle, enquire_link, sequencing), window (async window
// enforcement). Every PDU field layout implemented here must cite its SMPP 3.4
// spec section (see products_docs/engineering/guides/specs/README.md).
package smpp
