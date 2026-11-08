package mo

import mosvc "github.com/yakugate/yakugate/internal/mo"

// deliveredSet remembers the MO this process pushed to a customer's webhook
// and could not yet record (go-live test of 18 Oct 2026: P6 E11, 60 MO pushed
// twice in a MySQL cut; P14 A4, each MO pushed four times at the record queue
// limit): the next claim of such an MO writes its record and is not pushed
// again. It is the MO service's mosvc.Delivered, which smpp-server's bind
// hand-over uses too.
type deliveredSet = mosvc.Delivered
