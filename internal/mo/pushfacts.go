package mo

import (
	"strconv"

	"github.com/yakugate/yakugate/internal/model"
)

// PushFacts is what an MO webhook says beside the message itself (Saleem
// 4 Nov 2026): the subscriber's network, when it is known, and the vendor the
// message came from. Each is left out of the push when it is not known.
type PushFacts struct {
	// MCC and MNC are the subscriber's network: the vendor's own word when
	// its MO said, else our HLR data for the sender's number.
	MCC, MNC string
	// Ported is "true" or "false" when our HLR data has the number, else ""
	// (not known). A string, as every field of the push is.
	Ported string
	// VendorCID and VendorName are the vendor that gave us the message.
	VendorCID, VendorName string
}

// NumberLook is our HLR data for one number: a stored answer only, never a
// paid live lookup. ok=false: we do not have the number.
type NumberLook func(msisdn string) (mcc, mnc string, ported, ok bool)

// FactsFor works out an MO's push facts. look is our HLR data; nil = none.
// The vendor's cid and name are given by the caller.
func FactsFor(m model.Message, look NumberLook, vendorCID, vendorName string) PushFacts {
	f := PushFacts{MCC: m.MCC, MNC: m.MNC, VendorCID: vendorCID, VendorName: vendorName}
	if look != nil && m.Sender != "" {
		if mcc, mnc, ported, ok := look(m.Sender); ok {
			f.Ported = strconv.FormatBool(ported)
			// The vendor's word on the network wins; ours fills in.
			if f.MCC == "" && f.MNC == "" {
				f.MCC, f.MNC = mcc, mnc
			}
		}
	}
	return f
}

// Operator is MCC and MNC together ("41001"), as GET /sms/v4/operator
// answers it; "" when the network is not known.
func (f PushFacts) Operator() string {
	if f.MCC == "" {
		return ""
	}
	return f.MCC + f.MNC
}

// native is the facts in the gateway's own body: only those known.
func (f PushFacts) native(into map[string]string) {
	put := func(k, v string) {
		if v != "" {
			into[k] = v
		}
	}
	put("mcc", f.MCC)
	put("mnc", f.MNC)
	put("operator", f.Operator())
	put("ported", f.Ported)
	put("vendor", f.VendorCID)
	put("vendorName", f.VendorName)
}

// Sources is the facts under the names a webhook row that shapes its own
// request uses ({mcc}, {mnc}, {operator}, {ported}, {vendor}, {vendor_name}):
// always present, "" when not known, so a row's placeholder never stays
// unfilled.
func (f PushFacts) Sources() map[string]string {
	return map[string]string{
		"mcc": f.MCC, "mnc": f.MNC, "operator": f.Operator(), "ported": f.Ported,
		"vendor": f.VendorCID, "vendor_name": f.VendorName,
	}
}
