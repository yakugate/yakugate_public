package queues

import "context"

// SplitReader reads every vendor's vendors.queue_split as stored
// (dal.VendorSendSettings; the reading is in the DAL, which imports this
// package, so it is named here by its one method).
type SplitReader interface {
	SplitModes(ctx context.Context) map[string]string
}

// SplitModes reads every vendor's split mode (vendors.queue_split,
// config/0161), by cid. A config store without the column, or one that cannot
// be read, gives none: every vendor is then Fair. A nil reader gives none.
func SplitModes(ctx context.Context, r SplitReader) map[string]Mode {
	out := map[string]Mode{}
	if r == nil {
		return out
	}
	for cid, mode := range r.SplitModes(ctx) {
		out[cid] = ParseMode(mode)
	}
	return out
}
