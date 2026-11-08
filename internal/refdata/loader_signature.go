// The config store's change fingerprint, which tells the cache when to
// reload.

package refdata

import (
	"context"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
)

// signatureTables are the config tables the cache is built from. Every one
// carries updated_at (ON UPDATE), so COUNT(*) plus MAX(updated_at) changes
// on any insert, update or delete.
//
// operators is NOT one of them (review of QA SMPP-004, 2026-10-01): nothing
// in this cache is read from it — the number ranges are hlr.Ranges', with
// their own fingerprint and timer — and with no index on updated_at its
// COUNT/MAX was a full scan of every range (350k rows, ~260 ms on the dev
// box: most of the look's cost), paid on every 30 s look by every role.
var signatureTables = []string{
	"sms_service_config", "companies", "sms_service_dialects", "routes", "vendors", "templates",
	"spam_keywords", "blocklist", "quota_by_countries", "rate_cards", "rate_card_rates", "sender_ids",
	"auto_replace_text", "jwt_revocations", "gateway_settings", "http_endpoints", "hlr_providers",
}

// A table's mark (dal.ReferenceData.TableMark) also carries, for
// sms_service_config, a checksum of the columns a same-second change can
// touch without moving MAX(updated_at) (QA BUG-YK-ACC-004; is_disabled and
// abuse_throttle_tps, which the abuse monitor writes, QA 2026-10-08 B-05):
// the DAL's SignatureExtra (internal/dal/mysql/reference.go) says why.

// ChangeSignature is a cheap fingerprint of the config store: the cache
// reloads only when it moved (PO 2026-09-17: "rows that changed, every 30
// seconds"). A table this store does not have counts as empty.
//
// updated_at counts whole seconds, so a look can miss a change two ways (QA
// 2026-10-08, B-05). A row written in the same second as the look, after it:
// the count and the newest stamp read the same next time. And a row stamped
// in the future (written through a session on local time): every later
// change is older than it, so the newest stamp never moves. In both cases a
// table's newest stamp is not yet behind the store's own clock, and a
// fingerprint taken then is made unique, so the next look reloads again —
// the rule billing.Rater.signature already follows. It names no column, so
// it covers every table here.
func (l *MySQL) ChangeSignature(ctx context.Context) (string, error) {
	now, err := l.ref.Now(ctx)
	if err != nil {
		return "", err
	}
	var sb strings.Builder
	settled := true
	for _, t := range signatureTables {
		n, max, extra, err := l.ref.TableMark(ctx, t)
		if err != nil {
			// A table this store does not have (jwt_revocations lives in the
			// HLR store): MySQL's words, or the DAL's own test (SQL Server's
			// "Invalid object name").
			if strings.Contains(err.Error(), "doesn't exist") || strings.Contains(err.Error(), "Unknown column") || l.ref.MissingTable(err) {
				sb.WriteString(t + ":-;")
				continue
			}
			return "", err
		}
		sb.WriteString(t + ":" + strconv.FormatInt(n, 10) + "@" + max.String)
		if extra.Valid {
			sb.WriteString("#" + extra.String)
		}
		sb.WriteString(";")
		if !stampSettled(max, now) {
			settled = false
			l.noteFutureStamp(t, max.String, now.String)
		}
	}
	if !settled {
		sb.WriteString(unsettledMark + strconv.FormatInt(time.Now().UnixNano(), 10))
	}
	return sb.String(), nil
}

// unsettledMark starts the part that makes a fingerprint unique.
const unsettledMark = "~"

// stampSettled reports whether a table's newest stamp is behind the store's
// clock, so that nothing more can be written under it. Both values come from
// the same connection in the same form, so they compare as text.
func stampSettled(last, now dal.NullString) bool {
	return !last.Valid || !now.Valid || last.String < now.String
}

// noteFutureStamp says, once for each stamp, that a table holds a row dated
// ahead of the store's clock. Until the clock passes it every look reloads,
// which is right but wasteful, and somebody should know why.
func (l *MySQL) noteFutureStamp(table, last, now string) {
	if last <= now {
		return // the same second as the look: ordinary, and over by the next one
	}
	l.futureMu.Lock()
	seen := l.futureSeen[table] == last
	if !seen {
		if l.futureSeen == nil {
			l.futureSeen = map[string]string{}
		}
		l.futureSeen[table] = last
	}
	l.futureMu.Unlock()
	if !seen {
		log.Printf("[refdata] %s has a row stamped %s, ahead of the store's clock (%s) — it was written through a session "+
			"that is not on UTC; the cache reloads on every look until the clock passes it", table, last, now)
	}
}
