package pipeline

import (
	"context"
	"strings"

	"github.com/yakugate/yakugate/internal/refdata"
)

// --- stage 8/18: template check (optional per sender) --- [SCOPE P1-07]
// Runs AFTER auto-replace since 2026-08-03 (chunk-5 ruling 5.4): the check
// certifies what will actually be sent, so it matches the post-replacement
// body.
type templateStage struct{ ref *refdata.Mem }

func (templateStage) Name() string { return "template" }
func (s templateStage) Run(_ context.Context, e *Envelope) *Reject {
	// Two reasons to be here. ENFORCING is the account's own template gate.
	// OBSERVING is a spam rule with action='require_template' (config/0098),
	// which needs to know whether the body matches approved wording — but must
	// not turn the template gate on for an account that never asked for it.
	//
	// Gated on NeedsTemplateVerdict so that nobody pays for this unless some
	// rule in scope actually uses that action. With neither reason, the stage
	// returns exactly as it always did.
	enforcing := e.Account.TemplateCheck
	if !enforcing && !s.ref.NeedsTemplateVerdict(e.Account.CompanyID) {
		return nil
	}
	for _, t := range s.ref.Templates(e.Account.ID, e.Account.CompanyID) {
		// Shortcode scope (S-11, PO 2026-09-04): a template with senders
		// listed is available ONLY to those senders. Checked against the
		// sender as the validate stage settled it (the client's own, or the
		// account's default sender).
		//
		// A non-matching sender simply skips this template rather than
		// refusing outright: another template may still match, and if none
		// does the client gets the same 422 TEMPLATE_MISMATCH as any
		// unrecognised body (PO ruling). A caller probing with someone else's
		// wording therefore learns nothing about which templates exist.
		if !t.AllowsSender(e.Msg.Sender) {
			continue
		}
		// skip regexes that provably cannot match: an anchored pattern's
		// literal prefix must start the body; an unanchored pattern's
		// literal must at least appear somewhere in it
		if t.Prefix != "" {
			if t.Anchored {
				if !strings.HasPrefix(e.Msg.Body, t.Prefix) {
					continue
				}
			} else if !strings.Contains(e.Msg.Body, t.Prefix) {
				continue
			}
		}
		if t.Re.MatchString(e.Msg.Body) {
			e.TemplateMatched = true
			// OBSERVING only: record the verdict and change NOTHING. The
			// stamp and the forced priority below are the
			// template gate DOING its job, and an account with the gate off
			// must come out of this stage exactly as it went in.
			if !enforcing {
				return nil
			}
			e.Msg.TemplateID = t.ID // stamp the matched template (audit)
			// Force the template's own default priority when the client didn't
			// set one explicitly (Priority==0 sentinel, resolved from adapter.go
			// / stage_enqueue.go). [SCOPE P1-TEMPLATE-PRIORITY, S-11]
			// Only a value on the gateway's 1-9 scale (QA templates BUG-007):
			// refdata drops any other at load and says so; this is the
			// stage's own guard.
			if e.Msg.Priority == 0 && t.DefaultPriority >= 1 && t.DefaultPriority <= 9 {
				e.Msg.Priority = t.DefaultPriority
			}
			// The template's own ap1/ap2/apg1/apg2 (config/0130, in place of
			// the retired channel tag): each fills only a field the client
			// left empty — the client's own value always wins (Saleem
			// 2026-09-29), the same rule as the priority above.
			fillEmpty(&e.Msg.AP1, t.Pass.AP1)
			fillEmpty(&e.Msg.AP2, t.Pass.AP2)
			fillEmpty(&e.Msg.APG1, t.Pass.APG1)
			fillEmpty(&e.Msg.APG2, t.Pass.APG2)
			return nil
		}
	}
	// Nothing matched. Observing means the spam stage will decide what that is
	// worth; only the account's own gate refuses here.
	if !enforcing {
		return nil
	}
	return reject(422, "TEMPLATE_MISMATCH")
}

// fillEmpty sets *dst to v when the client left it empty.
func fillEmpty(dst *string, v string) {
	if *dst == "" && v != "" {
		*dst = v
	}
}
