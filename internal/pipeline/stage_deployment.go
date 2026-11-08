package pipeline

import (
	"context"
	"strings"

	"github.com/yakugate/yakugate/internal/deployment"
)

// --- stage 2b/19: deployment admission --- [SCOPE P1-02D]
//
// An account may be confined to named deployments (sms_service_config
// .deployments, config/0122). This is the gateway half of that: a submit that
// arrives at a deployment the account is not assigned to is refused here,
// before anything is spent on it.
//
// It sits beside ip-allow deliberately. Both answer the same shape of
// question — may this caller talk to THIS gateway at all — and both are one
// cheap comparison, so they belong above quiet hours, encoding, HLR and
// everything else that costs something.
//
// The refusal is deliberately uninformative to the client. It carries
// RESTRICTED_DEPLOYMENT and nothing else; which OTHER deployments the account
// lives on goes in Reject.Audit, which reaches the rejected-submit record and
// the server log but never the response. A customer pointed at the wrong
// endpoint needs their integrator to read our documentation, not a list of
// our other gateways' names returned to anyone holding a credential.
//
// An account with no assignment is allowed everywhere, so this stage is inert
// on a gateway nobody has configured — which is every gateway until somebody
// deliberately narrows an account.
type deploymentStage struct {
	// id is this process's deployment (YAKU_DEPLOYMENT_ID). Captured at
	// build time rather than read per message: it cannot change while the
	// process runs.
	id string
}

func (deploymentStage) Name() string { return "deployment" }

func (s deploymentStage) Run(_ context.Context, e *Envelope) *Reject {
	// No id at all means a single-deployment install that has never set one.
	// Nothing to enforce, and enforcing something here would refuse every
	// message on exactly the gateways least likely to be watched.
	if s.id == "" || e.Account == nil || e.Account.Deployments.Allows(s.id) {
		return nil
	}
	return &Reject{
		Code:   403,
		Reason: "RESTRICTED_DEPLOYMENT: this account is not served by this gateway",
		Audit:  "deployment=" + s.id + " allowed=" + strings.Join(e.Account.Deployments.IDs(), ","),
	}
}

// DeploymentID is this process's deployment, exported so an ingress edge can
// stamp it on the records it writes without importing the package twice.
func DeploymentID() string { return deployment.ID() }
