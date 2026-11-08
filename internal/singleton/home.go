package singleton

// A gateway-wide role runs only on the deployment the register lists it on
// (Saleem, 2026-10-26, issue 2 of the design audit — "strict"). The
// deployments register says where each gateway-wide role is EXPECTED
// (deployment_roles with is_singleton = 1; the Admin API allows one
// deployment per role): that deployment is the role's HOME.
//
//   - Listed on this deployment: it runs here. Finding the role held by
//     another deployment (an older build that took it over), it asks for it
//     back (singleton_claims, config/0139) and stands by for homeStandBy.
//   - Listed on another deployment: it never runs here, whether that one is
//     up or not. A copy holding it gives it up as soon as it reads that the
//     role was moved (homeEvery). With that deployment down the role runs
//     nowhere until it is back or an operator moves the role in the portal.
//   - The register cannot be read, or names more than one deployment: it
//     WAITS, and looks again every homeEvery (the supervisor every minute).
//   - Listed nowhere (no deployments set up, or the role left off every list):
//     first come, first served, as before — a single gateway with an empty
//     register keeps its billing.
//
// Until 2026-10-26 another deployment took a role over whenever its home was
// not asking for it (D-128, "auto takeover"), so a deployment started while
// the home was down ran the home's roles.

import (
	"context"
	"os"
	"time"

	"github.com/yakugate/yakugate/internal/dal"
)

const (
	// claimFresh is how long an ask from the home counts.
	claimFresh = 3 * time.Minute
	// claimLookEvery paces the holder's look for an ask, and the home's
	// writing of one while it stands by.
	claimLookEvery = 15 * time.Second
	// homeEvery paces how often the register is read again.
	homeEvery = time.Minute
)

// homeOf is the deployment the register lists a gateway-wide role on.
// known=false when the register cannot be read, or names more than one
// deployment (the Admin API refuses that): the role then waits. home="" with
// known=true: the role is listed nowhere. A store with no register at all (no
// table yet) is "listed nowhere". Read within 3 s (the DAL's RoleHome).
func homeOf(ctx context.Context, homes dal.RoleHomes, role string) (home string, known bool) {
	return homes.RoleHome(ctx, role)
}

// claimant is the deployment asking for a role back now ("" when nobody has
// asked within claimFresh).
func claimant(ctx context.Context, homes dal.RoleHomes, role string) string {
	return homes.Claimant(ctx, role, claimFresh)
}

// askBack writes the home's ask for its role.
func askBack(ctx context.Context, homes dal.RoleHomes, role, me string) {
	host, _ := os.Hostname()
	homes.AskBack(ctx, role, me, host, os.Getpid())
}

// dropAsk removes the ask once the home has its role again.
func dropAsk(ctx context.Context, homes dal.RoleHomes, role string) {
	homes.DropAsk(ctx, role)
}

// home is the role's home, read again at most every homeEvery; known as
// homeOf says. Caller holds the mutex.
func (l *Lease) home(ctx context.Context) (string, bool) {
	if time.Since(l.homeAt) >= homeEvery {
		l.homeID, l.homeKnown = homeOf(ctx, l.homes, l.role)
		l.homeAt = time.Now()
	}
	return l.homeID, l.homeKnown
}

// awayFrom is the role's home when the register lists it on another
// deployment; "" when it is listed here or nowhere, or cannot be read.
func (l *Lease) awayFrom(ctx context.Context) string {
	if h, known := l.home(ctx); known && h != "" && h != l.me {
		return h
	}
	return ""
}
