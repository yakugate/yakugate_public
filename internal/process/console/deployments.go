package console

import (
	"bufio"
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/yakugate/yakugate/internal/provision"
)

// Where clients connect, per deployment (D-137, config/0147).
//
// The register's client addresses: the HTTP address and the SMPP host and
// ports each customer is told on its "How to connect" sheet in the portal.
// The engine never reads them; it listens where its own settings say. One
// setting at a time, each shown with what it is now: Enter keeps it, "none"
// clears it. The rules are provision.UpdateDeploymentClient's, which are the
// Admin API's for the same row (deploymentRoutes.js). Adding, renaming or
// removing a deployment stays in the portal.

func (p *Process) deploymentsMenu(ctx context.Context, sc *bufio.Scanner) {
	for {
		rows, err := p.store.ListDeploymentClients(ctx)
		if err != nil {
			p.errf("  %v\n", err)
			return
		}
		if len(rows) == 0 {
			p.printf("\n  No deployment is registered yet. Add one in the portal (Deployments).\n")
			return
		}
		p.printf("\n  Where clients connect. The portal's \"How to connect\" sheet tells each customer these.\n\n")
		p.printf("   %-5s %-20s %-34s %s\n", "id", "deployment", "client API address", "SMPP")
		list := make([]opt, 0, len(rows))
		for _, d := range rows {
			id := d.DeploymentID
			if d.Disabled {
				id += " (off)"
			}
			list = append(list, row(d.ID, fmt.Sprintf("   %-5d %-20s %-34s %s",
				d.ID, trim(id, 20), trim(orDash(d.APIURL), 34), smppLine(d)), ""))
		}
		id, ok := p.chooseID(sc, "Open a deployment by id (0 to go back)", list)
		if !ok || id == 0 {
			return
		}
		p.editDeploymentClient(ctx, sc, id)
	}
}

func (p *Process) editDeploymentClient(ctx context.Context, sc *bufio.Scanner, id int64) {
	d, err := p.store.GetDeploymentClient(ctx, id)
	if err != nil {
		p.refusal(err)
		return
	}
	fields := provision.DeploymentClientFields()
	title := d.DeploymentID
	if d.Name != "" && d.Name != d.DeploymentID {
		title += " — " + d.Name
	}
	p.printf("\n  Change where clients connect to %s. Enter keeps a value; 0 goes back.\n", title)
	opts := []opt{}
	for i, f := range fields {
		opts = append(opts, opt{Key: strconv.Itoa(i + 1),
			Text: fmt.Sprintf("   %2d  %-20s %s", i+1, f.Label, trim(orDash(provision.DeploymentClientValue(d, f.Key)), 52))})
	}
	opts = append(opts, opt{Key: "0", Text: "    0  Back"})
	pick, ok := p.choose(sc, "Which setting", "0", opts)
	n, _ := strconv.Atoi(pick)
	if !ok || n == 0 {
		return
	}
	if n < 1 || n > len(fields) {
		p.errf("  There is no setting %d.\n", n)
		return
	}
	f := fields[n-1]
	p.printf("  %s\n", f.Hint)
	cur := provision.DeploymentClientValue(d, f.Key)
	for {
		val, ok := p.ask(sc, fmt.Sprintf("%s [%s]", f.Label, orDefault(cur, provision.MasterNumberNone)))
		if !ok || val == "0" {
			return
		}
		if val == "" || strings.TrimSpace(val) == cur {
			p.printf("  Nothing changed, so nothing was saved.\n")
			return
		}
		changed, err := p.store.UpdateDeploymentClient(ctx, id, f.Key, val)
		switch {
		case err != nil && changed:
			// Saved; only its audit row is missing, and the operator is told.
			p.okf("  Saved.\n")
			p.warnf("  %v\n", err)
			return
		case err != nil:
			p.refusal(err)
			if provision.Refused(err) {
				continue // asked again: only the bad answer is lost
			}
			return
		case !changed:
			p.printf("  Nothing changed, so nothing was saved.\n")
			return
		}
		p.okf("  Saved. The portal's \"How to connect\" sheet shows it at once.\n")
		return
	}
}

// smppLine is a deployment's SMPP host and ports in one short line.
func smppLine(d provision.DeploymentClient) string {
	if d.SMPPHost == "" && d.SMPPPort == 0 && d.SMPPTLSPort == 0 {
		return "—"
	}
	s := orDefault(d.SMPPHost, "(no host)")
	if d.SMPPPort != 0 {
		s += ":" + strconv.Itoa(d.SMPPPort)
	}
	if d.SMPPTLSPort != 0 {
		s += fmt.Sprintf(", TLS %d", d.SMPPTLSPort)
	}
	return s
}
