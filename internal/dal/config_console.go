package dal

import "context"

// ConsoleViews is what the console's own screens read beside the rulebook:
// the main menu's counts, a company's recent top-ups, the deployments to
// choose from, a rule's next free place.
type ConsoleViews interface {
	// MenuCount is how many of one thing the main menu shows: accounts,
	// companies, vendors, calls, plans, dialects, cards, spam, blocked or
	// operators. An unknown name is an error.
	MenuCount(ctx context.Context, what string) (int, error)
	// RecentLedger is a company's last 15 top-ups, corrections and refunds,
	// newest first; a row that cannot be read is left out.
	RecentLedger(ctx context.Context, companyID int64) ([]LedgerLine, error)
	// ActiveDeployments is every deployment that is not switched off, by id;
	// a row that cannot be read is left out.
	ActiveDeployments(ctx context.Context) ([]DeploymentName, error)
	// MaxRulePriority is the highest priority of an account's rules of one
	// direction (not Valid when it has none).
	MaxRulePriority(ctx context.Context, accountID int64, direction string) (NullInt64, error)
	// AccountUsername is an account's username.
	AccountUsername(ctx context.Context, id int64) (string, error)
}

// LedgerLine is one balance_history row as the console lists it: the
// amounts as the column writes them.
type LedgerLine struct {
	When, Type, Amount, After, Reason, By string
}

// DeploymentName is one deployment's id and name ("" when none).
type DeploymentName struct {
	ID, Name string
}
