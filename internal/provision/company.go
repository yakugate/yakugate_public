package provision

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"golang.org/x/text/currency"

	"github.com/yakugate/yakugate/internal/dal"
	"github.com/yakugate/yakugate/internal/platform/config"
)

// Companies. [SCOPE P14-COMPANY]
//
// A company is the tenant: balance, suspension, throughput and quotas all
// settle at company level, and every account and vendor must belong to one.
// So on a gateway with no portal, a console that cannot make a company cannot
// make anything else either — found 2026-09-18, running the console against
// stores the engine had built on its own, where the company list is empty.
//
// Creating one here asks what the Admin API requires — a name, the currency
// and the four contact email lists (QA API-007, 2026-09-28: a company with no
// billing email gets no invoice and no low-balance alert) — plus whether it is
// prepaid or postpaid (2026-10-02), and nothing that moves money. Balance is
// deliberately not asked: in the portal it moves only through POST
// /companies/:id/balance, atomically with its append-only balance_history
// row, never through the form. A console field that set it directly would be
// a way round the ledger.

// Company is what the console asks for.
type Company struct {
	Name string
	// MasterNumber is companies.master_number (config/0141): the operator's
	// own number for this customer. Optional; "" = none. See CleanMasterNumber.
	MasterNumber string
	Currency     string
	// BaseRate is companies.base_conversion_rate: how much of the system base
	// currency (config.BaseCurrency, USD unless set) ONE unit of this
	// company's currency is worth — 0.0036 for a PKR company under USD.
	// Required unless the company keeps the base currency itself, where it is
	// fixed at 1 (Saleem 2026-10-07). See CleanBaseRate.
	BaseRate string
	// Contact lists, each one or more addresses separated by commas. Stored
	// joined by ", " as the portal stores them.
	CompanyEmail string
	SupportEmail string
	BillingEmail string
	RatesEmail   string
	// Prepaid is companies.billing_config.prepaid: true = every charge is
	// taken from the balance, and the company's accounts are refused once it
	// runs out; false (the Admin API's default) = postpaid, charges are
	// invoiced and the balance is never drawn down. Stored as the number 1 or
	// 0, the only values the engine reads as such
	// (COALESCE(JSON_EXTRACT(billing_config,'$.prepaid'),0) = 1).
	Prepaid bool
}

// Billing words, as the console offers them and UpdateCompany takes them.
const (
	BillingPrepaid  = "prepaid"
	BillingPostpaid = "postpaid"
)

// BillingWord is "prepaid" or "postpaid".
func BillingWord(prepaid bool) string {
	if prepaid {
		return BillingPrepaid
	}
	return BillingPostpaid
}

// MasterNumberMax is the longest master number a company may carry.
const MasterNumberMax = 64

var masterNumberRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9 ._/+#-]*$`)

// CleanMasterNumber is a company's master number in its stored form: trimmed,
// runs of spaces made one, "" for none.
//
// The master number (Saleem 2026-10-06) is the number the operator knows the
// customer by outside the gateway — its own billing or CRM number. It is
// optional and not unique: several companies may be billed under one. It is
// shown wherever a company is named (invoices, the audit log, message logs
// and exports, the gateway's log files) and decides nothing.
//
// The rule is the Admin API's (services/masterNumber.js cleanMasterNumber) —
// change together: at most 64 characters; letters, digits, spaces and
// - _ . / + # only; the first a letter or digit.
func CleanMasterNumber(v string) (string, error) {
	v = strings.Join(strings.Fields(v), " ")
	if v == "" {
		return "", nil
	}
	if n := len([]rune(v)); n > MasterNumberMax {
		return "", refusef("a master number may be %d characters; this one is %d", MasterNumberMax, n)
	}
	if !masterNumberRE.MatchString(v) {
		return "", refusef("%q cannot be a master number: letters, digits, spaces and - _ . / + # only, starting with a letter or digit", v)
	}
	return v, nil
}

// Base Conversion Rate (Saleem 2026-10-07: "base conversion rate is required,
// each company's currency is converted into base currency and used in real
// time"). The gateway compares what a customer pays with what a vendor costs
// in the system base currency, because the two may be kept in different
// currencies, and the dashboards state margin in it. A company's way there is
// this one number.

// BaseRateMax is the largest rate a company may hold, and BaseRateMin the
// smallest the column (DECIMAL(20,10)) can keep — the Admin API's limits
// (companyRoutes.js MAX_BUSINESS_CONVERSION_RATE and its "too small" rule).
const (
	BaseRateMax = 100000000
	BaseRateMin = "0.0000000001"
)

var baseRateRE = regexp.MustCompile(`^\d{1,9}(\.\d{1,10})?$`)

// KeepsBaseCurrency reports whether a currency is the system base currency,
// so a company keeping it needs no conversion.
func KeepsBaseCurrency(cur string) bool {
	return strings.EqualFold(strings.TrimSpace(cur), config.BaseCurrency())
}

// CleanBaseRate is a company's Base Conversion Rate in its stored form.
//
// The rule is the Admin API's (companyRoutes.js checkCompany,
// assertBaseRate) — change together:
//   - a company that keeps the base currency: the rate is 1, whatever is
//     given — there is nothing to convert;
//   - any other currency: required; a plain number with at most ten
//     decimals, more than 0 and at most 100,000,000. 1 is a rate like any
//     other since config/0171 (3 Nov 2026): "not set" is 0, which no save
//     can give.
func CleanBaseRate(cur, v string) (string, error) {
	base := config.BaseCurrency()
	cur = strings.ToUpper(strings.TrimSpace(cur))
	if cur == base {
		return "1", nil
	}
	v = strings.TrimSpace(v)
	if v == "" {
		return "", refusef("a Base Conversion Rate is required: how much %s one %s is worth, like 0.0036. The loss check and the dashboards state this company's amounts in %s by it", base, cur, base)
	}
	if !baseRateRE.MatchString(v) {
		return "", refusef("%q is not a rate: give a number like 0.0036, with at most ten decimal places", v)
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil || f <= 0 {
		return "", refusef("a Base Conversion Rate must be more than 0; %q is not", v)
	}
	if f > BaseRateMax {
		return "", refusef("a Base Conversion Rate can be at most 100,000,000; %q is more", v)
	}
	// Stored as typed, less the zeros that say nothing.
	if strings.Contains(v, ".") {
		v = strings.TrimRight(strings.TrimRight(v, "0"), ".")
	}
	if w := strings.TrimLeft(v, "0"); w == "" || strings.HasPrefix(w, ".") {
		v = "0" + w
	} else {
		v = w
	}
	return v, nil
}

var emailRE = regexp.MustCompile(`^[^\s@,]+@[^\s@,]+\.[^\s@,]+$`)

// EmailList checks one comma-separated list and gives back its stored form.
// The rules are the Admin API's (companyRoutes.js emailList): every address
// valid, none twice, at most 512 characters once joined.
func EmailList(label, v string) (string, error) {
	var list []string
	seen := map[string]bool{}
	for _, e := range strings.Split(v, ",") {
		e = strings.TrimSpace(e)
		if e == "" {
			continue
		}
		if !emailRE.MatchString(e) {
			return "", refusef("%q is not an email address (%s email)", e, label)
		}
		if seen[strings.ToLower(e)] {
			return "", refusef("%q is in the %s emails twice", e, label)
		}
		seen[strings.ToLower(e)] = true
		list = append(list, e)
	}
	if len(list) == 0 {
		return "", refusef("at least one %s email is required", label)
	}
	joined := strings.Join(list, ", ")
	if len(joined) > 512 {
		return "", refusef("the %s emails are too long together: at most 512 characters, %d given", label, len(joined))
	}
	return joined, nil
}

// normalise trims the company and puts every email list in its stored form.
func (c *Company) normalise() error {
	c.Name = strings.TrimSpace(c.Name)
	c.Currency = strings.ToUpper(strings.TrimSpace(c.Currency))
	m, err := CleanMasterNumber(c.MasterNumber)
	if err != nil {
		return err
	}
	c.MasterNumber = m
	if currencyRE.MatchString(c.Currency) {
		r, err := CleanBaseRate(c.Currency, c.BaseRate)
		if err != nil {
			return err
		}
		c.BaseRate = r
	}
	for _, f := range []struct {
		label string
		v     *string
	}{{"company", &c.CompanyEmail}, {"support", &c.SupportEmail}, {"billing", &c.BillingEmail}, {"rates", &c.RatesEmail}} {
		j, err := EmailList(f.label, *f.v)
		if err != nil {
			return err
		}
		*f.v = j
	}
	return nil
}

// ValidateCompany checks a company before it is written.
func ValidateCompany(c Company) error {
	name := strings.TrimSpace(c.Name)
	if name == "" {
		return refusef("a company needs a name: it is how every account and vendor under it is recognised")
	}
	if n := len([]rune(name)); n > 128 {
		return refusef("a company name may be 128 characters; this one is %d", n)
	}
	if !currencyRE.MatchString(strings.TrimSpace(c.Currency)) {
		return refusef("%q is not a currency: it is three letters, like PKR or USD. It is the currency the company's balance is kept in", c.Currency)
	}
	// A real ISO 4217 code, as the Admin API asks (QA API-012).
	if _, err := currency.ParseISO(strings.ToUpper(strings.TrimSpace(c.Currency))); err != nil {
		return refusef("%q is not a currency code. It is the currency the company's balance is kept in, like PKR or USD", c.Currency)
	}
	cp := c
	return cp.normalise()
}

// CreateCompany writes the company with a fresh UUID, as the portal does.
func (s Store) CreateCompany(ctx context.Context, c Company) (int64, error) {
	if err := ValidateCompany(c); err != nil {
		return 0, err
	}
	if err := c.normalise(); err != nil {
		return 0, err
	}
	// The table does not make names unique, and the portal does not either.
	// Refusing here costs nothing and saves an operator picking between two
	// identical lines in every "which company?" list this console draws.
	taken, err := s.DB.CompanyNameTaken(ctx, c.Name)
	if err != nil {
		return 0, err
	}
	if taken {
		return 0, refusef("there is already a company called %q, and two with one name could not be told apart in a list", c.Name)
	}
	uuid, err := newUUID()
	if err != nil {
		return 0, err
	}
	// billing_config is written whole, {"prepaid": 1|0}, as the portal's
	// form sends it (Companies.jsx); the Admin API stores what it is sent.
	id, err := s.DB.InsertCompany(ctx, dal.CompanyInsert{
		UUID: uuid, Name: c.Name, MasterNumber: c.MasterNumber, Currency: c.Currency, BaseRate: c.BaseRate,
		CompanyEmail: c.CompanyEmail, SupportEmail: c.SupportEmail, BillingEmail: c.BillingEmail, RatesEmail: c.RatesEmail,
		Prepaid: boolInt(c.Prepaid),
	})
	if err != nil {
		return 0, fmt.Errorf("create the company: %w", err)
	}
	// The Admin API audits a create as companies.create (http/crud.js).
	return id, s.audit(ctx, auditEntry{
		Action: "companies.create", Entity: "company", Feature: "companies", EntityID: id,
		CompanyID: id, CompanyName: c.Name, Label: c.Name,
		Summary: "Created company " + c.Name + " from the console",
		Detail: map[string]any{"after": map[string]any{
			"name": c.Name, "master_number": nullStr(c.MasterNumber), "currency": c.Currency, "company_id": uuid,
			"base_conversion_rate": c.BaseRate,
			"company_email":        c.CompanyEmail, "support_email": c.SupportEmail,
			"billing_email": c.BillingEmail, "rates_email": c.RatesEmail,
			"billing_config": map[string]any{"prepaid": boolInt(c.Prepaid)},
		}},
	})
}

// newUUID makes a version-4 UUID, the shape crypto.randomUUID gives the portal.
func newUUID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("make a company id: %w", err)
	}
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}

// ── changing a company ──────────────────────────────────────────────────────

// CompanyDetail is one company as the console shows it.
type CompanyDetail struct {
	Company
	ID       int64
	UUID     string // companies.company_id
	Balance  string // four places, as stored
	Disabled bool
	// BaseProvider is companies.base_thirdparty_crp_id: the currency provider
	// whose live rate wins over BaseRate while it answers. Set in the portal.
	BaseProvider string
	// HotDays is companies.data_retention.hot_days (issue 25.3): days of
	// its records kept in the record database. A company that has only the
	// months of D-136 (hot_months) reads as that many times 30. 0 = the
	// gateway's YAKU_HOT_DAYS.
	HotDays int
	// Type is companies.type: selling, buying, both, or "" on an older row.
	Type string
	// TemplateApproval is companies.template_approval (config/0145): who
	// signs off its templates, one of TemplateApprovalLevels.
	TemplateApproval string
}

// GetCompany reads one company.
func (s Store) GetCompany(ctx context.Context, id int64) (CompanyDetail, error) {
	var d CompanyDetail
	r, err := s.DB.CompanyDetail(ctx, id)
	d.ID, d.UUID, d.Name, d.MasterNumber, d.Currency = r.ID, r.UUID, r.Name, r.MasterNumber, r.Currency
	d.CompanyEmail, d.SupportEmail, d.BillingEmail, d.RatesEmail = r.CompanyEmail, r.SupportEmail, r.BillingEmail, r.RatesEmail
	d.Prepaid, d.Balance, d.Disabled, d.BaseRate, d.BaseProvider, d.HotDays = r.Prepaid, r.Balance, r.Disabled, r.BaseRate, r.BaseProvider, r.HotDays
	d.Type, d.TemplateApproval = r.Type, r.TemplateApproval
	d.BaseRate = plainDecimal(d.BaseRate)
	if err == dal.ErrNoRows {
		return d, refusef("there is no company #%d", id)
	}
	if err != nil {
		return d, fmt.Errorf("read company %d: %w", id, err)
	}
	return d, nil
}

// companyEmailCols maps the console's keys to the four contact lists.
var companyEmailCols = []struct{ key, col, label string }{
	{"company_email", "company_email", "company"},
	{"support_email", "support_email", "support"},
	{"billing_email", "billing_email", "billing"},
	{"rates_email", "rates_email", "rates"},
}

// CompanyFields are the settings of a company the console may change. The
// rest of the portal's form (type, timezone, quotas, throughput, privacy,
// theme…) is the portal's; the currency cannot be changed (CompanyFixed).
func CompanyFields() []Field {
	return []Field{
		{Key: "name", Label: "Name", Kind: "text",
			Hint: "how the company is shown in every list. No two companies may share one"},
		{Key: "billing", Label: "Billing", Kind: "choice", Choices: []string{BillingPrepaid, BillingPostpaid},
			Hint: "prepaid = every charge is taken from the balance, and its accounts are refused once it runs out; postpaid = charges are invoiced and the balance is not used"},
		{Key: "company_email", Label: "Company emails", Kind: "text",
			Hint: "one or more addresses, separated by commas"},
		{Key: "support_email", Label: "Support emails", Kind: "text",
			Hint: "one or more addresses, separated by commas"},
		{Key: "billing_email", Label: "Billing emails", Kind: "text",
			Hint: "invoices and low-balance alerts go here; one or more addresses, separated by commas"},
		{Key: "rates_email", Label: "Rates emails", Kind: "text",
			Hint: "rate change notices go here; one or more addresses, separated by commas"},
		// Last, so the numbers operators know for the settings above stay.
		{Key: "master_number", Label: "Master number", Kind: "text",
			Hint: "your own number for this customer (billing or CRM), shown on its invoices, audit rows, message logs and log files. Optional; type " + MasterNumberNone + " to clear it"},
		{Key: "base_rate", Label: "Rate to " + config.BaseCurrency(), Kind: "text",
			Hint: "Base Conversion Rate: how much " + config.BaseCurrency() + " ONE unit of the company's currency is worth, like 0.0036. The loss check and the dashboards state this company's amounts in " + config.BaseCurrency() + " by it. Fixed at 1 for a company that keeps " + config.BaseCurrency()},
		// Last again (D-136), so the numbers of the settings above stay.
		{Key: "hot_days", Label: "Days of records kept", Kind: "int",
			Hint: "today and the days before it, up to this number, stay in the record database; every night at about 02:00 UTC the next older day goes to the archive. 1 to 730; 0 = the gateway's own (YAKU_HOT_DAYS, 90)"},
		// Last again (config/0145), so the numbers of the settings above stay.
		{Key: "template_approval", Label: "Template approval", Kind: "choice", Choices: TemplateApprovalLevels,
			Hint: "who signs off its message templates: NONE = approved when saved; COMPANY = the company only; ADMIN = an admin only (the default); ADMIN_OR_COMPANY = either; ADMIN_AND_COMPANY = one of each, in any order. Nobody signs a template they wrote. Not for a company we buy from"},
	}
}

// plainDecimal is a stored DECIMAL as a person types it: "0.0036000000" is
// 0.0036 and "1.0000000000" is 1.
func plainDecimal(v string) string {
	v = strings.TrimSpace(v)
	if strings.Contains(v, ".") {
		v = strings.TrimRight(strings.TrimRight(v, "0"), ".")
	}
	return v
}

// BaseRateSet reports whether a company has a way into the base currency the
// gateway believes: it keeps the base currency, or holds a rate above 0 (0 is
// "not set" since config/0171; 1 was, before it).
func BaseRateSet(d CompanyDetail) bool {
	if KeepsBaseCurrency(d.Currency) {
		return true
	}
	f, err := strconv.ParseFloat(d.BaseRate, 64)
	return err == nil && f > 0
}

// MasterNumberNone is what an operator types to clear a master number (Enter
// keeps the value, so an empty answer cannot mean "none").
const MasterNumberNone = "none"

// TemplateApprovalLevels are who may sign off a company's templates, as the
// Admin API spells them (services/templateApproval.js LEVELS — change
// together). ADMIN is the column's default.
var TemplateApprovalLevels = []string{"NONE", "COMPANY", "ADMIN", "ADMIN_OR_COMPANY", "ADMIN_AND_COMPANY"}

// DefaultTemplateApproval is the level a company has until one is chosen.
const DefaultTemplateApproval = "ADMIN"

// CompanyFixed is what a company cannot change once it exists.
func CompanyFixed() []FixedField {
	return []FixedField{{Key: "currency", Label: "Currency",
		Why: "its balance, invoices and rate cards are all kept in it"}}
}

// CompanyValue is a company's current value of one CompanyFields key, in the
// form UpdateCompany takes it — so "Enter keeps the value" is a real answer.
func CompanyValue(d CompanyDetail, key string) string {
	switch key {
	case "name":
		return d.Name
	case "master_number":
		return d.MasterNumber
	case "billing":
		return BillingWord(d.Prepaid)
	case "currency":
		return d.Currency
	case "base_rate":
		// The column's default of 1 on another currency is no rate at all:
		// there is nothing to keep, so Enter keeps nothing and 1 is refused.
		if !BaseRateSet(d) {
			return ""
		}
		return d.BaseRate
	case "company_email":
		return d.CompanyEmail
	case "support_email":
		return d.SupportEmail
	case "billing_email":
		return d.BillingEmail
	case "rates_email":
		return d.RatesEmail
	case "hot_days":
		return strconv.Itoa(d.HotDays)
	case "template_approval":
		// None stored reads as the default, as the Admin API's levelOf reads it.
		if d.TemplateApproval == "" {
			return DefaultTemplateApproval
		}
		return d.TemplateApproval
	}
	return ""
}

// MaxHotDays is the most days a company may keep in the record database (the
// Admin API's rule, services/recordSettings.js).
const MaxHotDays = 730

// UpdateCompany changes one setting of a company. changed=false (and no
// error) when the value is what is stored already: nothing is written, and
// no audit row says something moved.
//
// The Admin API's update rules (companyRoutes.js checkCompany and the crud
// update), each judged on the one field being changed, as the Admin API
// judges only the fields a save sends:
//   - name: trimmed, required, at most 128 characters; a name another
//     company has is refused, as CreateCompany refuses it (the Admin API's
//     checkCompany since 2026-10-02 — compared as the column compares, case
//     and accents ignored);
//   - master_number: CleanMasterNumber; "none" (MasterNumberNone) or an
//     empty value clears it;
//   - currency: cannot be changed once the company exists;
//   - an email list: every address valid, none twice, at most 512
//     characters (EmailList). Emptying one is refused (the Admin API refuses
//     emptying one the company has; this refuses an empty one always);
//   - billing: prepaid or postpaid, an operator's decision — the console is
//     one. Written into billing_config without touching any other key in it;
//   - base_rate: CleanBaseRate. A company that keeps the base currency has
//     no rate to change.
//
// Every saved edit moves updated_at on by at least a second and writes a
// companies.update audit row naming the fields, as the Admin API does: a
// portal form opened before this edit then refuses to save over it (QA
// BUG-033, companyRoutes.js assertUnchangedSince reads exactly those).
func (s Store) UpdateCompany(ctx context.Context, id int64, key, value string) (changed bool, err error) {
	cur, err := s.GetCompany(ctx, id)
	if err != nil {
		return false, err
	}
	value = strings.TrimSpace(value)
	var col string
	var before, after any
	switch key {
	case "name":
		if value == "" {
			return false, refusef("a company needs a name: it is how every account and vendor under it is recognised")
		}
		if n := len([]rune(value)); n > 128 {
			return false, refusef("a company name may be 128 characters; this one is %d", n)
		}
		if value == cur.Name {
			return false, nil
		}
		taken, err := s.DB.CompanyNameTakenByOther(ctx, value, id)
		if err != nil {
			return false, err
		}
		if taken {
			return false, refusef("there is already a company called %q, and two with one name could not be told apart in a list", value)
		}
		col, before, after = "name", cur.Name, value
	case "master_number":
		if strings.EqualFold(value, MasterNumberNone) {
			value = ""
		}
		m, err := CleanMasterNumber(value)
		if err != nil {
			return false, err
		}
		if m == cur.MasterNumber {
			return false, nil
		}
		// NULL, not "", for none — as the Admin API stores it.
		col, before, after = "master_number", nullStr(cur.MasterNumber), nullStr(m)
	case "base_rate":
		if KeepsBaseCurrency(cur.Currency) {
			if value == "" || value == cur.BaseRate {
				return false, nil
			}
			return false, refusef("%s keeps %s, the system base currency: there is nothing to convert, so its rate stays 1", cur.Name, config.BaseCurrency())
		}
		r, err := CleanBaseRate(cur.Currency, value)
		if err != nil {
			return false, err
		}
		if r == cur.BaseRate {
			return false, nil
		}
		col, before, after = "base_conversion_rate", cur.BaseRate, r
	case "hot_days":
		n, err := strconv.Atoi(value)
		if err != nil || n < 0 || n > MaxHotDays {
			return false, refusef("%q: the days kept are a whole number from 1 to %d (0 = the gateway's own)", value, MaxHotDays)
		}
		if n == cur.HotDays {
			return false, nil
		}
		return s.setHotDays(ctx, cur, n)
	case "template_approval":
		// A company we buy from is a supplier: it sends nothing through the
		// gateway, so it has no templates (templateApprovers.js appliesTo).
		// The Admin API leaves the field unsaved for it, so a form that
		// carries it for every company still saves; one setting asked on its
		// own is refused here, saying why.
		if strings.EqualFold(cur.Type, "buying") {
			return false, refusef("%s is a company we buy from: it sends nothing through the gateway, so it has no templates to approve", cur.Name)
		}
		lvl := strings.ToUpper(value)
		if !slices.Contains(TemplateApprovalLevels, lvl) {
			return false, refusef("%q: template approval is one of %s", value, strings.Join(TemplateApprovalLevels, ", "))
		}
		if was := CompanyValue(cur, "template_approval"); lvl == was {
			return false, nil
		}
		col, before, after = "template_approval", CompanyValue(cur, "template_approval"), lvl
	case "currency":
		if strings.EqualFold(value, cur.Currency) {
			return false, nil
		}
		return false, refusef("the currency cannot be changed once the company exists: its balance, invoices and rate cards are all kept in %s", cur.Currency)
	case "billing":
		var prepaid bool
		switch strings.ToLower(value) {
		case BillingPrepaid:
			prepaid = true
		case BillingPostpaid:
		default:
			return false, refusef("%q is not a billing model: prepaid or postpaid", value)
		}
		if prepaid == cur.Prepaid {
			return false, nil
		}
		return s.setPrepaid(ctx, cur, prepaid)
	default:
		for _, e := range companyEmailCols {
			if e.key != key {
				continue
			}
			list, err := EmailList(e.label, value)
			if err != nil {
				return false, err
			}
			old := CompanyValue(cur, key)
			if list == old {
				return false, nil
			}
			col, before, after = e.col, old, list
		}
		if col == "" {
			return false, refusef("%q is not a setting on a company", key)
		}
	}
	// col is one of a fixed set above, never typed by anyone.
	// after is text, or nil for NULL (master_number's none).
	v, ok := after.(string)
	if err := s.DB.SetCompanyColumn(ctx, id, col, dal.NullString{String: v, Valid: ok}); err != nil {
		return false, fmt.Errorf("update company %d: %w", id, err)
	}
	name := cur.Name
	if col == "name" {
		name = value
	}
	return true, s.audit(ctx, companyUpdateAudit(cur, name, col, before, after))
}

// setHotDays sets how many days of a company's records stay in the record
// database (data_retention.hot_days, issue 25.3) and takes away the months of
// D-136 (hot_months), which it replaces; 0 removes both, so the gateway's
// YAKU_HOT_DAYS applies. Every other key of data_retention stays.
func (s Store) setHotDays(ctx context.Context, cur CompanyDetail, n int) (bool, error) {
	var err error
	if n == 0 {
		err = s.DB.ClearCompanyHotDays(ctx, cur.ID)
	} else {
		err = s.DB.SetCompanyHotDays(ctx, cur.ID, n)
	}
	if err != nil {
		return false, fmt.Errorf("update company %d: %w", cur.ID, err)
	}
	return true, s.audit(ctx, companyUpdateAudit(cur, cur.Name, "data_retention", cur.HotDays, n))
}

// setPrepaid switches a company between prepaid and postpaid. saved=true
// once the row has changed, even if its audit row then failed.
func (s Store) setPrepaid(ctx context.Context, cur CompanyDetail, prepaid bool) (saved bool, err error) {
	old, err := s.DB.CompanyBillingConfig(ctx, cur.ID)
	if err != nil {
		return false, fmt.Errorf("read company %d's billing: %w", cur.ID, err)
	}
	// Only the prepaid key moves; anything else billing_config holds stays.
	// A JSON object is the only thing JSON_SET can add a key to.
	if err := s.DB.SetCompanyPrepaid(ctx, cur.ID, boolInt(prepaid)); err != nil {
		return false, fmt.Errorf("update company %d: %w", cur.ID, err)
	}
	now, err := s.DB.CompanyBillingConfig(ctx, cur.ID)
	if err != nil {
		return true, fmt.Errorf("saved, but company %d could not be read back for the audit row: %w", cur.ID, err)
	}
	return true, s.audit(ctx, companyUpdateAudit(cur, cur.Name, "billing_config", rawJSON(old), rawJSON(now)))
}

// companyUpdateAudit is the companies.update row the Admin API's crud update
// writes: the fields that moved, with their before and after.
func companyUpdateAudit(cur CompanyDetail, name, col string, before, after any) auditEntry {
	return auditEntry{
		Action: "companies.update", Entity: "company", Feature: "companies", EntityID: cur.ID,
		CompanyID: cur.ID, CompanyName: name, Label: name,
		Summary: "Updated company " + name + " from the console",
		Detail: map[string]any{
			"fields": []string{col},
			"before": map[string]any{col: before},
			"after":  map[string]any{col: after},
		},
	}
}

// rawJSON keeps a stored JSON value as JSON in an audit row (NULL = null).
func rawJSON(v dal.NullString) json.RawMessage {
	if !v.Valid || !json.Valid([]byte(v.String)) {
		return json.RawMessage("null")
	}
	return json.RawMessage(v.String)
}
