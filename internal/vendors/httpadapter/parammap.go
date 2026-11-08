package httpadapter

// One parameter's own value mapping (6 Nov 2026, Saleem: "each param should
// have its own value mapping … I will not be able to use the real value in
// two different params"; "when there is any variable in the text like {},
// ask the operator to select whole value matching and replacement, or
// selected variables' value mapping").
//
// A mapping changes ONLY the parameter it belongs to. Until this day a
// parameter's value mapping was laid over the whole call (Call.Transform), so
// mapping {encoding} for one parameter changed it in every other parameter,
// the body and the URL too — and a call-wide "translate our values" setting,
// tried the same day, had the same fault.
//
//	Whole  the parameter's finished value is matched and replaced
//	       ({encoding} gsm7 -> 0; or a fixed default "1" -> "Y").
//	Vars   inside a value written with several {variables}
//	       (<SMSRequest>…<urdu>{encoding}</urdu>…</SMSRequest>), each chosen
//	       variable is mapped before the value is written; the others go
//	       as they are.
//
// The body template has its own Vars (Call.BodyVars). Call.Transform stays
// for profiles written as adapter_config JSON, whose behaviour is unchanged.
type ParamMap struct {
	Whole map[string]string            `json:"whole,omitempty"`
	Vars  map[string]map[string]string `json:"vars,omitempty"`
}

// withVars is c with the chosen variables mapped — a copy; c is not changed.
func withVars(c map[string]string, vars map[string]map[string]string) map[string]string {
	if len(vars) == 0 {
		return c
	}
	out := make(map[string]string, len(c))
	for k, v := range c {
		out[k] = v
	}
	for k, m := range vars {
		if mapped, ok := m[out[k]]; ok {
			out[k] = mapped
		}
	}
	return out
}

// whole maps a finished value; a value not listed goes as it is.
func (pm ParamMap) whole(v string) string {
	if mapped, ok := pm.Whole[v]; ok {
		return mapped
	}
	return v
}

// fill writes one parameter's value with its own mapping: its variables
// first, then the whole value.
func (pm ParamMap) fill(tmpl string, c map[string]string, rend func(string, map[string]string) string) string {
	return pm.whole(rend(tmpl, withVars(c, pm.Vars)))
}

// list maps each item of a list value (a bulk call's recipients) as a whole.
func (pm ParamMap) list(items []string) []string {
	if len(pm.Whole) == 0 {
		return items
	}
	out := make([]string, len(items))
	for i, it := range items {
		out[i] = pm.whole(it)
	}
	return out
}

// adapterVars turns a mapping keyed by our source names ({encoding},
// {message_id}, {hlr.mcc}) into the adapter's own field names, as the
// template's placeholders are turned. A name the vocabulary does not know is
// left out (the Admin API refuses one on save).
func adapterVars(byName map[string]map[string]string, role string) map[string]map[string]string {
	if len(byName) == 0 {
		return nil
	}
	out := map[string]map[string]string{}
	for name, m := range byName {
		if len(m) == 0 {
			continue
		}
		src := name
		if role == "poll" && src == "message_id" {
			src = "operator_id"
		}
		key, ok := sourceTemplates[src]
		if !ok {
			continue
		}
		out[key] = m
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
