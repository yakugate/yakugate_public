package monitor

import "sync"

// Sections a package adds to every status report of its process, beside the
// role's own MonitorSections: one a process, whichever role it runs (the
// billing queue's booking, billing.Moments, 26 Oct 2026).
var (
	sectionsMu sync.RWMutex
	sections   = map[string]func() any{}
)

// AddSection puts fn's answer in this process's status reports under name.
// A later call with the same name replaces it; a nil answer is left out.
func AddSection(name string, fn func() any) {
	sectionsMu.Lock()
	defer sectionsMu.Unlock()
	if fn == nil {
		delete(sections, name)
		return
	}
	sections[name] = fn
}

// addSections puts the added sections in body (a role's own section of the
// same name wins).
func addSections(body map[string]any) {
	sectionsMu.RLock()
	defer sectionsMu.RUnlock()
	for name, fn := range sections {
		if _, taken := body[name]; taken {
			continue
		}
		if v := fn(); v != nil {
			body[name] = v
		}
	}
}
