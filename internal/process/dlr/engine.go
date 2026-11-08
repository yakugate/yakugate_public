package dlr

import (
	"github.com/yakugate/yakugate/internal/dal"
)

// engine is the config store's parts the shared engine packages read
// (dal.EngineConfig, opened by New beside traffic). A Process built by hand
// without it (a test) gets an empty one: every part is nil and each user
// does nothing, as with no config store before.
func (p *Process) engine() *dal.EngineConfig {
	if p.eng == nil {
		p.eng = &dal.EngineConfig{}
	}
	return p.eng
}
