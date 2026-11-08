package mysql

import "github.com/yakugate/yakugate/internal/dal"

// Records at acceptance committed together, without Redis (DAL plan, step 3,
// 30 Oct 2026). The group writer is dal.AcceptGroup since the record store
// moved behind the DAL (30 Oct 2026); these names stay for the callers of
// this package.

// AcceptGroup is a WriteAccepted that commits a process's calls together.
type AcceptGroup = dal.AcceptGroup

// NewAcceptGroup is the group writer over s's WriteAccepted.
func NewAcceptGroup(s *Store) *AcceptGroup { return dal.NewAcceptGroup(s) }
