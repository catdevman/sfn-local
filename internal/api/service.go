package api

import (
	"fmt"
	"io"
	"time"

	"github.com/catdevman/sfn-local/internal/api/gen"
	"github.com/catdevman/sfn-local/internal/exec"
	"github.com/catdevman/sfn-local/internal/store"
)

// Service handles the operations this process implements. The rest stay on
// Unimplemented and return from the HTTP layer while their surface row is deferred.
type Service struct {
	gen.Unimplemented
	Store   *store.Store
	Runner  *exec.Runner
	Account string
	Region  string
	Profile Profile
	Log     io.Writer
	// AliasRoll, when set, returns an integer in [0, 100) for alias routing.
	// Nil uses crypto/rand. A single route of weight 100 does not depend on it.
	AliasRoll func() int
	// ActivityPoll is how long GetActivityTask waits. Zero means 60 seconds.
	ActivityPoll time.Duration
}

func (s *Service) activityPoll() time.Duration {
	if s == nil || s.ActivityPoll <= 0 {
		return 60 * time.Second
	}
	return s.ActivityPoll
}

func (s *Service) now() time.Time {
	if s != nil && s.Runner != nil && s.Runner.Now != nil {
		return s.Runner.Now()
	}
	return time.Now()
}

func (s *Service) logf(format string, args ...any) {
	w := io.Writer(io.Discard)
	if s != nil && s.Log != nil {
		w = s.Log
	}
	fmt.Fprintf(w, format, args...)
}
