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
