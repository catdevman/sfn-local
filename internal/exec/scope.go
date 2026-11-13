package exec

import (
	"context"
	"encoding/json"
	"sync"
)

const (
	maxVarValue  = 262144
	maxVarAssign = 262144
	maxVarExec   = 10 << 20
)

// queryError is a States.QueryEvaluationError cause.
type queryError struct{ Cause string }

func (e *queryError) Error() string {
	if e == nil {
		return ""
	}
	return e.Cause
}

type scope struct {
	parent *scope
	tree   *varTree
	vals   map[string]any
	sizes  map[string]int
	kids   []*scope
}

type varTree struct {
	mu    sync.Mutex
	bytes int
}

type scopeKey struct{}
type entryVarsKey struct{}

func newRootScope() *scope {
	return &scope{tree: &varTree{}, vals: map[string]any{}, sizes: map[string]int{}}
}

func withScope(ctx context.Context, s *scope) context.Context {
	return context.WithValue(ctx, scopeKey{}, s)
}

func scopeOf(ctx context.Context) *scope {
	if ctx == nil {
		return nil
	}
	s, _ := ctx.Value(scopeKey{}).(*scope)
	return s
}

func withEntryVars(ctx context.Context, vars map[string]any) context.Context {
	return context.WithValue(ctx, entryVarsKey{}, vars)
}

func entryVars(ctx context.Context) map[string]any {
	if ctx != nil {
		if vars, ok := ctx.Value(entryVarsKey{}).(map[string]any); ok && vars != nil {
			return vars
		}
	}
	if s := scopeOf(ctx); s != nil {
		return s.snapshot()
	}
	return map[string]any{}
}

func (r *Runner) ensureScope(ctx context.Context, arn string) context.Context {
	if scopeOf(ctx) != nil {
		return ctx
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.scopes == nil {
		r.scopes = map[string]*scope{}
	}
	s := r.scopes[arn]
	if s == nil {
		s = newRootScope()
		r.scopes[arn] = s
	}
	return withScope(ctx, s)
}

func (r *Runner) dropScope(arn string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	delete(r.scopes, arn)
	r.mu.Unlock()
}

func (s *scope) child() *scope {
	if s == nil || s.tree == nil {
		return newRootScope()
	}
	c := &scope{parent: s, tree: s.tree, vals: map[string]any{}, sizes: map[string]int{}}
	s.tree.mu.Lock()
	s.kids = append(s.kids, c)
	s.tree.mu.Unlock()
	return c
}

func (s *scope) detach() {
	if s == nil || s.tree == nil || s.parent == nil {
		return
	}
	s.tree.mu.Lock()
	defer s.tree.mu.Unlock()
	if s.parent == nil {
		return
	}
	s.tree.bytes -= s.subtreeLocked()
	kept := s.parent.kids[:0]
	for _, kid := range s.parent.kids {
		if kid != s {
			kept = append(kept, kid)
		}
	}
	s.parent.kids = kept
	s.parent = nil
}

func (s *scope) subtreeLocked() int {
	n := 0
	for _, sz := range s.sizes {
		n += sz
	}
	for _, kid := range s.kids {
		n += kid.subtreeLocked()
	}
	return n
}

func (s *scope) snapshot() map[string]any {
	if s == nil || s.tree == nil {
		return map[string]any{}
	}
	s.tree.mu.Lock()
	flat := map[string]any{}
	var chain []*scope
	for cur := s; cur != nil; cur = cur.parent {
		chain = append(chain, cur)
	}
	for i := len(chain) - 1; i >= 0; i-- {
		for key, val := range chain[i].vals {
			flat[key] = val
		}
	}
	s.tree.mu.Unlock()
	out := make(map[string]any, len(flat))
	for key, val := range flat {
		out[key] = cloneJSON(val)
	}
	return out
}

func (s *scope) commit(assigns map[string]any) error {
	if len(assigns) == 0 {
		return nil
	}
	if s == nil || s.tree == nil {
		return &queryError{Cause: "variable scope is missing"}
	}
	sizes := make(map[string]int, len(assigns))
	total := 0
	for key, val := range assigns {
		raw, err := json.Marshal(val)
		if err != nil {
			return &queryError{Cause: err.Error()}
		}
		if len(raw) > maxVarValue {
			return &queryError{Cause: "variable value exceeded the 262144 byte limit"}
		}
		sizes[key] = len(raw)
		total += len(raw)
	}
	if total > maxVarAssign {
		return &queryError{Cause: "Assign exceeded the 262144 byte limit"}
	}
	s.tree.mu.Lock()
	defer s.tree.mu.Unlock()
	delta := 0
	for key, n := range sizes {
		delta += n - s.sizes[key]
	}
	if s.tree.bytes+delta > maxVarExec {
		return &queryError{Cause: "execution variables exceeded the 10485760 byte limit"}
	}
	if s.vals == nil {
		s.vals = map[string]any{}
		s.sizes = map[string]int{}
	}
	for key, val := range assigns {
		s.vals[key] = val
		s.sizes[key] = sizes[key]
	}
	s.tree.bytes += delta
	return nil
}

func cloneJSON(v any) any {
	text, err := compactValue(v)
	if err != nil {
		return v
	}
	out, err := parseJSON(text)
	if err != nil {
		return v
	}
	return out
}
