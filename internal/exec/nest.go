package exec

import (
	"context"
	"sync"

	"github.com/catdevman/sfn-local/internal/store"
)

type stopKey struct{}
type nestKey struct{}
type itemKey struct{}

// mapItem is the Map.Item value visible on $$ for one iteration.
type mapItem struct {
	Index int
	Value any
}

// nestFrame catches failures inside one Parallel branch or Map iteration
// so they do not complete the execution.
type nestFrame struct {
	mu     sync.Mutex
	failed bool
	name   string
	cause  string
}

func newFrame() *nestFrame { return &nestFrame{} }

func (f *nestFrame) record(name, cause string) {
	if f == nil {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failed {
		return
	}
	f.failed = true
	f.name = name
	f.cause = cause
}

func (f *nestFrame) snapshot() (bool, string, string) {
	if f == nil {
		return false, "", ""
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.failed, f.name, f.cause
}

func withStop(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, ok := ctx.Value(stopKey{}).(context.Context); ok {
		return ctx
	}
	return context.WithValue(ctx, stopKey{}, ctx)
}

func stopRequested(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	stop, _ := ctx.Value(stopKey{}).(context.Context)
	return stop != nil && stop.Err() != nil
}

func withNest(ctx context.Context) (context.Context, *nestFrame) {
	if ctx == nil {
		ctx = context.Background()
	}
	f := newFrame()
	return context.WithValue(ctx, nestKey{}, f), f
}

func nestOf(ctx context.Context) *nestFrame {
	if ctx == nil {
		return nil
	}
	f, _ := ctx.Value(nestKey{}).(*nestFrame)
	return f
}

func withItem(ctx context.Context, index int, value any) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, itemKey{}, mapItem{Index: index, Value: value})
}

type parentKey struct{}

// parentIter is the outermost Map iteration around the current state.
// Index 0 is a real iteration, so the value is a struct rather than a bare int.
type parentIter struct {
	Index int32
}

// withParentIter records index unless an outer Map iteration already did.
// A nested Map keeps the outer index so interleaved items stay distinguishable.
func withParentIter(ctx context.Context, index int32) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, ok := ctx.Value(parentKey{}).(parentIter); ok {
		return ctx
	}
	return context.WithValue(ctx, parentKey{}, parentIter{Index: index})
}

func parentIterOf(ctx context.Context) (int32, bool) {
	if ctx == nil {
		return 0, false
	}
	p, ok := ctx.Value(parentKey{}).(parentIter)
	return p.Index, ok
}

func stampParent(ctx context.Context, ev *store.HistoryEvent) {
	if ev == nil {
		return
	}
	if idx, ok := parentIterOf(ctx); ok {
		ev.ParentIndex = idx
		ev.HasParentIndex = true
	}
}

type outcome int

const (
	outcomeOK outcome = iota
	outcomeFail
	outcomeStop
	outcomeHalt
)

type branchResult struct {
	output  string
	outcome outcome
	errName string
	cause   string
}
