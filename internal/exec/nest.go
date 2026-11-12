package exec

import (
	"context"
	"sync"
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
