package exec

import (
	"context"
	"strconv"
	"strings"
	"sync"

	"github.com/catdevman/sfn-local/internal/asl"
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

type chainKey struct{}

// chainOf is the Parallel branch or Map iteration the context runs in. It
// is "" at the top level.
func chainOf(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	c, _ := ctx.Value(chainKey{}).(string)
	return c
}

func withChain(ctx context.Context, chain string) context.Context {
	return context.WithValue(ctx, chainKey{}, chain)
}

// branchChain names Parallel branch i of st, and iterChain Map iteration i.
func branchChain(ctx context.Context, st *asl.State, i int) string {
	return chainOf(ctx) + "/" + st.Name + "#" + strconv.Itoa(i)
}

func iterChain(ctx context.Context, st *asl.State, i int) string {
	return chainOf(ctx) + "/" + st.Name + "@" + strconv.Itoa(i)
}

// chainEvents is the part of a history on one chain.
func chainEvents(history []store.HistoryEvent, chain string) []store.HistoryEvent {
	var out []store.HistoryEvent
	for _, ev := range history {
		if ev.Chain == chain {
			out = append(out, ev)
		}
	}
	return out
}

// abortable are the state types AWS writes a StateAborted event for.
var abortable = map[string]bool{"Task": true, "Wait": true, "Map": true, "Parallel": true}

// abortChain writes <Type>StateAborted, innermost first, for each state on
// chain that was entered and has not exited, failed, or aborted. AWS does
// this for the branches and iterations of a Parallel or Map that fails. A
// Fail state has no aborted event.
func (r *Runner) abortChain(ctx context.Context, arn, chain string) {
	ex, ok := r.Store.Execution(arn)
	if !ok {
		return
	}
	var open []string
	for _, ev := range chainEvents(ex.History, chain) {
		switch {
		case strings.HasSuffix(ev.Type, "StateEntered"):
			open = append(open, strings.TrimSuffix(ev.Type, "StateEntered"))
		case len(open) == 0:
		case strings.HasSuffix(ev.Type, "StateExited"),
			ev.Type == open[len(open)-1]+"StateFailed",
			ev.Type == open[len(open)-1]+"StateAborted":
			open = open[:len(open)-1]
		}
	}
	for i := len(open) - 1; i >= 0; i-- {
		if abortable[open[i]] {
			r.emit(ctx, arn, store.HistoryEvent{Type: open[i] + "StateAborted", Chain: chain, Side: true})
		}
	}
}

func stampParent(ctx context.Context, ev *store.HistoryEvent) {
	if ev == nil {
		return
	}
	if ev.Chain == "" {
		ev.Chain = chainOf(ctx)
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
	// after is the event a Parallel or Map Succeeded or Failed event points
	// at: the last event of the last branch or iteration to succeed, or of
	// the one that failed. 0 is unset.
	after int64
}
