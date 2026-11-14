package exec

import (
	"context"
	"math"
	"sync"

	"github.com/catdevman/sfn-local/internal/asl"
	"github.com/catdevman/sfn-local/internal/jsonpath"
	"github.com/catdevman/sfn-local/internal/store"
)

const (
	iterPending = iota
	iterStarted
	iterSucceeded
	iterFailed
)

func (r *Runner) runMap(ctx context.Context, arn string, st *asl.State, input, effective string, effVal, ctxObj any) (string, string, bool, bool) {
	if st.Processor == nil {
		r.failCtx(ctx, arn, "States.Runtime", "Map has no iterator")
		return "", "", false, true
	}
	if st.Dist != nil {
		return r.runDistributed(ctx, arn, st, input, effVal, ctxObj)
	}
	// A redrive that resumes this state skips MapStateStarted and the
	// iterations that finished, as on AWS.
	resume := resumedHere(ctx, st)
	var startedID int64
	if resume != nil {
		ex, _ := r.Store.Execution(arn)
		startedID = startedAfter(ex.History, chainOf(ctx), "MapStateStarted", resume.entered)
	}
	retries := 0
	started := resume != nil
	for {
		if r.halt(ctx, arn) {
			return "", "", false, true
		}
		if r.pastDeadline(arn) {
			r.finish(arn, store.ExecTimedOut, "ExecutionTimedOut", "", false, "States.Timeout", "", true, false)
			return "", "", false, true
		}
		setRetryCount(ctxObj, retries)
		var items []any
		var err error
		var maxc *int64
		if st.JSONata {
			obj, _ := ctxObj.(map[string]any)
			items, err = r.evalItems(ctx, st, effVal, obj, entryVars(ctx))
			if err == nil {
				maxc, err = r.evalMaxConcurrency(ctx, st, effVal, ctxObj, entryVars(ctx))
			}
		} else {
			items, err = mapItems(st, effVal, ctxObj, entryVars(ctx))
			maxc = st.MaxConcurrency
		}
		if err != nil {
			next, output, terminal, done, again := r.retryOrFinish(ctx, arn, st, input, ctxObj, "MapStateFailed", failureFrom(err), &retries)
			if again {
				continue
			}
			return next, output, terminal, done
		}
		if !started {
			if len(items) > math.MaxInt32 {
				r.failCtx(ctx, arn, "States.Runtime", "Map items exceed the history length field")
				return "", "", false, true
			}
			id, ok := r.emitID(ctx, arn, store.HistoryEvent{Type: "MapStateStarted", Length: int32(len(items)), HasLength: true})
			if !ok {
				return "", "", false, true
			}
			startedID = id
			started = true
		}
		resuming := resume != nil && retries == 0
		if !resuming {
			r.Store.ClearResults(arn, chainOf(ctx)+"/"+st.Name+"@")
		}
		res := r.mapOnce(ctx, arn, st, effVal, items, retries, maxc, startedID, resuming)
		switch res.outcome {
		case outcomeOK:
			// MapStateSucceeded and MapStateExited both follow the last
			// MapIterationSucceeded, as on AWS.
			if !r.emit(ctx, arn, store.HistoryEvent{Type: "MapStateSucceeded", Previous: res.after, HasPrevious: res.after > 0, Side: true}) {
				return "", "", false, true
			}
			if res.after > 0 {
				r.Store.SetChainLast(arn, chainOf(ctx), res.after)
			}
			return r.completeTask(ctx, arn, st, input, res.output, ctxObj)
		case outcomeHalt:
			return "", "", false, true
		case outcomeStop:
			if stopRequested(ctx) {
				r.halt(ctx, arn)
			}
			return "", "", false, true
		}
		next, output, terminal, done, again := r.retryOrFinish(ctx, arn, st, input, ctxObj, "MapStateFailed", res, &retries)
		if again {
			continue
		}
		return next, output, terminal, done
	}
}

// retryOrFinish retries one failed attempt. again tells the caller to run
// the state once more. Otherwise the returned step result is final.
func (r *Runner) retryOrFinish(ctx context.Context, arn string, st *asl.State, input string, ctxObj any, failedEvent string, res branchResult, retries *int) (string, string, bool, bool, bool) {
	if rt, ok := pickRetry(st.Retry, res.errName); ok && int64(*retries+1) <= rt.MaxAttempts {
		if !r.retrySleep(ctx, arn, rt, *retries+1) {
			return "", "", false, true, false
		}
		*retries++
		return "", "", false, false, true
	}
	if failedEvent != "" && !r.emit(ctx, arn, store.HistoryEvent{Type: failedEvent, Previous: res.after, HasPrevious: res.after > 0}) {
		return "", "", false, true, false
	}
	next, output, terminal, done := r.afterFailure(ctx, arn, st, input, ctxObj, res)
	return next, output, terminal, done, false
}

func mapItems(st *asl.State, root, ctx any, vars map[string]any) ([]any, error) {
	path := st.ItemsPath
	if path == "" {
		path = "$"
	}
	got, found, err := jsonpath.GetBound(path, root, ctx, vars)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, missingPath(path)
	}
	arr, ok := got.([]any)
	if !ok {
		return nil, jsonpath.Runtime("ItemsPath " + path + " must yield an array")
	}
	return arr, nil
}

// mapOnce runs the iterations. A slot frees in index order, so MaxConcurrency
// 1 runs them one after another. An iteration's MapIterationStarted follows
// the last event of the iteration whose slot it took, or MapStateStarted, as
// on AWS. A resuming attempt reuses each finished iteration's result and
// resumes each unfinished one at its open state without a new
// MapIterationStarted.
func (r *Runner) mapOnce(ctx context.Context, arn string, st *asl.State, root any, items []any, retries int, maxc *int64, started int64, resuming bool) branchResult {
	n := len(items)
	if n == 0 {
		return branchResult{outcome: outcomeOK, output: "[]", after: started}
	}
	child, cancel := context.WithCancel(ctx)
	defer cancel()
	var sem chan struct{}
	if maxc != nil && *maxc > 0 {
		sem = make(chan struct{}, int(*maxc))
	}
	var history []store.HistoryEvent
	if resuming {
		ex, _ := r.Store.Execution(arn)
		history = ex.History
	}
	results := make([]string, n)
	status := make([]int, n)
	chains := make([]string, n)
	var mu sync.Mutex
	var failed bool
	var name, cause, failChain string
	slotPrev := started
	var lastSucceeded int64
	note := func(c, errName, errCause string) {
		mu.Lock()
		if !failed {
			failed = true
			name = errName
			cause = errCause
			failChain = c
			cancel()
		}
		mu.Unlock()
	}
	var wg sync.WaitGroup
dispatch:
	for i, elem := range items {
		chains[i] = iterChain(ctx, st, i)
		c := chains[i]
		if resuming {
			if out, ok := r.Store.Result(arn, c); ok {
				results[i], status[i] = out, iterSucceeded
				continue
			}
		}
		if child.Err() != nil || stopRequested(ctx) {
			break
		}
		if sem != nil {
			select {
			case sem <- struct{}{}:
			case <-child.Done():
				break dispatch
			}
		}
		wg.Add(1)
		go func(i int, elem any) {
			defer wg.Done()
			if sem != nil {
				defer func() { <-sem }()
			}
			if child.Err() != nil {
				return
			}
			mu.Lock()
			status[i] = iterStarted
			prev := slotPrev
			mu.Unlock()
			idx := int32(i)
			// Stamp this index on every event in the iteration. A nested Map keeps the outer index.
			iterCtx := withChain(withParentIter(child, idx), c)
			itemCtx := withItem(iterCtx, i, elem)
			var res branchResult
			if state, in, entered, open := store.OpenStateEvent(chainEvents(history, c)); resuming && open {
				res = r.runProgramFrom(itemCtx, arn, st.Processor, state, in, &resumeMark{name: state, entered: entered})
			} else {
				if !r.emit(iterCtx, arn, store.HistoryEvent{Type: "MapIterationStarted", Name: st.Name, Index: idx, HasIndex: true, Previous: prev, HasPrevious: true}) {
					return
				}
				input, err := r.iterationInput(iterCtx, arn, st, root, i, elem, retries)
				if err != nil {
					res = failureFrom(err)
				} else {
					// Map.Item stays visible inside the iterator. A nested Map replaces it.
					res = r.runProgram(itemCtx, arn, st.Processor, input)
				}
			}
			switch res.outcome {
			case outcomeOK:
				inner, _ := r.Store.ChainLast(arn, c)
				id, ok := r.emitID(iterCtx, arn, store.HistoryEvent{Type: "MapIterationSucceeded", Name: st.Name, Index: idx, HasIndex: true})
				if !ok {
					return
				}
				r.Store.SetResult(arn, c, res.output)
				mu.Lock()
				results[i] = res.output
				status[i] = iterSucceeded
				slotPrev = inner
				if id > lastSucceeded {
					lastSucceeded = id
				}
				mu.Unlock()
			case outcomeFail:
				r.emit(iterCtx, arn, store.HistoryEvent{Type: "MapIterationFailed", Name: st.Name, Index: idx, HasIndex: true, Side: true})
				r.abortChain(iterCtx, arn, c)
				mu.Lock()
				status[i] = iterFailed
				mu.Unlock()
				note(c, res.errName, res.cause)
			}
		}(i, elem)
	}
	wg.Wait()
	if !r.Store.Running(arn) {
		return branchResult{outcome: outcomeHalt}
	}
	abortOpen := func() {
		for i, stt := range status {
			if stt == iterStarted {
				r.abortChain(ctx, arn, chains[i])
				r.emit(withParentIter(ctx, int32(i)), arn, store.HistoryEvent{Type: "MapIterationAborted", Name: st.Name, Index: int32(i), HasIndex: true, Chain: chains[i], Side: true})
			}
		}
	}
	if stopRequested(ctx) {
		abortOpen()
		if r.Store.Running(arn) {
			r.emit(ctx, arn, store.HistoryEvent{Type: "MapStateAborted"})
		}
		return branchResult{outcome: outcomeStop}
	}
	if failed {
		abortOpen()
		if name == "" {
			name = "States.Runtime"
		}
		after, _ := r.Store.ChainLast(arn, failChain)
		return branchResult{outcome: outcomeFail, errName: name, cause: cause, after: after}
	}
	for _, stt := range status {
		if stt != iterSucceeded {
			abortOpen()
			if !r.Store.Running(arn) {
				return branchResult{outcome: outcomeHalt}
			}
			if stopRequested(ctx) {
				if r.Store.Running(arn) {
					r.emit(ctx, arn, store.HistoryEvent{Type: "MapStateAborted"})
				}
				return branchResult{outcome: outcomeStop}
			}
			return branchResult{outcome: outcomeFail, errName: "States.Runtime", cause: "a Map iteration did not finish"}
		}
	}
	arr := make([]any, n)
	for i, text := range results {
		val, err := parseJSON(text)
		if err != nil {
			return branchResult{outcome: outcomeFail, errName: "States.Runtime", cause: "iteration result is not JSON"}
		}
		arr[i] = val
	}
	out, err := compactValue(arr)
	if err != nil {
		return branchResult{outcome: outcomeFail, errName: "States.Runtime", cause: err.Error()}
	}
	if lastSucceeded == 0 {
		lastSucceeded = started
	}
	return branchResult{outcome: outcomeOK, output: out, after: lastSucceeded}
}

func (r *Runner) iterationInput(ctx context.Context, arn string, st *asl.State, root any, index int, elem any, retries int) (string, error) {
	if st.JSONata {
		if !st.HasItemSelector {
			return compactValue(elem)
		}
		ex, ok := r.Store.Execution(arn)
		if !ok {
			return "", jsonpath.Runtime("execution disappeared")
		}
		ctxObj := r.jsonataContext(withItem(ctx, index, elem), ex, st, r.now(), retries)
		val, err := r.evalRaw(ctx, st.ItemSelector, root, r.bindings(root, ctxObj, entryVars(ctx)))
		if err != nil {
			return "", err
		}
		return compactValue(val)
	}
	if !st.HasParameters && !st.HasItemSelector {
		return compactValue(elem)
	}
	raw := st.ItemSelector
	if st.HasParameters {
		raw = st.Parameters
	}
	ex, ok := r.Store.Execution(arn)
	if !ok {
		return "", jsonpath.Runtime("execution disappeared")
	}
	ctxObj := r.contextObject(withItem(ctx, index, elem), ex, st, r.now(), retries)
	_, text, err := evalParameters(raw, root, ctxObj, entryVars(ctx))
	return text, err
}
