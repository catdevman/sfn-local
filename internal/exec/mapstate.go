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
	retries := 0
	started := false
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
			if !r.emit(arn, store.HistoryEvent{Type: "MapStateStarted", Length: int32(len(items)), HasLength: true}) {
				return "", "", false, true
			}
			started = true
		}
		res := r.mapOnce(ctx, arn, st, effVal, items, retries, maxc)
		switch res.outcome {
		case outcomeOK:
			if !r.emit(arn, store.HistoryEvent{Type: "MapStateSucceeded"}) {
				return "", "", false, true
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
	if failedEvent != "" && !r.emit(arn, store.HistoryEvent{Type: failedEvent}) {
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

func (r *Runner) mapOnce(ctx context.Context, arn string, st *asl.State, root any, items []any, retries int, maxc *int64) branchResult {
	n := len(items)
	if n == 0 {
		return branchResult{outcome: outcomeOK, output: "[]"}
	}
	child, cancel := context.WithCancel(ctx)
	defer cancel()
	var sem chan struct{}
	if maxc != nil && *maxc > 0 {
		sem = make(chan struct{}, int(*maxc))
	}
	results := make([]string, n)
	status := make([]int, n)
	var mu sync.Mutex
	var failed bool
	var name, cause string
	note := func(errName, errCause string) {
		mu.Lock()
		if !failed {
			failed = true
			name = errName
			cause = errCause
			cancel()
		}
		mu.Unlock()
	}
	var wg sync.WaitGroup
	for i, elem := range items {
		if child.Err() != nil || stopRequested(ctx) {
			break
		}
		wg.Add(1)
		go func(i int, elem any) {
			defer wg.Done()
			if sem != nil {
				select {
				case sem <- struct{}{}:
					defer func() { <-sem }()
				case <-child.Done():
					return
				}
			}
			if child.Err() != nil {
				return
			}
			status[i] = iterStarted
			idx := int32(i)
			if !r.emit(arn, store.HistoryEvent{Type: "MapIterationStarted", Name: st.Name, Index: idx, HasIndex: true}) {
				return
			}
			input, err := r.iterationInput(child, arn, st, root, i, elem, retries)
			if err != nil {
				fr := failureFrom(err)
				r.emit(arn, store.HistoryEvent{Type: "MapIterationFailed", Name: st.Name, Index: idx, HasIndex: true})
				status[i] = iterFailed
				note(fr.errName, fr.cause)
				return
			}
			// Map.Item stays visible inside the iterator. A nested Map replaces it.
			res := r.runProgram(withItem(child, i, elem), arn, st.Processor, input)
			switch res.outcome {
			case outcomeOK:
				if !r.emit(arn, store.HistoryEvent{Type: "MapIterationSucceeded", Name: st.Name, Index: idx, HasIndex: true}) {
					return
				}
				results[i] = res.output
				status[i] = iterSucceeded
			case outcomeFail:
				r.emit(arn, store.HistoryEvent{Type: "MapIterationFailed", Name: st.Name, Index: idx, HasIndex: true})
				status[i] = iterFailed
				note(res.errName, res.cause)
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
				r.emit(arn, store.HistoryEvent{Type: "MapIterationAborted", Name: st.Name, Index: int32(i), HasIndex: true})
			}
		}
	}
	if stopRequested(ctx) {
		abortOpen()
		if r.Store.Running(arn) {
			r.emit(arn, store.HistoryEvent{Type: "MapStateAborted"})
		}
		return branchResult{outcome: outcomeStop}
	}
	if failed {
		abortOpen()
		if name == "" {
			name = "States.Runtime"
		}
		return branchResult{outcome: outcomeFail, errName: name, cause: cause}
	}
	for _, stt := range status {
		if stt != iterSucceeded {
			abortOpen()
			if !r.Store.Running(arn) {
				return branchResult{outcome: outcomeHalt}
			}
			if stopRequested(ctx) {
				if r.Store.Running(arn) {
					r.emit(arn, store.HistoryEvent{Type: "MapStateAborted"})
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
	return branchResult{outcome: outcomeOK, output: out}
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
