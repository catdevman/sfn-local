package exec

import (
	"context"
	"sync"

	"github.com/catdevman/sfn-local/internal/asl"
	"github.com/catdevman/sfn-local/internal/store"
)

func (r *Runner) runParallel(ctx context.Context, arn string, st *asl.State, input, effective string, ctxObj any) (string, string, bool, bool) {
	if len(st.Branches) == 0 {
		r.failCtx(ctx, arn, "States.Runtime", "Parallel has no branches")
		return "", "", false, true
	}
	// A redrive that resumes this state skips ParallelStateStarted and the
	// branches that finished. Each branch's first event follows
	// ParallelStateStarted, as on AWS.
	resume := resumedHere(ctx, st)
	var started int64
	if resume != nil {
		ex, _ := r.Store.Execution(arn)
		started = startedAfter(ex.History, chainOf(ctx), "ParallelStateStarted", resume.entered)
	} else {
		id, ok := r.emitID(ctx, arn, store.HistoryEvent{Type: "ParallelStateStarted"})
		if !ok {
			return "", "", false, true
		}
		started = id
	}
	retries := 0
	for {
		if r.halt(ctx, arn) {
			return "", "", false, true
		}
		if r.pastDeadline(arn) {
			r.finish(arn, store.ExecTimedOut, "ExecutionTimedOut", "", false, "States.Timeout", "", true, false)
			return "", "", false, true
		}
		setRetryCount(ctxObj, retries)
		resuming := resume != nil && retries == 0
		if !resuming {
			r.Store.ClearResults(arn, chainOf(ctx)+"/"+st.Name+"#")
		}
		res := r.parallelOnce(ctx, arn, st, effective, started, resuming)
		switch res.outcome {
		case outcomeOK:
			// ParallelStateSucceeded and ParallelStateExited both follow the
			// last branch event, as on AWS.
			if !r.emit(ctx, arn, store.HistoryEvent{Type: "ParallelStateSucceeded", Previous: res.after, HasPrevious: res.after > 0, Side: true}) {
				return "", "", false, true
			}
			if res.after > 0 {
				r.Store.SetChainLast(arn, chainOf(ctx), res.after)
			}
			return r.completeTask(ctx, arn, st, input, res.output, ctxObj)
		case outcomeHalt:
			return "", "", false, true
		case outcomeStop:
			// StopExecution has usually already completed the row. halt writes
			// ExecutionAborted only when this context is still the stop and the
			// row is RUNNING, and it skips a nested frame.
			if stopRequested(ctx) {
				r.halt(ctx, arn)
			}
			return "", "", false, true
		}
		next, output, terminal, done, again := r.retryOrFinish(ctx, arn, st, input, ctxObj, "ParallelStateFailed", res, &retries)
		if again {
			continue
		}
		return next, output, terminal, done
	}
}

// parallelOnce runs the branches. A resuming attempt reuses each finished
// branch's result and resumes each unfinished one at its open state.
func (r *Runner) parallelOnce(ctx context.Context, arn string, st *asl.State, input string, started int64, resuming bool) branchResult {
	branchCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	n := len(st.Branches)
	results := make([]string, n)
	okBranch := make([]bool, n)
	chains := make([]string, n)
	var history []store.HistoryEvent
	if resuming {
		ex, _ := r.Store.Execution(arn)
		history = ex.History
	}
	var mu sync.Mutex
	var failed bool
	var name, cause, failChain string
	var wg sync.WaitGroup
	for i, prog := range st.Branches {
		chains[i] = branchChain(ctx, st, i)
		c := chains[i]
		if resuming {
			if out, ok := r.Store.Result(arn, c); ok {
				results[i], okBranch[i] = out, true
				continue
			}
		}
		wg.Add(1)
		go func(i int, prog *asl.Program) {
			defer wg.Done()
			bctx := withChain(branchCtx, c)
			var res branchResult
			if state, in, entered, open := store.OpenStateEvent(chainEvents(history, c)); resuming && open {
				res = r.runProgramFrom(bctx, arn, prog, state, in, &resumeMark{name: state, entered: entered})
			} else {
				r.Store.SetChainLast(arn, c, started)
				res = r.runProgram(bctx, arn, prog, input)
			}
			mu.Lock()
			defer mu.Unlock()
			switch res.outcome {
			case outcomeOK:
				results[i] = res.output
				okBranch[i] = true
				r.Store.SetResult(arn, c, res.output)
			case outcomeFail:
				if !failed {
					failed = true
					name = res.errName
					cause = res.cause
					failChain = c
					cancel()
				}
			}
		}(i, prog)
	}
	wg.Wait()
	if !r.Store.Running(arn) {
		return branchResult{outcome: outcomeHalt}
	}
	if stopRequested(ctx) {
		return branchResult{outcome: outcomeStop}
	}
	abortOpen := func() {
		for i, ok := range okBranch {
			if !ok {
				r.abortChain(ctx, arn, chains[i])
			}
		}
	}
	if failed {
		abortOpen()
		after, _ := r.Store.ChainLast(arn, failChain)
		return branchResult{outcome: outcomeFail, errName: name, cause: cause, after: after}
	}
	for _, ok := range okBranch {
		if !ok {
			if stopRequested(ctx) || !r.Store.Running(arn) {
				return branchResult{outcome: outcomeStop}
			}
			abortOpen()
			return branchResult{outcome: outcomeFail, errName: "States.Runtime", cause: "a branch did not finish"}
		}
	}
	arr := make([]any, n)
	var after int64
	for i, text := range results {
		val, err := parseJSON(text)
		if err != nil {
			return branchResult{outcome: outcomeFail, errName: "States.Runtime", cause: "branch result is not JSON"}
		}
		arr[i] = val
		if id, ok := r.Store.ChainLast(arn, chains[i]); ok && id > after {
			after = id
		}
	}
	out, err := compactValue(arr)
	if err != nil {
		return branchResult{outcome: outcomeFail, errName: "States.Runtime", cause: err.Error()}
	}
	return branchResult{outcome: outcomeOK, output: out, after: after}
}

// afterFailure catches or fails a Parallel or Map attempt that will not retry.
// The caller has already appended the Failed event.
func (r *Runner) afterFailure(ctx context.Context, arn string, st *asl.State, input string, ctxObj any, res branchResult) (string, string, bool, bool) {
	if catcher, ok := pickCatch(st.Catch, res.errName); ok {
		return r.catchTask(ctx, arn, st, input, catcher, &taskFailure{Name: res.errName, Cause: res.cause}, ctxObj)
	}
	r.failCtx(ctx, arn, res.errName, res.cause)
	return "", "", false, true
}
