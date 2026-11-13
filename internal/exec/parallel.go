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
	if !r.emit(arn, store.HistoryEvent{Type: "ParallelStateStarted"}) {
		return "", "", false, true
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
		res := r.parallelOnce(ctx, arn, st, effective)
		switch res.outcome {
		case outcomeOK:
			if !r.emit(arn, store.HistoryEvent{Type: "ParallelStateSucceeded"}) {
				return "", "", false, true
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
		if rt, ok := pickRetry(st.Retry, res.errName); ok && int64(retries+1) <= rt.MaxAttempts {
			if !r.retrySleep(ctx, arn, rt, retries+1) {
				return "", "", false, true
			}
			retries++
			continue
		}
		if !r.emit(arn, store.HistoryEvent{Type: "ParallelStateFailed"}) {
			return "", "", false, true
		}
		return r.afterFailure(ctx, arn, st, input, ctxObj, res)
	}
}

func (r *Runner) parallelOnce(ctx context.Context, arn string, st *asl.State, input string) branchResult {
	branchCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	n := len(st.Branches)
	results := make([]string, n)
	okBranch := make([]bool, n)
	var mu sync.Mutex
	var failed bool
	var name, cause string
	var wg sync.WaitGroup
	for i, prog := range st.Branches {
		wg.Add(1)
		go func(i int, prog *asl.Program) {
			defer wg.Done()
			res := r.runProgram(branchCtx, arn, prog, input)
			mu.Lock()
			defer mu.Unlock()
			switch res.outcome {
			case outcomeOK:
				results[i] = res.output
				okBranch[i] = true
			case outcomeFail:
				if !failed {
					failed = true
					name = res.errName
					cause = res.cause
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
	if failed {
		return branchResult{outcome: outcomeFail, errName: name, cause: cause}
	}
	for _, ok := range okBranch {
		if !ok {
			if stopRequested(ctx) || !r.Store.Running(arn) {
				return branchResult{outcome: outcomeStop}
			}
			return branchResult{outcome: outcomeFail, errName: "States.Runtime", cause: "a branch did not finish"}
		}
	}
	arr := make([]any, n)
	for i, text := range results {
		val, err := parseJSON(text)
		if err != nil {
			return branchResult{outcome: outcomeFail, errName: "States.Runtime", cause: "branch result is not JSON"}
		}
		arr[i] = val
	}
	out, err := compactValue(arr)
	if err != nil {
		return branchResult{outcome: outcomeFail, errName: "States.Runtime", cause: err.Error()}
	}
	return branchResult{outcome: outcomeOK, output: out}
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
