package exec

import (
	"context"
	"errors"

	"github.com/catdevman/sfn-local/internal/asl"
	"github.com/catdevman/sfn-local/internal/jsonpath"
	"github.com/catdevman/sfn-local/internal/store"
)

// runProgram runs a nested state machine on the same execution. It does not
// succeed or fail the execution. A terminal state returns its output.
func (r *Runner) runProgram(ctx context.Context, arn string, prog *asl.Program, input string) branchResult {
	ctx, frame := withNest(ctx)
	if prog == nil {
		r.failCtx(ctx, arn, "States.Runtime", "nested state machine is missing")
		return frameResult(frame)
	}
	name := prog.StartAt
	for {
		if !r.Store.Running(arn) {
			return branchResult{outcome: outcomeHalt}
		}
		if stopRequested(ctx) || ctx.Err() != nil {
			if failed, _, _ := frame.snapshot(); failed {
				return frameResult(frame)
			}
			return branchResult{outcome: outcomeStop}
		}
		ex, ok := r.Store.Execution(arn)
		if !ok {
			return branchResult{outcome: outcomeHalt}
		}
		if m, exists := r.Store.Machine(ex.StateMachineARN); exists && m.Status == store.StatusDeleting {
			r.fail(arn, "States.Runtime", "The state machine has been deleted.")
			return branchResult{outcome: outcomeHalt}
		}
		if r.pastDeadline(arn) {
			r.finish(arn, store.ExecTimedOut, "ExecutionTimedOut", "", false, "States.Timeout", "", true, false)
			return branchResult{outcome: outcomeHalt}
		}
		st, ok := prog.States[name]
		if !ok {
			r.failCtx(ctx, arn, "States.Runtime", "Missing 'Next' target: "+name)
			return frameResult(frame)
		}
		next, output, terminal, done := r.step(ctx, arn, st, input)
		if done {
			if !r.Store.Running(arn) {
				return branchResult{outcome: outcomeHalt}
			}
			if failed, _, _ := frame.snapshot(); failed {
				return frameResult(frame)
			}
			if ctx.Err() != nil || stopRequested(ctx) {
				return branchResult{outcome: outcomeStop}
			}
			return branchResult{outcome: outcomeFail, errName: "States.Runtime", cause: "the state did not finish"}
		}
		if terminal {
			return branchResult{outcome: outcomeOK, output: output}
		}
		input = output
		name = next
	}
}

func frameResult(frame *nestFrame) branchResult {
	_, name, cause := frame.snapshot()
	if name == "" {
		name = "States.Runtime"
	}
	return branchResult{outcome: outcomeFail, errName: name, cause: cause}
}

func failureFrom(err error) branchResult {
	name := "States.Runtime"
	cause := ""
	if err != nil {
		cause = err.Error()
	}
	var ev *jsonpath.EvalError
	if errors.As(err, &ev) && ev != nil {
		name = ev.Name
		cause = ev.Cause
	}
	return branchResult{outcome: outcomeFail, errName: name, cause: cause}
}
