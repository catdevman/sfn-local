// Package exec runs one execution at a time on a worker. The HTTP accept
// loop never calls Run on its own goroutine's request path except to wait
// for StartSyncExecution.
package exec

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"
	"unicode/utf8"

	"github.com/catdevman/sfn-local/internal/asl"
	"github.com/catdevman/sfn-local/internal/store"
)

const maxPayload = 262144

// Runner executes Pass, Succeed, and Fail. Other state types fail the run.
type Runner struct {
	Store *store.Store
	// Now overrides the clock. Nil means time.Now.
	Now func() time.Time
	// Boundary runs before each state, outside the store lock.
	Boundary func(executionARN string)
	Log      io.Writer
}

// Start runs the execution on a new goroutine. The handle is finished when
// Run returns, including after a panic.
func (r *Runner) Start(handle *store.RunHandle, arn string) {
	if handle == nil {
		return
	}
	go func() {
		defer handle.Finish()
		r.Run(handle.Ctx, arn)
	}()
}

// Run drives one execution until it reaches a terminal status.
func (r *Runner) Run(ctx context.Context, arn string) {
	if ctx == nil {
		ctx = context.Background()
	}
	ex, ok := r.Store.Execution(arn)
	if !ok {
		return
	}
	prog, err := asl.Validate(ex.Definition)
	if err != nil {
		r.fail(arn, "States.Runtime", err.Error())
		return
	}
	input := ex.Input
	name := prog.StartAt
	for {
		if r.halt(ctx, arn) {
			return
		}
		if r.Boundary != nil {
			r.Boundary(arn)
		}
		if r.halt(ctx, arn) {
			return
		}
		if !r.alive(ex.StateMachineARN) {
			r.fail(arn, "States.Runtime", "The state machine has been deleted.")
			return
		}
		if r.pastDeadline(arn) {
			r.finish(arn, store.ExecTimedOut, "ExecutionTimedOut", "", false, "States.Timeout", "", true, false)
			return
		}
		st, ok := prog.States[name]
		if !ok {
			r.fail(arn, "States.Runtime", "Missing 'Next' target: "+name)
			return
		}
		switch st.Type {
		case "Pass", "Succeed", "Fail":
		default:
			r.fail(arn, "States.Runtime", "State type "+st.Type+" is not implemented by this local server.")
			return
		}
		next, output, terminal, done := r.step(arn, st, input)
		if done {
			return
		}
		if terminal {
			r.succeed(arn, output)
			return
		}
		input = output
		name = next
	}
}

func (r *Runner) step(arn string, st *asl.State, input string) (next, output string, terminal, done bool) {
	if err := r.entered(arn, st, input); err != nil {
		r.onAppendErr(arn, err)
		return "", "", false, true
	}
	if st.HasAssign {
		r.fail(arn, "States.Runtime", "Assign is not implemented by this local server.")
		return "", "", false, true
	}
	if st.HasParameters {
		r.fail(arn, "States.Runtime", "Parameters is not implemented by this local server.")
		return "", "", false, true
	}
	if st.HasResultSelector {
		r.fail(arn, "States.Runtime", "ResultSelector is not implemented by this local server.")
		return "", "", false, true
	}
	effective, err := applyInput(st, input)
	if err != nil {
		r.fail(arn, "States.Runtime", err.Error())
		return "", "", false, true
	}
	switch st.Type {
	case "Fail":
		r.failState(arn, st, effective)
		return "", "", false, true
	case "Pass":
		result := effective
		if st.HasResult {
			result, err = compactRaw(st.Result)
			if err != nil {
				r.fail(arn, "States.Runtime", err.Error())
				return "", "", false, true
			}
		}
		output, err = applyResult(st, input, result)
	case "Succeed":
		output, err = applyOutput(st, effective)
	}
	if err != nil {
		r.fail(arn, "States.Runtime", err.Error())
		return "", "", false, true
	}
	if st.Type != "Succeed" {
		output, err = applyOutput(st, output)
		if err != nil {
			r.fail(arn, "States.Runtime", err.Error())
			return "", "", false, true
		}
	}
	if len(output) > maxPayload {
		r.fail(arn, "States.DataLimitExceeded", "The state output exceeded the maximum size of 262144 bytes.")
		return "", "", false, true
	}
	if err := r.exited(arn, st, output); err != nil {
		r.onAppendErr(arn, err)
		return "", "", false, true
	}
	if st.End || st.Next == "" {
		return "", output, true, false
	}
	return st.Next, output, false, false
}

func (r *Runner) failState(arn string, st *asl.State, effective string) {
	if st.HasErrorPath || st.HasCausePath {
		r.fail(arn, "States.Runtime", "ErrorPath and CausePath are not implemented by this local server.")
		return
	}
	_ = effective
	errName := ""
	cause := ""
	hasErr := st.HasError
	hasCause := st.HasCause
	if hasErr {
		errName = clip(st.Error, 256)
	}
	if hasCause {
		cause = clip(st.Cause, 32768)
	}
	r.finish(arn, store.ExecFailed, "ExecutionFailed", "", false, errName, cause, hasErr, hasCause)
}

func applyInput(st *asl.State, input string) (string, error) {
	switch st.InputPath.Kind {
	case asl.PathDefault, asl.PathIdentity:
		return input, nil
	case asl.PathNull:
		return "{}", nil
	default:
		return "", fmt.Errorf("InputPath is not implemented by this local server")
	}
}

func applyResult(st *asl.State, original, result string) (string, error) {
	switch st.ResultPath.Kind {
	case asl.PathDefault, asl.PathIdentity:
		return result, nil
	case asl.PathNull:
		return original, nil
	default:
		return "", fmt.Errorf("ResultPath is not implemented by this local server")
	}
}

func applyOutput(st *asl.State, value string) (string, error) {
	switch st.OutputPath.Kind {
	case asl.PathDefault, asl.PathIdentity:
		return value, nil
	case asl.PathNull:
		return "{}", nil
	default:
		return "", fmt.Errorf("OutputPath is not implemented by this local server")
	}
}

func (r *Runner) entered(arn string, st *asl.State, input string) error {
	ev := store.HistoryEvent{
		Type: st.Type + "StateEntered", Time: r.now(), Name: st.Name,
		Input: input, HasInput: true,
	}
	return r.append(arn, ev, store.ExecRunning, "")
}

func (r *Runner) exited(arn string, st *asl.State, output string) error {
	ev := store.HistoryEvent{
		Type: st.Type + "StateExited", Time: r.now(), Name: st.Name,
		Output: output, HasOutput: true,
	}
	return r.append(arn, ev, store.ExecRunning, st.Next)
}

func (r *Runner) succeed(arn, output string) {
	ev := store.HistoryEvent{Type: "ExecutionSucceeded", Time: r.now(), Output: output, HasOutput: true}
	when := r.now()
	ex, err := r.Store.Complete(arn, store.ExecSucceeded, when, ev, output, true, "", "", false, false)
	if err != nil {
		return
	}
	r.log(arn, "", "ExecutionSucceeded", ex.Status, "")
}

func (r *Runner) fail(arn, errName, cause string) {
	r.finish(arn, store.ExecFailed, "ExecutionFailed", "", false, errName, cause, true, cause != "")
}

func (r *Runner) finish(arn, status, event, output string, hasOutput bool, errName, cause string, hasError, hasCause bool) {
	ev := store.HistoryEvent{Type: event, Time: r.now(), Output: output, HasOutput: hasOutput, Error: errName, HasError: hasError, Cause: cause, HasCause: hasCause}
	ex, err := r.Store.Complete(arn, status, r.now(), ev, output, hasOutput, errName, cause, hasError, hasCause)
	if err != nil {
		return
	}
	r.log(arn, "", event, ex.Status, "")
}

func (r *Runner) append(arn string, ev store.HistoryEvent, status, next string) error {
	if err := r.Store.Append(arn, ev); err != nil {
		return err
	}
	r.log(arn, ev.Name, ev.Type, status, next)
	return nil
}

func (r *Runner) onAppendErr(arn string, err error) {
	if err == store.ErrHistoryLimit {
		r.fail(arn, "States.Runtime", "The execution reached the maximum number of history events (25000).")
	}
}

// halt stops the worker when the execution is no longer RUNNING. A cancelled
// worker context aborts a run that StopExecution has not already finished.
func (r *Runner) halt(ctx context.Context, arn string) bool {
	if !r.Store.Running(arn) {
		return true
	}
	if ctx.Err() != nil {
		r.finish(arn, store.ExecAborted, "ExecutionAborted", "", false, "", "", false, false)
		return true
	}
	return false
}

func (r *Runner) alive(machineARN string) bool {
	m, ok := r.Store.Machine(machineARN)
	return ok && m.Status != store.StatusDeleting
}

func (r *Runner) pastDeadline(arn string) bool {
	ex, ok := r.Store.Execution(arn)
	if !ok {
		return false
	}
	return !r.now().Before(ex.Deadline)
}

func (r *Runner) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

func (r *Runner) log(arn, state, event, status, next string) {
	if r.Log == nil {
		return
	}
	fmt.Fprintf(r.Log, "executionArn=%s state=%s event=%s status=%s next=%s\n", arn, state, event, status, next)
}

func compactRaw(raw json.RawMessage) (string, error) {
	var v any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&v); err != nil {
		return "", err
	}
	return compactValue(v)
}

func compactValue(v any) (string, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(normalize(v)); err != nil {
		return "", err
	}
	out := bytes.TrimSpace(buf.Bytes())
	return string(out), nil
}

func normalize(v any) any {
	switch x := v.(type) {
	case json.Number:
		if i, err := x.Int64(); err == nil {
			return i
		}
		f, err := x.Float64()
		if err != nil {
			return x.String()
		}
		return f
	case map[string]any:
		for k, child := range x {
			x[k] = normalize(child)
		}
		return x
	case []any:
		for i, child := range x {
			x[i] = normalize(child)
		}
		return x
	default:
		return v
	}
}

func clip(s string, max int) string {
	if utf8.RuneCountInString(s) <= max {
		return s
	}
	r := []rune(s)
	if max <= 3 {
		return string(r[:max])
	}
	return string(r[:max-3]) + "..."
}

// Sweep drops terminal executions of DELETING machines once a second.
func Sweep(ctx context.Context, s *store.Store) {
	if ctx == nil {
		ctx = context.Background()
	}
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			s.Sweep()
		}
	}
}
