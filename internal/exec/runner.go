// Package exec runs one execution at a time on a worker. The HTTP accept
// loop never calls Run on its own goroutine's request path except to wait
// for StartSyncExecution. TestState calls RunState on the request path and
// stops after that one state.
package exec

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/catdevman/sfn-local/internal/asl"
	"github.com/catdevman/sfn-local/internal/mock"
	"github.com/catdevman/sfn-local/internal/sdk"
	"github.com/catdevman/sfn-local/internal/store"
)

const maxPayload = 262144

// Runner executes Pass, Succeed, Fail, Choice, Wait, Task, Parallel, and inline Map.
type Runner struct {
	Store *store.Store
	// Now overrides the clock. Nil means time.Now.
	Now func() time.Time
	// Boundary runs before each state, outside the store lock.
	Boundary func(executionARN string)
	Log      io.Writer
	// WaitScale is the absolute WAIT_TIME_SCALE. Nil means Wait is not scaled.
	// Retry sleeps do not read this field.
	WaitScale *float32
	// Sleep replaces the interruptible Wait and Retry delay. Nil sleeps on the wall clock.
	Sleep func(ctx context.Context, d time.Duration) error
	// Mocks is the SFN_MOCK_CONFIG file. Nil reports the file as missing.
	Mocks *mock.Repo
	// LambdaEndpoint is LAMBDA_ENDPOINT. Empty means a direct function ARN
	// or lambda:invoke that is not mocked fails as an unsupported resource.
	LambdaEndpoint string
	// StepFunctionsEndpoint gates nested states:startExecution. Empty means
	// the resource is unsupported. .sync is states:startExecution only.
	StepFunctionsEndpoint string
	Region                string
	Account               string
	// SDK runs arn:aws:states:::aws-sdk: resources and the DynamoDB, SQS,
	// SNS, and EventBridge integrations. Nil means they are unsupported.
	SDK *sdk.Caller
	// Invoke replaces the Lambda HTTP call. Nil uses LambdaEndpoint.
	Invoke func(ctx context.Context, call LambdaCall) LambdaReply
	// Rand returns a FULL-jitter delay in 0..max inclusive. Nil uses math/rand.
	Rand func(max int64) int64

	mu         sync.Mutex
	attempts   map[string]map[string]int
	stateMocks map[string]*StateMock
	scopes     map[string]*scope
	testSeq    uint64
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
// A redrive starts at the outermost state that was entered and not exited.
// A Task, Parallel, or Map there resumes without a new entered event, as on
// AWS, and a Parallel or Map resumes inside its unfinished branches. Other
// states are entered again.
// Mock attempt counters and the variable root stay until the run succeeds,
// so a later redrive continues both.
func (r *Runner) Run(ctx context.Context, arn string) {
	ctx = withStop(ctx)
	defer func() {
		ex, ok := r.Store.Execution(arn)
		if !ok || ex.Status == store.ExecSucceeded {
			r.dropScope(arn)
			r.clearAttempts(arn)
		}
	}()
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
	var resume *resumeMark
	if ex.RedriveCount > 0 {
		state, resumed, entered, open := store.OpenStateEvent(chainEvents(ex.History, ""))
		if !open {
			r.fail(arn, "States.Runtime", "the execution has no state to redrive")
			return
		}
		name = state
		input = resumed
		resume = &resumeMark{name: state, entered: entered}
	}
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
		if !r.alive(ex.Machine()) {
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
		case "Pass", "Succeed", "Fail", "Choice", "Wait", "Task", "Parallel", "Map":
		default:
			r.fail(arn, "States.Runtime", "State type "+st.Type+" is not implemented by this local server.")
			return
		}
		stepCtx := ctx
		if resume != nil {
			stepCtx = context.WithValue(ctx, resumeKey{}, resume)
			resume = nil
		}
		next, output, terminal, done := r.step(stepCtx, arn, st, input)
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

func (r *Runner) entered(ctx context.Context, arn string, st *asl.State, input string) error {
	ev := store.HistoryEvent{
		Type: st.Type + "StateEntered", Time: r.now(), Name: st.Name,
		Input: input, HasInput: true,
	}
	stampParent(ctx, &ev)
	return r.append(arn, ev, store.ExecRunning, "")
}

func (r *Runner) exited(ctx context.Context, arn string, st *asl.State, output string) error {
	ev := store.HistoryEvent{
		Type: st.Type + "StateExited", Time: r.now(), Name: st.Name,
		Output: output, HasOutput: true,
	}
	stampParent(ctx, &ev)
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

// failCtx records a nested state failure on the branch frame. At the top
// level it fails the execution.
func (r *Runner) failCtx(ctx context.Context, arn, errName, cause string) {
	if f := nestOf(ctx); f != nil {
		f.record(errName, cause)
		return
	}
	r.fail(arn, errName, cause)
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
// stop context aborts a top-level run that StopExecution has not already
// finished. A nested frame, including a sibling cancel, returns without
// writing ExecutionAborted so the Parallel or Map coordinator can finish.
func (r *Runner) halt(ctx context.Context, arn string) bool {
	if !r.Store.Running(arn) {
		return true
	}
	if ctx == nil {
		return false
	}
	stopped := stopRequested(ctx)
	local := ctx.Err() != nil
	if !stopped && !local {
		return false
	}
	if nestOf(ctx) != nil || !stopped {
		return true
	}
	r.finish(arn, store.ExecAborted, "ExecutionAborted", "", false, "", "", false, false)
	return true
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
	v, err := parseJSON(string(raw))
	if err != nil {
		return "", err
	}
	return compactValue(v)
}

func parseJSON(s string) (any, error) {
	dec := json.NewDecoder(strings.NewReader(s))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	return normalize(v), nil
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
