package exec

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/catdevman/sfn-local/internal/asl"
	"github.com/catdevman/sfn-local/internal/store"
)

// StateMock is the TestState mock for one execution. A set mock replaces the
// mock file for every attempt. Result is the task result, not a Lambda
// response envelope, so Payload is not peeled off.
type StateMock struct {
	Result    json.RawMessage
	HasResult bool
	Error     string
	Cause     string
	HasThrow  bool
}

// StateRun is one state to execute. RunState does not follow Next.
type StateRun struct {
	Definition string
	Input      string
	RoleArn    string
	Region     string
	Account    string
	State      *asl.State
	Deadline   time.Time
	Mock       *StateMock
}

// StateResult is the outcome of that one state.
// Status is SUCCEEDED, FAILED, or CAUGHT_ERROR. RETRIABLE is not produced:
// the task handler runs its retry loop, and the result is the outcome of
// that loop.
type StateResult struct {
	Status    string
	Output    string
	HasOutput bool
	Error     string
	Cause     string
	HasError  bool
	HasCause  bool
	Next      string
}

// RunState executes one state on a temporary execution and removes that
// execution before returning. The temporary row is not a stored state machine,
// so the worker's deleted-machine check does not apply.
func (r *Runner) RunState(ctx context.Context, spec StateRun) (StateResult, error) {
	if r == nil || r.Store == nil {
		return StateResult{}, fmt.Errorf("no runner")
	}
	if spec.State == nil {
		return StateResult{}, fmt.Errorf("no state")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	region, account := spec.Region, spec.Account
	if region == "" {
		region = "us-east-1"
	}
	if account == "" {
		account = "123456789012"
	}
	now := r.now()
	name := r.nextTestName(now)
	machineARN := fmt.Sprintf("arn:aws:states:%s:%s:stateMachine:TestState", region, account)
	execARN := fmt.Sprintf("arn:aws:states:%s:%s:execution:TestState:%s", region, account, name)
	deadline := spec.Deadline
	if deadline.IsZero() {
		deadline = now.Add(5 * time.Minute)
	}
	ex := store.Execution{
		ARN:             execARN,
		Name:            name,
		StateMachineARN: machineARN,
		Definition:      spec.Definition,
		RoleArn:         spec.RoleArn,
		Input:           spec.Input,
		Status:          store.ExecRunning,
		Type:            store.TypeStandard,
		Started:         now,
		Deadline:        deadline,
	}
	stored, handle, existed := r.Store.PutExecution(ex)
	if existed {
		return StateResult{}, fmt.Errorf("test execution already exists")
	}
	defer func() {
		r.Store.Cancel(stored.ARN)
		handle.Finish()
		r.clearAttempts(stored.ARN)
		r.clearStateMock(stored.ARN)
		r.Store.Drop(stored.ARN)
	}()
	if spec.Mock != nil {
		r.setStateMock(stored.ARN, spec.Mock)
	}
	if r.pastDeadline(stored.ARN) {
		r.finish(stored.ARN, store.ExecTimedOut, "ExecutionTimedOut", "", false, "States.Timeout", "", true, false)
		return r.testResult(stored.ARN, "", "", true, true), nil
	}
	next, output, terminal, done := r.step(ctx, stored.ARN, spec.State, spec.Input)
	return r.testResult(stored.ARN, next, output, terminal, done), nil
}

func (r *Runner) testResult(arn, next, output string, terminal, done bool) StateResult {
	ex, ok := r.Store.Execution(arn)
	if !ok {
		return StateResult{Status: "FAILED", Error: "States.Runtime", HasError: true, Cause: "execution disappeared", HasCause: true}
	}
	if ex.Status != store.ExecRunning {
		res := StateResult{Status: "FAILED"}
		if ex.HasError {
			res.Error = ex.Error
			res.HasError = true
		}
		if ex.HasCause {
			res.Cause = ex.Cause
			res.HasCause = true
		}
		return res
	}
	if done {
		return StateResult{Status: "FAILED", Error: "States.Runtime", HasError: true, Cause: "the state did not finish", HasCause: true}
	}
	res := StateResult{Status: "SUCCEEDED", Output: output, HasOutput: true}
	if name, cause, caught := lastCaught(ex); caught {
		res.Status = "CAUGHT_ERROR"
		if name != "" {
			res.Error = name
			res.HasError = true
		}
		if cause != "" {
			res.Cause = cause
			res.HasCause = true
		}
	}
	if next != "" && !terminal {
		res.Next = next
	}
	return res
}

// lastCaught reports the latest task failure when no later attempt succeeded.
// A retry that then succeeds is not a caught error.
func lastCaught(ex store.Execution) (name, cause string, caught bool) {
	for _, ev := range ex.History {
		switch ev.Type {
		case "TaskFailed", "TaskTimedOut", "LambdaFunctionFailed", "LambdaFunctionTimedOut":
			caught = true
			name, cause = ev.Error, ev.Cause
		case "TaskSucceeded", "LambdaFunctionSucceeded":
			caught = false
			name, cause = "", ""
		}
	}
	return name, cause, caught
}

func (r *Runner) nextTestName(now time.Time) string {
	r.mu.Lock()
	r.testSeq++
	n := r.testSeq
	r.mu.Unlock()
	return fmt.Sprintf("teststate-%d-%d", now.UnixNano(), n)
}

func (r *Runner) setStateMock(arn string, m *StateMock) {
	if r == nil || m == nil {
		return
	}
	cp := *m
	if m.Result != nil {
		cp.Result = append(json.RawMessage(nil), m.Result...)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stateMocks == nil {
		r.stateMocks = map[string]*StateMock{}
	}
	r.stateMocks[arn] = &cp
}

func (r *Runner) clearStateMock(arn string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.stateMocks, arn)
}

func (r *Runner) stateMock(arn string) (StateMock, bool) {
	if r == nil {
		return StateMock{}, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	m, ok := r.stateMocks[arn]
	if !ok || m == nil {
		return StateMock{}, false
	}
	return *m, true
}
