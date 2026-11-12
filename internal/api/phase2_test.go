package api

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/catdevman/sfn-local/internal/api/gen"
	"github.com/catdevman/sfn-local/internal/store"
)

func TestPhase2Fixtures(t *testing.T) {
	s := newService()
	choice := `{
	  "StartAt": "C",
	  "States": {
	    "C": {"Type": "Choice", "Choices": [{"Variable": "$.n", "NumericGreaterThan": 1, "Next": "Yes"}], "Default": "No"},
	    "Yes": {"Type": "Succeed"},
	    "No": {"Type": "Fail", "Error": "TooSmall", "Cause": "n<=1"}
	  }
	}`
	ex := runMachine(t, s, "choice", choice, "run", `{"n":2}`)
	if ex.Status != store.ExecSucceeded || ex.Output != `{"n":2}` {
		t.Fatalf("choice %s %s %s", ex.Status, ex.Output, ex.Cause)
	}
	assertTypes(t, ex, "ExecutionStarted", "ChoiceStateEntered", "ChoiceStateExited", "SucceedStateEntered", "SucceedStateExited", "ExecutionSucceeded")

	mismatch := `{"StartAt":"C","States":{"C":{"Type":"Choice","Choices":[{"Variable":"$.n","StringEquals":"1","Next":"Yes"}],"Default":"No"},"Yes":{"Type":"Succeed"},"No":{"Type":"Succeed"}}}`
	ex = runMachine(t, s, "mismatch", mismatch, "run", `{"n":1}`)
	if ex.Status != store.ExecSucceeded || ex.Output != `{"n":1}` {
		t.Fatalf("mismatch %s %s %s", ex.Status, ex.Output, ex.Cause)
	}

	isFalse := `{"StartAt":"C","States":{"C":{"Type":"Choice","Choices":[{"Variable":"$.s","IsTimestamp":false,"Next":"Yes"}],"Default":"No"},"Yes":{"Type":"Succeed"},"No":{"Type":"Fail","Error":"No","Cause":"no"}}}`
	ex = runMachine(t, s, "is-false", isFalse, "run", `{"s":"nope"}`)
	if ex.Status != store.ExecSucceeded || ex.Output != `{"s":"nope"}` {
		t.Fatalf("is false %s %s %s", ex.Status, ex.Output, ex.Cause)
	}
	isTrue := `{"StartAt":"C","States":{"C":{"Type":"Choice","Choices":[{"Variable":"$.s","IsTimestamp":true,"Next":"Yes"}],"Default":"No"},"Yes":{"Type":"Fail","Error":"Yes","Cause":"yes"},"No":{"Type":"Succeed"}}}`
	ex = runMachine(t, s, "is-true", isTrue, "run", `{"s":"nope"}`)
	if ex.Status != store.ExecSucceeded || ex.Output != `{"s":"nope"}` {
		t.Fatalf("is true %s %s %s", ex.Status, ex.Output, ex.Cause)
	}

	short := `{"StartAt":"C","States":{"C":{"Type":"Choice","Choices":[{"And":[{"Variable":"$.n","NumericEquals":0},{"Variable":"$.missing","StringEquals":"x"}],"Next":"Yes"}],"Default":"No"},"Yes":{"Type":"Fail","Error":"Yes","Cause":"yes"},"No":{"Type":"Succeed"}}}`
	ex = runMachine(t, s, "and", short, "run", `{"n":1}`)
	if ex.Status != store.ExecSucceeded || ex.Output != `{"n":1}` {
		t.Fatalf("and %s %s %s", ex.Status, ex.Output, ex.Cause)
	}
	reads := `{"StartAt":"C","States":{"C":{"Type":"Choice","Choices":[{"And":[{"Variable":"$.n","NumericEquals":1},{"Variable":"$.missing","StringEquals":"x"}],"Next":"Yes"}],"Default":"No"},"Yes":{"Type":"Succeed"},"No":{"Type":"Succeed"}}}`
	ex = runMachine(t, s, "and-read", reads, "run", `{"n":1}`)
	if ex.Status != store.ExecFailed || ex.Error != "States.Runtime" || !strings.Contains(ex.Cause, "not found") {
		t.Fatalf("and read %s %s", ex.Error, ex.Cause)
	}
	assertTypes(t, ex, "ExecutionStarted", "ChoiceStateEntered", "ExecutionFailed")

	or := `{"StartAt":"C","States":{"C":{"Type":"Choice","Choices":[{"Or":[{"Variable":"$.n","NumericEquals":1},{"Variable":"$.missing","StringEquals":"x"}],"Next":"Yes"}],"Default":"No"},"Yes":{"Type":"Succeed"},"No":{"Type":"Succeed"}}}`
	ex = runMachine(t, s, "or", or, "run", `{"n":1}`)
	if ex.Status != store.ExecSucceeded {
		t.Fatalf("or %s %s", ex.Status, ex.Cause)
	}
	not := `{"StartAt":"C","States":{"C":{"Type":"Choice","Choices":[{"Not":{"Variable":"$.n","NumericEquals":0},"Next":"Yes"}],"Default":"No"},"Yes":{"Type":"Succeed"},"No":{"Type":"Fail","Error":"No","Cause":"no"}}}`
	ex = runMachine(t, s, "not", not, "run", `{"n":1}`)
	if ex.Status != store.ExecSucceeded {
		t.Fatalf("not %s %s", ex.Status, ex.Cause)
	}

	params := `{"StartAt":"P","States":{"P":{"Type":"Pass","Parameters":{"name.$":"$.user.name","static":1},"End":true}}}`
	ex = runMachine(t, s, "params", params, "run", `{"user":{"name":"ada"}}`)
	if ex.Output != `{"name":"ada","static":1}` {
		t.Fatalf("params %s %s", ex.Output, ex.Cause)
	}
	bad := `{"StartAt":"P","States":{"P":{"Type":"Pass","Parameters":{"a.$":"$.missing"},"Result":{"ok":true},"End":true}}}`
	ex = runMachine(t, s, "bad-params", bad, "run", `{}`)
	if ex.Status != store.ExecFailed || ex.Error != "States.Runtime" {
		t.Fatalf("bad params %s %s", ex.Error, ex.Cause)
	}
	assertTypes(t, ex, "ExecutionStarted", "PassStateEntered", "ExecutionFailed")

	variable := `{"StartAt":"P","States":{"P":{"Type":"Pass","InputPath":"$saved","End":true}}}`
	ex = runMachine(t, s, "var", variable, "run", `{}`)
	if ex.Error != "States.Runtime" || !strings.Contains(ex.Cause, "not enabled") {
		t.Fatalf("variable %s %s", ex.Error, ex.Cause)
	}

	inNull := `{"StartAt":"P","States":{"P":{"Type":"Pass","InputPath":null,"End":true}}}`
	ex = runMachine(t, s, "in-null", inNull, "run", `{"n":1}`)
	if ex.Output != `{}` {
		t.Fatal(ex.Output)
	}
	resultNull := `{"StartAt":"P","States":{"P":{"Type":"Pass","Result":{"ok":true},"ResultPath":null,"End":true}}}`
	ex = runMachine(t, s, "result-null", resultNull, "run", `{"n":1}`)
	if ex.Output != `{"n":1}` {
		t.Fatal(ex.Output)
	}
	outNull := `{"StartAt":"P","States":{"P":{"Type":"Pass","OutputPath":null,"End":true}}}`
	ex = runMachine(t, s, "out-null", outNull, "run", `{"n":1}`)
	if ex.Output != `{}` {
		t.Fatal(ex.Output)
	}

	nested := `{"StartAt":"P","States":{"P":{"Type":"Pass","Result":{"ok":true},"ResultPath":"$.a.b","End":true}}}`
	ex = runMachine(t, s, "nested", nested, "run", `{"a":{}}`)
	if ex.Output != `{"a":{"b":{"ok":true}}}` {
		t.Fatalf("nested %s %s", ex.Output, ex.Cause)
	}
	clash := `{"StartAt":"P","States":{"P":{"Type":"Pass","Result":1,"ResultPath":"$.a.b","End":true}}}`
	ex = runMachine(t, s, "clash", clash, "run", `{"a":1}`)
	if ex.Error != "States.ReferencePathConflict" {
		t.Fatalf("clash %s %s", ex.Error, ex.Cause)
	}

	none := `{"StartAt":"C","States":{"C":{"Type":"Choice","Choices":[{"Variable":"$.n","NumericEquals":0,"Next":"Yes"}]},"Yes":{"Type":"Succeed"}}}`
	ex = runMachine(t, s, "none", none, "run", `{"n":1}`)
	if ex.Error != "States.Runtime" || !strings.Contains(ex.Cause, "Failed to transition out of the state") {
		t.Fatalf("none %s %s", ex.Error, ex.Cause)
	}
	assertTypes(t, ex, "ExecutionStarted", "ChoiceStateEntered", "ExecutionFailed")
}

func TestPhase2ContextAndWait(t *testing.T) {
	s := newService()
	start := time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC)
	setClock(s, start)
	ctxDef := `{"StartAt":"P","States":{"P":{"Type":"Pass","Parameters":{"name.$":"$$.Execution.Name","state.$":"$$.State.Name","sm.$":"$$.StateMachine.Name","n.$":"$$.Execution.Input.n","t.$":"$$.Execution.StartTime"},"End":true}}}`
	ex := runMachine(t, s, "ctx", ctxDef, "run1", `{"n":1}`)
	want := `{"n":1,"name":"run1","sm":"ctx","state":"P","t":"2026-11-10T00:00:00.000Z"}`
	if ex.Output != want {
		t.Fatalf("context %s want %s cause %s", ex.Output, want, ex.Cause)
	}
	retry := `{"StartAt":"P","States":{"P":{"Type":"Pass","Parameters":{"c.$":"$$.State.RetryCount"},"End":true}}}`
	ex = runMachine(t, s, "retry-count", retry, "run", `{}`)
	if ex.Error != "States.Runtime" || !strings.Contains(ex.Cause, "not found") {
		t.Fatalf("retry count %s %s", ex.Error, ex.Cause)
	}

	zero := float32(0)
	s.Runner.WaitScale = &zero
	s.Runner.Sleep = func(context.Context, time.Duration) error {
		t.Fatal("scaled-away wait slept")
		return nil
	}
	waitDef := `{"StartAt":"W","States":{"W":{"Type":"Wait","Seconds":2,"Next":"S"},"S":{"Type":"Succeed"}}}`
	began := time.Now()
	ex = runMachine(t, s, "wait0", waitDef, "run", `{"n":1}`)
	if ex.Status != store.ExecSucceeded || ex.Output != `{"n":1}` || time.Since(began) > time.Second {
		t.Fatalf("wait0 %s %s %s", ex.Status, ex.Output, ex.Cause)
	}

	s.Runner.WaitScale = nil
	var slept time.Duration
	s.Runner.Sleep = func(_ context.Context, d time.Duration) error {
		slept = d
		return nil
	}
	ex = runMachine(t, s, "wait", waitDef, "run", `{"n":1}`)
	if ex.Status != store.ExecSucceeded || ex.Output != `{"n":1}` || slept != 2*time.Second {
		t.Fatalf("wait %s slept %s cause %s", ex.Status, slept, ex.Cause)
	}
	half := float32(0.5)
	s.Runner.WaitScale = &half
	slept = 0
	one := `{"StartAt":"W","States":{"W":{"Type":"Wait","Seconds":1,"End":true}}}`
	ex = runMachine(t, s, "half", one, "run", `{"n":1}`)
	if slept != 500*time.Millisecond || ex.Output != `{"n":1}` {
		t.Fatalf("half %s %s", slept, ex.Output)
	}

	s.Runner.WaitScale = nil
	past := `{"StartAt":"W","States":{"W":{"Type":"Wait","Timestamp":"2020-01-01T00:00:00Z","End":true}}}`
	slept = -1
	s.Runner.Sleep = func(context.Context, time.Duration) error {
		slept = 1
		return nil
	}
	ex = runMachine(t, s, "past", past, "run", `{}`)
	if ex.Status != store.ExecSucceeded || slept != -1 {
		t.Fatalf("past %s slept %s %s", ex.Status, slept, ex.Cause)
	}
	bare := `{"StartAt":"W","States":{"W":{"Type":"Wait","Timestamp":"2020-01-01T00:00:00","End":true}}}`
	ex = runMachine(t, s, "bare-ts", bare, "run", `{}`)
	if ex.Error != "States.Runtime" || !strings.Contains(ex.Cause, "ISO-8601") {
		t.Fatalf("bare %s %s", ex.Error, ex.Cause)
	}
	assertTypes(t, ex, "ExecutionStarted", "WaitStateEntered", "ExecutionFailed")

	pathWait := `{"StartAt":"W","States":{"W":{"Type":"Wait","SecondsPath":"$.s","End":true}}}`
	slept = 0
	s.Runner.Sleep = func(_ context.Context, d time.Duration) error {
		slept = d
		return nil
	}
	ex = runMachine(t, s, "sec-path", pathWait, "run", `{"s":1}`)
	if slept != time.Second || ex.Status != store.ExecSucceeded {
		t.Fatalf("seconds path %s %s %s", slept, ex.Status, ex.Cause)
	}

	started := make(chan struct{})
	s.Runner.Sleep = func(ctx context.Context, d time.Duration) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}
	sm := createMachine(t, s, "stop-wait", `{"StartAt":"W","States":{"W":{"Type":"Wait","Seconds":30,"End":true}}}`, roleARN)
	run := startExec(t, s, *sm.StateMachineArn, "run", `{}`)
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("wait did not sleep")
	}
	if _, err := s.StopExecution(context.Background(), &gen.StopExecutionInput{ExecutionArn: run.ExecutionArn}); err != nil {
		t.Fatal(err)
	}
	stopped := waitExec(t, s, *run.ExecutionArn)
	if stopped.Status != store.ExecAborted {
		t.Fatal(stopped.Status)
	}
	assertTypes(t, stopped, "ExecutionStarted", "WaitStateEntered", "ExecutionAborted")

	failPath := `{"StartAt":"F","States":{"F":{"Type":"Fail","ErrorPath":"$.err","CausePath":"$.why"}}}`
	ex = runMachine(t, s, "paths", failPath, "run", `{"err":"Boom","why":"no"}`)
	if ex.Error != "Boom" || ex.Cause != "no" {
		t.Fatalf("fail path %s %s", ex.Error, ex.Cause)
	}
}

func TestPhase2PayloadLimit(t *testing.T) {
	s := newService()
	big := strings.Repeat("a", 262145)
	def := `{"StartAt":"P","States":{"P":{"Type":"Pass","Parameters":{"a":"` + big + `"},"End":true}}}`
	ex := runMachine(t, s, "big", def, "run", `{}`)
	if ex.Error != "States.DataLimitExceeded" || !strings.Contains(ex.Cause, "Parameters") {
		t.Fatalf("%s %s", ex.Error, ex.Cause)
	}
	assertTypes(t, ex, "ExecutionStarted", "PassStateEntered", "ExecutionFailed")
}

func runMachine(t *testing.T, s *Service, name, def, execName, input string) store.Execution {
	t.Helper()
	sm := createMachine(t, s, name, def, roleARN)
	out := startExec(t, s, *sm.StateMachineArn, execName, input)
	return waitExec(t, s, *out.ExecutionArn)
}
