package api

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/catdevman/sfn-local/internal/api/gen"
	"github.com/catdevman/sfn-local/internal/store"
)

func TestRedriveResumesOpenState(t *testing.T) {
	s := newService()
	def := `{
		"QueryLanguage": "JSONata",
		"StartAt": "Store",
		"States": {
			"Store": {"Type": "Pass", "Assign": {"label": "kept"}, "Next": "Once"},
			"Once": {"Type": "Pass", "Output": "{% $states.context.Execution.RedriveCount > 0 ? $label : $lookup($states, \"nope\") %}", "Next": "Show"},
			"Show": {"Type": "Pass", "Output": "{% $states.context.Execution.RedriveCount %}", "End": true}
		}
	}`
	sm := createMachine(t, s, "redrive-keep", def, roleARN)
	var n atomic.Int32
	hold := make(chan struct{})
	var closeHold sync.Once
	release := func() { closeHold.Do(func() { close(hold) }) }
	defer release()
	s.Runner.Boundary = func(string) {
		if n.Add(1) == 3 {
			<-hold
		}
	}
	started := startExec(t, s, *sm.StateMachineArn, "run", `{}`)
	ex := waitExec(t, s, *started.ExecutionArn)
	if ex.Status != store.ExecFailed || ex.Error != "States.QueryEvaluationError" || ex.RedriveCount != 0 {
		t.Fatalf("%s %s %d", ex.Status, ex.Error, ex.RedriveCount)
	}
	if countEvents(ex, "PassStateEntered") != 2 || countEvents(ex, "PassStateExited") != 1 {
		t.Fatalf("first history %v", typesOf(ex))
	}
	desc, err := s.DescribeExecution(context.Background(), &gen.DescribeExecutionInput{ExecutionArn: started.ExecutionArn})
	if err != nil || desc.RedriveCount != nil {
		t.Fatalf("describe before redrive %+v %v", desc, err)
	}
	token := "again"
	first, err := s.RedriveExecution(context.Background(), &gen.RedriveExecutionInput{ExecutionArn: started.ExecutionArn, ClientToken: &token})
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.RedriveExecution(context.Background(), &gen.RedriveExecutionInput{ExecutionArn: started.ExecutionArn, ClientToken: &token})
	if err != nil {
		t.Fatal(err)
	}
	if first.RedriveDate == nil || second.RedriveDate == nil || first.RedriveDate.Seconds() != second.RedriveDate.Seconds() {
		t.Fatalf("idempotent dates %v %v", first.RedriveDate, second.RedriveDate)
	}
	other := "other"
	if _, err := s.RedriveExecution(context.Background(), &gen.RedriveExecutionInput{ExecutionArn: started.ExecutionArn, ClientToken: &other}); err == nil {
		t.Fatal("different token while running was accepted")
	} else if api := mustAPI(t, err); api.ErrorName() != "ExecutionNotRedrivable" || !strings.Contains(api.Error(), "RUNNING") {
		t.Fatal(api.Error())
	}
	release()
	ex = waitExec(t, s, *started.ExecutionArn)
	if ex.Status != store.ExecSucceeded || ex.RedriveCount != 1 {
		t.Fatalf("%s count %d output %s cause %s", ex.Status, ex.RedriveCount, ex.Output, ex.Cause)
	}
	jsonEqual(t, ex.Output, `1`)
	if countEvents(ex, "PassStateEntered") != 4 || countEvents(ex, "ExecutionRedriven") != 1 {
		t.Fatalf("history %v", typesOf(ex))
	}
	storeEntered := 0
	for _, ev := range ex.History {
		if ev.Type == "PassStateEntered" && ev.Name == "Store" {
			storeEntered++
		}
		if ev.Type == "ExecutionRedriven" && (!ev.HasRedrive || ev.RedriveCount != 1) {
			t.Fatalf("redriven details %+v", ev)
		}
	}
	if storeEntered != 1 {
		t.Fatalf("Store entered %d times", storeEntered)
	}
	desc, err = s.DescribeExecution(context.Background(), &gen.DescribeExecutionInput{ExecutionArn: started.ExecutionArn})
	if err != nil || desc.RedriveCount == nil || *desc.RedriveCount != 1 || desc.RedriveDate == nil {
		t.Fatalf("describe %+v %v", desc, err)
	}
	hist, err := s.GetExecutionHistory(context.Background(), &gen.GetExecutionHistoryInput{ExecutionArn: started.ExecutionArn})
	if err != nil || hist.Events == nil {
		t.Fatal(err)
	}
	seen := false
	for _, ev := range *hist.Events {
		if ev.Type == nil || *ev.Type != gen.HistoryEventTypeExecutionRedriven {
			continue
		}
		seen = true
		if ev.ExecutionRedrivenEventDetails == nil || ev.ExecutionRedrivenEventDetails.RedriveCount == nil || *ev.ExecutionRedrivenEventDetails.RedriveCount != 1 {
			t.Fatalf("history details %+v", ev.ExecutionRedrivenEventDetails)
		}
	}
	if !seen {
		t.Fatal("history omitted ExecutionRedriven")
	}
	if _, err := s.RedriveExecution(context.Background(), &gen.RedriveExecutionInput{ExecutionArn: started.ExecutionArn}); err == nil {
		t.Fatal("succeeded execution was redriven")
	} else if api := mustAPI(t, err); api.ErrorName() != "ExecutionNotRedrivable" {
		t.Fatal(api.Error())
	}
}

func TestRedriveKeepsJSONPathContext(t *testing.T) {
	s := newService()
	def := `{"StartAt":"P","States":{"P":{"Type":"Pass","Parameters":{"c.$":"$$.Execution.RedriveCount"},"End":true}}}`
	sm := createMachine(t, s, "redrive-path", def, roleARN)
	started := startExec(t, s, *sm.StateMachineArn, "run", `{}`)
	ex := waitExec(t, s, *started.ExecutionArn)
	if ex.Error != "States.Runtime" || !strings.Contains(ex.Cause, "not found") {
		t.Fatalf("%s %s", ex.Error, ex.Cause)
	}
	if _, err := s.RedriveExecution(context.Background(), &gen.RedriveExecutionInput{ExecutionArn: started.ExecutionArn}); err != nil {
		t.Fatal(err)
	}
	ex = waitExec(t, s, *started.ExecutionArn)
	if ex.Status != store.ExecFailed || ex.RedriveCount != 1 || ex.Error != "States.Runtime" || !strings.Contains(ex.Cause, "not found") {
		t.Fatalf("%s %s %s %d", ex.Status, ex.Error, ex.Cause, ex.RedriveCount)
	}
	if countEvents(ex, "PassStateEntered") != 2 || countEvents(ex, "ExecutionRedriven") != 1 {
		t.Fatalf("%v", typesOf(ex))
	}
}

func TestRedriveKeepsMockAttempts(t *testing.T) {
	s := newService()
	s.Runner.Mocks = writeMock(t, `{
		"StateMachines": {"retry-mock": {"TestCases": {"T": {"Call": "Seq"}}}},
		"MockedResponses": {"Seq": {"0": {"Throw": {"Error": "CustomError", "Cause": "once"}}, "1": {"Return": {"ok": true}}}}
	}`)
	def := `{"StartAt":"Call","States":{"Call":{"Type":"Task","Resource":"` + lambdaARN + `","End":true}}}`
	sm := createMachine(t, s, "retry-mock", def, roleARN)
	started := startExec(t, s, *sm.StateMachineArn+"#T", "run", `{}`)
	ex := waitExec(t, s, *started.ExecutionArn)
	if ex.Status != store.ExecFailed || ex.Error != "CustomError" {
		t.Fatalf("%s %s %s", ex.Status, ex.Error, ex.Cause)
	}
	if _, err := s.RedriveExecution(context.Background(), &gen.RedriveExecutionInput{ExecutionArn: started.ExecutionArn}); err != nil {
		t.Fatal(err)
	}
	ex = waitExec(t, s, *started.ExecutionArn)
	if ex.Status != store.ExecSucceeded {
		t.Fatalf("%s %s %s", ex.Status, ex.Error, ex.Cause)
	}
	jsonEqual(t, ex.Output, `{"ok":true}`)
}

func TestRedriveAfterAbort(t *testing.T) {
	s := newService()
	startedWait := make(chan struct{})
	var first atomic.Bool
	s.Runner.Sleep = func(ctx context.Context, _ time.Duration) error {
		if first.CompareAndSwap(false, true) {
			close(startedWait)
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	}
	def := `{"StartAt":"W","States":{"W":{"Type":"Wait","Seconds":30,"End":true}}}`
	sm := createMachine(t, s, "wait-stop", def, roleARN)
	started := startExec(t, s, *sm.StateMachineArn, "run", `{}`)
	select {
	case <-startedWait:
	case <-time.After(2 * time.Second):
		t.Fatal("wait did not start")
	}
	if _, err := s.StopExecution(context.Background(), &gen.StopExecutionInput{ExecutionArn: started.ExecutionArn}); err != nil {
		t.Fatal(err)
	}
	ex := waitExec(t, s, *started.ExecutionArn)
	if ex.Status != store.ExecAborted || countEvents(ex, "WaitStateExited") != 0 {
		t.Fatalf("%s %v", ex.Status, typesOf(ex))
	}
	if _, err := s.RedriveExecution(context.Background(), &gen.RedriveExecutionInput{ExecutionArn: started.ExecutionArn}); err != nil {
		t.Fatal(err)
	}
	ex = waitExec(t, s, *started.ExecutionArn)
	if ex.Status != store.ExecSucceeded || ex.RedriveCount != 1 || countEvents(ex, "WaitStateEntered") != 2 || countEvents(ex, "ExecutionAborted") != 1 {
		t.Fatalf("%s %d %v", ex.Status, ex.RedriveCount, typesOf(ex))
	}
}

func TestRedriveRefusals(t *testing.T) {
	s := newService()
	if _, err := s.RedriveExecution(context.Background(), &gen.RedriveExecutionInput{ExecutionArn: sp("nope")}); err == nil {
		t.Fatal("invalid arn accepted")
	} else if api := mustAPI(t, err); api.ErrorName() != "InvalidArn" {
		t.Fatal(api.Error())
	}
	missing := "arn:aws:states:us-east-1:123456789012:execution:missing:run"
	if _, err := s.RedriveExecution(context.Background(), &gen.RedriveExecutionInput{ExecutionArn: sp(missing)}); err == nil {
		t.Fatal("missing execution accepted")
	} else if api := mustAPI(t, err); api.ErrorName() != "ExecutionDoesNotExist" {
		t.Fatal(api.Error())
	}

	okDef := `{"StartAt":"P","States":{"P":{"Type":"Pass","End":true}}}`
	sm := createMachine(t, s, "done", okDef, roleARN)
	started := startExec(t, s, *sm.StateMachineArn, "run", `{}`)
	if ex := waitExec(t, s, *started.ExecutionArn); ex.Status != store.ExecSucceeded {
		t.Fatal(ex.Status)
	}
	if _, err := s.RedriveExecution(context.Background(), &gen.RedriveExecutionInput{ExecutionArn: started.ExecutionArn}); err == nil {
		t.Fatal("succeeded accepted")
	} else if api := mustAPI(t, err); api.ErrorName() != "ExecutionNotRedrivable" || !strings.Contains(api.Error(), "SUCCEEDED") {
		t.Fatal(api.Error())
	}

	typ := gen.StateMachineTypeEXPRESS
	esm, err := s.CreateStateMachine(context.Background(), &gen.CreateStateMachineInput{
		Name: sp("fast"), Definition: sp(`{"StartAt":"F","States":{"F":{"Type":"Fail","Error":"Boom","Cause":"x"}}}`), RoleArn: sp(roleARN), Type: &typ,
	})
	if err != nil {
		t.Fatal(err)
	}
	er := startExec(t, s, *esm.StateMachineArn, "run", `{}`)
	if ex := waitExec(t, s, *er.ExecutionArn); ex.Status != store.ExecFailed || !ex.Express {
		t.Fatalf("%s express %v", ex.Status, ex.Express)
	}
	if _, err := s.RedriveExecution(context.Background(), &gen.RedriveExecutionInput{ExecutionArn: er.ExecutionArn}); err == nil {
		t.Fatal("express accepted")
	} else if api := mustAPI(t, err); api.ErrorName() != "ExecutionNotRedrivable" || !strings.Contains(api.Error(), "EXPRESS") {
		t.Fatal(api.Error())
	}

	release := make(chan struct{})
	s.Runner.Sleep = func(ctx context.Context, _ time.Duration) error {
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	defer close(release)
	waitDef := `{"StartAt":"W","States":{"W":{"Type":"Wait","Seconds":30,"End":true}}}`
	wsm := createMachine(t, s, "running", waitDef, roleARN)
	wr := startExec(t, s, *wsm.StateMachineArn, "run", `{}`)
	deadline := time.Now().Add(2 * time.Second)
	for {
		ex, ok := s.Store.Execution(*wr.ExecutionArn)
		if ok && countEvents(ex, "WaitStateEntered") == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("running wait did not enter")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if _, err := s.RedriveExecution(context.Background(), &gen.RedriveExecutionInput{ExecutionArn: wr.ExecutionArn}); err == nil {
		t.Fatal("running accepted")
	} else if api := mustAPI(t, err); api.ErrorName() != "ExecutionNotRedrivable" || !strings.Contains(api.Error(), "RUNNING") {
		t.Fatal(api.Error())
	}
}
