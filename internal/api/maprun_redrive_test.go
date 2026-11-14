package api

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/catdevman/sfn-local/internal/api/gen"
	"github.com/catdevman/sfn-local/internal/exec"
	"github.com/catdevman/sfn-local/internal/store"
)

// flakyLambda fails inputs holding "bad" while broken is set, and counts
// calls by input.
func flakyLambda(s *Service, broken *atomic.Bool) map[string]int {
	var mu sync.Mutex
	calls := map[string]int{}
	s.Runner.LambdaEndpoint = "http://lambda.invalid"
	s.Runner.Invoke = func(ctx context.Context, call exec.LambdaCall) exec.LambdaReply {
		mu.Lock()
		calls[call.Payload]++
		mu.Unlock()
		if broken.Load() && strings.Contains(call.Payload, "bad") {
			return exec.LambdaReply{Status: 200, FunctionError: "Unhandled", Payload: `{"errorType":"Item.Bad","errorMessage":"bad item"}`}
		}
		return exec.LambdaReply{Status: 200, Payload: call.Payload}
	}
	return calls
}

func flakyMap(execType string) string {
	return `{"StartAt":"M","States":{"M":{"Type":"Map","MaxConcurrency":1,
		"ItemProcessor":{"ProcessorConfig":{"Mode":"DISTRIBUTED","ExecutionType":"` + execType + `"},"StartAt":"Call",
			"States":{"Call":{"Type":"Task","Resource":"arn:aws:lambda:us-east-1:123456789012:function:work","End":true}}},"End":true}}}`
}

func TestMapRunRedrive(t *testing.T) {
	s := newService()
	var broken atomic.Bool
	broken.Store(true)
	calls := flakyLambda(s, &broken)
	ex := runMachine(t, s, "redrive", flakyMap("STANDARD"), "run", `["a","bad","c","d"]`)
	if ex.Status != store.ExecFailed || ex.Error != "States.ExceedToleratedFailureThreshold" {
		t.Fatalf("%s %s %s", ex.Status, ex.Error, ex.Cause)
	}
	run := s.Store.MapRuns(ex.ARN)[0]
	if run.Status != store.MapRunFailed || run.Children[2] != "" || run.Children[3] != "" {
		t.Fatalf("first run %+v", run)
	}
	failedChild := run.Children[1]
	ctx := context.Background()
	if _, err := s.RedriveExecution(ctx, &gen.RedriveExecutionInput{ExecutionArn: &failedChild}); mustAPI(t, err).ErrorName() != "ExecutionNotRedrivable" {
		t.Fatalf("child redrive %v", err)
	}

	redriveStatus := func(arn string) string {
		t.Helper()
		d, err := s.DescribeExecution(ctx, &gen.DescribeExecutionInput{ExecutionArn: &arn})
		if err != nil {
			t.Fatal(err)
		}
		return string(*d.RedriveStatus) + " " + valueOf(d.RedriveStatusReason)
	}
	if got := redriveStatus(failedChild); got != "REDRIVABLE_BY_MAP_RUN " {
		t.Fatalf("child %q", got)
	}
	if got := redriveStatus(run.Children[0]); got != "NOT_REDRIVABLE Execution is SUCCEEDED and cannot be redriven" {
		t.Fatalf("succeeded child %q", got)
	}
	if got := redriveStatus(ex.ARN); got != "REDRIVABLE " {
		t.Fatalf("parent %q", got)
	}

	broken.Store(false)
	if _, err := s.RedriveExecution(ctx, &gen.RedriveExecutionInput{ExecutionArn: &ex.ARN}); err != nil {
		t.Fatal(err)
	}
	ex = waitExec(t, s, ex.ARN)
	if got := redriveStatus(ex.ARN); got != "NOT_REDRIVABLE Execution is SUCCEEDED and cannot be redriven" {
		t.Fatalf("redriven parent %q", got)
	}
	if ex.Status != store.ExecSucceeded {
		t.Fatalf("%s %s %s %v", ex.Status, ex.Error, ex.Cause, typesOf(ex))
	}
	jsonEqual(t, ex.Output, `["a","bad","c","d"]`)
	if calls[`"a"`] != 1 || calls[`"bad"`] != 2 || calls[`"c"`] != 1 {
		t.Fatalf("calls %v", calls)
	}
	want := []string{"ExecutionStarted", "MapStateEntered", "MapStateStarted", "MapRunStarted", "MapRunFailed", "MapStateFailed", "ExecutionFailed",
		"ExecutionRedriven", "MapRunRedriven", "MapRunSucceeded", "MapStateSucceeded", "MapStateExited", "ExecutionSucceeded"}
	if strings.Join(typesOf(ex), ",") != strings.Join(want, ",") {
		t.Fatalf("history %v", typesOf(ex))
	}
	// MapRunRedriven follows MapStateStarted, and MapStateExited follows it.
	if ex.History[8].PreviousID != 3 || ex.History[11].PreviousID != 9 {
		t.Fatalf("previous ids %d %d", ex.History[8].PreviousID, ex.History[11].PreviousID)
	}
	runs := s.Store.MapRuns(ex.ARN)
	if len(runs) != 1 || runs[0].Children[1] != failedChild {
		t.Fatalf("runs %+v", runs)
	}
	child, _ := s.Store.Execution(failedChild)
	if child.Status != store.ExecSucceeded || child.RedriveCount != 1 {
		t.Fatalf("child %s %d", child.Status, child.RedriveCount)
	}
	// The child resumes its Task without entering it again. The first
	// event after the redrive follows the original entered event.
	if countEvents(child, "TaskStateEntered") != 1 {
		t.Fatalf("child history %v", typesOf(child))
	}
	for i, ev := range child.History {
		if ev.Type == "ExecutionRedriven" && child.History[i+1].PreviousID != 2 {
			t.Fatalf("resumed event %+v", child.History[i+1])
		}
	}
	for filter, want := range map[gen.ExecutionRedriveFilter]int{gen.ExecutionRedriveFilterREDRIVEN: 1, gen.ExecutionRedriveFilterNOT_REDRIVEN: 3} {
		list, err := s.ListExecutions(ctx, &gen.ListExecutionsInput{MapRunArn: &run.ARN, RedriveFilter: &filter})
		if err != nil || len(*list.Executions) != want {
			t.Fatalf("%s: %v %v", filter, list, err)
		}
	}
	hist, err := s.GetExecutionHistory(ctx, &gen.GetExecutionHistoryInput{ExecutionArn: &ex.ARN})
	if err != nil {
		t.Fatal(err)
	}
	var redriven *gen.MapRunRedrivenEventDetails
	for _, ev := range *hist.Events {
		if ev.MapRunRedrivenEventDetails != nil {
			redriven = ev.MapRunRedrivenEventDetails
		}
	}
	if redriven == nil || *redriven.MapRunArn != run.ARN || *redriven.RedriveCount != 1 {
		t.Fatalf("MapRunRedriven %+v", redriven)
	}
	desc, err := s.DescribeMapRun(ctx, &gen.DescribeMapRunInput{MapRunArn: &run.ARN})
	if err != nil {
		t.Fatal(err)
	}
	if *desc.Status != "SUCCEEDED" || *desc.RedriveCount != 1 || desc.RedriveDate == nil || *desc.ItemCounts.Succeeded != 4 || *desc.ItemCounts.Failed != 0 || *desc.ExecutionCounts.PendingRedrive != 0 {
		t.Fatalf("describe %+v %+v", desc, desc.ItemCounts)
	}
}

func TestMapRunRedriveExpressAndRetry(t *testing.T) {
	s := newService()
	var broken atomic.Bool
	broken.Store(true)
	flakyLambda(s, &broken)
	ex := runMachine(t, s, "express", flakyMap("EXPRESS"), "run", `["bad","b"]`)
	if ex.Status != store.ExecFailed {
		t.Fatalf("%s %s", ex.Status, ex.Error)
	}
	first := s.Store.MapRuns(ex.ARN)[0].Children[0]

	// The redriven run fails again, so a second redrive resumes it again.
	if _, err := s.RedriveExecution(context.Background(), &gen.RedriveExecutionInput{ExecutionArn: &ex.ARN}); err != nil {
		t.Fatal(err)
	}
	if ex = waitExec(t, s, ex.ARN); ex.Status != store.ExecFailed || countEvents(ex, "MapRunRedriven") != 1 {
		t.Fatalf("%s %v", ex.Status, typesOf(ex))
	}
	broken.Store(false)
	if _, err := s.RedriveExecution(context.Background(), &gen.RedriveExecutionInput{ExecutionArn: &ex.ARN}); err != nil {
		t.Fatal(err)
	}
	ex = waitExec(t, s, ex.ARN)
	if ex.Status != store.ExecSucceeded || ex.Output != `["bad","b"]` {
		t.Fatalf("%s %s %s", ex.Status, ex.Output, ex.Cause)
	}
	runs := s.Store.MapRuns(ex.ARN)
	if len(runs) != 1 || runs[0].RedriveCount != 2 || runs[0].Children[0] == first {
		t.Fatalf("runs %+v", runs)
	}
	if child, _ := s.Store.Execution(runs[0].Children[0]); !child.Express || child.RedriveCount != 0 {
		t.Fatalf("replacement child %+v", child)
	}

	// A Retry after the redriven run fails starts a new run.
	retried := `{"StartAt":"M","States":{"M":{"Type":"Map","Retry":[{"ErrorEquals":["States.ALL"],"MaxAttempts":1,"IntervalSeconds":0}],
		"ItemProcessor":{"ProcessorConfig":{"Mode":"DISTRIBUTED","ExecutionType":"STANDARD"},"StartAt":"Call",
			"States":{"Call":{"Type":"Task","Resource":"arn:aws:lambda:us-east-1:123456789012:function:work","End":true}}},"End":true}}}`
	broken.Store(true)
	rx := runMachine(t, s, "retried", retried, "run", `["bad"]`)
	if rx.Status != store.ExecFailed || len(s.Store.MapRuns(rx.ARN)) != 2 {
		t.Fatalf("%s %d", rx.Status, len(s.Store.MapRuns(rx.ARN)))
	}
	if _, err := s.RedriveExecution(context.Background(), &gen.RedriveExecutionInput{ExecutionArn: &rx.ARN}); err != nil {
		t.Fatal(err)
	}
	rx = waitExec(t, s, rx.ARN)
	runs = s.Store.MapRuns(rx.ARN)
	if rx.Status != store.ExecFailed || len(runs) != 3 || runs[1].RedriveCount != 1 || runs[0].RedriveCount != 0 {
		t.Fatalf("%s %d %v", rx.Status, len(runs), typesOf(rx))
	}
}

func TestMapRunRedriveAfterStop(t *testing.T) {
	s := newService()
	var block atomic.Bool
	block.Store(true)
	started := make(chan struct{}, 4)
	s.Runner.LambdaEndpoint = "http://lambda.invalid"
	s.Runner.Invoke = func(ctx context.Context, call exec.LambdaCall) exec.LambdaReply {
		if block.Load() {
			started <- struct{}{}
			<-ctx.Done()
			return exec.LambdaReply{Err: ctx.Err()}
		}
		return exec.LambdaReply{Status: 200, Payload: call.Payload}
	}
	sm := createMachine(t, s, "stopped", flakyMap("STANDARD"), roleARN)
	out := startExec(t, s, *sm.StateMachineArn, "run", `[1,2]`)
	<-started
	ctx := context.Background()
	if _, err := s.StopExecution(ctx, &gen.StopExecutionInput{ExecutionArn: out.ExecutionArn}); err != nil {
		t.Fatal(err)
	}
	if ex := waitExec(t, s, *out.ExecutionArn); ex.Status != store.ExecAborted {
		t.Fatalf("parent %s", ex.Status)
	}
	waitFor(t, func() bool { return s.Store.MapRuns(*out.ExecutionArn)[0].Status == store.MapRunAborted })
	aborted := s.Store.MapRuns(*out.ExecutionArn)[0].Children[0]
	waitFor(t, func() bool { c, _ := s.Store.Execution(aborted); return c.Status == store.ExecAborted })

	block.Store(false)
	if _, err := s.RedriveExecution(ctx, &gen.RedriveExecutionInput{ExecutionArn: out.ExecutionArn}); err != nil {
		t.Fatal(err)
	}
	ex := waitExec(t, s, *out.ExecutionArn)
	if ex.Status != store.ExecSucceeded || ex.Output != `[1,2]` {
		t.Fatalf("%s %s %s %v", ex.Status, ex.Output, ex.Cause, typesOf(ex))
	}
	run := s.Store.MapRuns(ex.ARN)[0]
	if run.Status != store.MapRunSucceeded || run.RedriveCount != 1 || run.Children[0] != aborted || run.Executions.Succeeded != 2 {
		t.Fatalf("run %+v", run)
	}
}
