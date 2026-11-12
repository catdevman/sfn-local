package api

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/catdevman/sfn-local/internal/api/gen"
	"github.com/catdevman/sfn-local/internal/store"
)

func startExec(t *testing.T, s *Service, sm, name, input string) *gen.StartExecutionOutput {
	t.Helper()
	in := &gen.StartExecutionInput{StateMachineArn: sp(sm)}
	if name != "" {
		in.Name = sp(name)
	}
	if input != "" {
		in.Input = sp(input)
	}
	out, err := s.StartExecution(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func waitExec(t *testing.T, s *Service, arn string) store.Execution {
	t.Helper()
	select {
	case <-s.Store.Finished(arn):
	case <-time.After(3 * time.Second):
		t.Fatal("execution did not finish")
	}
	ex, ok := s.Store.Execution(arn)
	if !ok {
		t.Fatalf("missing %s", arn)
	}
	return ex
}

func assertTypes(t *testing.T, ex store.Execution, want ...string) {
	t.Helper()
	got := make([]string, len(ex.History))
	for i, ev := range ex.History {
		got[i] = ev.Type
		if i == 0 && (ev.ID != 1 || ev.PreviousID != 0) {
			t.Fatalf("first event id %d previous %d", ev.ID, ev.PreviousID)
		}
		if i > 0 && ev.PreviousID != ex.History[i-1].ID {
			t.Fatalf("previous %d at %d", ev.PreviousID, i)
		}
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("events %v", got)
	}
}

func jsonText(t *testing.T, v any) string {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

type gate struct {
	arrived chan struct{}
	release chan struct{}
	once    sync.Once
	rel     sync.Once
}

func newGate() *gate {
	return &gate{arrived: make(chan struct{}), release: make(chan struct{})}
}

func (g *gate) Wait(string) {
	g.once.Do(func() { close(g.arrived) })
	<-g.release
}

func (g *gate) waitArrived(t *testing.T) {
	t.Helper()
	select {
	case <-g.arrived:
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not reach a state boundary")
	}
}

func (g *gate) Release() { g.rel.Do(func() { close(g.release) }) }

func setClock(s *Service, start time.Time) *time.Time {
	cur := start
	s.Runner.Now = func() time.Time { return cur }
	return &cur
}

func TestPassSucceedFail(t *testing.T) {
	s := newService()
	var log bytes.Buffer
	s.Runner.Log = &log
	sm := createMachine(t, s, "pass", `{"StartAt":"P","States":{"P":{"Type":"Pass","Result":{"ok":true},"End":true}}}`, roleARN)
	out := startExec(t, s, *sm.StateMachineArn, "run1", `{"n":1}`)
	ex := waitExec(t, s, *out.ExecutionArn)
	if ex.Status != store.ExecSucceeded || ex.Output != `{"ok":true}` {
		t.Fatalf("%s %s", ex.Status, ex.Output)
	}
	assertTypes(t, ex, "ExecutionStarted", "PassStateEntered", "PassStateExited", "ExecutionSucceeded")
	if !strings.Contains(log.String(), "state=P") || !strings.Contains(log.String(), "event=PassStateExited") || !strings.Contains(log.String(), "executionArn=") {
		t.Fatal(log.String())
	}
	desc, err := s.DescribeExecution(context.Background(), &gen.DescribeExecutionInput{ExecutionArn: out.ExecutionArn})
	if err != nil {
		t.Fatal(err)
	}
	text := jsonText(t, desc)
	if strings.Contains(text, `"error"`) || strings.Contains(text, `"cause"`) || *desc.Output != `{"ok":true}` {
		t.Fatal(text)
	}
	hist, err := s.GetExecutionHistory(context.Background(), &gen.GetExecutionHistoryInput{ExecutionArn: out.ExecutionArn})
	if err != nil || hist.Events == nil || len(*hist.Events) != 4 {
		t.Fatal(err, hist)
	}
	if (*hist.Events)[0].PreviousEventId == nil || *(*hist.Events)[0].PreviousEventId != 0 {
		t.Fatal("previousEventId 0 was omitted")
	}
	if !strings.Contains(jsonText(t, hist), `"previousEventId":0`) {
		t.Fatal(jsonText(t, hist))
	}

	html := createMachine(t, s, "html", `{"StartAt":"P","States":{"P":{"Type":"Pass","Result":{"a":"<"},"End":true}}}`, roleARN)
	htmlRun := startExec(t, s, *html.StateMachineArn, "run", `{}`)
	if got := waitExec(t, s, *htmlRun.ExecutionArn); got.Output != `{"a":"<"}` {
		t.Fatal(got.Output)
	}

	paths := createMachine(t, s, "paths", `{"StartAt":"P","States":{"P":{"Type":"Pass","InputPath":null,"ResultPath":null,"OutputPath":"$","End":true}}}`, roleARN)
	pathRun := startExec(t, s, *paths.StateMachineArn, "run", `{"n":1}`)
	if got := waitExec(t, s, *pathRun.ExecutionArn); got.Output != `{"n":1}` {
		t.Fatal(got.Output)
	}
	empty := createMachine(t, s, "empty-out", `{"StartAt":"P","States":{"P":{"Type":"Pass","OutputPath":null,"End":true}}}`, roleARN)
	emptyRun := startExec(t, s, *empty.StateMachineArn, "run", `{"n":1}`)
	if got := waitExec(t, s, *emptyRun.ExecutionArn); got.Output != `{}` {
		t.Fatal(got.Output)
	}

	ok := createMachine(t, s, "succeed", `{"StartAt":"S","States":{"S":{"Type":"Succeed"}}}`, roleARN)
	okRun := startExec(t, s, *ok.StateMachineArn, "run", `{"n":1}`)
	okEx := waitExec(t, s, *okRun.ExecutionArn)
	if okEx.Output != `{"n":1}` {
		t.Fatal(okEx.Output)
	}
	assertTypes(t, okEx, "ExecutionStarted", "SucceedStateEntered", "SucceedStateExited", "ExecutionSucceeded")

	fail := createMachine(t, s, "fail", `{"StartAt":"F","States":{"F":{"Type":"Fail","Error":"Boom","Cause":"no"}}}`, roleARN)
	failRun := startExec(t, s, *fail.StateMachineArn, "run", `{}`)
	failEx := waitExec(t, s, *failRun.ExecutionArn)
	if failEx.Status != store.ExecFailed || failEx.Error != "Boom" || failEx.Cause != "no" {
		t.Fatalf("%+v", failEx)
	}
	assertTypes(t, failEx, "ExecutionStarted", "FailStateEntered", "ExecutionFailed")
	failDesc, err := s.DescribeExecution(context.Background(), &gen.DescribeExecutionInput{ExecutionArn: failRun.ExecutionArn})
	if err != nil || strings.Contains(jsonText(t, failDesc), `"error"`) {
		t.Fatal(jsonText(t, failDesc), err)
	}
	include := false
	stripped, err := s.GetExecutionHistory(context.Background(), &gen.GetExecutionHistoryInput{
		ExecutionArn: failRun.ExecutionArn, IncludeExecutionData: &include,
	})
	if err != nil {
		t.Fatal(err)
	}
	started := (*stripped.Events)[0].ExecutionStartedEventDetails
	if started == nil || started.Input != nil || started.RoleArn == nil {
		t.Fatalf("%+v", started)
	}
	failed := (*stripped.Events)[2].ExecutionFailedEventDetails
	if failed == nil || failed.Error == nil || *failed.Error != "Boom" {
		t.Fatalf("%+v", failed)
	}
	reverse := true
	rev, err := s.GetExecutionHistory(context.Background(), &gen.GetExecutionHistoryInput{
		ExecutionArn: failRun.ExecutionArn, ReverseOrder: &reverse,
	})
	if err != nil || *(*rev.Events)[0].Type != gen.HistoryEventTypeExecutionFailed {
		t.Fatal(rev, err)
	}

	bare := createMachine(t, s, "bare-fail", `{"StartAt":"F","States":{"F":{"Type":"Fail"}}}`, roleARN)
	bareRun := startExec(t, s, *bare.StateMachineArn, "run", `{}`)
	bareEx := waitExec(t, s, *bareRun.ExecutionArn)
	if bareEx.HasError || bareEx.HasCause {
		t.Fatal("omitted error was stored")
	}
	long := strings.Repeat("E", 300)
	clipped := createMachine(t, s, "clip", `{"StartAt":"F","States":{"F":{"Type":"Fail","Error":"`+long+`","Cause":"c"}}}`, roleARN)
	clipRun := startExec(t, s, *clipped.StateMachineArn, "run", `{}`)
	clipEx := waitExec(t, s, *clipRun.ExecutionArn)
	if len(clipEx.Error) != 256 || !strings.HasSuffix(clipEx.Error, "...") {
		t.Fatalf("%d %s", len(clipEx.Error), clipEx.Error)
	}
}

func TestStopIdempotencyAndUnimplementedStates(t *testing.T) {
	s := newService()
	g := newGate()
	defer g.Release()
	s.Runner.Boundary = g.Wait
	sm := createMachine(t, s, "hold", passDef, roleARN)
	running := startExec(t, s, *sm.StateMachineArn, "run", `{}`)
	g.waitArrived(t)
	desc, err := s.DescribeExecution(context.Background(), &gen.DescribeExecutionInput{ExecutionArn: running.ExecutionArn})
	if err != nil || *desc.Status != gen.ExecutionStatusRUNNING || strings.Contains(jsonText(t, desc), `"output"`) {
		t.Fatal(jsonText(t, desc), err)
	}
	again := startExec(t, s, *sm.StateMachineArn, "run", `{}`)
	if *again.ExecutionArn != *running.ExecutionArn || again.StartDate.Seconds() != running.StartDate.Seconds() {
		t.Fatal("idempotent start changed the row")
	}
	_, err = s.StartExecution(context.Background(), &gen.StartExecutionInput{
		StateMachineArn: sm.StateMachineArn, Name: sp("run"), Input: sp(`{"a":1}`),
	})
	if api := mustAPI(t, err); api.ErrorName() != "ExecutionAlreadyExists" || !strings.Contains(api.Error(), *running.ExecutionArn) {
		t.Fatal(api)
	}
	_, err = s.StartExecution(context.Background(), &gen.StartExecutionInput{
		StateMachineArn: sm.StateMachineArn, Name: sp("run"), Input: sp(`{ "a" : 1 }`),
	})
	if mustAPI(t, err).ErrorName() != "ExecutionAlreadyExists" {
		t.Fatal(err)
	}
	stop1, err := s.StopExecution(context.Background(), &gen.StopExecutionInput{ExecutionArn: running.ExecutionArn, Error: sp("Stop"), Cause: sp("asked")})
	if err != nil {
		t.Fatal(err)
	}
	stop2, err := s.StopExecution(context.Background(), &gen.StopExecutionInput{ExecutionArn: running.ExecutionArn})
	if err != nil || stop2.StopDate.Seconds() != stop1.StopDate.Seconds() {
		t.Fatal(stop2, err)
	}
	g.Release()
	stopped := waitExec(t, s, *running.ExecutionArn)
	if stopped.Status != store.ExecAborted || stopped.Error != "Stop" || stopped.Cause != "asked" {
		t.Fatalf("%+v", stopped)
	}
	assertTypes(t, stopped, "ExecutionStarted", "ExecutionAborted")
	_, err = s.StartExecution(context.Background(), &gen.StartExecutionInput{
		StateMachineArn: sm.StateMachineArn, Name: sp("run"), Input: sp(`{}`),
	})
	if mustAPI(t, err).ErrorName() != "ExecutionAlreadyExists" {
		t.Fatal(err)
	}

	s.Runner.Boundary = nil
	anon := startExec(t, s, *sm.StateMachineArn, "", "")
	anonEx := waitExec(t, s, *anon.ExecutionArn)
	if anonEx.Input != `{}` {
		t.Fatal(anonEx.Input)
	}
	if !strings.Contains(anonEx.Name, "-") {
		t.Fatal(anonEx.Name)
	}

	_, err = s.StartExecution(context.Background(), &gen.StartExecutionInput{StateMachineArn: sm.StateMachineArn, Name: sp("bad"), Input: sp(`not-json`)})
	if api := mustAPI(t, err); api.ErrorName() != "InvalidExecutionInput" || !strings.Contains(api.Error(), "Invalid State Machine Execution Input") {
		t.Fatal(api)
	}
	_, err = s.StartExecution(context.Background(), &gen.StartExecutionInput{StateMachineArn: sm.StateMachineArn, Name: sp("trail"), Input: sp(`{}{}`)})
	if api := mustAPI(t, err); !strings.Contains(api.Error(), "trailing tokens") {
		t.Fatal(api)
	}
	_, err = s.StartExecution(context.Background(), &gen.StartExecutionInput{
		StateMachineArn: sp(*sm.StateMachineArn + "#A#B"), Name: sp("m"), Input: sp(`{}`),
	})
	if mustAPI(t, err).ErrorName() != "InvalidArn" {
		t.Fatal(err)
	}
	mocked := startExec(t, s, *sm.StateMachineArn+"#Case", "mock", `{}`)
	if !strings.Contains(*mocked.ExecutionArn, ":execution:hold:mock") {
		t.Fatal(*mocked.ExecutionArn)
	}
	_ = waitExec(t, s, *mocked.ExecutionArn)

	if _, err := s.DeleteStateMachine(context.Background(), &gen.DeleteStateMachineInput{StateMachineArn: sm.StateMachineArn}); err != nil {
		t.Fatal(err)
	}
	_, err = s.StartExecution(context.Background(), &gen.StartExecutionInput{StateMachineArn: sm.StateMachineArn, Name: sp("late"), Input: sp(`{}`)})
	if api := mustAPI(t, err); api.ErrorName() != "StateMachineDeleting" || !strings.Contains(api.Error(), *sm.StateMachineArn) {
		t.Fatal(api)
	}

	choice := createMachine(t, s, "choice", `{"StartAt":"C","States":{"C":{"Type":"Choice","Choices":[{"Variable":"$.n","NumericEquals":1,"Next":"P"}],"Default":"P"},"P":{"Type":"Pass","End":true}}}`, roleARN)
	choiceRun := startExec(t, s, *choice.StateMachineArn, "run", `{"n":1}`)
	choiceEx := waitExec(t, s, *choiceRun.ExecutionArn)
	if choiceEx.Status != store.ExecSucceeded || choiceEx.Output != `{"n":1}` {
		t.Fatalf("%s %s %s", choiceEx.Status, choiceEx.Output, choiceEx.Cause)
	}
	assertTypes(t, choiceEx, "ExecutionStarted", "ChoiceStateEntered", "ChoiceStateExited", "PassStateEntered", "PassStateExited", "ExecutionSucceeded")
	missing := createMachine(t, s, "missing", `{"StartAt":"P","States":{"P":{"Type":"Pass","Next":"Gone"}}}`, roleARN)
	missingRun := startExec(t, s, *missing.StateMachineArn, "run", `{}`)
	missingEx := waitExec(t, s, *missingRun.ExecutionArn)
	assertTypes(t, missingEx, "ExecutionStarted", "PassStateEntered", "PassStateExited", "ExecutionFailed")
	if !strings.Contains(missingEx.Cause, "Missing 'Next' target: Gone") {
		t.Fatal(missingEx.Cause)
	}
	params := createMachine(t, s, "params", `{"StartAt":"P","States":{"P":{"Type":"Pass","Parameters":{"a":1},"End":true}}}`, roleARN)
	paramsRun := startExec(t, s, *params.StateMachineArn, "run", `{}`)
	paramsEx := waitExec(t, s, *paramsRun.ExecutionArn)
	if paramsEx.Status != store.ExecSucceeded || paramsEx.Output != `{"a":1}` {
		t.Fatalf("%s %s", paramsEx.Status, paramsEx.Output)
	}
	other := createMachine(t, s, "path", `{"StartAt":"P","States":{"P":{"Type":"Pass","InputPath":"$.n","End":true}}}`, roleARN)
	otherRun := startExec(t, s, *other.StateMachineArn, "run", `{"n":1}`)
	otherEx := waitExec(t, s, *otherRun.ExecutionArn)
	if otherEx.Status != store.ExecSucceeded || otherEx.Output != `1` {
		t.Fatalf("%s %s %s", otherEx.Status, otherEx.Output, otherEx.Cause)
	}
	assign := createMachine(t, s, "assign", `{"StartAt":"P","States":{"P":{"Type":"Pass","Assign":{"a":1},"End":true}}}`, roleARN)
	assignRun := startExec(t, s, *assign.StateMachineArn, "run", `{}`)
	assignEx := waitExec(t, s, *assignRun.ExecutionArn)
	assertTypes(t, assignEx, "ExecutionStarted", "PassStateEntered", "ExecutionFailed")
	if !strings.Contains(assignEx.Cause, "Assign is not implemented") {
		t.Fatal(assignEx.Cause)
	}
}

func TestSyncExpressAndList(t *testing.T) {
	s := newService()
	standard := createMachine(t, s, "std", passDef, roleARN)
	_, err := s.StartSyncExecution(context.Background(), &gen.StartSyncExecutionInput{StateMachineArn: standard.StateMachineArn, Input: sp(`{}`)})
	if api := mustAPI(t, err); api.ErrorName() != "StateMachineTypeNotSupported" || !strings.Contains(api.Error(), "StartSyncExecution is not supported for STANDARD State Machines") {
		t.Fatal(api)
	}
	_, err = s.StartSyncExecution(context.Background(), &gen.StartSyncExecutionInput{StateMachineArn: sp(*standard.StateMachineArn + "#Case")})
	if api := mustAPI(t, err); api.ErrorName() != "UnsupportedOperation" || api.StatusCode() != 500 || api.ShapeID() != "com.amazonaws.sfn#UnsupportedOperation" || !strings.Contains(api.Error(), "Service integration mocking is not supported for the StartSyncExecution operation.") {
		t.Fatal(api)
	}
	s.Profile = ProfileSFNLocal
	_, err = s.StartSyncExecution(context.Background(), &gen.StartSyncExecutionInput{StateMachineArn: sp(*standard.StateMachineArn + "#Case")})
	if api := mustAPI(t, err); api.ShapeID() != jarUnsupported {
		t.Fatal(api.ShapeID())
	}
	s.Profile = ProfileSmithy

	typ := gen.StateMachineTypeEXPRESS
	express, err := s.CreateStateMachine(context.Background(), &gen.CreateStateMachineInput{
		Name: sp("fast"), Definition: sp(`{"StartAt":"P","States":{"P":{"Type":"Pass","Result":{"ok":true},"End":true}}}`), RoleArn: sp(roleARN), Type: &typ,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.ListExecutions(context.Background(), &gen.ListExecutionsInput{StateMachineArn: express.StateMachineArn})
	if api := mustAPI(t, err); api.ErrorName() != "InvalidParameterValue" || api.StatusCode() != 400 || !strings.Contains(api.Error(), "This operation is not supported by this type of state machine") {
		t.Fatal(api)
	}
	_, err = s.ListExecutions(context.Background(), &gen.ListExecutionsInput{})
	if api := mustAPI(t, err); api.ErrorName() != "ValidationException" || !strings.Contains(api.Error(), "stateMachineArn") {
		t.Fatal(api)
	}
	sync, err := s.StartSyncExecution(context.Background(), &gen.StartSyncExecutionInput{
		StateMachineArn: express.StateMachineArn, Name: sp("run1"), Input: sp(`{"n":1}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if *sync.Status != gen.SyncExecutionStatusSUCCEEDED || *sync.Output != `{"ok":true}` || sync.BillingDetails != nil {
		t.Fatalf("%s", jsonText(t, sync))
	}
	if _, ok := s.Store.Execution(*sync.ExecutionArn); ok {
		t.Fatal("express history was kept")
	}
	real := *sync.ExecutionArn
	_, err = s.DescribeExecution(context.Background(), &gen.DescribeExecutionInput{ExecutionArn: sp(real)})
	if api := mustAPI(t, err); api.ErrorName() != "InvalidArn" || !strings.Contains(api.Error(), real) || strings.Contains(api.Error(), expressPlaceholderMessage) {
		t.Fatal(api)
	}
	placeholder := "arn:aws:states:us-east-1:123456789012:express:stateMachineName:run1"
	for _, arn := range []string{placeholder} {
		_, err = s.GetExecutionHistory(context.Background(), &gen.GetExecutionHistoryInput{ExecutionArn: sp(arn)})
		if api := mustAPI(t, err); !strings.Contains(api.Error(), expressPlaceholderMessage) {
			t.Fatal(api)
		}
	}
	_, err = s.StopExecution(context.Background(), &gen.StopExecutionInput{ExecutionArn: sp(real)})
	if api := mustAPI(t, err); !strings.Contains(api.Error(), "Invalid Arn: '"+real+"'") {
		t.Fatal(api)
	}
	_, err = s.DescribeStateMachineForExecution(context.Background(), &gen.DescribeStateMachineForExecutionInput{ExecutionArn: sp(placeholder)})
	if api := mustAPI(t, err); !strings.Contains(api.Error(), expressPlaceholderMessage) {
		t.Fatal(api)
	}
	missing := ExecutionARN("us-east-1", "123456789012", "std", "missing", false)
	_, err = s.DescribeExecution(context.Background(), &gen.DescribeExecutionInput{ExecutionArn: sp(missing)})
	if mustAPI(t, err).ErrorName() != "ExecutionDoesNotExist" {
		t.Fatal(err)
	}

	failDef := `{"StartAt":"F","States":{"F":{"Type":"Fail","Error":"Boom","Cause":"no"}}}`
	failSM, err := s.CreateStateMachine(context.Background(), &gen.CreateStateMachineInput{
		Name: sp("fast-fail"), Definition: sp(failDef), RoleArn: sp(roleARN), Type: &typ,
	})
	if err != nil {
		t.Fatal(err)
	}
	failed, err := s.StartSyncExecution(context.Background(), &gen.StartSyncExecutionInput{
		StateMachineArn: failSM.StateMachineArn, Name: sp("run"), Input: sp(`{}`),
	})
	if err != nil || *failed.Status != gen.SyncExecutionStatusFAILED || *failed.Error != "Boom" || *failed.Cause != "no" {
		t.Fatalf("%s %v", jsonText(t, failed), err)
	}

	clk := setClock(s, time.Unix(1_700_000_000, 0).UTC())
	first := startExec(t, s, *standard.StateMachineArn, "a", `{}`)
	_ = waitExec(t, s, *first.ExecutionArn)
	*clk = clk.Add(time.Second)
	second := startExec(t, s, *standard.StateMachineArn, "b", `{}`)
	_ = waitExec(t, s, *second.ExecutionArn)
	filter := gen.ExecutionStatusSUCCEEDED
	listed, err := s.ListExecutions(context.Background(), &gen.ListExecutionsInput{StateMachineArn: standard.StateMachineArn, StatusFilter: &filter})
	if err != nil || len(*listed.Executions) < 2 {
		t.Fatal(listed, err)
	}
	if *(*listed.Executions)[0].Name != "b" || *(*listed.Executions)[1].Name != "a" {
		t.Fatalf("order %s %s", *(*listed.Executions)[0].Name, *(*listed.Executions)[1].Name)
	}
}

func TestSnapshotDeletingTimeoutAndHistoryCap(t *testing.T) {
	s := newService()
	g := newGate()
	defer g.Release()
	s.Runner.Boundary = g.Wait
	sm := createMachine(t, s, "snap", `{"StartAt":"P","States":{"P":{"Type":"Pass","Result":{"ok":true},"End":true}}}`, roleARN)
	run := startExec(t, s, *sm.StateMachineArn, "run", `{"n":1}`)
	g.waitArrived(t)
	if _, err := s.UpdateStateMachine(context.Background(), &gen.UpdateStateMachineInput{
		StateMachineArn: sm.StateMachineArn, Definition: sp(`{"StartAt":"F","States":{"F":{"Type":"Fail","Error":"Boom","Cause":"no"}}}`),
	}); err != nil {
		t.Fatal(err)
	}
	g.Release()
	ex := waitExec(t, s, *run.ExecutionArn)
	if ex.Status != store.ExecSucceeded || ex.Output != `{"ok":true}` || ex.Definition == `{"StartAt":"F","States":{"F":{"Type":"Fail","Error":"Boom","Cause":"no"}}}` {
		t.Fatalf("snapshot %s %s", ex.Status, ex.Output)
	}
	live, err := s.DescribeStateMachineForExecution(context.Background(), &gen.DescribeStateMachineForExecutionInput{ExecutionArn: run.ExecutionArn})
	if err != nil || !strings.Contains(*live.Definition, "Fail") || live.UpdateDate == nil {
		t.Fatalf("%+v %v", live, err)
	}

	hold := newGate()
	defer hold.Release()
	s.Runner.Boundary = hold.Wait
	kept := createMachine(t, s, "kept", passDef, roleARN)
	keptRun := startExec(t, s, *kept.StateMachineArn, "run", `{}`)
	hold.waitArrived(t)
	if _, err := s.DeleteStateMachine(context.Background(), &gen.DeleteStateMachineInput{StateMachineArn: kept.StateMachineArn}); err != nil {
		t.Fatal(err)
	}
	s.Store.Sweep()
	if _, ok := s.Store.Machine(*kept.StateMachineArn); !ok {
		t.Fatal("sweep dropped a machine with a running standard execution")
	}
	hold.Release()
	keptEx := waitExec(t, s, *keptRun.ExecutionArn)
	if keptEx.Status != store.ExecFailed || keptEx.Error != "States.Runtime" || keptEx.Cause != "The state machine has been deleted." {
		t.Fatalf("%s %s %s", keptEx.Status, keptEx.Error, keptEx.Cause)
	}
	s.Store.Sweep()
	if _, ok := s.Store.Machine(*kept.StateMachineArn); ok {
		t.Fatal("terminal standard execution kept the machine")
	}

	expressGate := newGate()
	defer expressGate.Release()
	s.Runner.Boundary = expressGate.Wait
	typ := gen.StateMachineTypeEXPRESS
	fast, err := s.CreateStateMachine(context.Background(), &gen.CreateStateMachineInput{
		Name: sp("fleet"), Definition: sp(passDef), RoleArn: sp(roleARN), Type: &typ,
	})
	if err != nil {
		t.Fatal(err)
	}
	syncDone := make(chan *gen.StartSyncExecutionOutput, 1)
	syncErr := make(chan error, 1)
	go func() {
		out, err := s.StartSyncExecution(context.Background(), &gen.StartSyncExecutionInput{
			StateMachineArn: fast.StateMachineArn, Name: sp("run"), Input: sp(`{}`),
		})
		if err != nil {
			syncErr <- err
			return
		}
		syncDone <- out
	}()
	expressGate.waitArrived(t)
	if _, err := s.DeleteStateMachine(context.Background(), &gen.DeleteStateMachineInput{StateMachineArn: fast.StateMachineArn}); err != nil {
		t.Fatal(err)
	}
	s.Store.Sweep()
	if _, ok := s.Store.Machine(*fast.StateMachineArn); ok {
		t.Fatal("express execution kept the machine")
	}
	expressGate.Release()
	select {
	case out := <-syncDone:
		if *out.Status != gen.SyncExecutionStatusFAILED || *out.Error != "States.Runtime" || *out.Cause != "The state machine has been deleted." {
			t.Fatal(jsonText(t, out))
		}
	case err := <-syncErr:
		t.Fatal(err)
	case <-time.After(3 * time.Second):
		t.Fatal("sync did not return")
	}

	s.Runner.Boundary = nil
	clk := setClock(s, time.Unix(1_700_000_000, 0).UTC())
	timed := createMachine(t, s, "timed", `{"StartAt":"P","TimeoutSeconds":1,"States":{"P":{"Type":"Pass","End":true}}}`, roleARN)
	block := newGate()
	defer block.Release()
	s.Runner.Boundary = block.Wait
	timedRun := startExec(t, s, *timed.StateMachineArn, "run", `{}`)
	block.waitArrived(t)
	*clk = clk.Add(2 * time.Second)
	block.Release()
	timedEx := waitExec(t, s, *timedRun.ExecutionArn)
	if timedEx.Status != store.ExecTimedOut || timedEx.Error != "States.Timeout" {
		t.Fatalf("%s %s %s", timedEx.Status, timedEx.Error, timedEx.Cause)
	}
	assertTypes(t, timedEx, "ExecutionStarted", "ExecutionTimedOut")

	s.Runner.Boundary = nil
	s.Runner.Now = nil
	capGate := newGate()
	defer capGate.Release()
	s.Runner.Boundary = capGate.Wait
	capped := createMachine(t, s, "capped", passDef, roleARN)
	capRun := startExec(t, s, *capped.StateMachineArn, "run", `{}`)
	capGate.waitArrived(t)
	for i := 0; i < 24998; i++ {
		if err := s.Store.Append(*capRun.ExecutionArn, store.HistoryEvent{Type: "PassStateEntered", Time: time.Unix(1, 0)}); err != nil {
			t.Fatal(i, err)
		}
	}
	capGate.Release()
	capEx := waitExec(t, s, *capRun.ExecutionArn)
	if capEx.Status != store.ExecFailed || capEx.Error != "States.Runtime" || !strings.Contains(capEx.Cause, "25000") || len(capEx.History) != 25000 {
		t.Fatalf("%s %s %s len %d", capEx.Status, capEx.Error, capEx.Cause, len(capEx.History))
	}
	if capEx.History[len(capEx.History)-1].Type != "ExecutionFailed" {
		t.Fatal(capEx.History[len(capEx.History)-1].Type)
	}

	huge := strings.Repeat("a", 262144)
	wide := createMachine(t, s, "wide", `{"StartAt":"P","States":{"P":{"Type":"Pass","Result":"`+huge+`","End":true}}}`, roleARN)
	wideRun := startExec(t, s, *wide.StateMachineArn, "run", `{}`)
	wideEx := waitExec(t, s, *wideRun.ExecutionArn)
	if wideEx.Error != "States.DataLimitExceeded" || !strings.Contains(wideEx.Cause, "262144") {
		t.Fatalf("%s %s", wideEx.Error, wideEx.Cause)
	}
	assertTypes(t, wideEx, "ExecutionStarted", "PassStateEntered", "ExecutionFailed")
}
