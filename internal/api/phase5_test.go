package api

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/catdevman/sfn-local/internal/api/gen"
	"github.com/catdevman/sfn-local/internal/exec"
	"github.com/catdevman/sfn-local/internal/store"
)

const callbackResource = "arn:aws:states:::sqs:sendMessage.waitForTaskToken"

func TestActivitySuccess(t *testing.T) {
	s := newService()
	arn := createActivity(t, s, "work")
	execARN := startTask(t, s, "act", arn, `"HeartbeatSeconds":30,"Parameters":{"token.$":"$$.Task.Token","n.$":"$.n"}`, "run", `{"n":1}`)
	scheduled := waitHistory(t, s, execARN, "ActivityScheduled")
	token := jsonField(t, scheduled.Input, "token")
	_, err := s.SendTaskHeartbeat(context.Background(), &gen.SendTaskHeartbeatInput{TaskToken: sp(token)})
	if api := mustAPI(t, err); api.ErrorName() != "TaskDoesNotExist" || !strings.Contains(api.Error(), "Task Does Not Exist: '"+token+"'") {
		t.Fatal(api)
	}
	got := pollTask(t, s, arn, "worker-a")
	if str(got.TaskToken) != token || !strings.Contains(str(got.Input), `"n":1`) || !strings.Contains(str(got.Input), token) {
		t.Fatalf("%+v", got)
	}
	if _, err := s.SendTaskSuccess(context.Background(), &gen.SendTaskSuccessInput{TaskToken: sp(token), Output: sp(` { "ok" : true } `)}); err != nil {
		t.Fatal(err)
	}
	ex := waitExec(t, s, execARN)
	if ex.Status != store.ExecSucceeded || ex.Output != `{"ok":true}` {
		t.Fatalf("%s %s %s", ex.Status, ex.Output, ex.Cause)
	}
	assertTypes(t, ex, "ExecutionStarted", "TaskStateEntered", "ActivityScheduled", "ActivityStarted", "ActivitySucceeded", "TaskStateExited", "ExecutionSucceeded")
	started, ok := findEvent(ex, "ActivityStarted")
	if !ok || !started.HasWorker || started.Worker != "worker-a" {
		t.Fatalf("%+v", started)
	}
	_, err = s.SendTaskSuccess(context.Background(), &gen.SendTaskSuccessInput{TaskToken: sp(token), Output: sp(`{}`)})
	if api := mustAPI(t, err); api.ErrorName() != "TaskDoesNotExist" || !strings.Contains(api.Error(), "Task Does Not Exist: '"+token+"'") {
		t.Fatal(api)
	}
	hist, err := s.GetExecutionHistory(context.Background(), &gen.GetExecutionHistoryInput{ExecutionArn: sp(execARN)})
	if err != nil {
		t.Fatal(err)
	}
	var sawWorker, sawOutput bool
	for _, ev := range *hist.Events {
		if ev.ActivityStartedEventDetails != nil && str(ev.ActivityStartedEventDetails.WorkerName) == "worker-a" {
			sawWorker = true
		}
		if ev.ActivitySucceededEventDetails != nil && str(ev.ActivitySucceededEventDetails.Output) == `{"ok":true}` {
			sawOutput = true
		}
		if ev.ActivityScheduledEventDetails != nil {
			if ev.ActivityScheduledEventDetails.HeartbeatInSeconds == nil || *ev.ActivityScheduledEventDetails.HeartbeatInSeconds != 30 {
				t.Fatalf("heartbeat %+v", ev.ActivityScheduledEventDetails)
			}
			if ev.ActivityScheduledEventDetails.TimeoutInSeconds != nil {
				t.Fatal("unset timeout was emitted")
			}
		}
	}
	if !sawWorker || !sawOutput {
		t.Fatal("history details")
	}
	off := false
	quiet, err := s.GetExecutionHistory(context.Background(), &gen.GetExecutionHistoryInput{ExecutionArn: sp(execARN), IncludeExecutionData: &off})
	if err != nil {
		t.Fatal(err)
	}
	for _, ev := range *quiet.Events {
		if ev.ActivityScheduledEventDetails != nil && ev.ActivityScheduledEventDetails.Input != nil {
			t.Fatal("input included")
		}
		if ev.ActivitySucceededEventDetails != nil && ev.ActivitySucceededEventDetails.Output != nil {
			t.Fatal("output included")
		}
	}
}

func TestActivityPollDoesNotBlockLists(t *testing.T) {
	s := newService()
	s.ActivityPoll = 50 * time.Millisecond
	arn := createActivity(t, s, "idle")
	start := time.Now()
	out, err := s.GetActivityTask(context.Background(), &gen.GetActivityTaskInput{ActivityArn: sp(arn)})
	if err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > time.Second {
		t.Fatalf("empty poll took %s", time.Since(start))
	}
	raw, err := json.Marshal(out)
	if err != nil || string(raw) != "{}" {
		t.Fatalf("%s %v", raw, err)
	}
	unknown := "arn:aws:states:us-east-1:123456789012:activity:missing"
	_, err = s.GetActivityTask(context.Background(), &gen.GetActivityTaskInput{ActivityArn: sp(unknown)})
	if api := mustAPI(t, err); api.ErrorName() != "ActivityDoesNotExist" || !strings.Contains(api.Error(), "Activity Does Not Exist: '"+unknown+"'") {
		t.Fatal(api)
	}
	_, err = s.GetActivityTask(context.Background(), &gen.GetActivityTaskInput{ActivityArn: sp("activity")})
	if api := mustAPI(t, err); api.ErrorName() != "InvalidArn" || !strings.Contains(api.Error(), "Invalid Arn: 'activity'") {
		t.Fatal(api)
	}

	s.ActivityPoll = 2 * time.Second
	done := make(chan time.Duration, 1)
	go func() {
		began := time.Now()
		out, err := s.GetActivityTask(context.Background(), &gen.GetActivityTaskInput{ActivityArn: sp(arn)})
		if err != nil || out == nil || out.TaskToken != nil {
			done <- -1
			return
		}
		done <- time.Since(began)
	}()
	time.Sleep(20 * time.Millisecond)
	listBegan := time.Now()
	if _, err := s.ListActivities(context.Background(), &gen.ListActivitiesInput{}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ListStateMachines(context.Background(), &gen.ListStateMachinesInput{}); err != nil {
		t.Fatal(err)
	}
	if time.Since(listBegan) > 500*time.Millisecond {
		t.Fatalf("lists blocked for %s", time.Since(listBegan))
	}
	select {
	case took := <-done:
		if took < 1500*time.Millisecond || took > 3500*time.Millisecond {
			t.Fatalf("poll %s", took)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("poll did not return")
	}
}

func TestActivityFailureCatchAndTimeout(t *testing.T) {
	s := newService()
	arn := createActivity(t, s, "work")
	failedARN := startTask(t, s, "fail", arn, "", "run", `{}`)
	got := pollTask(t, s, arn, "")
	token := str(got.TaskToken)
	if _, err := s.SendTaskFailure(context.Background(), &gen.SendTaskFailureInput{TaskToken: sp(token), Error: sp("Boom"), Cause: sp("nope")}); err != nil {
		t.Fatal(err)
	}
	ex := waitExec(t, s, failedARN)
	if ex.Status != store.ExecFailed || ex.Error != "Boom" || ex.Cause != "nope" || countEvents(ex, "ActivityFailed") != 1 || countEvents(ex, "TaskStateExited") != 0 {
		t.Fatalf("%s %s %s %v", ex.Status, ex.Error, ex.Cause, typesOf(ex))
	}
	started, ok := findEvent(ex, "ActivityStarted")
	if !ok || started.HasWorker {
		t.Fatalf("%+v", started)
	}
	_, err := s.SendTaskSuccess(context.Background(), &gen.SendTaskSuccessInput{TaskToken: sp(token), Output: sp(`{}`)})
	if api := mustAPI(t, err); api.ErrorName() != "TaskDoesNotExist" {
		t.Fatal(api)
	}
}

func TestActivityCatchRetryAndTimeout(t *testing.T) {
	s := newService()
	arn := createActivity(t, s, "work")
	catchDef := `{"StartAt":"T","States":{"T":{"Type":"Task","Resource":"` + arn + `","Catch":[{"ErrorEquals":["States.TaskFailed"],"Next":"P"}],"End":true},"P":{"Type":"Pass","Result":{"ok":true},"End":true}}}`
	sm := createMachine(t, s, "catch", catchDef, roleARN)
	run := startExec(t, s, *sm.StateMachineArn, "run", `{}`)
	got := pollTask(t, s, arn, "w")
	if _, err := s.SendTaskFailure(context.Background(), &gen.SendTaskFailureInput{TaskToken: got.TaskToken, Error: sp("Boom"), Cause: sp("nope")}); err != nil {
		t.Fatal(err)
	}
	ex := waitExec(t, s, *run.ExecutionArn)
	if ex.Status != store.ExecSucceeded || ex.Output != `{"ok":true}` || countEvents(ex, "ActivityFailed") != 1 || countEvents(ex, "TaskStateExited") != 1 {
		t.Fatalf("%s %s %s %v", ex.Status, ex.Output, ex.Cause, typesOf(ex))
	}

	timeoutDef := `{"StartAt":"T","States":{"T":{"Type":"Task","Resource":"` + arn + `","TimeoutSeconds":0,"Parameters":{"token.$":"$$.Task.Token"},"Catch":[{"ErrorEquals":["States.TaskFailed"],"Next":"P"}],"End":true},"P":{"Type":"Pass","Result":{"caught":true},"End":true}}}`
	sm = createMachine(t, s, "timeout-miss", timeoutDef, roleARN)
	run = startExec(t, s, *sm.StateMachineArn, "run", `{}`)
	ex = waitExec(t, s, *run.ExecutionArn)
	if ex.Status != store.ExecFailed || ex.Error != "States.Timeout" || ex.Output == `{"caught":true}` || countEvents(ex, "ActivityScheduled") != 1 || countEvents(ex, "ActivityTimedOut") != 1 || countEvents(ex, "ActivityStarted") != 0 || countEvents(ex, "TaskStateExited") != 0 {
		t.Fatalf("%s %s %s %v", ex.Status, ex.Error, ex.Output, typesOf(ex))
	}
	timed := jsonField(t, findEventMust(t, ex, "ActivityScheduled").Input, "token")
	// Captured from amazon/aws-stepfunctions-local:2.0.0. The argument is the
	// activity ARN, not the task token. SendTaskFailure and SendTaskHeartbeat
	// use the same message.
	wantTimeout := "Task Timed Out: '" + arn + "'"
	for _, check := range []struct {
		name string
		err  error
	}{
		{"success", sendTaskSuccess(t, s, timed)},
		{"failure", sendTaskFailure(t, s, timed)},
		{"heartbeat", sendTaskHeartbeat(t, s, timed)},
	} {
		api := mustAPI(t, check.err)
		if api.ErrorName() != "TaskTimedOut" || api.Error() != "TaskTimedOut: "+wantTimeout {
			t.Fatalf("%s %s", check.name, api.Error())
		}
	}

	retryDef := `{"StartAt":"T","States":{"T":{"Type":"Task","Resource":"` + arn + `","TimeoutSeconds":0,"Parameters":{"token.$":"$$.Task.Token"},"Retry":[{"ErrorEquals":["States.Timeout"],"IntervalSeconds":0,"MaxAttempts":1}],"End":true}}}`
	sm = createMachine(t, s, "retry", retryDef, roleARN)
	run = startExec(t, s, *sm.StateMachineArn, "run", `{}`)
	ex = waitExec(t, s, *run.ExecutionArn)
	if ex.Status != store.ExecFailed || ex.Error != "States.Timeout" || countEvents(ex, "ActivityScheduled") != 2 || countEvents(ex, "ActivityTimedOut") != 2 || countEvents(ex, "TaskStateExited") != 0 {
		t.Fatalf("%s %s %v", ex.Error, ex.Cause, typesOf(ex))
	}
	var tokens []string
	for _, ev := range ex.History {
		if ev.Type == "ActivityScheduled" {
			tokens = append(tokens, jsonField(t, ev.Input, "token"))
		}
	}
	if len(tokens) != 2 || tokens[0] == "" || tokens[0] == tokens[1] {
		t.Fatal(tokens)
	}

	parallel := `{"StartAt":"P","States":{"P":{"Type":"Parallel","Branches":[{"StartAt":"T","States":{"T":{"Type":"Task","Resource":"` + arn + `","TimeoutSeconds":0,"End":true}}}],"End":true}}}`
	sm = createMachine(t, s, "par", parallel, roleARN)
	run = startExec(t, s, *sm.StateMachineArn, "run", `{}`)
	ex = waitExec(t, s, *run.ExecutionArn)
	if ex.Status != store.ExecFailed || ex.Error != "States.Timeout" || countEvents(ex, "ActivityTimedOut") != 1 || countEvents(ex, "ParallelStateFailed") != 1 || countEvents(ex, "ExecutionAborted") != 0 {
		t.Fatalf("%s %s %v", ex.Error, ex.Cause, typesOf(ex))
	}
}

func TestActivityHeartbeat(t *testing.T) {
	s := newService()
	arn := createActivity(t, s, "work")
	immediate := startTask(t, s, "hb0", arn, `"HeartbeatSeconds":0,"TimeoutSeconds":1`, "run", `{}`)
	_ = pollTask(t, s, arn, "w")
	ex := waitExec(t, s, immediate)
	if ex.Status != store.ExecFailed || ex.Error != "States.Timeout" || countEvents(ex, "ActivityStarted") != 1 || countEvents(ex, "ActivityTimedOut") != 1 || countEvents(ex, "ExecutionTimedOut") != 0 {
		t.Fatalf("%s %s %v", ex.Error, ex.Cause, typesOf(ex))
	}

	noTaskTimeout := startTask(t, s, "hb-only", arn, `"HeartbeatSeconds":0`, "run2", `{}`)
	_ = pollTask(t, s, arn, "w")
	ex = waitExec(t, s, noTaskTimeout)
	if ex.Status != store.ExecFailed || ex.Error != "States.Timeout" || countEvents(ex, "ActivityTimedOut") != 1 || countEvents(ex, "ExecutionTimedOut") != 0 {
		t.Fatalf("%s %s %v", ex.Error, ex.Cause, typesOf(ex))
	}

	resetARN := startTask(t, s, "reset", arn, `"HeartbeatSeconds":1,"TimeoutSeconds":30`, "run3", `{}`)
	got := pollTask(t, s, arn, "w")
	time.Sleep(600 * time.Millisecond)
	if _, err := s.SendTaskHeartbeat(context.Background(), &gen.SendTaskHeartbeatInput{TaskToken: got.TaskToken}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(600 * time.Millisecond)
	if _, err := s.SendTaskSuccess(context.Background(), &gen.SendTaskSuccessInput{TaskToken: got.TaskToken, Output: sp(`{"ok":true}`)}); err != nil {
		t.Fatal(err)
	}
	ex = waitExec(t, s, resetARN)
	if ex.Status != store.ExecSucceeded || countEvents(ex, "ActivityTimedOut") != 0 {
		t.Fatalf("%s %s %v", ex.Status, ex.Error, typesOf(ex))
	}

	_, err := s.CreateStateMachine(context.Background(), &gen.CreateStateMachineInput{
		Name: sp("bad-hb"), RoleArn: sp(roleARN),
		Definition: sp(taskDef(arn, `"HeartbeatSeconds":2,"TimeoutSeconds":2`)),
	})
	if api := mustAPI(t, err); api.ErrorName() != "InvalidDefinition" || !strings.Contains(api.Error(), "HeartbeatSeconds must be less than TimeoutSeconds") {
		t.Fatal(api)
	}
	pathARN := startTask(t, s, "paths", arn, `"HeartbeatSecondsPath":"$.hb","TimeoutSecondsPath":"$.timeout"`, "run4", `{"hb":5,"timeout":5}`)
	ex = waitExec(t, s, pathARN)
	if ex.Status != store.ExecFailed || ex.Error != "States.Runtime" || !strings.Contains(ex.Cause, "HeartbeatSeconds must be less than TimeoutSeconds") || countEvents(ex, "ActivityScheduled") != 0 {
		t.Fatalf("%s %s %v", ex.Error, ex.Cause, typesOf(ex))
	}
}

func TestCallbackWaitAndMock(t *testing.T) {
	s := newService()
	invoked := false
	s.Runner.Invoke = func(context.Context, exec.LambdaCall) exec.LambdaReply {
		invoked = true
		return exec.LambdaReply{Err: errInvoked()}
	}
	execARN := startTask(t, s, "cb", callbackResource, `"Parameters":{"token.$":"$$.Task.Token"}`, "run", `{}`)
	waitHistory(t, s, execARN, "TaskSubmitted")
	ex, ok := s.Store.Execution(execARN)
	if !ok {
		t.Fatal("missing")
	}
	scheduled := findEventMust(t, ex, "TaskScheduled")
	if scheduled.ResourceType != "sqs" || scheduled.Resource != "sendMessage.waitForTaskToken" || scheduled.Region != "us-east-1" || !scheduled.HasParameters {
		t.Fatalf("%+v", scheduled)
	}
	token := jsonField(t, scheduled.Parameters, "token")
	submitted := findEventMust(t, ex, "TaskSubmitted")
	if !submitted.HasOutput || submitted.Output != "" {
		t.Fatalf("%+v", submitted)
	}
	if _, err := s.SendTaskSuccess(context.Background(), &gen.SendTaskSuccessInput{TaskToken: sp(token), Output: sp(`{"ok":true}`)}); err != nil {
		t.Fatal(err)
	}
	ex = waitExec(t, s, execARN)
	if invoked || ex.Status != store.ExecSucceeded || ex.Output != `{"ok":true}` {
		t.Fatalf("invoked %v %s %s %s", invoked, ex.Status, ex.Output, ex.Cause)
	}
	assertTypes(t, ex, "ExecutionStarted", "TaskStateEntered", "TaskScheduled", "TaskStarted", "TaskSubmitted", "TaskSucceeded", "TaskStateExited", "ExecutionSucceeded")
	hist, err := s.GetExecutionHistory(context.Background(), &gen.GetExecutionHistoryInput{ExecutionArn: sp(execARN)})
	if err != nil {
		t.Fatal(err)
	}
	for _, ev := range *hist.Events {
		if ev.TaskSubmittedEventDetails != nil && str(ev.TaskSubmittedEventDetails.Output) != "" {
			t.Fatalf("%+v", ev.TaskSubmittedEventDetails)
		}
	}

	s.Runner.Mocks = writeMock(t, `{
		"StateMachines": {"mocked": {"TestCases": {"C": {"T": "Ok"}}}},
		"MockedResponses": {"Ok": {"0": {"Return": {"ok": true}}}}
	}`)
	sm := createMachine(t, s, "mocked", taskDef(callbackResource, `"Parameters":{"token.$":"$$.Task.Token"}`), roleARN)
	run := startExec(t, s, *sm.StateMachineArn+"#C", "m", `{}`)
	mex := waitExec(t, s, *run.ExecutionArn)
	if mex.Status != store.ExecSucceeded || mex.Output != `{"ok":true}` || countEvents(mex, "TaskSubmitted") != 0 || countEvents(mex, "TaskScheduled") != 1 {
		t.Fatalf("%s %s %v", mex.Status, mex.Output, typesOf(mex))
	}
	if jsonField(t, findEventMust(t, mex, "TaskScheduled").Parameters, "token") == "" {
		t.Fatal("mock callback had no token")
	}

	stopARN := startTask(t, s, "stop", callbackResource, `"Parameters":{"token.$":"$$.Task.Token"}`, "stop", `{}`)
	waitHistory(t, s, stopARN, "TaskSubmitted")
	stopped, _ := s.Store.Execution(stopARN)
	stopToken := jsonField(t, findEventMust(t, stopped, "TaskScheduled").Parameters, "token")
	if _, err := s.StopExecution(context.Background(), &gen.StopExecutionInput{ExecutionArn: sp(stopARN)}); err != nil {
		t.Fatal(err)
	}
	sex := waitExec(t, s, stopARN)
	if sex.Status != store.ExecAborted || countEvents(sex, "ExecutionAborted") != 1 || countEvents(sex, "TaskSucceeded") != 0 || countEvents(sex, "TaskFailed") != 0 {
		t.Fatalf("%s %v", sex.Status, typesOf(sex))
	}
	_, err = s.SendTaskSuccess(context.Background(), &gen.SendTaskSuccessInput{TaskToken: sp(stopToken), Output: sp(`{}`)})
	if api := mustAPI(t, err); api.ErrorName() != "TaskDoesNotExist" || !strings.Contains(api.Error(), "Task Does Not Exist: '"+stopToken+"'") {
		t.Fatal(api)
	}

	cb := startTask(t, s, "cb-hb", callbackResource, `"HeartbeatSeconds":0,"Parameters":{"token.$":"$$.Task.Token"}`, "hb", `{}`)
	cex := waitExec(t, s, cb)
	if cex.Status != store.ExecFailed || cex.Error != "States.Timeout" || countEvents(cex, "TaskSubmitted") != 1 || countEvents(cex, "TaskTimedOut") != 1 || countEvents(cex, "ExecutionTimedOut") != 0 {
		t.Fatalf("%s %s %v", cex.Error, cex.Cause, typesOf(cex))
	}
	cbToken := jsonField(t, findEventMust(t, cex, "TaskScheduled").Parameters, "token")
	cbTimeout := "Task Timed Out: 'arn:aws:states:us-east-1:123456789012:sqs:sendMessage.waitForTaskToken'"
	if api := mustAPI(t, sendTaskSuccess(t, s, cbToken)); api.ErrorName() != "TaskTimedOut" || api.Error() != "TaskTimedOut: "+cbTimeout {
		t.Fatal(api.Error())
	}
	if api := mustAPI(t, sendTaskHeartbeat(t, s, cbToken)); api.ErrorName() != "TaskTimedOut" || api.Error() != "TaskTimedOut: "+cbTimeout {
		t.Fatal(api.Error())
	}

	noTestLeak(t, s)
	out, err := s.TestState(context.Background(), &gen.TestStateInput{
		Definition: sp(`{"Type":"Task","Resource":"` + callbackResource + `","Parameters":{"token.$":"$$.Task.Token"},"End":true}`),
		Mock:       &gen.MockInput{Result: sp(`{"ok":true}`)},
	})
	if err != nil || statusOf(out) != "SUCCEEDED" || str(out.Output) != `{"ok":true}` {
		t.Fatalf("%v %+v", err, out)
	}
}

func TestTaskTokenRejections(t *testing.T) {
	s := newService()
	_, err := s.SendTaskSuccess(context.Background(), &gen.SendTaskSuccessInput{TaskToken: sp("missing"), Output: sp(`{}`)})
	if api := mustAPI(t, err); api.ErrorName() != "InvalidToken" || !strings.Contains(api.Error(), "Invalid Token: 'missing'") {
		t.Fatal(api)
	}
	_, err = s.SendTaskSuccess(context.Background(), &gen.SendTaskSuccessInput{TaskToken: sp("missing"), Output: sp(`not-json`)})
	if api := mustAPI(t, err); api.ErrorName() != "InvalidOutput" || !strings.Contains(api.Error(), "Invalid Output:") {
		t.Fatal(api)
	}
	_, err = s.SendTaskSuccess(context.Background(), &gen.SendTaskSuccessInput{TaskToken: sp("missing"), Output: sp(strings.Repeat("1", 262145))})
	if api := mustAPI(t, err); api.ErrorName() != "InvalidOutput" || !strings.Contains(api.Error(), "262144") {
		t.Fatal(api)
	}
	_, err = s.SendTaskHeartbeat(context.Background(), &gen.SendTaskHeartbeatInput{TaskToken: sp("missing")})
	if api := mustAPI(t, err); api.ErrorName() != "InvalidToken" {
		t.Fatal(api)
	}
}

func createActivity(t *testing.T, s *Service, name string) string {
	t.Helper()
	out, err := s.CreateActivity(context.Background(), &gen.CreateActivityInput{Name: sp(name)})
	if err != nil {
		t.Fatal(err)
	}
	return str(out.ActivityArn)
}

func sendTaskSuccess(t *testing.T, s *Service, token string) error {
	t.Helper()
	_, err := s.SendTaskSuccess(context.Background(), &gen.SendTaskSuccessInput{TaskToken: sp(token), Output: sp(`{"late":true}`)})
	return err
}

func sendTaskFailure(t *testing.T, s *Service, token string) error {
	t.Helper()
	_, err := s.SendTaskFailure(context.Background(), &gen.SendTaskFailureInput{TaskToken: sp(token), Error: sp("Boom"), Cause: sp("no")})
	return err
}

func sendTaskHeartbeat(t *testing.T, s *Service, token string) error {
	t.Helper()
	_, err := s.SendTaskHeartbeat(context.Background(), &gen.SendTaskHeartbeatInput{TaskToken: sp(token)})
	return err
}

func taskDef(resource, fields string) string {
	if fields != "" {
		fields = "," + fields
	}
	return `{"StartAt":"T","States":{"T":{"Type":"Task","Resource":"` + resource + `"` + fields + `,"End":true}}}`
}

func startTask(t *testing.T, s *Service, name, resource, fields, run, input string) string {
	t.Helper()
	sm := createMachine(t, s, name, taskDef(resource, fields), roleARN)
	out := startExec(t, s, *sm.StateMachineArn, run, input)
	return *out.ExecutionArn
}

func pollTask(t *testing.T, s *Service, arn, worker string) *gen.GetActivityTaskOutput {
	t.Helper()
	in := &gen.GetActivityTaskInput{ActivityArn: sp(arn)}
	if worker != "" {
		in.WorkerName = sp(worker)
	}
	out, err := s.GetActivityTask(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if out == nil || out.TaskToken == nil || *out.TaskToken == "" {
		t.Fatalf("%+v", out)
	}
	return out
}

func waitHistory(t *testing.T, s *Service, arn, typ string) store.HistoryEvent {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		ex, ok := s.Store.Execution(arn)
		if ok {
			for _, ev := range ex.History {
				if ev.Type == typ {
					return ev
				}
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", typ)
	return store.HistoryEvent{}
}

func findEvent(ex store.Execution, typ string) (store.HistoryEvent, bool) {
	for _, ev := range ex.History {
		if ev.Type == typ {
			return ev, true
		}
	}
	return store.HistoryEvent{}, false
}

func findEventMust(t *testing.T, ex store.Execution, typ string) store.HistoryEvent {
	t.Helper()
	ev, ok := findEvent(ex, typ)
	if !ok {
		t.Fatalf("missing %s in %v", typ, typesOf(ex))
	}
	return ev
}

func jsonField(t *testing.T, text, key string) string {
	t.Helper()
	var doc map[string]any
	if err := json.Unmarshal([]byte(text), &doc); err != nil {
		t.Fatalf("%s: %v", text, err)
	}
	s, _ := doc[key].(string)
	if s == "" {
		t.Fatalf("missing %s in %s", key, text)
	}
	return s
}

func errInvoked() error { return errString("invoked") }

type errString string

func (e errString) Error() string { return string(e) }
