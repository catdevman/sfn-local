package api

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/catdevman/sfn-local/internal/api/gen"
	"github.com/catdevman/sfn-local/internal/exec"
)

func TestTestStateOneState(t *testing.T) {
	t.Run("pass", func(t *testing.T) {
		s := newService()
		noTestLeak(t, s)
		out := mustTest(t, s, `{"Type":"Pass","End":true}`, `{"z":1}`)
		if got := statusOf(out); got != "SUCCEEDED" || str(out.Output) != `{"z":1}` || out.NextState != nil || out.Error != nil {
			t.Fatalf("%s output %s next %v err %v", got, str(out.Output), out.NextState, out.Error)
		}
	})
	t.Run("input path", func(t *testing.T) {
		s := newService()
		noTestLeak(t, s)
		out := mustTest(t, s, `{"Type":"Pass","InputPath":"$.a","End":true}`, `{"a":{"n":1},"b":2}`)
		if str(out.Output) != `{"n":1}` || statusOf(out) != "SUCCEEDED" {
			t.Fatalf("%+v", out)
		}
	})
	t.Run("succeed and fail", func(t *testing.T) {
		s := newService()
		noTestLeak(t, s)
		out := mustTest(t, s, `{"Type":"Succeed"}`, `{"k":1}`)
		if statusOf(out) != "SUCCEEDED" || str(out.Output) != `{"k":1}` || out.NextState != nil {
			t.Fatalf("%+v", out)
		}
		out = mustTest(t, s, `{"Type":"Fail","Error":"Boom","Cause":"no"}`, `{}`)
		if statusOf(out) != "FAILED" || str(out.Error) != "Boom" || str(out.Cause) != "no" || out.Output != nil || out.NextState != nil {
			t.Fatalf("%+v", out)
		}
	})
	t.Run("choice stops", func(t *testing.T) {
		s := newService()
		noTestLeak(t, s)
		def := `{"StartAt":"C","States":{"C":{"Type":"Choice","Choices":[{"Variable":"$.ok","BooleanEquals":true,"Next":"Boom"}],"Default":"Later"},"Boom":{"Type":"Fail","Error":"Nope","Cause":"ran"},"Later":{"Type":"Pass","End":true}}}`
		out := mustTest(t, s, def, `{"ok":true}`)
		if statusOf(out) != "SUCCEEDED" || str(out.NextState) != "Boom" || str(out.Output) != `{"ok":true}` || out.Error != nil {
			t.Fatalf("%+v", out)
		}
	})
	t.Run("wait stops", func(t *testing.T) {
		s := newService()
		noTestLeak(t, s)
		def := `{"StartAt":"W","States":{"W":{"Type":"Wait","Seconds":0,"Next":"Boom"},"Boom":{"Type":"Fail","Error":"Nope","Cause":"ran"}}}`
		out := mustTest(t, s, def, `{"z":1}`)
		if statusOf(out) != "SUCCEEDED" || str(out.NextState) != "Boom" || str(out.Output) != `{"z":1}` {
			t.Fatalf("%+v", out)
		}
	})
	t.Run("state name", func(t *testing.T) {
		s := newService()
		noTestLeak(t, s)
		def := `{"StartAt":"P","States":{"P":{"Type":"Pass","Result":{"from":"P"},"Next":"S"},"S":{"Type":"Pass","Result":{"from":"S"},"End":true}}}`
		out := mustTest(t, s, def, `{}`)
		if statusOf(out) != "SUCCEEDED" || str(out.Output) != `{"from":"P"}` || str(out.NextState) != "S" {
			t.Fatalf("start %+v", out)
		}
		named, err := s.TestState(context.Background(), &gen.TestStateInput{Definition: sp(def), StateName: sp("S"), Input: sp(`{}`)})
		if err != nil {
			t.Fatal(err)
		}
		if statusOf(named) != "SUCCEEDED" || str(named.Output) != `{"from":"S"}` || named.NextState != nil {
			t.Fatalf("named %+v", named)
		}
		_, err = s.TestState(context.Background(), &gen.TestStateInput{Definition: sp(`{"Type":"Pass","End":true}`), StateName: sp("P")})
		if api := mustAPI(t, err); api.ErrorName() != "InvalidDefinition" || !strings.Contains(api.Error(), "stateName") {
			t.Fatalf("%v", err)
		}
		_, err = s.TestState(context.Background(), &gen.TestStateInput{Definition: sp(def), StateName: sp("Missing")})
		if api := mustAPI(t, err); api.ErrorName() != "InvalidDefinition" || !strings.Contains(api.Error(), "Missing") {
			t.Fatalf("%v", err)
		}
	})
}

func TestTestStateTaskAndLambda(t *testing.T) {
	s := newService()
	noTestLeak(t, s)
	task := `{"Type":"Task","Resource":"arn:aws:states:::sqs:sendMessage","Parameters":{},"End":true}`
	out := mustTest(t, s, task, `{}`)
	if statusOf(out) != "FAILED" || str(out.Error) != "States.Runtime" || !strings.Contains(str(out.Cause), "unsupported resource") || !strings.Contains(str(out.Cause), "sqs:sendMessage") {
		t.Fatalf("%+v", out)
	}
	mocked, err := s.TestState(context.Background(), &gen.TestStateInput{
		Definition: sp(`{"Type":"Task","Resource":"arn:aws:states:::sqs:sendMessage","Parameters":{},"ResultSelector":{"id.$":"$.MessageId"},"End":true}`),
		Input:      sp(`{}`),
		Mock:       &gen.MockInput{Result: sp(`{"MessageId":"abc"}`)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if statusOf(mocked) != "SUCCEEDED" || str(mocked.Output) != `{"id":"abc"}` {
		t.Fatalf("%+v", mocked)
	}

	var sleeps []time.Duration
	s.Runner.Sleep = func(ctx context.Context, d time.Duration) error {
		sleeps = append(sleeps, d)
		return nil
	}
	retried, err := s.TestState(context.Background(), &gen.TestStateInput{
		Definition: sp(`{"Type":"Task","Resource":"arn:aws:states:::sqs:sendMessage","Parameters":{},"Retry":[{"ErrorEquals":["States.ALL"],"IntervalSeconds":1,"MaxAttempts":1}],"End":true}`),
		Input:      sp(`{}`),
		Mock:       &gen.MockInput{ErrorOutput: &gen.MockErrorOutput{Error: sp("Boom"), Cause: sp("once")}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if statusOf(retried) != "FAILED" || str(retried.Error) != "Boom" || str(retried.Cause) != "once" || len(sleeps) != 1 || sleeps[0] != time.Second {
		t.Fatalf("sleeps %v out %+v", sleeps, retried)
	}

	caught, err := s.TestState(context.Background(), &gen.TestStateInput{
		Definition: sp(`{"StartAt":"Call","States":{"Call":{"Type":"Task","Resource":"arn:aws:states:::sqs:sendMessage","Parameters":{},"Retry":[{"ErrorEquals":["States.ALL"],"IntervalSeconds":0,"MaxAttempts":0}],"Catch":[{"ErrorEquals":["CustomError"],"Next":"Later","ResultPath":"$.err"}],"End":true},"Later":{"Type":"Pass","Result":{"ran":true},"End":true}}}`),
		Input:      sp(`{"a":1}`),
		Mock:       &gen.MockInput{ErrorOutput: &gen.MockErrorOutput{Error: sp("CustomError"), Cause: sp("once")}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if statusOf(caught) != "CAUGHT_ERROR" || str(caught.Error) != "CustomError" || str(caught.Cause) != "once" || str(caught.NextState) != "Later" || str(caught.Output) != `{"a":1,"err":{"Error":"CustomError","Cause":"once"}}` {
		t.Fatalf("%+v", caught)
	}
	if str(caught.Output) == `{"ran":true}` {
		t.Fatalf("successor ran: %+v", caught)
	}

	called := 0
	s.Runner.LambdaEndpoint = "http://lambda.invalid"
	s.Runner.Invoke = func(ctx context.Context, call exec.LambdaCall) exec.LambdaReply {
		called++
		if call.Function != "unused" || call.Payload != `{"a":1}` {
			t.Fatalf("call %+v", call)
		}
		return exec.LambdaReply{Status: 200, Payload: `{"from":"lambda"}`}
	}
	invoked, err := s.TestState(context.Background(), &gen.TestStateInput{
		Definition: sp(`{"Type":"Task","Resource":"` + lambdaARN + `","End":true}`),
		Input:      sp(`{"a":1}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if called != 1 || statusOf(invoked) != "SUCCEEDED" || str(invoked.Output) != `{"from":"lambda"}` {
		t.Fatalf("called %d %+v", called, invoked)
	}
	mockedLambda, err := s.TestState(context.Background(), &gen.TestStateInput{
		Definition: sp(`{"Type":"Task","Resource":"` + lambdaARN + `","End":true}`),
		Input:      sp(`{}`),
		Mock:       &gen.MockInput{Result: sp(`{"ok":true}`)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if called != 1 || statusOf(mockedLambda) != "SUCCEEDED" || str(mockedLambda.Output) != `{"ok":true}` {
		t.Fatalf("called %d %+v", called, mockedLambda)
	}
}

func TestTestStateRefusesUnimplemented(t *testing.T) {
	s := newService()
	noTestLeak(t, s)
	activity := `{"Type":"Task","Resource":"arn:aws:states:us-east-1:123456789012:activity:work","End":true}`
	_, err := s.TestState(context.Background(), &gen.TestStateInput{
		Definition: sp(activity),
		Mock:       &gen.MockInput{Result: sp(`{"ok":true}`)},
	})
	if api := mustAPI(t, err); api.ErrorName() != "ValidationException" || !strings.Contains(api.Error(), "Activity tasks are not implemented") {
		t.Fatalf("%v", err)
	}
	_, err = s.TestState(context.Background(), &gen.TestStateInput{
		Definition: sp(`{"Type":"Pass","End":true}`),
		Mock:       &gen.MockInput{Result: sp(`{}`)},
	})
	if api := mustAPI(t, err); api.ErrorName() != "ValidationException" || !strings.Contains(api.Error(), "mock can only be specified") {
		t.Fatalf("%v", err)
	}
	_, err = s.TestState(context.Background(), &gen.TestStateInput{
		Definition: sp(`{"StartAt":"P","QueryLanguage":"JSONata","States":{"P":{"Type":"Pass","End":true}}}`),
	})
	if api := mustAPI(t, err); api.ErrorName() != "InvalidDefinition" || !strings.Contains(api.Error(), "QueryLanguage") {
		t.Fatalf("%v", err)
	}
	_, err = s.TestState(context.Background(), &gen.TestStateInput{Definition: sp(passDef), Variables: sp(`{}`)})
	if api := mustAPI(t, err); api.ErrorName() != "ValidationException" || !strings.Contains(api.Error(), "variables are not implemented") {
		t.Fatalf("%v", err)
	}
	_, err = s.TestState(context.Background(), &gen.TestStateInput{Definition: sp(passDef), Context: sp(`{}`)})
	if api := mustAPI(t, err); api.ErrorName() != "ValidationException" || !strings.Contains(api.Error(), "context can only be specified") {
		t.Fatalf("%v", err)
	}
	_, err = s.TestState(context.Background(), &gen.TestStateInput{
		Definition: sp(`{"Type":"Task","Resource":"arn:aws:states:::sqs:sendMessage","Parameters":{},"End":true}`),
		Mock:       &gen.MockInput{Result: sp(`{}`)},
		Context:    sp(`{}`),
	})
	if api := mustAPI(t, err); api.ErrorName() != "ValidationException" || !strings.Contains(api.Error(), "context is not implemented") {
		t.Fatalf("%v", err)
	}
	_, err = s.TestState(context.Background(), &gen.TestStateInput{
		Definition:         sp(passDef),
		StateConfiguration: &gen.TestStateConfiguration{},
	})
	if api := mustAPI(t, err); api.ErrorName() != "ValidationException" || !strings.Contains(api.Error(), "stateConfiguration is not implemented") {
		t.Fatalf("%v", err)
	}
	_, err = s.TestState(context.Background(), &gen.TestStateInput{Definition: sp(passDef), Input: sp(`not-json`)})
	if api := mustAPI(t, err); api.ErrorName() != "InvalidExecutionInput" {
		t.Fatalf("%v", err)
	}
	_, err = s.TestState(context.Background(), &gen.TestStateInput{Definition: sp(passDef), RoleArn: sp("arn:aws:iam::123456789012:role")})
	if api := mustAPI(t, err); api.ErrorName() != "InvalidArn" || !strings.Contains(api.Error(), "Invalid Role Arn") {
		t.Fatalf("%v", err)
	}
	out := mustTestRole(t, s)
	if statusOf(out) != "SUCCEEDED" {
		t.Fatalf("%+v", out)
	}
	deadline, err := s.TestState(context.Background(), &gen.TestStateInput{
		Definition: sp(`{"StartAt":"P","TimeoutSeconds":0,"States":{"P":{"Type":"Pass","End":true}}}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if statusOf(deadline) != "FAILED" || str(deadline.Error) != "States.Timeout" || deadline.Output != nil {
		t.Fatalf("%+v", deadline)
	}
}

func TestTestStateDoesNotPublish(t *testing.T) {
	s := newService()
	sm := createMachine(t, s, "hold", passDef, roleARN)
	out := mustTest(t, s, passDef, "")
	if statusOf(out) != "SUCCEEDED" || str(out.Output) != `{}` {
		t.Fatalf("%+v", out)
	}
	list, err := s.ListExecutions(context.Background(), &gen.ListExecutionsInput{StateMachineArn: sm.StateMachineArn})
	if err != nil {
		t.Fatal(err)
	}
	if list.Executions == nil || len(*list.Executions) != 0 {
		t.Fatalf("published %+v", list.Executions)
	}
	arn := StateMachineARN(s.region(), s.account(), "TestState")
	if rows := s.Store.Executions(arn); len(rows) != 0 {
		t.Fatalf("leaked %d", len(rows))
	}
	if _, ok := s.Store.Machine(arn); ok {
		t.Fatal("stored a TestState machine")
	}
	if _, ok := s.Store.Machine(*sm.StateMachineArn); !ok {
		t.Fatal("real machine disappeared")
	}
}

func mustTest(t *testing.T, s *Service, def, input string) *gen.TestStateOutput {
	t.Helper()
	in := &gen.TestStateInput{Definition: sp(def)}
	if input != "" {
		in.Input = sp(input)
	}
	out, err := s.TestState(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func mustTestRole(t *testing.T, s *Service) *gen.TestStateOutput {
	t.Helper()
	out, err := s.TestState(context.Background(), &gen.TestStateInput{Definition: sp(passDef), RoleArn: sp(roleARN)})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func noTestLeak(t *testing.T, s *Service) {
	t.Helper()
	machines := len(s.Store.Machines())
	t.Cleanup(func() {
		arn := StateMachineARN(s.region(), s.account(), "TestState")
		if rows := s.Store.Executions(arn); len(rows) != 0 {
			t.Errorf("leaked %d test executions", len(rows))
		}
		if len(s.Store.Machines()) != machines {
			t.Errorf("machines %d want %d", len(s.Store.Machines()), machines)
		}
	})
}

func statusOf(out *gen.TestStateOutput) string {
	if out == nil || out.Status == nil {
		return ""
	}
	return string(*out.Status)
}

func str(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}
