package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/catdevman/sfn-local/internal/api/gen"
	"github.com/catdevman/sfn-local/internal/exec"
	"github.com/catdevman/sfn-local/internal/mock"
	"github.com/catdevman/sfn-local/internal/store"
)

const lambdaARN = "arn:aws:lambda:us-east-1:123456789012:function:unused"

func writeMock(t *testing.T, body string) *mock.Repo {
	t.Helper()
	path := filepath.Join(t.TempDir(), "MockConfigFile.json")
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return &mock.Repo{Path: path}
}

func countEvents(ex store.Execution, typ string) int {
	n := 0
	for _, ev := range ex.History {
		if ev.Type == typ {
			n++
		}
	}
	return n
}

func eventIndex(ex store.Execution, typ string) int {
	for i, ev := range ex.History {
		if ev.Type == typ {
			return i
		}
	}
	return -1
}

func jsonEqual(t *testing.T, got, want string) {
	t.Helper()
	var a, b any
	if err := json.Unmarshal([]byte(got), &a); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(want), &b); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("got %s want %s", got, want)
	}
}

func TestRetryOnceAndCatch(t *testing.T) {
	s := newService()
	s.Runner.Mocks = writeMock(t, `{
		"StateMachines": {"retry-sm": {"TestCases": {"RetryOnce": {"Call": "Flaky"}, "Catch": {"Call": "Always"}}}},
		"MockedResponses": {
			"Flaky": {
				"0": {"Throw": {"Error": "CustomError", "Cause": "once"}},
				"1": {"Return": {"ok": true}}
			},
			"Always": {"0": {"Throw": {"Error": "CustomError", "Cause": "once"}}}
		}
	}`)
	retryDef := `{"StartAt":"Call","States":{"Call":{"Type":"Task","Resource":"` + lambdaARN + `","Retry":[{"ErrorEquals":["CustomError"],"IntervalSeconds":0,"MaxAttempts":2}],"End":true}}}`
	sm := createMachine(t, s, "retry-sm", retryDef, roleARN)
	run := startExec(t, s, *sm.StateMachineArn+"#RetryOnce", "run", `{}`)
	ex := waitExec(t, s, *run.ExecutionArn)
	if ex.Status != store.ExecSucceeded || ex.Output != `{"ok":true}` {
		t.Fatalf("%s %s %s", ex.Status, ex.Output, ex.Cause)
	}
	if countEvents(ex, "TaskStateEntered") != 1 || countEvents(ex, "TaskStateExited") != 1 {
		t.Fatalf("entered %d exited %d", countEvents(ex, "TaskStateEntered"), countEvents(ex, "TaskStateExited"))
	}
	if countEvents(ex, "LambdaFunctionScheduled") != 2 || countEvents(ex, "LambdaFunctionSucceeded") != 1 {
		t.Fatalf("lambda events %+v", typesOf(ex))
	}

	catchDef := `{"StartAt":"Call","States":{"Call":{"Type":"Task","Resource":"` + lambdaARN + `","Catch":[{"ErrorEquals":["CustomError"],"ResultPath":"$.err","Next":"Done"}],"End":true},"Done":{"Type":"Succeed"}}}`
	caught := createMachine(t, s, "catch-sm", catchDef, roleARN)
	s.Runner.Mocks = writeMock(t, `{
		"StateMachines": {"catch-sm": {"TestCases": {"Catch": {"Call": "Always"}}}},
		"MockedResponses": {"Always": {"0": {"Throw": {"Error": "CustomError", "Cause": "once"}}}}
	}`)
	crun := startExec(t, s, *caught.StateMachineArn+"#Catch", "run", `{}`)
	cex := waitExec(t, s, *crun.ExecutionArn)
	if cex.Status != store.ExecSucceeded {
		t.Fatalf("%s %s %s", cex.Status, cex.Output, cex.Cause)
	}
	if cex.Output != `{"err":{"Error":"CustomError","Cause":"once"}}` {
		jsonEqual(t, cex.Output, `{"err":{"Error":"CustomError","Cause":"once"}}`)
		t.Fatalf("order %s", cex.Output)
	}
	exited := eventIndex(cex, "TaskStateExited")
	if exited < 0 || eventIndex(cex, "SucceedStateEntered") < exited || eventIndex(cex, "SucceedStateExited") < exited || eventIndex(cex, "ExecutionSucceeded") < exited {
		t.Fatalf("%v", typesOf(cex))
	}
	if countEvents(cex, "TaskStateEntered") != 1 {
		t.Fatal(countEvents(cex, "TaskStateEntered"))
	}
}

func TestMockFileAtStart(t *testing.T) {
	s := newService()
	sm := createMachine(t, s, "plain", passDef, roleARN)
	missing := filepath.Join(t.TempDir(), "absent.json")
	s.Runner.Mocks = &mock.Repo{Path: missing}
	out := startExec(t, s, *sm.StateMachineArn, "free", `{}`)
	ex := waitExec(t, s, *out.ExecutionArn)
	if ex.Status != store.ExecSucceeded {
		t.Fatal(ex.Status)
	}
	_, err := s.StartExecution(context.Background(), &gen.StartExecutionInput{StateMachineArn: sp(*sm.StateMachineArn + "#Missing"), Name: sp("m"), Input: sp(`{}`)})
	if api := mustAPI(t, err); api.ErrorName() != "InvalidDefinition" || !strings.Contains(api.Error(), missing) {
		t.Fatal(api)
	}
	s.Runner.Mocks = writeMock(t, `{"StateMachines":{"plain":{"TestCases":{"Known":{}}}},"MockedResponses":{}}`)
	_, err = s.StartExecution(context.Background(), &gen.StartExecutionInput{StateMachineArn: sp(*sm.StateMachineArn + "#"), Name: sp("trail"), Input: sp(`{}`)})
	if api := mustAPI(t, err); api.ErrorName() != "InvalidDefinition" || !strings.Contains(api.Error(), "Unknown mock test") {
		t.Fatal(api)
	}
	_, err = s.StartExecution(context.Background(), &gen.StartExecutionInput{StateMachineArn: sp(*sm.StateMachineArn + "#Other"), Name: sp("o"), Input: sp(`{}`)})
	if api := mustAPI(t, err); api.ErrorName() != "InvalidDefinition" || !strings.Contains(api.Error(), "Unknown mock test: Other") {
		t.Fatal(api)
	}
	overlap := writeMock(t, `{
		"StateMachines": {"plain": {"TestCases": {"Bad": {"S": "R"}}}},
		"MockedResponses": {"R": {"0-2": {"Return": 1}, "1": {"Return": 2}}}
	}`)
	s.Runner.Mocks = overlap
	_, err = s.StartExecution(context.Background(), &gen.StartExecutionInput{StateMachineArn: sp(*sm.StateMachineArn + "#Bad"), Name: sp("b"), Input: sp(`{}`)})
	if api := mustAPI(t, err); api.ErrorName() != "InvalidDefinition" || !strings.Contains(api.Error(), "Overlapping mock selectors in response R") {
		t.Fatal(api)
	}
	_, err = s.StartExecution(context.Background(), &gen.StartExecutionInput{StateMachineArn: sp(*sm.StateMachineArn + "#A#B"), Name: sp("two"), Input: sp(`{}`)})
	if mustAPI(t, err).ErrorName() != "InvalidArn" {
		t.Fatal(err)
	}
}

func TestMockReloadKeepsAttemptAndClearsRetry(t *testing.T) {
	s := newService()
	path := filepath.Join(t.TempDir(), "MockConfigFile.json")
	body := `{
		"StateMachines": {"loop": {"TestCases": {"Keep": {"T": "Seq"}}}},
		"MockedResponses": {"Seq": {
			"0": {"Throw": {"Error": "CustomError", "Cause": "a"}},
			"1": {"Return": {"n": 1}},
			"2": {"Throw": {"Error": "CustomError", "Cause": "b"}},
			"3": {"Return": {"n": 2}}
		}}
	}`
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	s.Runner.Mocks = &mock.Repo{Path: path}
	scale := float32(0)
	s.Runner.WaitScale = &scale
	var mu sync.Mutex
	var slept []time.Duration
	s.Runner.Sleep = func(_ context.Context, d time.Duration) error {
		mu.Lock()
		slept = append(slept, d)
		mu.Unlock()
		return nil
	}
	var visits int
	s.Runner.Boundary = func(arn string) {
		visits++
		if visits > 12 {
			s.Store.Cancel(arn)
		}
	}
	def := `{"StartAt":"T","States":{"T":{"Type":"Task","Resource":"` + lambdaARN + `","Retry":[{"ErrorEquals":["CustomError"],"IntervalSeconds":2,"MaxAttempts":3,"BackoffRate":2}],"Next":"C"},"C":{"Type":"Choice","Choices":[{"Variable":"$.n","NumericEquals":1,"Next":"T"}],"Default":"Done"},"Done":{"Type":"Succeed"}}}`
	sm := createMachine(t, s, "loop", def, roleARN)
	run := startExec(t, s, *sm.StateMachineArn+"#Keep", "run", `{}`)
	ex := waitExec(t, s, *run.ExecutionArn)
	if ex.Status != store.ExecSucceeded || ex.Output != `{"n":2}` {
		t.Fatalf("%s %s %s", ex.Status, ex.Output, ex.Cause)
	}
	if !reflect.DeepEqual(slept, []time.Duration{2 * time.Second, 2 * time.Second}) {
		t.Fatalf("sleeps %v", slept)
	}
}

func TestMockBehaviors(t *testing.T) {
	s := newService()
	called := false
	s.Runner.Invoke = func(context.Context, exec.LambdaCall) exec.LambdaReply {
		called = true
		return exec.LambdaReply{Status: 200, Payload: `{"no":true}`}
	}
	s.Runner.LambdaEndpoint = "http://127.0.0.1:9"
	s.Runner.Mocks = writeMock(t, `{
		"StateMachines": {"behave": {"TestCases": {"Miss": {"Call": "OnlyOne"}, "Act": {"Call": "Act"}, "Gen": {"Call": "Gen"}, "Pay": {"Call": "Pay"}, "Fn": {"Call": "Fn"}, "Time": {"Call": "Time"}}}},
		"MockedResponses": {
			"OnlyOne": {"1": {"Return": {"n": 1}}},
			"Act": {"0": {"Return": {"n": 1}}},
			"Gen": {"0": {"Return": {"Payload": {"n": 1}}}},
			"Pay": {"0": {"Return": {"Payload": {"ok": true}, "StatusCode": 200}}},
			"Fn": {"0": {"Return": {"FunctionError": "Unhandled", "Payload": {"errorType": "Boom", "errorMessage": "no"}}}},
			"Time": {"0": {"Throw": {"Error": "connection timeout", "Cause": "slow"}}}
		}
	}`)
	miss := createMachine(t, s, "behave", `{"StartAt":"Call","States":{"Call":{"Type":"Task","Resource":"`+lambdaARN+`","End":true}}}`, roleARN)
	run := startExec(t, s, *miss.StateMachineArn+"#Miss", "m", `{}`)
	ex := waitExec(t, s, *run.ExecutionArn)
	if ex.Status != store.ExecFailed || !strings.Contains(ex.Cause, "no mock data was found for attempt 0 of state Call") {
		t.Fatalf("%s %s", ex.Error, ex.Cause)
	}
	if called {
		t.Fatal("missing attempt called the network")
	}

	s.Runner.Mocks = writeMock(t, `{
		"StateMachines": {"sel": {"TestCases": {"S": {"Call": "Raw"}}}},
		"MockedResponses": {"Raw": {"0": {"Return": {"payload": {"n": 1}, "extra": true}}}}
	}`)
	sel := createMachine(t, s, "sel", `{"StartAt":"Call","States":{"Call":{"Type":"Task","Resource":"`+lambdaARN+`","ResultSelector":{"n.$":"$.payload.n"},"End":true}}}`, roleARN)
	srun := startExec(t, s, *sel.StateMachineArn+"#S", "s", `{}`)
	sex := waitExec(t, s, *srun.ExecutionArn)
	if sex.Status != store.ExecSucceeded || sex.Output != `{"n":1}` {
		t.Fatalf("%s %s %s", sex.Status, sex.Output, sex.Cause)
	}

	s.Runner.Mocks = writeMock(t, `{
		"StateMachines": {"act": {"TestCases": {"A": {"Call": "Nope"}}}},
		"MockedResponses": {"Nope": {"0": {"Return": {"ok": true}}}}
	}`)
	actARN := "arn:aws:states:us-east-1:123456789012:activity:work"
	act := createMachine(t, s, "act", `{"StartAt":"Call","States":{"Call":{"Type":"Task","Resource":"`+actARN+`","End":true}}}`, roleARN)
	arun := startExec(t, s, *act.StateMachineArn+"#A", "a", `{}`)
	aex := waitExec(t, s, *arun.ExecutionArn)
	if aex.Status != store.ExecFailed || !strings.Contains(aex.Cause, "cannot be mocked") || countEvents(aex, "ActivityScheduled") != 0 {
		t.Fatalf("%s %s %v", aex.Error, aex.Cause, typesOf(aex))
	}

	s.Runner.LambdaEndpoint = ""
	s.Runner.Mocks = nil
	bad := createMachine(t, s, "badres", `{"StartAt":"Call","States":{"Call":{"Type":"Task","Resource":"arn:aws:states:::sqs:sendMessage","End":true}}}`, roleARN)
	brun := startExec(t, s, *bad.StateMachineArn, "b", `{}`)
	bex := waitExec(t, s, *brun.ExecutionArn)
	if bex.Status != store.ExecFailed || !strings.Contains(bex.Cause, "arn:aws:states:::sqs:sendMessage") || !strings.Contains(bex.Cause, "unsupported resource") {
		t.Fatalf("%s %s", bex.Error, bex.Cause)
	}
	empty := createMachine(t, s, "emptyep", `{"StartAt":"Call","States":{"Call":{"Type":"Task","Resource":"`+lambdaARN+`","End":true}}}`, roleARN)
	erun := startExec(t, s, *empty.StateMachineArn, "e", `{}`)
	eex := waitExec(t, s, *erun.ExecutionArn)
	if eex.Status != store.ExecFailed || !strings.Contains(eex.Cause, lambdaARN) || !strings.Contains(eex.Cause, "unsupported resource") {
		t.Fatalf("%s %s", eex.Error, eex.Cause)
	}

	s.Runner.Mocks = writeMock(t, `{
		"StateMachines": {"gen": {"TestCases": {"G": {"Call": "Box"}, "T": {"Call": "Slow"}}}},
		"MockedResponses": {
			"Box": {"0": {"Return": {"Payload": {"n": 1}}}},
			"Slow": {"0": {"Throw": {"Error": "connection timeout", "Cause": "slow"}}, "1": {"Return": {"ok": true}}}
		}
	}`)
	gen := createMachine(t, s, "gen", `{"StartAt":"Call","States":{"Call":{"Type":"Task","Resource":"arn:aws:states:::sqs:sendMessage","End":true}}}`, roleARN)
	grun := startExec(t, s, *gen.StateMachineArn+"#G", "g", `{}`)
	gex := waitExec(t, s, *grun.ExecutionArn)
	if gex.Status != store.ExecSucceeded || gex.Output != `{"Payload":{"n":1}}` {
		t.Fatalf("%s %s %s", gex.Status, gex.Output, gex.Cause)
	}
	timed := createMachine(t, s, "timed", `{"StartAt":"Call","States":{"Call":{"Type":"Task","Resource":"arn:aws:states:::sqs:sendMessage","Retry":[{"ErrorEquals":["States.TaskFailed"],"IntervalSeconds":0,"MaxAttempts":2}],"End":true}}}`, roleARN)
	s.Runner.Mocks = writeMock(t, `{
		"StateMachines": {"timed": {"TestCases": {"T": {"Call": "Slow"}}}},
		"MockedResponses": {"Slow": {"0": {"Throw": {"Error": "connection timeout", "Cause": "slow"}}, "1": {"Return": {"ok": true}}}}
	}`)
	trun := startExec(t, s, *timed.StateMachineArn+"#T", "t", `{}`)
	tex := waitExec(t, s, *trun.ExecutionArn)
	if tex.Status != store.ExecFailed || tex.Error != "States.Timeout" || countEvents(tex, "TaskTimedOut") != 1 || countEvents(tex, "TaskStateExited") != 0 {
		t.Fatalf("%s %s %v", tex.Error, tex.Cause, typesOf(tex))
	}
	all := createMachine(t, s, "all", `{"StartAt":"Call","States":{"Call":{"Type":"Task","Resource":"arn:aws:states:::sqs:sendMessage","Retry":[{"ErrorEquals":["States.ALL"],"IntervalSeconds":0,"MaxAttempts":2}],"End":true}}}`, roleARN)
	s.Runner.Mocks = writeMock(t, `{
		"StateMachines": {"all": {"TestCases": {"T": {"Call": "Slow"}}}},
		"MockedResponses": {"Slow": {"0": {"Throw": {"Error": "connection timeout", "Cause": "slow"}}, "1": {"Return": {"ok": true}}}}
	}`)
	ar := startExec(t, s, *all.StateMachineArn+"#T", "a", `{}`)
	ax := waitExec(t, s, *ar.ExecutionArn)
	if ax.Status != store.ExecSucceeded || ax.Output != `{"ok":true}` {
		t.Fatalf("%s %s %s", ax.Status, ax.Output, ax.Cause)
	}

	s.Runner.Mocks = writeMock(t, `{
		"StateMachines": {"pay": {"TestCases": {"P": {"Call": "Env"}}}},
		"MockedResponses": {"Env": {"0": {"Return": {"Payload": {"ok": true}, "StatusCode": 200}}}}
	}`)
	pay := createMachine(t, s, "pay", `{"StartAt":"Call","States":{"Call":{"Type":"Task","Resource":"`+lambdaARN+`","End":true}}}`, roleARN)
	pr := startExec(t, s, *pay.StateMachineArn+"#P", "p", `{}`)
	px := waitExec(t, s, *pr.ExecutionArn)
	if px.Status != store.ExecSucceeded || px.Output != `{"ok":true}` {
		t.Fatalf("payload %s %s %s", px.Status, px.Output, px.Cause)
	}
	s.Runner.Mocks = writeMock(t, `{
		"StateMachines": {"fnerr": {"TestCases": {"P": {"Call": "Env"}}}},
		"MockedResponses": {"Env": {"0": {"Return": {"FunctionError": "Unhandled", "Payload": {"errorType": "Boom", "errorMessage": "no"}}}}}
	}`)
	fn := createMachine(t, s, "fnerr", `{"StartAt":"Call","States":{"Call":{"Type":"Task","Resource":"`+lambdaARN+`","End":true}}}`, roleARN)
	fr := startExec(t, s, *fn.StateMachineArn+"#P", "f", `{}`)
	fx := waitExec(t, s, *fr.ExecutionArn)
	if fx.Status != store.ExecFailed || fx.Error != "Boom" || fx.Cause != "no" || countEvents(fx, "LambdaFunctionFailed") != 1 {
		t.Fatalf("%s %s %v", fx.Error, fx.Cause, typesOf(fx))
	}
	s.Runner.Mocks = writeMock(t, `{
		"StateMachines": {"lto": {"TestCases": {"P": {"Call": "Env"}}}},
		"MockedResponses": {"Env": {"0": {"Throw": {"Error": "connection timeout", "Cause": "slow"}}}}
	}`)
	lto := createMachine(t, s, "lto", `{"StartAt":"Call","States":{"Call":{"Type":"Task","Resource":"`+lambdaARN+`","End":true}}}`, roleARN)
	lr := startExec(t, s, *lto.StateMachineArn+"#P", "l", `{}`)
	lx := waitExec(t, s, *lr.ExecutionArn)
	if lx.Status != store.ExecFailed || lx.Error != "States.Timeout" || countEvents(lx, "LambdaFunctionTimedOut") != 1 || countEvents(lx, "TaskStateExited") != 0 {
		t.Fatalf("%s %s %v", lx.Error, lx.Cause, typesOf(lx))
	}
}

func TestCatchResultPathNullAndMaxAttempts(t *testing.T) {
	s := newService()
	s.Runner.Mocks = writeMock(t, `{
		"StateMachines": {"keep": {"TestCases": {"K": {"Call": "Boom"}}}},
		"MockedResponses": {"Boom": {"0": {"Throw": {"Error": "CustomError", "Cause": "x"}}}}
	}`)
	def := `{"StartAt":"Call","States":{"Call":{"Type":"Task","Resource":"` + lambdaARN + `","Catch":[{"ErrorEquals":["CustomError"],"ResultPath":null,"Next":"Done"}],"End":true},"Done":{"Type":"Succeed"}}}`
	sm := createMachine(t, s, "keep", def, roleARN)
	run := startExec(t, s, *sm.StateMachineArn+"#K", "k", `{"a":1}`)
	ex := waitExec(t, s, *run.ExecutionArn)
	if ex.Status != store.ExecSucceeded || ex.Output != `{"a":1}` {
		t.Fatalf("%s %s %s", ex.Status, ex.Output, ex.Cause)
	}
	s.Runner.Mocks = writeMock(t, `{
		"StateMachines": {"zero": {"TestCases": {"Z": {"Call": "Boom"}}}},
		"MockedResponses": {"Boom": {"0": {"Throw": {"Error": "CustomError", "Cause": "x"}}, "1": {"Return": {"ok": true}}}}
	}`)
	zero := createMachine(t, s, "zero", `{"StartAt":"Call","States":{"Call":{"Type":"Task","Resource":"`+lambdaARN+`","Retry":[{"ErrorEquals":["CustomError"],"IntervalSeconds":5,"MaxAttempts":0}],"End":true}}}`, roleARN)
	var slept bool
	s.Runner.Sleep = func(context.Context, time.Duration) error {
		slept = true
		return nil
	}
	zr := startExec(t, s, *zero.StateMachineArn+"#Z", "z", `{}`)
	zx := waitExec(t, s, *zr.ExecutionArn)
	if zx.Status != store.ExecFailed || zx.Error != "CustomError" || slept || countEvents(zx, "TaskStateExited") != 0 {
		t.Fatalf("%s %s slept %v %v", zx.Error, zx.Cause, slept, typesOf(zx))
	}
}

func TestTaskTimeoutAndLambdaHTTP(t *testing.T) {
	s := newService()
	invoked := false
	s.Runner.Invoke = func(context.Context, exec.LambdaCall) exec.LambdaReply {
		invoked = true
		return exec.LambdaReply{Status: 200, Payload: `{"ok":true}`}
	}
	s.Runner.LambdaEndpoint = "http://lambda.invalid"
	sm := createMachine(t, s, "t0", `{"StartAt":"Call","States":{"Call":{"Type":"Task","Resource":"`+lambdaARN+`","TimeoutSeconds":0,"End":true}}}`, roleARN)
	run := startExec(t, s, *sm.StateMachineArn, "z", `{}`)
	ex := waitExec(t, s, *run.ExecutionArn)
	if ex.Status != store.ExecFailed || ex.Error != "States.Timeout" || invoked || countEvents(ex, "LambdaFunctionTimedOut") != 1 || countEvents(ex, "TaskStateExited") != 0 {
		t.Fatalf("%s %s invoked %v %v", ex.Error, ex.Cause, invoked, typesOf(ex))
	}

	s.Runner.Invoke = func(ctx context.Context, call exec.LambdaCall) exec.LambdaReply {
		if call.Function != "fn" || call.Qualifier != "prod" {
			t.Errorf("call %+v", call)
		}
		dl, ok := ctx.Deadline()
		if !ok || time.Until(dl) > 2*time.Second {
			t.Errorf("deadline %v %v", ok, time.Until(dl))
		}
		return exec.LambdaReply{Err: context.DeadlineExceeded}
	}
	short := createMachine(t, s, "t1", `{"StartAt":"Call","States":{"Call":{"Type":"Task","Resource":"arn:aws:lambda:::function:fn:prod","TimeoutSeconds":1,"End":true}}}`, roleARN)
	sr := startExec(t, s, *short.StateMachineArn, "s", `{"n":1}`)
	sx := waitExec(t, s, *sr.ExecutionArn)
	if sx.Status != store.ExecFailed || sx.Error != "States.Timeout" {
		t.Fatalf("%s %s", sx.Error, sx.Cause)
	}

	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Amz-Invocation-Type") != "RequestResponse" {
			t.Errorf("header %s", r.Header.Get("X-Amz-Invocation-Type"))
		}
		if !strings.Contains(r.URL.Path, "/2015-03-31/functions/fn/invocations") {
			t.Errorf("path %s", r.URL.Path)
		}
		switch r.URL.Query().Get("mode") {
		default:
			w.Write([]byte(`{"ok":true}`))
		}
	}))
	defer srv.Close()
	s.Runner.Invoke = nil
	s.Runner.LambdaEndpoint = srv.URL
	okm := createMachine(t, s, "httpok", `{"StartAt":"Call","States":{"Call":{"Type":"Task","Resource":"arn:aws:lambda:us-east-1:123456789012:function:fn","End":true}}}`, roleARN)
	or := startExec(t, s, *okm.StateMachineArn, "o", `{"n":1}`)
	ox := waitExec(t, s, *or.ExecutionArn)
	if ox.Status != store.ExecSucceeded || ox.Output != `{"ok":true}` {
		t.Fatalf("%s %s %s", ox.Status, ox.Output, ox.Cause)
	}

	badSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Amz-Function-Error") == "" && r.URL.Query().Get("q") == "" {
			w.Header().Set("X-Amz-Function-Error", "Unhandled")
			w.Write([]byte(`{"errorType":"Boom","errorMessage":"no"}`))
			return
		}
		w.WriteHeader(http.StatusForbidden)
	}))
	defer badSrv.Close()
	s.Runner.LambdaEndpoint = badSrv.URL
	fm := createMachine(t, s, "httpfe", `{"StartAt":"Call","States":{"Call":{"Type":"Task","Resource":"arn:aws:lambda:us-east-1:123456789012:function:fn","End":true}}}`, roleARN)
	fr := startExec(t, s, *fm.StateMachineArn, "f", `{}`)
	fx := waitExec(t, s, *fr.ExecutionArn)
	if fx.Status != store.ExecFailed || fx.Error != "Boom" || fx.Cause != "no" {
		t.Fatalf("%s %s", fx.Error, fx.Cause)
	}
	s.Runner.Invoke = func(context.Context, exec.LambdaCall) exec.LambdaReply {
		return exec.LambdaReply{Status: 403, Payload: "no"}
	}
	pr := startExec(t, s, *fm.StateMachineArn, "p", `{}`)
	px := waitExec(t, s, *pr.ExecutionArn)
	if px.Status != store.ExecFailed || px.Error != "Lambda.ServiceException" || !strings.Contains(px.Cause, "403") {
		t.Fatalf("%s %s", px.Error, px.Cause)
	}
}

func TestMockHashReloadAndIdempotentStart(t *testing.T) {
	s := newService()
	path := filepath.Join(t.TempDir(), "MockConfigFile.json")
	first := `{"StateMachines":{"reload":{"TestCases":{"R":{"Call":"A"}}}},"MockedResponses":{"A":{"0":{"Return":{"n":1}}}}}`
	second := `{"StateMachines":{"reload":{"TestCases":{"R":{"Call":"A"}}}},"MockedResponses":{"A":{"0":{"Return":{"n":2}}}}}`
	if err := os.WriteFile(path, []byte(first), 0o644); err != nil {
		t.Fatal(err)
	}
	s.Runner.Mocks = &mock.Repo{Path: path}
	s.Runner.Boundary = func(string) {
		_ = os.WriteFile(path, []byte(second), 0o644)
	}
	sm := createMachine(t, s, "reload", `{"StartAt":"Call","States":{"Call":{"Type":"Task","Resource":"`+lambdaARN+`","End":true}}}`, roleARN)
	run := startExec(t, s, *sm.StateMachineArn+"#R", "r", `{}`)
	ex := waitExec(t, s, *run.ExecutionArn)
	if ex.Status != store.ExecSucceeded || ex.Output != `{"n":2}` {
		t.Fatalf("%s %s %s", ex.Status, ex.Output, ex.Cause)
	}

	s.Runner.Boundary = nil
	g := newGate()
	defer g.Release()
	s.Runner.Boundary = g.Wait
	hold := createMachine(t, s, "hold3", passDef, roleARN)
	s.Runner.Mocks = writeMock(t, `{"StateMachines":{"hold3":{"TestCases":{"Case":{}}}},"MockedResponses":{}}`)
	firstRun := startExec(t, s, *hold.StateMachineArn+"#Case", "same", `{}`)
	g.waitArrived(t)
	os.Remove(s.Runner.Mocks.Path)
	again := startExec(t, s, *hold.StateMachineArn+"#Case", "same", `{}`)
	if *again.ExecutionArn != *firstRun.ExecutionArn {
		t.Fatal(*again.ExecutionArn)
	}
	g.Release()
	if got := waitExec(t, s, *firstRun.ExecutionArn); got.Status != store.ExecSucceeded {
		t.Fatal(got.Status)
	}
}

func TestParallelStaysUnimplemented(t *testing.T) {
	s := newService()
	sm := createMachine(t, s, "par", `{"StartAt":"P","States":{"P":{"Type":"Parallel","Branches":[{"StartAt":"S","States":{"S":{"Type":"Succeed"}}}],"End":true}}}`, roleARN)
	run := startExec(t, s, *sm.StateMachineArn, "p", `{}`)
	ex := waitExec(t, s, *run.ExecutionArn)
	if ex.Status != store.ExecFailed || !strings.Contains(ex.Cause, "State type Parallel is not implemented") || countEvents(ex, "ParallelStateEntered") != 0 {
		t.Fatalf("%s %s", ex.Error, ex.Cause)
	}
}

func typesOf(ex store.Execution) []string {
	out := make([]string, len(ex.History))
	for i, ev := range ex.History {
		out[i] = ev.Type
	}
	return out
}
