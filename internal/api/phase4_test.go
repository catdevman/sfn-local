package api

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/catdevman/sfn-local/internal/api/gen"
	"github.com/catdevman/sfn-local/internal/exec"
	"github.com/catdevman/sfn-local/internal/store"
)

func TestPhase4InlineMap(t *testing.T) {
	s := newService()
	const def = `{"StartAt":"M","States":{"M":{"Type":"Map","ItemsPath":"$.items","MaxConcurrency":2,"Iterator":{"StartAt":"P","States":{"P":{"Type":"Pass","Parameters":{"n.$":"$.n"},"End":true}}},"End":true}}}`
	ex := runPhase4(t, s, "map", def, "run", `{"items":[{"n":1},{"n":2}]}`)
	if ex.Status != store.ExecSucceeded {
		t.Fatalf("%s %s %s", ex.Status, ex.Error, ex.Cause)
	}
	jsonEqual(t, ex.Output, `[{"n":1},{"n":2}]`)
	assertCounts(t, ex, map[string]int{
		"ExecutionStarted":      1,
		"MapStateEntered":       1,
		"MapStateStarted":       1,
		"MapIterationStarted":   2,
		"PassStateEntered":      2,
		"PassStateExited":       2,
		"MapIterationSucceeded": 2,
		"MapStateSucceeded":     1,
		"MapStateExited":        1,
		"ExecutionSucceeded":    1,
	})
	assertBefore(t, ex, "MapStateEntered", "MapStateStarted")
	assertBefore(t, ex, "MapStateStarted", "MapIterationStarted")
	assertBefore(t, ex, "MapStateSucceeded", "MapStateExited")
	for _, ev := range ex.History {
		if ev.Type == "MapStateStarted" && (!ev.HasLength || ev.Length != 2) {
			t.Fatalf("length %+v", ev)
		}
	}
	hist, err := s.GetExecutionHistory(context.Background(), &gen.GetExecutionHistoryInput{ExecutionArn: sp(ex.ARN)})
	if err != nil || hist.Events == nil {
		t.Fatal(err)
	}
	indexes := map[int32]bool{}
	for _, ev := range *hist.Events {
		if ev.Type == nil || !ev.Type.Valid() {
			t.Fatalf("api history type %v", ev.Type)
		}
		switch *ev.Type {
		case gen.HistoryEventTypeMapStateStarted:
			if ev.MapStateStartedEventDetails == nil || ev.MapStateStartedEventDetails.Length == nil || *ev.MapStateStartedEventDetails.Length != 2 {
				t.Fatalf("api length %+v", ev.MapStateStartedEventDetails)
			}
		case gen.HistoryEventTypeMapIterationStarted, gen.HistoryEventTypeMapIterationSucceeded:
			d := iterationDetails(ev)
			if d == nil || d.Name == nil || *d.Name != "M" || d.Index == nil {
				t.Fatalf("%s %+v", *ev.Type, d)
			}
			indexes[*d.Index] = true
		case gen.HistoryEventTypeMapStateSucceeded, gen.HistoryEventTypeMapStateFailed, gen.HistoryEventTypeMapStateAborted:
			if ev.MapStateStartedEventDetails != nil {
				t.Fatal("succeeded event has started details")
			}
		}
	}
	if !indexes[0] || !indexes[1] {
		t.Fatalf("indexes %v", indexes)
	}

	empty := runPhase4(t, s, "empty", `{"StartAt":"M","States":{"M":{"Type":"Map","ItemsPath":"$.items","Iterator":{"StartAt":"P","States":{"P":{"Type":"Pass","End":true}}},"End":true}}}`, "run", `{"items":[]}`)
	jsonEqual(t, empty.Output, `[]`)
	if countEvents(empty, "MapStateStarted") != 1 || countEvents(empty, "MapIterationStarted") != 0 || countEvents(empty, "MapStateSucceeded") != 1 {
		t.Fatalf("%v", typesOf(empty))
	}
	for _, ev := range empty.History {
		if ev.Type == "MapStateStarted" && (!ev.HasLength || ev.Length != 0) {
			t.Fatalf("empty length %+v", ev)
		}
	}

	missing := runPhase4(t, s, "missing", def, "run", `{}`)
	if missing.Error != "States.Runtime" || !strings.Contains(missing.Cause, "was not found") || countEvents(missing, "MapStateStarted") != 0 || countEvents(missing, "MapStateFailed") != 1 {
		t.Fatalf("%s %s %v", missing.Error, missing.Cause, typesOf(missing))
	}
	bad := runPhase4(t, s, "bad", def, "run", `{"items":1}`)
	if bad.Error != "States.Runtime" || !strings.Contains(bad.Cause, "must yield an array") {
		t.Fatalf("%s %s", bad.Error, bad.Cause)
	}

	inline := `{"StartAt":"M","States":{"M":{"Type":"Map","ItemProcessor":{"ProcessorConfig":{"Mode":"INLINE"},"StartAt":"P","States":{"P":{"Type":"Pass","End":true}}},"End":true}}}`
	got := runPhase4(t, s, "inline", inline, "run", `[1,2]`)
	jsonEqual(t, got.Output, `[1,2]`)

	dist := `{"StartAt":"M","States":{"M":{"Type":"Map","End":true,"ItemProcessor":{"ProcessorConfig":{"Mode":"DISTRIBUTED"},"StartAt":"P","States":{"P":{"Type":"Pass","End":true}}}}}}`
	_, err = s.CreateStateMachine(context.Background(), &gen.CreateStateMachineInput{Name: sp("dist"), Definition: sp(dist), RoleArn: sp(roleARN)})
	if api := mustAPI(t, err); api.ErrorName() != "InvalidDefinition" || !strings.Contains(api.Error(), "DISTRIBUTED") {
		t.Fatal(err)
	}
}

func TestPhase4Parallel(t *testing.T) {
	s := newService()
	release := make(chan struct{})
	entered := make(chan struct{})
	var once sync.Once
	s.Runner.Sleep = func(ctx context.Context, d time.Duration) error {
		once.Do(func() { close(entered) })
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	def := `{"StartAt":"P","States":{"P":{"Type":"Parallel","Branches":[{"StartAt":"W","States":{"W":{"Type":"Wait","Seconds":30,"Next":"A"},"A":{"Type":"Pass","Result":1,"End":true}}},{"StartAt":"B","States":{"B":{"Type":"Pass","Result":2,"End":true}}}],"End":true}}}`
	sm := createMachine(t, s, "order", def, roleARN)
	out := startExec(t, s, *sm.StateMachineArn, "run", `{}`)
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("first branch did not wait")
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		cur, ok := s.Store.Execution(*out.ExecutionArn)
		if ok && exitIndex(cur, "2") >= 0 && exitIndex(cur, "1") < 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	cur, _ := s.Store.Execution(*out.ExecutionArn)
	if exitIndex(cur, "2") < 0 || exitIndex(cur, "1") >= 0 {
		t.Fatalf("branch 2 did not finish first: %v", typesOf(cur))
	}
	close(release)
	ex := waitExec(t, s, *out.ExecutionArn)
	assertEnum(t, ex)
	jsonEqual(t, ex.Output, `[1,2]`)
	if exitIndex(ex, "2") > exitIndex(ex, "1") {
		t.Fatal("completion order leaked into the result array history")
	}
	assertCounts(t, ex, map[string]int{
		"ExecutionStarted":       1,
		"ParallelStateEntered":   1,
		"ParallelStateStarted":   1,
		"WaitStateEntered":       1,
		"WaitStateExited":        1,
		"PassStateEntered":       2,
		"PassStateExited":        2,
		"ParallelStateSucceeded": 1,
		"ParallelStateExited":    1,
		"ExecutionSucceeded":     1,
	})
	assertParallelOrder(t, ex)
	hist, err := s.GetExecutionHistory(context.Background(), &gen.GetExecutionHistoryInput{ExecutionArn: out.ExecutionArn})
	if err != nil {
		t.Fatal(err)
	}
	for _, ev := range *hist.Events {
		if ev.Type == nil || !ev.Type.Valid() {
			t.Fatalf("api type %v", ev.Type)
		}
		if *ev.Type == gen.HistoryEventTypeParallelStateStarted || *ev.Type == gen.HistoryEventTypeParallelStateSucceeded || *ev.Type == gen.HistoryEventTypeParallelStateFailed {
			raw, err := json.Marshal(ev)
			if err != nil {
				t.Fatal(err)
			}
			var obj map[string]json.RawMessage
			if err := json.Unmarshal(raw, &obj); err != nil {
				t.Fatal(err)
			}
			if len(obj) != 4 {
				t.Fatalf("%s details %s", *ev.Type, raw)
			}
		}
	}

	params := `{"StartAt":"P","States":{"P":{"Type":"Parallel","Parameters":{"k":1},"Branches":[{"StartAt":"A","States":{"A":{"Type":"Pass","End":true}}},{"StartAt":"B","States":{"B":{"Type":"Pass","Result":2,"End":true}}}],"ResultSelector":{"first.$":"$[0]","second.$":"$[1]"},"End":true}}}`
	selected := runPhase4(t, s, "select", params, "run", `{"ignored":true}`)
	jsonEqual(t, selected.Output, `{"first":{"k":1},"second":2}`)

	caught := `{"StartAt":"P","States":{"P":{"Type":"Parallel","Branches":[{"StartAt":"F","States":{"F":{"Type":"Fail","Error":"Boom","Cause":"no"}}},{"StartAt":"A","States":{"A":{"Type":"Pass","Result":1,"End":true}}}],"Catch":[{"ErrorEquals":["Boom"],"Next":"Later","ResultPath":"$.err"}],"End":true},"Later":{"Type":"Succeed"}}}`
	ex = runPhase4(t, s, "catch", caught, "run", `{"a":1}`)
	if ex.Status != store.ExecSucceeded {
		t.Fatalf("%s %s %s", ex.Status, ex.Error, ex.Cause)
	}
	jsonEqual(t, ex.Output, `{"a":1,"err":{"Error":"Boom","Cause":"no"}}`)
	if countEvents(ex, "ParallelStateFailed") != 1 || countEvents(ex, "ParallelStateExited") != 1 || countEvents(ex, "TaskStateExited") != 0 || countEvents(ex, "ParallelStateSucceeded") != 0 || countEvents(ex, "ParallelStateAborted") != 0 || countEvents(ex, "SucceedStateEntered") != 1 {
		t.Fatalf("%v", typesOf(ex))
	}
	assertBefore(t, ex, "ParallelStateFailed", "ParallelStateExited")
	assertBefore(t, ex, "ParallelStateExited", "SucceedStateEntered")

	s.Runner.LambdaEndpoint = "http://lambda.invalid"
	waitEntered := make(chan struct{})
	var waitOnce sync.Once
	s.Runner.Sleep = func(ctx context.Context, d time.Duration) error {
		waitOnce.Do(func() { close(waitEntered) })
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Second):
			return nil
		}
	}
	s.Runner.Invoke = func(ctx context.Context, call exec.LambdaCall) exec.LambdaReply {
		select {
		case <-waitEntered:
		case <-ctx.Done():
			return exec.LambdaReply{Err: ctx.Err()}
		case <-time.After(2 * time.Second):
			t.Errorf("wait did not start")
			return exec.LambdaReply{Err: context.DeadlineExceeded}
		}
		return exec.LambdaReply{Status: 200, FunctionError: "Unhandled", Payload: `{"errorType":"Boom","errorMessage":"no"}`}
	}
	cancelDef := `{"StartAt":"P","States":{"P":{"Type":"Parallel","Branches":[{"StartAt":"W","States":{"W":{"Type":"Wait","Seconds":30,"End":true}}},{"StartAt":"T","States":{"T":{"Type":"Task","Resource":"` + lambdaARN + `","End":true}}}],"End":true}}}`
	began := time.Now()
	ex = runPhase4(t, s, "cancel", cancelDef, "run", `{}`)
	if time.Since(began) >= time.Second {
		t.Fatalf("sibling cancel took %s", time.Since(began))
	}
	if ex.Status != store.ExecFailed || ex.Error != "Boom" || countEvents(ex, "ParallelStateFailed") != 1 || countEvents(ex, "ParallelStateSucceeded") != 0 || countEvents(ex, "ParallelStateAborted") != 0 || countEvents(ex, "WaitStateExited") != 0 || countEvents(ex, "ExecutionFailed") != 1 {
		t.Fatalf("%s %s %s %v", ex.Status, ex.Error, ex.Cause, typesOf(ex))
	}
}

func TestPhase4MapContext(t *testing.T) {
	s := newService()
	item := `{"StartAt":"M","States":{"M":{"Type":"Map","ItemsPath":"$.items","ItemSelector":{"i.$":"$$.Map.Item.Index","n.$":"$$.Map.Item.Value.n"},"Iterator":{"StartAt":"P","States":{"P":{"Type":"Pass","End":true}}},"End":true}}}`
	ex := runPhase4(t, s, "item", item, "run", `{"items":[{"n":1},{"n":2}]}`)
	jsonEqual(t, ex.Output, `[{"i":0,"n":1},{"i":1,"n":2}]`)

	params := `{"StartAt":"M","States":{"M":{"Type":"Map","ItemsPath":"$.items","Parameters":{"i.$":"$$.Map.Item.Index","n.$":"$$.Map.Item.Value.n"},"Iterator":{"StartAt":"P","States":{"P":{"Type":"Pass","End":true}}},"End":true}}}`
	ex = runPhase4(t, s, "params", params, "run", `{"items":[{"n":1},{"n":2}]}`)
	jsonEqual(t, ex.Output, `[{"i":0,"n":1},{"i":1,"n":2}]`)

	root := `{"StartAt":"M","States":{"M":{"Type":"Map","ItemsPath":"$.items","ItemSelector":{"i.$":"$$.Map.Item.Index","n.$":"$.items[0].n"},"Iterator":{"StartAt":"P","States":{"P":{"Type":"Pass","End":true}}},"End":true}}}`
	ex = runPhase4(t, s, "root", root, "run", `{"items":[{"n":1},{"n":2}]}`)
	jsonEqual(t, ex.Output, `[{"i":0,"n":1},{"i":1,"n":1}]`)

	inner := `{"StartAt":"M","States":{"M":{"Type":"Map","ItemsPath":"$.items","Iterator":{"StartAt":"P","States":{"P":{"Type":"Pass","Parameters":{"i.$":"$$.Map.Item.Index","v.$":"$$.Map.Item.Value.n"},"End":true}}},"End":true}}}`
	ex = runPhase4(t, s, "inner", inner, "run", `{"items":[{"n":1},{"n":2}]}`)
	jsonEqual(t, ex.Output, `[{"i":0,"v":1},{"i":1,"v":2}]`)

	wrapped := `{"StartAt":"M","States":{"M":{"Type":"Map","InputPath":"$.wrap","ItemsPath":"$.items","Iterator":{"StartAt":"P","States":{"P":{"Type":"Pass","Parameters":{"n.$":"$.n"},"End":true}}},"End":true}}}`
	ex = runPhase4(t, s, "wrap", wrapped, "run", `{"wrap":{"items":[{"n":7}]},"other":1}`)
	jsonEqual(t, ex.Output, `[{"n":7}]`)

	var current, maxSeen atomic.Int32
	s.Runner.Sleep = func(ctx context.Context, d time.Duration) error {
		n := current.Add(1)
		for {
			old := maxSeen.Load()
			if n <= old || maxSeen.CompareAndSwap(old, n) {
				break
			}
		}
		timer := time.NewTimer(30 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			current.Add(-1)
			return ctx.Err()
		case <-timer.C:
			current.Add(-1)
			return nil
		}
	}
	bound := `{"StartAt":"M","States":{"M":{"Type":"Map","ItemsPath":"$.items","MaxConcurrency":1,"Iterator":{"StartAt":"W","States":{"W":{"Type":"Wait","Seconds":1,"End":true}}},"End":true}}}`
	ex = runPhase4(t, s, "bound", bound, "run", `{"items":[1,2]}`)
	jsonEqual(t, ex.Output, `[1,2]`)
	if maxSeen.Load() != 1 {
		t.Fatalf("max concurrency %d", maxSeen.Load())
	}

	for _, mc := range []string{"0", ""} {
		name := "open"
		open := `{"StartAt":"M","States":{"M":{"Type":"Map","ItemsPath":"$.items","Iterator":{"StartAt":"W","States":{"W":{"Type":"Wait","Seconds":1,"End":true}}},"End":true}}}`
		if mc == "0" {
			name = "zero"
			open = `{"StartAt":"M","States":{"M":{"Type":"Map","ItemsPath":"$.items","MaxConcurrency":0,"Iterator":{"StartAt":"W","States":{"W":{"Type":"Wait","Seconds":1,"End":true}}},"End":true}}}`
		}
		var entered atomic.Int32
		release := make(chan struct{})
		s.Runner.Sleep = func(ctx context.Context, d time.Duration) error {
			if entered.Add(1) == 2 {
				close(release)
			}
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(2 * time.Second):
				return context.DeadlineExceeded
			}
		}
		ex = runPhase4(t, s, name, open, "run", `{"items":[1,2]}`)
		jsonEqual(t, ex.Output, `[1,2]`)
		if entered.Load() != 2 {
			t.Fatalf("%s entered %d", name, entered.Load())
		}
	}

	s.Runner.LambdaEndpoint = "http://lambda.invalid"
	enteredWait := make(chan struct{})
	var waitOnce sync.Once
	s.Runner.Sleep = func(ctx context.Context, d time.Duration) error {
		waitOnce.Do(func() { close(enteredWait) })
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Second):
			return nil
		}
	}
	s.Runner.Invoke = func(ctx context.Context, call exec.LambdaCall) exec.LambdaReply {
		select {
		case <-enteredWait:
		case <-ctx.Done():
			return exec.LambdaReply{Err: ctx.Err()}
		case <-time.After(2 * time.Second):
			t.Errorf("map wait did not start")
			return exec.LambdaReply{Err: context.DeadlineExceeded}
		}
		return exec.LambdaReply{Status: 200, FunctionError: "Unhandled", Payload: `{"errorType":"Boom","errorMessage":"no"}`}
	}
	sibling := `{"StartAt":"M","States":{"M":{"Type":"Map","ItemsPath":"$.items","Iterator":{"StartAt":"T","States":{"T":{"Type":"Choice","Choices":[{"Variable":"$.wait","BooleanEquals":true,"Next":"W"}],"Default":"L"},"W":{"Type":"Wait","Seconds":30,"End":true},"L":{"Type":"Task","Resource":"` + lambdaARN + `","End":true}}},"End":true}}}`
	ex = runPhase4(t, s, "sibling", sibling, "run", `{"items":[{"wait":true},{"wait":false}]}`)
	if ex.Status != store.ExecFailed || ex.Error != "Boom" || countEvents(ex, "MapIterationFailed") != 1 || countEvents(ex, "MapIterationAborted") != 1 || countEvents(ex, "MapStateFailed") != 1 || countEvents(ex, "MapStateSucceeded") != 0 || countEvents(ex, "MapStateAborted") != 0 {
		t.Fatalf("%s %s %s %v", ex.Status, ex.Error, ex.Cause, typesOf(ex))
	}
	for _, ev := range ex.History {
		if ev.Type == "MapIterationAborted" && (!ev.HasIndex || ev.Index != 0) {
			t.Fatalf("aborted %+v", ev)
		}
		if ev.Type == "MapIterationFailed" && (!ev.HasIndex || ev.Index != 1) {
			t.Fatalf("failed %+v", ev)
		}
	}

	caught := `{"StartAt":"M","States":{"M":{"Type":"Map","ItemsPath":"$.items","Iterator":{"StartAt":"F","States":{"F":{"Type":"Fail","Error":"Boom","Cause":"no"}}},"Catch":[{"ErrorEquals":["Boom"],"Next":"Later","ResultPath":"$.err"}],"End":true},"Later":{"Type":"Succeed"}}}`
	ex = runPhase4(t, s, "catch", caught, "run", `{"items":[1]}`)
	jsonEqual(t, ex.Output, `{"items":[1],"err":{"Error":"Boom","Cause":"no"}}`)
	if countEvents(ex, "MapStateFailed") != 1 || countEvents(ex, "MapStateExited") != 1 || countEvents(ex, "TaskStateExited") != 0 || countEvents(ex, "MapStateSucceeded") != 0 {
		t.Fatalf("%v", typesOf(ex))
	}

	s.Runner.Invoke = nil
	s.Runner.Sleep = func(context.Context, time.Duration) error { return nil }
	retry := `{"StartAt":"M","States":{"M":{"Type":"Map","ItemsPath":"$.missing","Retry":[{"ErrorEquals":["States.ALL"],"IntervalSeconds":1,"MaxAttempts":1}],"Iterator":{"StartAt":"P","States":{"P":{"Type":"Pass","End":true}}},"End":true}}}`
	var slept []time.Duration
	zero := float32(0)
	s.Runner.WaitScale = &zero
	s.Runner.Sleep = func(_ context.Context, d time.Duration) error {
		slept = append(slept, d)
		return nil
	}
	waited := runPhase4(t, s, "scaled-wait", `{"StartAt":"W","States":{"W":{"Type":"Wait","Seconds":5,"End":true}}}`, "run", `{}`)
	if waited.Status != store.ExecSucceeded || len(slept) != 0 {
		t.Fatalf("wait scale %s slept %v", waited.Status, slept)
	}
	ex = runPhase4(t, s, "retry-path", retry, "run", `{}`)
	if ex.Error != "States.Runtime" || !strings.Contains(ex.Cause, "was not found") || countEvents(ex, "MapStateStarted") != 0 || countEvents(ex, "MapStateFailed") != 1 || len(slept) != 1 || slept[0] != time.Second {
		t.Fatalf("%s %s slept %v %v", ex.Error, ex.Cause, slept, typesOf(ex))
	}
}

func TestPhase4RetryCountAndIntrinsics(t *testing.T) {
	s := newService()
	s.Runner.Mocks = writeMock(t, `{
		"StateMachines": {"ctx": {"TestCases": {"Zero": {"Call": "Ok"}, "One": {"Call": "Flaky"}}}},
		"MockedResponses": {
			"Ok": {"0": {"Return": {"ok": true}}},
			"Flaky": {"0": {"Throw": {"Error": "CustomError", "Cause": "once"}}, "1": {"Return": {"ok": true}}}
		}
	}`)
	task := `{"StartAt":"Call","States":{"Call":{"Type":"Task","Resource":"` + lambdaARN + `","ResultSelector":{"c.$":"$$.State.RetryCount"},"Retry":[{"ErrorEquals":["CustomError"],"IntervalSeconds":0,"MaxAttempts":1}],"End":true}}}`
	sm := createMachine(t, s, "ctx", task, roleARN)
	z := startExec(t, s, *sm.StateMachineArn+"#Zero", "z", `{}`)
	ex := waitExec(t, s, *z.ExecutionArn)
	assertEnum(t, ex)
	jsonEqual(t, ex.Output, `{"c":0}`)

	one := startExec(t, s, *sm.StateMachineArn+"#One", "one", `{}`)
	ex = waitExec(t, s, *one.ExecutionArn)
	assertEnum(t, ex)
	jsonEqual(t, ex.Output, `{"c":1}`)

	branch := `{"StartAt":"P","States":{"P":{"Type":"Parallel","Branches":[{"StartAt":"Call","States":{"Call":{"Type":"Task","Resource":"` + lambdaARN + `","End":true}}}],"Retry":[{"ErrorEquals":["States.ALL"],"IntervalSeconds":0,"MaxAttempts":1}],"ResultSelector":{"c.$":"$$.State.RetryCount"},"End":true}}}`
	s.Runner.Mocks = writeMock(t, `{
		"StateMachines": {"par": {"TestCases": {"One": {"Call": "Flaky"}}}},
		"MockedResponses": {"Flaky": {"0": {"Throw": {"Error": "CustomError", "Cause": "once"}}, "1": {"Return": {"ok": true}}}}
	}`)
	psm := createMachine(t, s, "par", branch, roleARN)
	pr := startExec(t, s, *psm.StateMachineArn+"#One", "run", `{}`)
	ex = waitExec(t, s, *pr.ExecutionArn)
	assertEnum(t, ex)
	jsonEqual(t, ex.Output, `{"c":1}`)
	if countEvents(ex, "ParallelStateFailed") != 0 || countEvents(ex, "ParallelStateSucceeded") != 1 {
		t.Fatalf("%v", typesOf(ex))
	}

	mapped := `{"StartAt":"M","States":{"M":{"Type":"Map","ItemsPath":"$.items","Iterator":{"StartAt":"Call","States":{"Call":{"Type":"Task","Resource":"` + lambdaARN + `","End":true}}},"Retry":[{"ErrorEquals":["States.ALL"],"IntervalSeconds":0,"MaxAttempts":1}],"ResultSelector":{"c.$":"$$.State.RetryCount"},"End":true}}}`
	s.Runner.Mocks = writeMock(t, `{
		"StateMachines": {"mp": {"TestCases": {"One": {"Call": "Flaky"}}}},
		"MockedResponses": {"Flaky": {"0": {"Throw": {"Error": "CustomError", "Cause": "once"}}, "1": {"Return": {"ok": true}}}}
	}`)
	msm := createMachine(t, s, "mp", mapped, roleARN)
	mr := startExec(t, s, *msm.StateMachineArn+"#One", "run", `{"items":[1]}`)
	ex = waitExec(t, s, *mr.ExecutionArn)
	assertEnum(t, ex)
	jsonEqual(t, ex.Output, `{"c":1}`)
	if countEvents(ex, "MapStateStarted") != 1 || countEvents(ex, "MapIterationFailed") != 1 || countEvents(ex, "MapIterationSucceeded") != 1 || countEvents(ex, "MapStateSucceeded") != 1 {
		t.Fatalf("%v", typesOf(ex))
	}

	redrive := runPhase4(t, s, "redrive", `{"StartAt":"P","States":{"P":{"Type":"Pass","Parameters":{"c.$":"$$.Execution.RedriveCount"},"End":true}}}`, "run", `{}`)
	if redrive.Error != "States.Runtime" || !strings.Contains(redrive.Cause, "not found") {
		t.Fatalf("%s %s", redrive.Error, redrive.Cause)
	}
	s.Runner.Mocks = writeMock(t, `{
		"StateMachines": {"redrive-task": {"TestCases": {"Zero": {"Call": "Ok"}}}},
		"MockedResponses": {"Ok": {"0": {"Return": {"ok": true}}}}
	}`)
	rt := `{"StartAt":"Call","States":{"Call":{"Type":"Task","Resource":"` + lambdaARN + `","ResultSelector":{"c.$":"$$.Execution.RedriveCount"},"End":true}}}`
	rsm := createMachine(t, s, "redrive-task", rt, roleARN)
	rr := startExec(t, s, *rsm.StateMachineArn+"#Zero", "run", `{}`)
	ex = waitExec(t, s, *rr.ExecutionArn)
	assertEnum(t, ex)
	if ex.Error != "States.Runtime" || !strings.Contains(ex.Cause, "not found") {
		t.Fatalf("%s %s %s", ex.Status, ex.Error, ex.Cause)
	}

	params := map[string]string{
		"s.$":      `States.Format('id={}', 'A')`,
		"braces.$": `States.Format('\\{\\}')`,
		"trail.$":  `States.Format('a\\')`,
		"j.$":      `States.StringToJson('{"a":1}')`,
		"t.$":      `States.JsonToString({"a":1})`,
		"a.$":      `States.Array(1, "a", true)`,
		"empty.$":  `States.Array()`,
		"p.$":      `States.ArrayPartition([1,2,3,4,5], 2)`,
		"c.$":      `States.ArrayContains([1, "b"], "1")`,
		"cf.$":     `States.ArrayContains([1, 2], 3)`,
		"r.$":      `States.ArrayRange(0, 10, 3)`,
		"re.$":     `States.ArrayRange(5, 1, 1)`,
		"rd.$":     `States.ArrayRange(5, 1, -1)`,
		"g.$":      `States.ArrayGetItem([10, 20, 30], 1)`,
		"gn.$":     `States.ArrayGetItem([10, 20, 30], 5)`,
		"l.$":      `States.ArrayLength([1, 2, 3])`,
		"lz.$":     `States.ArrayLength([])`,
		"u.$":      `States.ArrayUnique(["b", "a", "b"])`,
		"e.$":      `States.Base64Encode('ab')`,
		"d.$":      `States.Base64Decode('YWI=')`,
		"h.$":      `States.Hash('ab', 'MD5')`,
		"m.$":      `States.JsonMerge({"a":1,"b":2}, {"b":3,"c":4}, false)`,
		"add.$":    `States.MathAdd(1.5, 2.5)`,
		"rnd.$":    `States.MathRandom(0, 10, 1)`,
		"sp.$":     `States.StringSplit('a--b', '-')`,
		"sx.$":     `States.StringSplit('a-b', '-x')`,
	}
	raw, err := json.Marshal(params)
	if err != nil {
		t.Fatal(err)
	}
	call := `{"StartAt":"P","States":{"P":{"Type":"Pass","Parameters":` + string(raw) + `,"End":true}}}`
	ex = runPhase4(t, s, "intrinsic", call, "run", `{}`)
	if ex.Status != store.ExecSucceeded {
		t.Fatalf("%s %s %s", ex.Status, ex.Error, ex.Cause)
	}
	jsonEqual(t, ex.Output, `{
		"s":"id=A","braces":"{}","trail":"a","j":{"a":1},"t":"{\"a\":1}",
		"a":[1,"a",true],"empty":[],"p":[[1,2],[3,4],[5]],"c":true,"cf":false,
		"r":[0,3,6,9],"re":[],"rd":[5,4,3,2,1],"g":20,"gn":null,"l":3,"lz":0,
		"u":["b","a"],"e":"YWI=","d":"ab","h":"187ef4436122d1cc2f40dc2b92f0eba0",
		"m":{"a":1,"b":3,"c":4},"add":5,"rnd":5,"sp":["a","b"],"sx":["a","b"]
	}`)

	idDef := `{"StartAt":"P","States":{"P":{"Type":"Pass","Parameters":{"id.$":"States.UUID()"},"End":true}}}`
	ex = runPhase4(t, s, "uuid", idDef, "run", `{}`)
	var idDoc struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal([]byte(ex.Output), &idDoc); err != nil {
		t.Fatal(err)
	}
	if !regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`).MatchString(idDoc.ID) {
		t.Fatal(ex.Output)
	}

	for i, expr := range []string{
		`States.Format('{}')`,
		`States.Format('{')`,
		`States.Nope()`,
		`States.MathAdd(2147483647, 1)`,
		`States.MathAdd(2147483647.5, 0)`,
		`States.ArrayPartition([1], 0)`,
		`States.ArrayRange(0, 1, 0)`,
		`States.ArrayGetItem([10, 20, 30], 3)`,
		`States.ArrayGetItem([10, 20, 30], -1)`,
		`States.JsonMerge({"a":1}, {"b":2}, true)`,
	} {
		quoted, err := json.Marshal(expr)
		if err != nil {
			t.Fatal(err)
		}
		bad := `{"StartAt":"P","States":{"P":{"Type":"Pass","Parameters":{"v.$":` + string(quoted) + `},"End":true}}}`
		ex = runPhase4(t, s, fmt.Sprintf("bad%d", i), bad, "run", `{}`)
		if ex.Error != "States.Runtime" || strings.Contains(ex.Cause, "IntrinsicFailure") {
			t.Fatalf("%s -> %s %s", expr, ex.Error, ex.Cause)
		}
		if expr == `States.Nope()` && !strings.Contains(ex.Cause, "intrinsic") {
			t.Fatalf("closed %s", ex.Cause)
		}
	}

	unsupported := `{"StartAt":"P","States":{"P":{"Type":"Parallel","Branches":[{"StartAt":"T","States":{"T":{"Type":"Task","Resource":"arn:aws:states:::dynamodb:getItem","End":true}}}],"End":true}}}`
	ex = runPhase4(t, s, "unsupported", unsupported, "run", `{}`)
	if ex.Error != "States.Runtime" || !strings.Contains(ex.Cause, "unsupported resource") || !strings.Contains(ex.Cause, "dynamodb:getItem") || countEvents(ex, "ParallelStateFailed") != 1 || countEvents(ex, "ActivityScheduled") != 0 {
		t.Fatalf("%s %s %v", ex.Error, ex.Cause, typesOf(ex))
	}
	activity := `{"StartAt":"P","States":{"P":{"Type":"Parallel","Branches":[{"StartAt":"T","States":{"T":{"Type":"Task","Resource":"arn:aws:states:::ecs:runTask","End":true}}}],"End":true}}}`
	ex = runPhase4(t, s, "activity", activity, "run", `{}`)
	if ex.Error != "States.Runtime" || !strings.Contains(ex.Cause, "unsupported resource") || !strings.Contains(ex.Cause, "ecs:runTask") || countEvents(ex, "ActivityScheduled") != 0 || countEvents(ex, "ActivityStarted") != 0 {
		t.Fatalf("%s %s %v", ex.Error, ex.Cause, typesOf(ex))
	}
}

func TestPhase4TestState(t *testing.T) {
	s := newService()
	noTestLeak(t, s)
	parallel := `{"StartAt":"P","States":{"P":{"Type":"Parallel","Branches":[{"StartAt":"A","States":{"A":{"Type":"Pass","Result":1,"End":true}}},{"StartAt":"B","States":{"B":{"Type":"Pass","Result":2,"End":true}}}],"Next":"Later"},"Later":{"Type":"Pass","Result":{"ran":true},"End":true}}}`
	out := mustTest(t, s, parallel, `{}`)
	if statusOf(out) != "SUCCEEDED" || str(out.Output) != `[1,2]` || str(out.NextState) != "Later" {
		t.Fatalf("%+v", out)
	}
	mapDef := `{"StartAt":"M","States":{"M":{"Type":"Map","ItemsPath":"$.items","MaxConcurrency":2,"Iterator":{"StartAt":"P","States":{"P":{"Type":"Pass","Parameters":{"n.$":"$.n"},"End":true}}},"Next":"Later"},"Later":{"Type":"Pass","Result":{"ran":true},"End":true}}}`
	out = mustTest(t, s, mapDef, `{"items":[{"n":1},{"n":2}]}`)
	if statusOf(out) != "SUCCEEDED" || str(out.NextState) != "Later" {
		t.Fatalf("%+v", out)
	}
	jsonEqual(t, str(out.Output), `[{"n":1},{"n":2}]`)
}

func TestPhase4StopMap(t *testing.T) {
	s := newService()
	entered := make(chan struct{})
	var once sync.Once
	s.Runner.Sleep = func(ctx context.Context, d time.Duration) error {
		once.Do(func() { close(entered) })
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(30 * time.Second):
			return nil
		}
	}
	def := `{"StartAt":"M","States":{"M":{"Type":"Map","ItemsPath":"$.items","Iterator":{"StartAt":"W","States":{"W":{"Type":"Wait","Seconds":30,"End":true}}},"End":true}}}`
	sm := createMachine(t, s, "stop", def, roleARN)
	began := time.Now()
	out := startExec(t, s, *sm.StateMachineArn, "run", `{"items":[1]}`)
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("map did not wait")
	}
	if _, err := s.StopExecution(context.Background(), &gen.StopExecutionInput{ExecutionArn: out.ExecutionArn}); err != nil {
		t.Fatal(err)
	}
	ex := waitExec(t, s, *out.ExecutionArn)
	if time.Since(began) >= time.Second {
		t.Fatalf("stop took %s", time.Since(began))
	}
	assertEnum(t, ex)
	if ex.Status != store.ExecAborted || countEvents(ex, "ExecutionAborted") != 1 || countEvents(ex, "ExecutionFailed") != 0 {
		t.Fatalf("%s %s %v", ex.Status, ex.Error, typesOf(ex))
	}
}

func runPhase4(t *testing.T, s *Service, name, def, execName, input string) store.Execution {
	t.Helper()
	ex := runMachine(t, s, name, def, execName, input)
	assertEnum(t, ex)
	return ex
}

func assertEnum(t *testing.T, ex store.Execution) {
	t.Helper()
	if len(ex.History) == 0 {
		t.Fatal("no history")
	}
	for i, ev := range ex.History {
		if !gen.HistoryEventType(ev.Type).Valid() {
			t.Fatalf("history type %q is not on the HistoryEventType enum", ev.Type)
		}
		if strings.Contains(ev.Type, "ParallelBranch") {
			t.Fatalf("invented %s", ev.Type)
		}
		if i == 0 && (ev.ID != 1 || ev.PreviousID != 0) {
			t.Fatalf("first id %d previous %d", ev.ID, ev.PreviousID)
		}
		// The event after ExecutionStarted points at 0, as on AWS. Every other
		// event points at an earlier one. TestAWSRecordings checks which.
		// A stopped execution's ExecutionAborted points at 0 too.
		if i == 1 && ev.PreviousID != 0 || i > 1 && ev.Type != "ExecutionAborted" && (ev.PreviousID <= 0 || ev.PreviousID >= ev.ID) {
			t.Fatalf("previous %d at %d", ev.PreviousID, i)
		}
	}
}

func assertCounts(t *testing.T, ex store.Execution, want map[string]int) {
	t.Helper()
	assertEnum(t, ex)
	got := map[string]int{}
	for _, ev := range ex.History {
		got[ev.Type]++
	}
	for typ, n := range want {
		if got[typ] != n {
			t.Fatalf("%s count %d want %d in %v", typ, got[typ], n, got)
		}
	}
	for typ, n := range got {
		if _, ok := want[typ]; !ok {
			t.Fatalf("unexpected %s x%d in %v", typ, n, got)
		}
	}
}

func assertBefore(t *testing.T, ex store.Execution, earlier, later string) {
	t.Helper()
	a, b := eventIndex(ex, earlier), eventIndex(ex, later)
	if a < 0 || b < 0 || a >= b {
		t.Fatalf("%s at %d, %s at %d", earlier, a, later, b)
	}
}

func assertParallelOrder(t *testing.T, ex store.Execution) {
	t.Helper()
	started := eventIndex(ex, "ParallelStateStarted")
	if started < 0 {
		t.Fatal("no ParallelStateStarted")
	}
	for i, ev := range ex.History {
		if i >= started {
			break
		}
		if strings.HasPrefix(ev.Type, "Parallel") || strings.HasPrefix(ev.Type, "Execution") {
			continue
		}
		t.Fatalf("%s before ParallelStateStarted", ev.Type)
	}
	succ, exited := eventIndex(ex, "ParallelStateSucceeded"), eventIndex(ex, "ParallelStateExited")
	if succ < 0 || exited < 0 || succ >= exited {
		t.Fatalf("succeeded %d exited %d", succ, exited)
	}
}

func exitIndex(ex store.Execution, output string) int {
	for i, ev := range ex.History {
		if ev.Type == "PassStateExited" && ev.Output == output {
			return i
		}
	}
	return -1
}

func iterationDetails(ev gen.HistoryEvent) *gen.MapIterationEventDetails {
	switch {
	case ev.MapIterationStartedEventDetails != nil:
		return ev.MapIterationStartedEventDetails
	case ev.MapIterationSucceededEventDetails != nil:
		return ev.MapIterationSucceededEventDetails
	case ev.MapIterationFailedEventDetails != nil:
		return ev.MapIterationFailedEventDetails
	case ev.MapIterationAbortedEventDetails != nil:
		return ev.MapIterationAbortedEventDetails
	default:
		return nil
	}
}
