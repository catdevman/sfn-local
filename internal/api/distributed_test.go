package api

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/catdevman/sfn-local/internal/api/gen"
	"github.com/catdevman/sfn-local/internal/exec"
	"github.com/catdevman/sfn-local/internal/store"
)

func TestDistributedMapRun(t *testing.T) {
	s := newService()
	def := `{"StartAt":"Lines","States":{"Lines":{"Type":"Map","ItemsPath":"$.lines","Label":"lines","MaxConcurrency":2,
		"ItemSelector":{"sku.$":"$$.Map.Item.Value.sku","index.$":"$$.Map.Item.Index"},
		"ItemProcessor":{"ProcessorConfig":{"Mode":"DISTRIBUTED","ExecutionType":"STANDARD"},"StartAt":"Pick",
			"States":{"Pick":{"Type":"Pass","Parameters":{"picked.$":"$.sku","n.$":"$.index"},"End":true}}},
		"ResultPath":"$.picked","End":true}}}`
	ex := runMachine(t, s, "dmap", def, "run", `{"lines":[{"sku":"pen"},{"sku":"desk"},{"sku":"book"}]}`)
	if ex.Status != store.ExecSucceeded {
		t.Fatalf("%s %s %s %v", ex.Status, ex.Error, ex.Cause, typesOf(ex))
	}
	jsonEqual(t, ex.Output, `{"lines":[{"sku":"pen"},{"sku":"desk"},{"sku":"book"}],"picked":[{"picked":"pen","n":0},{"picked":"desk","n":1},{"picked":"book","n":2}]}`)
	want := []string{"ExecutionStarted", "MapStateEntered", "MapRunStarted", "MapRunSucceeded", "MapStateExited", "ExecutionSucceeded"}
	if strings.Join(typesOf(ex), ",") != strings.Join(want, ",") {
		t.Fatalf("history %v", typesOf(ex))
	}

	ctx := context.Background()
	runs, err := s.ListMapRuns(ctx, &gen.ListMapRunsInput{ExecutionArn: &ex.ARN})
	if err != nil || len(*runs.MapRuns) != 1 {
		t.Fatalf("list map runs %v %v", runs, err)
	}
	item := (*runs.MapRuns)[0]
	runARN := *item.MapRunArn
	if !strings.HasPrefix(runARN, "arn:aws:states:us-east-1:123456789012:mapRun:dmap/lines:") || *item.StateMachineArn != ex.StateMachineARN || item.StopDate == nil {
		t.Fatalf("map run item %+v", item)
	}
	if ev := ex.History[2]; ev.MapRunARN != runARN {
		t.Fatalf("MapRunStarted %+v", ev)
	}
	desc, err := s.DescribeMapRun(ctx, &gen.DescribeMapRunInput{MapRunArn: &runARN})
	if err != nil {
		t.Fatal(err)
	}
	if *desc.Status != "SUCCEEDED" || *desc.MaxConcurrency != 2 || *desc.ItemCounts.Succeeded != 3 || *desc.ItemCounts.Total != 3 || *desc.ExecutionCounts.Succeeded != 3 || *desc.ItemCounts.Pending != 0 {
		t.Fatalf("describe %+v %+v", desc, desc.ItemCounts)
	}

	list, err := s.ListExecutions(ctx, &gen.ListExecutionsInput{MapRunArn: &runARN})
	if err != nil || len(*list.Executions) != 3 {
		t.Fatalf("children %v %v", list, err)
	}
	child := (*list.Executions)[0]
	if !strings.HasPrefix(*child.ExecutionArn, "arn:aws:states:us-east-1:123456789012:execution:dmap/lines:") || *child.StateMachineArn != "arn:aws:states:us-east-1:123456789012:stateMachine:dmap/lines" || *child.Status != "SUCCEEDED" {
		t.Fatalf("child %+v", child)
	}
	cd, err := s.DescribeExecution(ctx, &gen.DescribeExecutionInput{ExecutionArn: child.ExecutionArn})
	if err != nil || valueOf(cd.MapRunArn) != runARN {
		t.Fatalf("describe child %+v %v", cd, err)
	}
	hist, err := s.GetExecutionHistory(ctx, &gen.GetExecutionHistoryInput{ExecutionArn: child.ExecutionArn})
	if err != nil || len(*hist.Events) != 4 || *(*hist.Events)[0].Type != "ExecutionStarted" || *(*hist.Events)[3].Type != "ExecutionSucceeded" {
		t.Fatalf("child history %v %v", hist, err)
	}
	sm, err := s.DescribeStateMachineForExecution(ctx, &gen.DescribeStateMachineForExecutionInput{ExecutionArn: child.ExecutionArn})
	if err != nil || *sm.Name != "dmap/lines" || valueOf(sm.Label) != "lines" || valueOf(sm.MapRunArn) != runARN || !strings.Contains(*sm.Definition, `"StartAt":"Pick"`) {
		t.Fatalf("child machine %+v %v", sm, err)
	}
	if mine, _ := s.ListExecutions(ctx, &gen.ListExecutionsInput{StateMachineArn: &ex.StateMachineARN}); len(*mine.Executions) != 1 {
		t.Fatalf("machine listing includes children: %d", len(*mine.Executions))
	}
	if _, err := s.ListExecutions(ctx, &gen.ListExecutionsInput{StateMachineArn: &ex.StateMachineARN, MapRunArn: &runARN}); mustAPI(t, err).ErrorName() != "ValidationException" {
		t.Fatalf("both filters %v", err)
	}
	if _, err := s.UpdateMapRun(ctx, &gen.UpdateMapRunInput{MapRunArn: &runARN, MaxConcurrency: i32p(1)}); mustAPI(t, err).ErrorName() != "ValidationException" {
		t.Fatalf("update finished run %v", err)
	}
	missing := "arn:aws:states:us-east-1:123456789012:mapRun:dmap/lines:nope"
	if _, err := s.DescribeMapRun(ctx, &gen.DescribeMapRunInput{MapRunArn: &missing}); mustAPI(t, err).ErrorName() != "ResourceNotFound" {
		t.Fatalf("missing run %v", err)
	}
}

func TestDistributedMapBatching(t *testing.T) {
	s := newService()
	def := `{"StartAt":"Batch","States":{"Batch":{"Type":"Map","ItemsPath":"$.items",
		"ItemBatcher":{"MaxItemsPerBatch":2,"BatchInput":{"tag.$":"$.tag"}},
		"ItemProcessor":{"ProcessorConfig":{"Mode":"DISTRIBUTED","ExecutionType":"EXPRESS"},"StartAt":"Echo",
			"States":{"Echo":{"Type":"Pass","End":true}}},"End":true}}}`
	ex := runMachine(t, s, "batch", def, "run", `{"tag":"t1","items":[1,2,3,4,5]}`)
	if ex.Status != store.ExecSucceeded {
		t.Fatalf("%s %s %s", ex.Status, ex.Error, ex.Cause)
	}
	jsonEqual(t, ex.Output, `[{"BatchInput":{"tag":"t1"},"Items":[1,2]},{"BatchInput":{"tag":"t1"},"Items":[3,4]},{"BatchInput":{"tag":"t1"},"Items":[5]}]`)
	run := s.Store.MapRuns(ex.ARN)[0]
	if run.Items.Total != 5 || run.Items.Succeeded != 5 || run.Executions.Total != 3 {
		t.Fatalf("counts %+v %+v", run.Items, run.Executions)
	}
	for _, child := range s.Store.MapRunExecutions(run.ARN) {
		if !child.Express || !strings.Contains(child.ARN, ":express:batch/") {
			t.Fatalf("express child %s", child.ARN)
		}
	}
}

const failingChild = `"ItemProcessor":{"ProcessorConfig":{"Mode":"DISTRIBUTED","ExecutionType":"STANDARD"},"StartAt":"Check",
	"States":{"Check":{"Type":"Choice","Choices":[{"Variable":"$.bad","BooleanEquals":true,"Next":"Boom"}],"Default":"Ok"},
	"Boom":{"Type":"Fail","Error":"Item.Bad","Cause":"bad item"},"Ok":{"Type":"Pass","Result":"ok","End":true}}}`

func TestDistributedMapFailureThreshold(t *testing.T) {
	s := newService()
	strict := `{"StartAt":"M","States":{"M":{"Type":"Map",` + failingChild + `,
		"Catch":[{"ErrorEquals":["States.ExceedToleratedFailureThreshold"],"Next":"Caught"}],"End":true},
		"Caught":{"Type":"Pass","End":true}}}`
	ex := runMachine(t, s, "strict", strict, "run", `[{"bad":false},{"bad":true},{"bad":false}]`)
	if ex.Status != store.ExecSucceeded {
		t.Fatalf("%s %s %s %v", ex.Status, ex.Error, ex.Cause, typesOf(ex))
	}
	jsonEqual(t, ex.Output, `{"Error":"States.ExceedToleratedFailureThreshold","Cause":"The specified tolerated failure threshold was exceeded"}`)
	if countEvents(ex, "MapRunFailed") != 1 || countEvents(ex, "MapRunSucceeded") != 0 {
		t.Fatalf("history %v", typesOf(ex))
	}
	if run := s.Store.MapRuns(ex.ARN)[0]; run.Status != store.MapRunFailed || run.Items.Failed != 1 {
		t.Fatalf("run %+v", run)
	}

	tolerant := `{"StartAt":"M","States":{"M":{"Type":"Map","ToleratedFailurePercentage":50,` + failingChild + `,"End":true}}}`
	tx := runMachine(t, s, "tolerant", tolerant, "run", `[{"bad":false},{"bad":true},{"bad":false}]`)
	if tx.Status != store.ExecSucceeded {
		t.Fatalf("%s %s %s", tx.Status, tx.Error, tx.Cause)
	}
	var out []any
	if err := json.Unmarshal([]byte(tx.Output), &out); err != nil || len(out) != 3 || out[0] != "ok" || out[2] != "ok" {
		t.Fatalf("output %s", tx.Output)
	}
	rec, _ := out[1].(map[string]any)
	if rec["Status"] != "FAILED" || rec["Error"] != "Item.Bad" || rec["Cause"] != "bad item" || rec["Input"] != `{"bad":true}` || !strings.Contains(rec["ExecutionArn"].(string), ":execution:tolerant/") {
		t.Fatalf("failed record %v", rec)
	}

	counted := `{"StartAt":"M","States":{"M":{"Type":"Map","ToleratedFailureCount":1,` + failingChild + `,"End":true}}}`
	cx := runMachine(t, s, "counted", counted, "run", `[{"bad":true},{"bad":true},{"bad":false}]`)
	if cx.Status != store.ExecFailed || cx.Error != "States.ExceedToleratedFailureThreshold" {
		t.Fatalf("count %s %s", cx.Status, cx.Error)
	}
}

func TestDistributedMapFlatten(t *testing.T) {
	s := newService()
	def := `{"StartAt":"M","States":{"M":{"Type":"Map","ResultWriter":{"WriterConfig":{"Transformation":"FLATTEN"}},
		"ItemProcessor":{"ProcessorConfig":{"Mode":"DISTRIBUTED","ExecutionType":"EXPRESS"},"StartAt":"P",
			"States":{"P":{"Type":"Pass","Parameters":{"pair.$":"States.Array($.a, $.b)"},"OutputPath":"$.pair","End":true}}},"End":true}}}`
	ex := runMachine(t, s, "flat", def, "run", `[{"a":1,"b":2},{"a":3,"b":4}]`)
	if ex.Status != store.ExecSucceeded {
		t.Fatalf("%s %s %s", ex.Status, ex.Error, ex.Cause)
	}
	jsonEqual(t, ex.Output, `[1,2,3,4]`)
}

func TestDistributedMapConcurrency(t *testing.T) {
	s := newService()
	var running, peak int32
	s.Runner.LambdaEndpoint = "http://lambda.invalid"
	s.Runner.Invoke = func(ctx context.Context, call exec.LambdaCall) exec.LambdaReply {
		n := atomic.AddInt32(&running, 1)
		for {
			p := atomic.LoadInt32(&peak)
			if n <= p || atomic.CompareAndSwapInt32(&peak, p, n) {
				break
			}
		}
		time.Sleep(30 * time.Millisecond)
		atomic.AddInt32(&running, -1)
		return exec.LambdaReply{Status: 200, Payload: call.Payload}
	}
	def := `{"StartAt":"M","States":{"M":{"Type":"Map","MaxConcurrencyPath":"$.limit","ItemsPath":"$.items",
		"ItemProcessor":{"ProcessorConfig":{"Mode":"DISTRIBUTED","ExecutionType":"STANDARD"},"StartAt":"Call",
			"States":{"Call":{"Type":"Task","Resource":"arn:aws:lambda:us-east-1:123456789012:function:work","End":true}}},"End":true}}}`
	ex := runMachine(t, s, "conc", def, "run", `{"limit":2,"items":[1,2,3,4,5,6]}`)
	if ex.Status != store.ExecSucceeded || ex.Output != `[1,2,3,4,5,6]` {
		t.Fatalf("%s %s %s %s", ex.Status, ex.Output, ex.Error, ex.Cause)
	}
	if peak != 2 {
		t.Fatalf("peak concurrency %d", peak)
	}
}

func TestDistributedMapStopAbortsChildren(t *testing.T) {
	s := newService()
	var once sync.Once
	started := make(chan struct{})
	s.Runner.LambdaEndpoint = "http://lambda.invalid"
	s.Runner.Invoke = func(ctx context.Context, call exec.LambdaCall) exec.LambdaReply {
		once.Do(func() { close(started) })
		<-ctx.Done()
		return exec.LambdaReply{Err: ctx.Err()}
	}
	def := `{"StartAt":"M","States":{"M":{"Type":"Map",
		"ItemProcessor":{"ProcessorConfig":{"Mode":"DISTRIBUTED","ExecutionType":"STANDARD"},"StartAt":"Call",
			"States":{"Call":{"Type":"Task","Resource":"arn:aws:lambda:us-east-1:123456789012:function:work","End":true}}},"End":true}}}`
	sm := createMachine(t, s, "stop", def, roleARN)
	out := startExec(t, s, *sm.StateMachineArn, "run", `[1,2]`)
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("children did not start")
	}
	if _, err := s.StopExecution(context.Background(), &gen.StopExecutionInput{ExecutionArn: out.ExecutionArn}); err != nil {
		t.Fatal(err)
	}
	ex := waitExec(t, s, *out.ExecutionArn)
	if ex.Status != store.ExecAborted {
		t.Fatalf("parent %s", ex.Status)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		run := s.Store.MapRuns(ex.ARN)[0]
		done := run.Status == store.MapRunAborted
		for _, child := range s.Store.MapRunExecutions(run.ARN) {
			done = done && child.Status == store.ExecAborted
		}
		if done {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("run %s children %v", run.Status, s.Store.MapRunExecutions(run.ARN))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestDistributedMapJSONata(t *testing.T) {
	s := newService()
	def := `{"QueryLanguage":"JSONata","StartAt":"M","States":{"M":{"Type":"Map","Items":"{% $states.input.xs %}",
		"MaxConcurrency":"{% $states.input.limit %}","ToleratedFailureCount":"{% 0 %}",
		"ItemSelector":{"x":"{% $states.context.Map.Item.Value %}","i":"{% $states.context.Map.Item.Index %}"},
		"ItemProcessor":{"ProcessorConfig":{"Mode":"DISTRIBUTED","ExecutionType":"STANDARD"},"StartAt":"P",
			"States":{"P":{"Type":"Pass","Output":"{% $states.input.x * 10 + $states.input.i %}","End":true}}},"End":true}}}`
	ex := runMachine(t, s, "jsonata", def, "run", `{"xs":[1,2,3],"limit":1}`)
	if ex.Status != store.ExecSucceeded {
		t.Fatalf("%s %s %s %v", ex.Status, ex.Error, ex.Cause, typesOf(ex))
	}
	jsonEqual(t, ex.Output, `[10,21,32]`)
}

func TestDistributedMapValidation(t *testing.T) {
	s := newService()
	child := `"StartAt":"P","States":{"P":{"Type":"Pass","End":true}}`
	for name, tc := range map[string]struct{ def, typ, want string }{
		"no type":   {`{"StartAt":"M","States":{"M":{"Type":"Map","ItemProcessor":{"ProcessorConfig":{"Mode":"DISTRIBUTED"},` + child + `},"End":true}}}`, "", "ExecutionType"},
		"express":   {`{"StartAt":"M","States":{"M":{"Type":"Map","ItemProcessor":{"ProcessorConfig":{"Mode":"DISTRIBUTED","ExecutionType":"EXPRESS"},` + child + `},"End":true}}}`, "EXPRESS", "EXPRESS state machine"},
		"label":     {`{"StartAt":"M","States":{"M":{"Type":"Map","Label":"a b","ItemProcessor":{"ProcessorConfig":{"Mode":"DISTRIBUTED","ExecutionType":"STANDARD"},` + child + `},"End":true}}}`, "", "Label"},
		"batcher":   {`{"StartAt":"M","States":{"M":{"Type":"Map","ItemBatcher":{},"ItemProcessor":{"ProcessorConfig":{"Mode":"DISTRIBUTED","ExecutionType":"STANDARD"},` + child + `},"End":true}}}`, "", "MaxItemsPerBatch"},
		"inline":    {`{"StartAt":"M","States":{"M":{"Type":"Map","ToleratedFailureCount":1,"ItemProcessor":{` + child + `},"End":true}}}`, "", "requires ProcessorConfig Mode DISTRIBUTED"},
		"both":      {`{"StartAt":"M","States":{"M":{"Type":"Map","MaxConcurrency":1,"MaxConcurrencyPath":"$.n","ItemProcessor":{"ProcessorConfig":{"Mode":"DISTRIBUTED","ExecutionType":"STANDARD"},` + child + `},"End":true}}}`, "", "mutually exclusive"},
		"pct range": {`{"StartAt":"M","States":{"M":{"Type":"Map","ToleratedFailurePercentage":101,"ItemProcessor":{"ProcessorConfig":{"Mode":"DISTRIBUTED","ExecutionType":"STANDARD"},` + child + `},"End":true}}}`, "", "0..100"},
		"writer":    {`{"StartAt":"M","States":{"M":{"Type":"Map","ResultWriter":{},"ItemProcessor":{"ProcessorConfig":{"Mode":"DISTRIBUTED","ExecutionType":"STANDARD"},` + child + `},"End":true}}}`, "", "WriterConfig"},
	} {
		in := &gen.CreateStateMachineInput{Name: sp(strings.ReplaceAll(name, " ", "-")), Definition: sp(tc.def), RoleArn: sp(roleARN)}
		if tc.typ != "" {
			typ := gen.StateMachineType(tc.typ)
			in.Type = &typ
		}
		_, err := s.CreateStateMachine(context.Background(), in)
		if api := mustAPI(t, err); api.ErrorName() != "InvalidDefinition" || !strings.Contains(api.Error(), tc.want) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func i32p(n int32) *int32 { return &n }

func TestDistributedMapUpdateConcurrency(t *testing.T) {
	s := newService()
	var running, peak int32
	release := make(chan struct{})
	s.Runner.LambdaEndpoint = "http://lambda.invalid"
	s.Runner.Invoke = func(ctx context.Context, call exec.LambdaCall) exec.LambdaReply {
		n := atomic.AddInt32(&running, 1)
		for {
			p := atomic.LoadInt32(&peak)
			if n <= p || atomic.CompareAndSwapInt32(&peak, p, n) {
				break
			}
		}
		<-release
		atomic.AddInt32(&running, -1)
		return exec.LambdaReply{Status: 200, Payload: call.Payload}
	}
	def := `{"StartAt":"M","States":{"M":{"Type":"Map","MaxConcurrency":1,
		"ItemProcessor":{"ProcessorConfig":{"Mode":"DISTRIBUTED","ExecutionType":"STANDARD"},"StartAt":"Call",
			"States":{"Call":{"Type":"Task","Resource":"arn:aws:lambda:us-east-1:123456789012:function:work","End":true}}},"End":true}}}`
	sm := createMachine(t, s, "update", def, roleARN)
	out := startExec(t, s, *sm.StateMachineArn, "run", `[1,2,3]`)
	waitFor(t, func() bool { return atomic.LoadInt32(&running) == 1 })
	run := s.Store.MapRuns(*out.ExecutionArn)[0]
	if _, err := s.UpdateMapRun(context.Background(), &gen.UpdateMapRunInput{MapRunArn: &run.ARN, MaxConcurrency: i32p(3)}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return atomic.LoadInt32(&running) == 3 })
	close(release)
	ex := waitExec(t, s, *out.ExecutionArn)
	if ex.Status != store.ExecSucceeded || peak != 3 {
		t.Fatalf("%s peak %d", ex.Status, peak)
	}
	if got, _ := s.Store.MapRun(run.ARN); got.MaxConcurrency != 3 {
		t.Fatalf("stored %d", got.MaxConcurrency)
	}
}

func waitFor(t *testing.T, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
