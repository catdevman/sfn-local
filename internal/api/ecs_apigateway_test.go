package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/catdevman/sfn-local/internal/api/gen"
	"github.com/catdevman/sfn-local/internal/sdk"
	"github.com/catdevman/sfn-local/internal/store"
)

// fakeECS answers RunTask, DescribeTasks, and StopTask in AWS JSON 1.1. Each
// DescribeTasks call advances the task one status. exit is the container's
// exit code once the task stops. A negative exit leaves it out.
type fakeECS struct {
	mu        sync.Mutex
	failures  bool
	runErr    bool
	neverStop bool
	exit      int
	describes int
	stopped   []string
	runs      []map[string]any
}

const ecsTaskARN = "arn:aws:ecs:us-east-1:123456789012:task/c/abc"

func (f *fakeECS) task(status string) map[string]any {
	container := map[string]any{"name": "app", "lastStatus": status}
	if status == "STOPPED" && f.exit >= 0 {
		container["exitCode"] = f.exit
	}
	return map[string]any{
		"taskArn":    ecsTaskARN,
		"clusterArn": "arn:aws:ecs:us-east-1:123456789012:cluster/c",
		"lastStatus": status,
		"containers": []any{container},
	}
}

func (f *fakeECS) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	body, _ := io.ReadAll(r.Body)
	var in map[string]any
	_ = json.Unmarshal(body, &in)
	w.Header().Set("Content-Type", "application/x-amz-json-1.1")
	reply := map[string]any{}
	switch strings.TrimPrefix(r.Header.Get("X-Amz-Target"), "AmazonEC2ContainerServiceV20141113.") {
	case "RunTask":
		f.runs = append(f.runs, in)
		if f.runErr {
			w.WriteHeader(http.StatusBadRequest)
			io.WriteString(w, `{"__type":"ClusterNotFoundException","message":"Cluster not found."}`)
			return
		}
		if f.failures {
			reply["tasks"] = []any{}
			reply["failures"] = []any{map[string]any{"arn": "arn:aws:ecs:us-east-1:123456789012:container-instance/x", "reason": "RESOURCE:MEMORY"}}
		} else {
			reply["tasks"] = []any{f.task("PROVISIONING")}
			reply["failures"] = []any{}
		}
	case "DescribeTasks":
		f.describes++
		status := "RUNNING"
		if f.describes >= 2 && !f.neverStop {
			status = "STOPPED"
		}
		reply["tasks"] = []any{f.task(status)}
	case "StopTask":
		task, _ := in["task"].(string)
		f.stopped = append(f.stopped, task)
		reply["task"] = f.task("STOPPED")
	default:
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, `{"__type":"UnknownOperationException"}`)
		return
	}
	json.NewEncoder(w).Encode(reply)
}

func ecsService(t *testing.T, f *fakeECS) *Service {
	t.Helper()
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	s := newService()
	s.Runner.SDK = sdk.New(sdk.Config{Region: "us-east-1", Endpoints: map[string]string{"ecs": srv.URL}})
	s.Runner.SyncPoll = 5 * time.Millisecond
	return s
}

func ecsMachine(resource, extra string) string {
	return `{"StartAt":"T","States":{"T":{"Type":"Task","Resource":"` + resource + `",` +
		`"Parameters":{"Cluster":"c","TaskDefinition":"td:1","LaunchType":"FARGATE"}` + extra + `,"End":true}}}`
}

func TestECSRunTask(t *testing.T) {
	f := &fakeECS{}
	s := ecsService(t, f)
	ex := runMachine(t, s, "ecs-rr", ecsMachine("arn:aws:states:::ecs:runTask", ""), "run", `{}`)
	if ex.Status != store.ExecSucceeded {
		t.Fatalf("%s %s %s %v", ex.Status, ex.Error, ex.Cause, typesOf(ex))
	}
	if !strings.Contains(ex.Output, `"TaskArn":"`+ecsTaskARN+`"`) || f.describes != 0 {
		t.Fatalf("output %s describes %d", ex.Output, f.describes)
	}
	if f.runs[0]["cluster"] != "c" || f.runs[0]["taskDefinition"] != "td:1" || f.runs[0]["launchType"] != "FARGATE" {
		t.Fatalf("request %v", f.runs[0])
	}

	// Request-response returns Failures and does not fail.
	f.failures = true
	ex = runMachine(t, s, "ecs-rr-failures", ecsMachine("arn:aws:states:::ecs:runTask", ""), "run", `{}`)
	if ex.Status != store.ExecSucceeded || !strings.Contains(ex.Output, "RESOURCE:MEMORY") {
		t.Fatalf("%s %s %s", ex.Status, ex.Error, ex.Output)
	}

	f.failures, f.runErr = false, true
	ex = runMachine(t, s, "ecs-rr-error", ecsMachine("arn:aws:states:::ecs:runTask", ""), "run", `{}`)
	if ex.Status != store.ExecFailed || ex.Error != "ECS.ClusterNotFoundException" || !strings.HasPrefix(ex.Cause, "Cluster not found. (Service: ECS, Status Code: 400") {
		t.Fatalf("%s %s %s", ex.Status, ex.Error, ex.Cause)
	}
}

func TestECSRunTaskSync(t *testing.T) {
	f := &fakeECS{}
	s := ecsService(t, f)
	ex := runMachine(t, s, "ecs-sync", ecsMachine("arn:aws:states:::ecs:runTask.sync", ""), "run", `{}`)
	if ex.Status != store.ExecSucceeded {
		t.Fatalf("%s %s %s %v", ex.Status, ex.Error, ex.Cause, typesOf(ex))
	}
	assertTypes(t, ex, "ExecutionStarted", "TaskStateEntered", "TaskScheduled", "TaskStarted", "TaskSubmitted", "TaskSucceeded", "TaskStateExited", "ExecutionSucceeded")
	if jsonField(t, ex.Output, "LastStatus") != "STOPPED" || jsonField(t, ex.Output, "TaskArn") != ecsTaskARN || f.describes != 2 {
		t.Fatalf("output %s describes %d", ex.Output, f.describes)
	}
	if submitted := findEventMust(t, ex, "TaskSubmitted"); !strings.Contains(submitted.Output, "PROVISIONING") {
		t.Fatalf("submitted %s", submitted.Output)
	}

	f.describes, f.exit = 0, 1
	ex = runMachine(t, s, "ecs-sync-exit", ecsMachine("arn:aws:states:::ecs:runTask.sync", ""), "run", `{}`)
	if ex.Status != store.ExecFailed || ex.Error != "States.TaskFailed" || !strings.Contains(ex.Cause, `"ExitCode":1`) {
		t.Fatalf("%s %s %s", ex.Status, ex.Error, ex.Cause)
	}

	f.describes, f.exit = 0, -1
	ex = runMachine(t, s, "ecs-sync-noexit", ecsMachine("arn:aws:states:::ecs:runTask.sync", ""), "run", `{}`)
	if ex.Status != store.ExecFailed || ex.Error != "States.TaskFailed" {
		t.Fatalf("%s %s %s", ex.Status, ex.Error, ex.Cause)
	}

	f.describes, f.exit, f.failures = 0, 0, true
	ex = runMachine(t, s, "ecs-sync-failures", ecsMachine("arn:aws:states:::ecs:runTask.sync", ""), "run", `{}`)
	if ex.Status != store.ExecFailed || ex.Error != "AmazonECS.Unknown" || !strings.Contains(ex.Cause, "RESOURCE:MEMORY") || f.describes != 0 {
		t.Fatalf("%s %s %s", ex.Status, ex.Error, ex.Cause)
	}

	f.failures, f.runErr = false, true
	ex = runMachine(t, s, "ecs-sync-error", ecsMachine("arn:aws:states:::ecs:runTask.sync", ""), "run", `{}`)
	if ex.Status != store.ExecFailed || ex.Error != "ECS.ClusterNotFoundException" || countEvents(ex, "TaskSubmitFailed") != 1 {
		t.Fatalf("%s %s %s %v", ex.Status, ex.Error, ex.Cause, typesOf(ex))
	}
}

func TestECSRunTaskSyncTimeoutStopsTask(t *testing.T) {
	f := &fakeECS{neverStop: true}
	s := ecsService(t, f)
	ex := runMachine(t, s, "ecs-timeout", ecsMachine("arn:aws:states:::ecs:runTask.sync", `,"TimeoutSeconds":1`), "run", `{}`)
	if ex.Status != store.ExecFailed || ex.Error != "States.Timeout" || countEvents(ex, "TaskTimedOut") != 1 {
		t.Fatalf("%s %s %s %v", ex.Status, ex.Error, ex.Cause, typesOf(ex))
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.stopped) != 1 || f.stopped[0] != ecsTaskARN {
		t.Fatalf("stopped %v", f.stopped)
	}
}

func TestECSRunTaskSyncStopExecutionStopsTask(t *testing.T) {
	f := &fakeECS{neverStop: true}
	s := ecsService(t, f)
	sm := createMachine(t, s, "ecs-stop", ecsMachine("arn:aws:states:::ecs:runTask.sync", ""), roleARN)
	out := startExec(t, s, *sm.StateMachineArn, "run", `{}`)
	waitHistory(t, s, *out.ExecutionArn, "TaskSubmitted")
	if _, err := s.StopExecution(context.Background(), &gen.StopExecutionInput{ExecutionArn: out.ExecutionArn}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		f.mu.Lock()
		defer f.mu.Unlock()
		return len(f.stopped) == 1
	})
}

func TestECSRunTaskCallback(t *testing.T) {
	f := &fakeECS{}
	s := ecsService(t, f)
	def := `{"StartAt":"T","States":{"T":{"Type":"Task","Resource":"arn:aws:states:::ecs:runTask.waitForTaskToken",` +
		`"Parameters":{"Cluster":"c","TaskDefinition":"td:1","Overrides":{"ContainerOverrides":[{"Name":"app","Environment":[{"Name":"TOKEN","Value.$":"$$.Task.Token"}]}]}},"End":true}}}`
	sm := createMachine(t, s, "ecs-cb", def, roleARN)
	out := startExec(t, s, *sm.StateMachineArn, "run", `{}`)
	submitted := waitHistory(t, s, *out.ExecutionArn, "TaskSubmitted")
	if !strings.Contains(submitted.Output, ecsTaskARN) {
		t.Fatalf("submitted %s", submitted.Output)
	}
	f.mu.Lock()
	overrides, _ := f.runs[0]["overrides"].(map[string]any)
	f.mu.Unlock()
	container := overrides["containerOverrides"].([]any)[0].(map[string]any)
	token := container["environment"].([]any)[0].(map[string]any)["value"].(string)
	if _, err := s.SendTaskSuccess(context.Background(), &gen.SendTaskSuccessInput{TaskToken: sp(token), Output: sp(`{"done":true}`)}); err != nil {
		t.Fatal(err)
	}
	if ex := waitExec(t, s, *out.ExecutionArn); ex.Status != store.ExecSucceeded || ex.Output != `{"done":true}` {
		t.Fatalf("%s %s %s", ex.Status, ex.Error, ex.Output)
	}

	f.failures = true
	ex := runMachine(t, s, "ecs-cb-failures", ecsMachine("arn:aws:states:::ecs:runTask.waitForTaskToken", ""), "run", `{}`)
	if ex.Status != store.ExecFailed || ex.Error != "AmazonECS.Unknown" {
		t.Fatalf("%s %s %s %v", ex.Status, ex.Error, ex.Cause, typesOf(ex))
	}
}

// apiGatewayService points API_GATEWAY_ENDPOINT at h.
func apiGatewayService(t *testing.T, h http.HandlerFunc) *Service {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	s := newService()
	s.Runner.APIGatewayEndpoint = srv.URL
	return s
}

func apiGatewayMachine(params string) string {
	return `{"StartAt":"T","States":{"T":{"Type":"Task","Resource":"arn:aws:states:::apigateway:invoke","Parameters":` + params + `,"End":true}}}`
}

func TestAPIGatewayInvoke(t *testing.T) {
	var got *http.Request
	var gotBody string
	s := apiGatewayService(t, func(w http.ResponseWriter, r *http.Request) {
		got = r
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		switch r.URL.Path {
		case "/prod/bills":
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			w.Header().Add("X-Thing", "a")
			w.Header().Add("X-Thing", "b")
			io.WriteString(w, `{"myBills":[]}`)
		case "/text":
			w.Header().Set("Content-Type", "text/plain")
			io.WriteString(w, `{"not":"parsed"}`)
		case "/denied":
			w.WriteHeader(http.StatusForbidden)
			io.WriteString(w, `{"message":"Missing Authentication Token"}`)
		default:
			w.WriteHeader(http.StatusCreated)
		}
	})

	ex := runMachine(t, s, "apigw-get", apiGatewayMachine(`{"ApiEndpoint":"abc123.execute-api.us-east-1.amazonaws.com","Method":"GET",`+
		`"Headers":{"key":["value1","value2"]},"Stage":"prod","Path":"bills","QueryParameters":{"billId":["123456"]},"RequestBody":{},"AuthType":"NO_AUTH"}`), "run", `{}`)
	if ex.Status != store.ExecSucceeded {
		t.Fatalf("%s %s %s", ex.Status, ex.Error, ex.Cause)
	}
	if got.Host != "abc123.execute-api.us-east-1.amazonaws.com" {
		t.Fatalf("host %s", got.Host)
	}
	if got.Method != "GET" || got.URL.RawQuery != "billId=123456" || strings.Join(got.Header.Values("Key"), ",") != "value1,value2" || gotBody != "" {
		t.Fatalf("request %s %s %v %q", got.Method, got.URL, got.Header, gotBody)
	}
	var out map[string]any
	if err := json.Unmarshal([]byte(ex.Output), &out); err != nil {
		t.Fatal(err)
	}
	headers, _ := out["Headers"].(map[string]any)
	if out["StatusCode"] != float64(200) || out["StatusText"] != "OK" || len(headers["X-Thing"].([]any)) != 2 {
		t.Fatalf("output %s", ex.Output)
	}
	jsonEqual(t, mustCompact(t, out["ResponseBody"]), `{"myBills":[]}`)

	ex = runMachine(t, s, "apigw-post", apiGatewayMachine(`{"ApiEndpoint":"abc123.execute-api.us-east-1.amazonaws.com","Method":"POST","Path":"/bills/add","RequestBody":{"billId":"b","gone":null}}`), "run", `{}`)
	if ex.Status != store.ExecSucceeded || got.URL.Path != "/bills/add" || gotBody != `{"billId":"b"}` || got.Header.Get("Content-Type") != "application/json" {
		t.Fatalf("%s %s %s %q", ex.Status, ex.Cause, got.URL, gotBody)
	}
	if jsonField(t, ex.Output, "StatusText") != "Created" || !strings.Contains(ex.Output, `"ResponseBody":""`) {
		t.Fatalf("output %s", ex.Output)
	}

	ex = runMachine(t, s, "apigw-nulls", apiGatewayMachine(`{"ApiEndpoint":"abc123.execute-api.us-east-1.amazonaws.com","Method":"PUT","AllowNullValues":true,"RequestBody":{"kept":null}}`), "run", `{}`)
	if ex.Status != store.ExecSucceeded || gotBody != `{"kept":null}` {
		t.Fatalf("%s %s %q", ex.Status, ex.Cause, gotBody)
	}

	ex = runMachine(t, s, "apigw-string", apiGatewayMachine(`{"ApiEndpoint":"abc123.execute-api.us-east-1.amazonaws.com","Method":"POST","RequestBody":"raw text","Headers":{"Content-Type":["text/plain"]}}`), "run", `{}`)
	if ex.Status != store.ExecSucceeded || gotBody != "raw text" || got.Header.Get("Content-Type") != "text/plain" {
		t.Fatalf("%s %s %q", ex.Status, ex.Cause, gotBody)
	}

	ex = runMachine(t, s, "apigw-text", apiGatewayMachine(`{"ApiEndpoint":"abc123.execute-api.us-east-1.amazonaws.com","Method":"GET","Path":"text"}`), "run", `{}`)
	if ex.Status != store.ExecSucceeded || !strings.Contains(ex.Output, `"ResponseBody":"{\"not\":\"parsed\"}"`) {
		t.Fatalf("%s %s %s", ex.Status, ex.Cause, ex.Output)
	}

	ex = runMachine(t, s, "apigw-403", apiGatewayMachine(`{"ApiEndpoint":"abc123.execute-api.us-east-1.amazonaws.com","Method":"GET","Path":"denied"}`), "run", `{}`)
	if ex.Status != store.ExecFailed || ex.Error != "ApiGateway.403" || ex.Cause != `{"message":"Missing Authentication Token"}` {
		t.Fatalf("%s %s %s", ex.Status, ex.Error, ex.Cause)
	}

	for name, params := range map[string]string{
		"apigw-forbidden": `{"ApiEndpoint":"a.execute-api.us-east-1.amazonaws.com","Method":"GET","Headers":{"X-Amz-Date":["x"]}}`,
		"apigw-host":      `{"ApiEndpoint":"a.execute-api.us-east-1.amazonaws.com","Method":"GET","Headers":{"host":["x"]}}`,
		"apigw-method":    `{"ApiEndpoint":"a.execute-api.us-east-1.amazonaws.com","Method":"TRACE"}`,
		"apigw-endpoint":  `{"Method":"GET"}`,
		"apigw-header":    `{"ApiEndpoint":"a.execute-api.us-east-1.amazonaws.com","Method":"GET","Headers":{"k":"v"}}`,
		"apigw-auth":      `{"ApiEndpoint":"a.execute-api.us-east-1.amazonaws.com","Method":"GET","AuthType":"COGNITO"}`,
	} {
		ex = runMachine(t, s, name, apiGatewayMachine(params), "run", `{}`)
		if ex.Status != store.ExecFailed || ex.Error != "States.Runtime" || !strings.HasPrefix(ex.Cause, "invalid apigateway:invoke parameters: ") {
			t.Errorf("%s: %s %s %s", name, ex.Status, ex.Error, ex.Cause)
		}
	}
}

func TestAPIGatewayEndpointAPIID(t *testing.T) {
	var host string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host = r.URL.Path
	}))
	defer srv.Close()
	s := newService()
	s.Runner.APIGatewayEndpoint = srv.URL + "/{apiId}/"
	ex := runMachine(t, s, "apigw-id", apiGatewayMachine(`{"ApiEndpoint":"xyz789.execute-api.us-east-1.amazonaws.com","Method":"GET","Stage":"dev","Path":"p"}`), "run", `{}`)
	if ex.Status != store.ExecSucceeded || host != "/xyz789/dev/p" {
		t.Fatalf("%s %s %s", ex.Status, ex.Cause, host)
	}
}

func TestAPIGatewayCallback(t *testing.T) {
	tokens := make(chan string, 1)
	s := apiGatewayService(t, func(w http.ResponseWriter, r *http.Request) {
		tokens <- r.Header.Get("TaskToken")
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"queued":true}`)
	})
	def := `{"StartAt":"T","States":{"T":{"Type":"Task","Resource":"arn:aws:states:::apigateway:invoke.waitForTaskToken","Parameters":` +
		`{"ApiEndpoint":"abc123.execute-api.us-east-1.amazonaws.com","Method":"POST","Headers":{"TaskToken.$":"States.Array($$.Task.Token)"},"Path":"bills/add","RequestBody":{"billId":"b"}},"End":true}}}`
	sm := createMachine(t, s, "apigw-cb", def, roleARN)
	out := startExec(t, s, *sm.StateMachineArn, "run", `{}`)
	token := <-tokens
	submitted := waitHistory(t, s, *out.ExecutionArn, "TaskSubmitted")
	if !strings.Contains(submitted.Output, `"queued":true`) {
		t.Fatalf("submitted %s", submitted.Output)
	}
	if _, err := s.SendTaskSuccess(context.Background(), &gen.SendTaskSuccessInput{TaskToken: sp(token), Output: sp(`{"paid":true}`)}); err != nil {
		t.Fatal(err)
	}
	if ex := waitExec(t, s, *out.ExecutionArn); ex.Status != store.ExecSucceeded || ex.Output != `{"paid":true}` {
		t.Fatalf("%s %s %s", ex.Status, ex.Error, ex.Output)
	}
}

func mustCompact(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
