package api

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/catdevman/sfn-local/internal/sdk"
	"github.com/catdevman/sfn-local/internal/store"
)

// fakeAthena answers in AWS JSON 1.1. Each GetQueryExecution advances the
// query one state, ending in final. hang keeps it RUNNING.
type fakeAthena struct {
	mu      sync.Mutex
	final   string
	hang    bool
	badSQL  bool
	reads   int
	stopped []string
}

func (f *fakeAthena) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	body, _ := io.ReadAll(r.Body)
	var in map[string]any
	_ = json.Unmarshal(body, &in)
	w.Header().Set("Content-Type", "application/x-amz-json-1.1")
	var reply any
	switch strings.TrimPrefix(r.Header.Get("X-Amz-Target"), "AmazonAthena.") {
	case "StartQueryExecution":
		if f.badSQL {
			w.WriteHeader(http.StatusBadRequest)
			io.WriteString(w, `{"__type":"InvalidRequestException","Message":"line 1:1: mismatched input"}`)
			return
		}
		reply = map[string]any{"QueryExecutionId": "q-1"}
	case "GetQueryExecution":
		f.reads++
		state := []string{"QUEUED", "RUNNING", f.final}[min(f.reads-1, 2)]
		if f.hang {
			state = "RUNNING"
		}
		status := map[string]any{"State": state}
		if state == "FAILED" {
			status["StateChangeReason"] = "TABLE_NOT_FOUND: line 1:15: Table 'x' does not exist"
		}
		reply = map[string]any{"QueryExecution": map[string]any{"QueryExecutionId": in["QueryExecutionId"], "Query": "SELECT 1", "Status": status}}
	case "StopQueryExecution":
		id, _ := in["QueryExecutionId"].(string)
		f.stopped = append(f.stopped, id)
		reply = map[string]any{}
	case "GetQueryResults":
		reply = map[string]any{"ResultSet": map[string]any{"Rows": []any{map[string]any{"Data": []any{map[string]any{"VarCharValue": "1"}}}}}}
	default:
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, `{"__type":"UnknownOperationException"}`)
		return
	}
	json.NewEncoder(w).Encode(reply)
}

func athenaService(t *testing.T, f *fakeAthena) *Service {
	t.Helper()
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	s := newService()
	s.Runner.SDK = sdk.New(sdk.Config{Region: "us-east-1", Endpoints: map[string]string{"athena": srv.URL}})
	s.Runner.SyncPoll = 5 * time.Millisecond
	return s
}

func athenaMachine(resource, params, extra string) string {
	return `{"StartAt":"T","States":{"T":{"Type":"Task","Resource":"` + resource + `","Parameters":` + params + extra + `,"End":true}}}`
}

const athenaQuery = `{"QueryString":"SELECT 1","WorkGroup":"primary","ResultConfiguration":{"OutputLocation":"s3://b/out/"}}`

func TestAthenaRequestResponse(t *testing.T) {
	f := &fakeAthena{final: "SUCCEEDED"}
	s := athenaService(t, f)
	ex := runMachine(t, s, "athena-start", athenaMachine("arn:aws:states:::athena:startQueryExecution", athenaQuery, ""), "run", `{}`)
	if ex.Status != store.ExecSucceeded || ex.Output != `{"QueryExecutionId":"q-1"}` || f.reads != 0 {
		t.Fatalf("%s %s %s %s", ex.Status, ex.Error, ex.Cause, ex.Output)
	}
	ex = runMachine(t, s, "athena-results", athenaMachine("arn:aws:states:::athena:getQueryResults", `{"QueryExecutionId":"q-1"}`, ""), "run", `{}`)
	if ex.Status != store.ExecSucceeded || !strings.Contains(ex.Output, `"VarCharValue":"1"`) {
		t.Fatalf("%s %s %s", ex.Status, ex.Error, ex.Output)
	}
	ex = runMachine(t, s, "athena-stop", athenaMachine("arn:aws:states:::athena:stopQueryExecution", `{"QueryExecutionId":"q-1"}`, ""), "run", `{}`)
	if ex.Status != store.ExecSucceeded || len(f.stopped) != 1 {
		t.Fatalf("%s %s %v", ex.Status, ex.Error, f.stopped)
	}
	f.badSQL = true
	ex = runMachine(t, s, "athena-bad", athenaMachine("arn:aws:states:::athena:startQueryExecution", athenaQuery, ""), "run", `{}`)
	if ex.Status != store.ExecFailed || ex.Error != "Athena.InvalidRequestException" || !strings.HasPrefix(ex.Cause, "line 1:1: mismatched input (Service: Athena, Status Code: 400") {
		t.Fatalf("%s %s %s", ex.Status, ex.Error, ex.Cause)
	}
}

func TestAthenaSync(t *testing.T) {
	f := &fakeAthena{final: "SUCCEEDED"}
	s := athenaService(t, f)
	ex := runMachine(t, s, "athena-sync", athenaMachine("arn:aws:states:::athena:startQueryExecution.sync", athenaQuery, ""), "run", `{}`)
	if ex.Status != store.ExecSucceeded || f.reads != 3 {
		t.Fatalf("%s %s %s %d", ex.Status, ex.Error, ex.Cause, f.reads)
	}
	assertTypes(t, ex, "ExecutionStarted", "TaskStateEntered", "TaskScheduled", "TaskStarted", "TaskSubmitted", "TaskSucceeded", "TaskStateExited", "ExecutionSucceeded")
	if !strings.Contains(ex.Output, `"State":"SUCCEEDED"`) || !strings.HasPrefix(ex.Output, `{"QueryExecution":`) {
		t.Fatalf("output %s", ex.Output)
	}
	if submitted := findEventMust(t, ex, "TaskSubmitted"); submitted.Output != `{"QueryExecutionId":"q-1"}` {
		t.Fatalf("submitted %s", submitted.Output)
	}

	for _, final := range []string{"FAILED", "CANCELLED"} {
		f.reads, f.final = 0, final
		ex = runMachine(t, s, "athena-sync-"+strings.ToLower(final), athenaMachine("arn:aws:states:::athena:startQueryExecution.sync", athenaQuery, ""), "run", `{}`)
		if ex.Status != store.ExecFailed || ex.Error != "States.TaskFailed" || !strings.Contains(ex.Cause, `"State":"`+final+`"`) {
			t.Fatalf("%s: %s %s %s", final, ex.Status, ex.Error, ex.Cause)
		}
	}

	f.badSQL = true
	ex = runMachine(t, s, "athena-sync-bad", athenaMachine("arn:aws:states:::athena:startQueryExecution.sync", athenaQuery, ""), "run", `{}`)
	if ex.Status != store.ExecFailed || ex.Error != "Athena.InvalidRequestException" || countEvents(ex, "TaskSubmitFailed") != 1 {
		t.Fatalf("%s %s %v", ex.Status, ex.Error, typesOf(ex))
	}
}

func TestAthenaSyncTimeoutStopsQuery(t *testing.T) {
	f := &fakeAthena{hang: true}
	s := athenaService(t, f)
	ex := runMachine(t, s, "athena-timeout", athenaMachine("arn:aws:states:::athena:startQueryExecution.sync", athenaQuery, `,"TimeoutSeconds":1`), "run", `{}`)
	if ex.Status != store.ExecFailed || ex.Error != "States.Timeout" {
		t.Fatalf("%s %s %s", ex.Status, ex.Error, ex.Cause)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.stopped) != 1 || f.stopped[0] != "q-1" {
		t.Fatalf("stopped %v", f.stopped)
	}
}
