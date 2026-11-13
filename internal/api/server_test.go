package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/catdevman/sfn-local/internal/api/gen"
	"github.com/catdevman/sfn-local/internal/exec"
	"github.com/catdevman/sfn-local/internal/store"
)

func TestDeferredOperationsAreModeled(t *testing.T) {
	srv := prodServer(t, ProfileSmithy)
	body := call(t, srv, "AWSStepFunctions.ListMapRuns", `{"executionArn":"arn:aws:states:us-east-1:123456789012:execution:sm:run"}`, contentTypeJSON10)
	if body.Type != "com.amazonaws.sfn#ValidationException" || body.Message != "Distributed Map is not supported by this local server." {
		t.Fatalf("%+v", body)
	}
	if body.Status != 400 || body.Header.Get("Content-Type") != contentTypeJSON10 {
		t.Fatalf("status %d type %s", body.Status, body.Header.Get("Content-Type"))
	}
	if body.Header.Get("X-Amzn-Errortype") != body.Type {
		t.Fatalf("errortype %s", body.Header.Get("X-Amzn-Errortype"))
	}

	def := `{"StartAt":"P","States":{"P":{"Type":"Pass","End":true}}}`
	create := `{"name":"rebuild-check","definition":` + jsonString(def) + `,"roleArn":"arn:aws:iam::123456789012:role/local"}`
	code, raw := callRaw(t, srv, "AWSStepFunctions.CreateStateMachine", create)
	if code != 200 || !strings.Contains(string(raw), "stateMachineArn") {
		t.Fatalf("create %d %s", code, raw)
	}
	code, raw = callRaw(t, srv, "AWSStepFunctions.ValidateStateMachineDefinition", `{"definition":`+jsonString(def)+`}`)
	if code != 200 || !strings.Contains(string(raw), `"result":"OK"`) || !strings.Contains(string(raw), `"diagnostics":[]`) {
		t.Fatalf("validate %d %s", code, raw)
	}
	code, raw = callRaw(t, srv, "AWSStepFunctions.TestState", `{"definition":`+jsonString(def)+`}`)
	if code != 200 || !strings.Contains(string(raw), `"status":"SUCCEEDED"`) || !strings.Contains(string(raw), `"output":"{}"`) {
		t.Fatalf("test state %d %s", code, raw)
	}
	mapRun := call(t, srv, "AWSStepFunctions.DescribeMapRun", `{"mapRunArn":"arn:aws:states:us-east-1:123456789012:mapRun:sm:1"}`, contentTypeJSON10)
	if mapRun.Code() != "ValidationException" || mapRun.Message != "Distributed Map is not supported by this local server." {
		t.Fatalf("%+v", mapRun)
	}
}

func TestValidationPrecedesDeferred(t *testing.T) {
	srv := prodServer(t, ProfileSmithy)
	got := call(t, srv, "AWSStepFunctions.DescribeMapRun", `{}`, contentTypeJSON10)
	if got.Code() != "ValidationException" || !strings.Contains(got.Message, "Member must not be null") {
		t.Fatalf("%+v", got)
	}
	got = call(t, srv, "AWSStepFunctions.ListStateMachines", `{"maxResults":1001}`, contentTypeJSON10)
	if got.Code() != "ValidationException" || !strings.Contains(got.Message, "less than or equal to 1000") {
		t.Fatalf("%+v", got)
	}
	code, raw := callRaw(t, srv, "AWSStepFunctions.ListStateMachines", `{"maxResults":0,"unknown":true}`)
	if code != 200 || !strings.Contains(string(raw), `"stateMachines":[]`) {
		t.Fatalf("list %d %s", code, raw)
	}
}

func TestUnknownOperationAndMissingHeader(t *testing.T) {
	srv := prodServer(t, ProfileSmithy)
	got := call(t, srv, "AWSStepFunctions.NotARealOperation", `{}`, contentTypeJSON10)
	if got.Status != 400 || got.Type != UnknownOperationType || !strings.Contains(got.Message, "AWSStepFunctions.NotARealOperation") {
		t.Fatalf("%+v", got)
	}
	req := httptest.NewRequest(http.MethodPost, "/proxy/path", strings.NewReader(`{}`))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	got = decodeRec(t, rec)
	if got.Status != 400 || got.Type != "com.amazonaws.sfn#MissingRequiredParameter" || !strings.Contains(got.Message, "X-Amz-Target") {
		t.Fatalf("%+v", got)
	}
	got = call(t, srv, "NoDot", `{}`, contentTypeJSON10)
	if got.Type != "com.amazonaws.sfn#MissingRequiredParameter" {
		t.Fatalf("%+v", got)
	}
}

func TestCompatProfile(t *testing.T) {
	smithy := prodServer(t, ProfileSmithy)
	compat := prodServer(t, ProfileSFNLocal)
	forbidden := []string{jarModelPrefix, jarCoralPrefix, jarModelDot}
	for _, name := range []string{"CreateStateMachine", "TestState", "DescribeMapRun", "GetActivityTask"} {
		body := `{}`
		if name == "GetActivityTask" {
			body = `{"activityArn":"arn:aws:states:us-east-1:123456789012:activity:a"}`
		}
		if name == "DescribeMapRun" {
			body = `{"mapRunArn":"arn:aws:states:us-east-1:123456789012:mapRun:sm:1"}`
		}
		got := call(t, smithy, "AWSStepFunctions."+name, body, contentTypeJSON10)
		for _, bad := range forbidden {
			if strings.Contains(got.Type, bad) || strings.Contains(string(got.Raw), bad) {
				t.Fatalf("default profile emitted %s in %+v", bad, got)
			}
		}
	}
	code, raw := callRaw(t, compat, "AWSStepFunctions.ListStateMachines", `{}`)
	if code != 200 {
		t.Fatalf("compat list %d %s", code, raw)
	}
	for _, bad := range forbidden {
		if strings.Contains(string(raw), bad) {
			t.Fatalf("compat list emitted %s in %s", bad, raw)
		}
	}
	task := call(t, compat, "AWSStepFunctions.GetActivityTask", `{"activityArn":"arn:aws:states:us-east-1:123456789012:activity:a"}`, contentTypeJSON10)
	if task.Type != "com.amazonaws.sfn#ActivityDoesNotExist" || !strings.Contains(task.Message, "Activity Does Not Exist:") {
		t.Fatalf("compat activity %+v", task)
	}
	for name := range jarAbsent {
		got := call(t, compat, "AWSStepFunctions."+name, `{}`, contentTypeJSON10)
		if got.Status != 500 || got.Type != jarUnsupported || got.Message != "Unsupported Operation: '"+name+"'" {
			t.Fatalf("%s %+v", name, got)
		}
	}
	if len(jarAbsent) != 13 {
		t.Fatalf("jar-absent set %d", len(jarAbsent))
	}
	if JarAbsent("TestState") {
		t.Fatal("TestState is implemented and must not be short-circuited")
	}
	tested := call(t, compat, "AWSStepFunctions.TestState", `{}`, contentTypeJSON10)
	if tested.Status != 400 || tested.Type != "com.amazonaws.sfn#ValidationException" || !strings.Contains(tested.Message, "Member must not be null") {
		t.Fatalf("compat TestState %+v", tested)
	}
	for _, bad := range forbidden {
		if strings.Contains(tested.Type, bad) || strings.Contains(string(tested.Raw), bad) {
			t.Fatalf("compat TestState emitted %s in %+v", bad, tested)
		}
	}
	missing := httptest.NewRecorder()
	compat.ServeHTTP(missing, httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`)))
	got := decodeRec(t, missing)
	if got.Type != jarMissingHdr {
		t.Fatalf("compat missing header %+v", got)
	}
}

func TestProtocolEdges(t *testing.T) {
	var log bytes.Buffer
	base := prodServer(t, ProfileSmithy)
	base.Log = &log
	req := httptest.NewRequest(http.MethodPost, "/anywhere", strings.NewReader(""))
	req.Header.Set("x-amz-target", "AWSStepFunctions.ListStateMachines")
	rec := httptest.NewRecorder()
	base.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatal(rec.Body)
	}
	text := log.String()
	if !strings.Contains(text, "ListStateMachines => {}") || !strings.Contains(text, "[200] ListStateMachines <= ") {
		t.Fatalf("access log %q", text)
	}
	got := call(t, base, "AWSStepFunctions.ListStateMachines", `{}{}`, contentTypeJSON10)
	if got.Code() != "ValidationException" || !strings.Contains(got.Message, "trailing") {
		t.Fatalf("%+v", got)
	}
	req = httptest.NewRequest(http.MethodGet, "/not-a-console", nil)
	rec = httptest.NewRecorder()
	base.ServeHTTP(rec, req)
	got = decodeRec(t, rec)
	if got.Status != 400 || got.Message == "" || got.Raw == nil {
		t.Fatalf("%+v", got)
	}
	req = httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set(targetHeader, "AWSStepFunctions.ListStateMachines")
	rec = httptest.NewRecorder()
	base.ServeHTTP(rec, req)
	if rec.Code != 400 {
		t.Fatal(rec.Body)
	}
	big := bytes.Repeat([]byte("a"), maxBody+1)
	req = httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(big))
	req.Header.Set(targetHeader, "AWSStepFunctions.ListStateMachines")
	rec = httptest.NewRecorder()
	base.ServeHTTP(rec, req)
	got = decodeRec(t, rec)
	if got.Status != 400 || !strings.Contains(got.Message, "too large") {
		t.Fatalf("%+v", got)
	}
}

func TestConsolePage(t *testing.T) {
	var log bytes.Buffer
	base := prodServer(t, ProfileSmithy)
	base.Log = &log
	for _, path := range []string{"/", "/console", "/console/", "/dashboard/stop"} {
		log.Reset()
		rec := httptest.NewRecorder()
		base.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("%s %d %s", path, rec.Code, rec.Body)
		}
		if strings.Contains(rec.Header().Get("Content-Type"), "text/html") {
			t.Fatalf("%s served html", path)
		}
		got := decodeRec(t, rec)
		if !strings.Contains(got.Message, "HTTP method must be POST") {
			t.Fatalf("%s %+v", path, got)
		}
		if log.Len() == 0 {
			t.Fatalf("%s was not access-logged", path)
		}
	}
	for _, path := range []string{"/dashboard", "/dashboard/"} {
		log.Reset()
		rec := httptest.NewRecorder()
		base.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("%s %d %s", path, rec.Code, rec.Body)
		}
		if !strings.HasPrefix(rec.Header().Get("Content-Type"), "text/html") {
			t.Fatalf("%s content-type %s", path, rec.Header().Get("Content-Type"))
		}
		if rec.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("%s cache %s", path, rec.Header().Get("Cache-Control"))
		}
		if rec.Header().Get("X-Content-Type-Options") != "nosniff" {
			t.Fatalf("%s nosniff %s", path, rec.Header().Get("X-Content-Type-Options"))
		}
		body := rec.Body.Bytes()
		if !bytes.Contains(body, []byte("/dashboard/assets/htmx.min.js")) || !bytes.Contains(body, []byte("/dashboard/assets/tailwind.css")) {
			t.Fatalf("%s missing assets", path)
		}
		if !bytes.Contains(body, []byte("Create one with the AWS CLI")) {
			t.Fatalf("%s empty state %s", path, body)
		}
		if log.Len() != 0 {
			t.Fatalf("%s logged %q", path, log.String())
		}
	}
	for _, asset := range []struct {
		path, ctype, needle string
	}{
		{"/dashboard/assets/htmx.min.js", "text/javascript", "htmx"},
		{"/dashboard/assets/tailwind.css", "text/css", "fill-red-50"},
	} {
		log.Reset()
		rec := httptest.NewRecorder()
		base.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, asset.path, nil))
		if rec.Code != http.StatusOK || !strings.HasPrefix(rec.Header().Get("Content-Type"), asset.ctype) {
			t.Fatalf("%s %d %s", asset.path, rec.Code, rec.Header().Get("Content-Type"))
		}
		if !bytes.Contains(rec.Body.Bytes(), []byte(asset.needle)) {
			t.Fatalf("%s missing %s", asset.path, asset.needle)
		}
		if log.Len() != 0 {
			t.Fatalf("%s logged %q", asset.path, log.String())
		}
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/dashboard", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", contentTypeJSON10)
	req.Header.Set(targetHeader, "AWSStepFunctions.ListStateMachines")
	base.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || rec.Header().Get("Content-Type") != contentTypeJSON10 {
		t.Fatalf("post /dashboard %d %s %s", rec.Code, rec.Header().Get("Content-Type"), rec.Body)
	}
}

func TestImplementedHandlerWire(t *testing.T) {
	surface, err := ParseSurface(`
[[operation]]
name = "ListStateMachines"
status = "implemented"
`)
	if err != nil {
		t.Fatal(err)
	}
	stub := &listStub{}
	var log bytes.Buffer
	srv := &Server{Handlers: stub, Surface: surface, Profile: ProfileSmithy, Log: &log}
	ts := gen.NewTimestamp(1715990400.123)
	stub.out = &gen.ListStateMachinesOutput{StateMachines: &gen.StateMachineList{{
		Name:         strPtr(""),
		CreationDate: &ts,
	}}}
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"maxResults":0}`))
	req.Header.Set(targetHeader, "AWSStepFunctions.ListStateMachines")
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatal(rec.Body)
	}
	if rec.Header().Get("Content-Type") != contentTypeJSON10 {
		t.Fatal(rec.Header().Get("Content-Type"))
	}
	var raw map[string]any
	dec := json.NewDecoder(bytes.NewReader(rec.Body.Bytes()))
	dec.UseNumber()
	if err := dec.Decode(&raw); err != nil {
		t.Fatal(err)
	}
	items := raw["stateMachines"].([]any)
	item := items[0].(map[string]any)
	if item["name"] != "" {
		t.Fatalf("empty name omitted: %v", item)
	}
	num, ok := item["creationDate"].(json.Number)
	if !ok {
		t.Fatalf("timestamp type %T", item["creationDate"])
	}
	f, err := num.Float64()
	if err != nil || math.Abs(f-1715990400.123) > 1e-3 {
		t.Fatalf("timestamp %v", item["creationDate"])
	}
	if strings.ContainsAny(num.String(), "eE") {
		t.Fatalf("scientific timestamp %s", num.String())
	}
	if stub.max == nil || *stub.max != 0 {
		t.Fatalf("presence lost: %v", stub.max)
	}

	stub.err = errors.New("boom\ngoroutine 1 [running]\nstack")
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`))
	req.Header.Set(targetHeader, "AWSStepFunctions.ListStateMachines")
	srv.Profile = ProfileSmithy
	srv.ServeHTTP(rec, req)
	got := decodeRec(t, rec)
	if got.Status != 500 || got.Type != "com.amazonaws.sfn#InternalFailure" || got.Message != "boom" {
		t.Fatalf("%+v", got)
	}
	if strings.Contains(got.Message, "goroutine") || strings.Contains(string(got.Raw), "goroutine") {
		t.Fatalf("stack leaked %s", got.Raw)
	}
	srv.Profile = ProfileSFNLocal
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`))
	req.Header.Set(targetHeader, "AWSStepFunctions.ListStateMachines")
	srv.ServeHTTP(rec, req)
	got = decodeRec(t, rec)
	if got.Type != jarInternal {
		t.Fatalf("compat internal %+v", got)
	}
}

func TestDeferredDoesNotCallHandler(t *testing.T) {
	stub := &listStub{}
	srv := prodServer(t, ProfileSmithy)
	srv.Handlers = stub
	call(t, srv, "AWSStepFunctions.ListMapRuns", `{"executionArn":"arn:aws:states:us-east-1:123456789012:execution:sm:run"}`, contentTypeJSON10)
	if stub.called {
		t.Fatal("deferred operation called the handler")
	}
}

type listStub struct {
	gen.Unimplemented
	called bool
	max    *int32
	out    *gen.ListStateMachinesOutput
	err    error
}

func (s *listStub) ListStateMachines(_ context.Context, in *gen.ListStateMachinesInput) (*gen.ListStateMachinesOutput, error) {
	s.called = true
	if in != nil {
		s.max = in.MaxResults
	}
	if s.err != nil {
		return nil, s.err
	}
	return s.out, nil
}

func prodServer(t *testing.T, profile Profile) *Server {
	t.Helper()
	surface, err := LoadSurface()
	if err != nil {
		t.Fatal(err)
	}
	st := store.New()
	runner := &exec.Runner{Store: st}
	return &Server{
		Handlers: &Service{Store: st, Runner: runner, Account: "123456789012", Region: "us-east-1", Profile: profile, Log: io.Discard},
		Surface:  surface,
		Profile:  profile,
		Log:      io.Discard,
	}
}

func callRaw(t *testing.T, srv *Server, target, body string) (int, []byte) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set(targetHeader, target)
	req.Header.Set("Content-Type", contentTypeJSON10)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	return rec.Code, rec.Body.Bytes()
}

type wire struct {
	Status  int
	Header  http.Header
	Type    string
	Message string
	Raw     json.RawMessage
}

func (w wire) Code() string {
	if i := strings.LastIndex(w.Type, "#"); i >= 0 {
		w.Type = w.Type[i+1:]
	}
	if i := strings.IndexByte(w.Type, ':'); i >= 0 {
		w.Type = w.Type[:i]
	}
	return w.Type
}

func call(t *testing.T, srv *Server, target, body, contentType string) wire {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set(targetHeader, target)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	return decodeRec(t, rec)
}

func decodeRec(t *testing.T, rec *httptest.ResponseRecorder) wire {
	t.Helper()
	var payload struct {
		Type    string `json:"__type"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("status %d body %s: %v", rec.Code, rec.Body.Bytes(), err)
	}
	if payload.Type == "" || payload.Message == "" {
		t.Fatalf("missing code or message: %s", rec.Body.Bytes())
	}
	return wire{Status: rec.Code, Header: rec.Header(), Type: payload.Type, Message: payload.Message, Raw: rec.Body.Bytes()}
}

func jsonString(s string) string {
	raw, _ := json.Marshal(s)
	return string(raw)
}

func strPtr(s string) *string { return &s }
