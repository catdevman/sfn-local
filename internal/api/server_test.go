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
)

func TestDeferredOperationsAreModeled(t *testing.T) {
	srv := prodServer(t, ProfileSmithy)
	body := call(t, srv, "AWSStepFunctions.ListStateMachines", `{}`, contentTypeJSON10)
	if body.Type != "com.amazonaws.sfn#ValidationException" || !strings.Contains(body.Message, "ListStateMachines is not implemented") {
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
	got := call(t, srv, "AWSStepFunctions.CreateStateMachine", create, contentTypeJSON)
	if got.Code() != "ValidationException" || !strings.Contains(got.Message, "CreateStateMachine is not implemented") {
		t.Fatalf("%+v", got)
	}
	valid := call(t, srv, "AWSStepFunctions.ValidateStateMachineDefinition", `{"definition":`+jsonString(def)+`}`, "")
	if valid.Code() != "ValidationException" || !strings.Contains(valid.Message, "ValidateStateMachineDefinition is not implemented") {
		t.Fatalf("%+v", valid)
	}
	testState := call(t, srv, "AWSStepFunctions.TestState", `{"definition":`+jsonString(def)+`}`, contentTypeJSON10)
	if testState.Code() != "ValidationException" || !strings.Contains(testState.Message, "TestState is not implemented") {
		t.Fatalf("%+v", testState)
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
	got = call(t, srv, "AWSStepFunctions.ListStateMachines", `{"maxResults":0,"unknown":true}`, contentTypeJSON10)
	if !strings.Contains(got.Message, "not implemented") {
		t.Fatalf("%+v", got)
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
	for _, name := range []string{"ListStateMachines", "CreateStateMachine", "TestState", "DescribeMapRun"} {
		got := call(t, smithy, "AWSStepFunctions."+name, `{}`, contentTypeJSON10)
		for _, bad := range forbidden {
			if strings.Contains(got.Type, bad) || strings.Contains(string(got.Raw), bad) {
				t.Fatalf("default profile emitted %s in %+v", bad, got)
			}
		}
	}
	listed := call(t, compat, "AWSStepFunctions.ListStateMachines", `{}`, contentTypeJSON10)
	if listed.Type != "com.amazonaws.sfn#ValidationException" {
		t.Fatalf("compat kept an empty jar map, got %+v", listed)
	}
	for name := range jarAbsent {
		got := call(t, compat, "AWSStepFunctions."+name, `{}`, contentTypeJSON10)
		if got.Status != 500 || got.Type != jarUnsupported || got.Message != "Unsupported Operation: '"+name+"'" {
			t.Fatalf("%s %+v", name, got)
		}
	}
	if len(jarAbsent) != 14 {
		t.Fatalf("jar-absent set %d", len(jarAbsent))
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
	if rec.Code != 400 {
		t.Fatal(rec.Body)
	}
	text := log.String()
	if !strings.Contains(text, "ListStateMachines => {}") || !strings.Contains(text, "[400] ListStateMachines <= ") {
		t.Fatalf("access log %q", text)
	}
	got := call(t, base, "AWSStepFunctions.ListStateMachines", `{}{}`, contentTypeJSON10)
	if got.Code() != "ValidationException" || !strings.Contains(got.Message, "trailing") {
		t.Fatalf("%+v", got)
	}
	req = httptest.NewRequest(http.MethodGet, "/", nil)
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
	call(t, srv, "AWSStepFunctions.ListStateMachines", `{}`, contentTypeJSON10)
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
	return &Server{Handlers: &Service{}, Surface: surface, Profile: profile, Log: io.Discard}
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
