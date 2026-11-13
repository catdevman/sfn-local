package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/catdevman/sfn-local/internal/api/gen"
)

func TestColorCaughtStaysFailed(t *testing.T) {
	events := []histLite{
		{Type: "ParallelStateEntered", Entered: "CheckStock"},
		{Type: "ParallelStateFailed"},
		{Type: "ParallelStateExited", Exited: "CheckStock"},
		{Type: "PassStateEntered", Entered: "Explain"},
		{Type: "PassStateExited", Exited: "Explain"},
		{Type: "ExecutionSucceeded"},
	}
	got := colorStates([]string{"CheckStock", "Explain", "BothOk"}, events, "SUCCEEDED")
	if got["CheckStock"] != "failed" || got["Explain"] != "succeeded" || got["BothOk"] != "idle" {
		t.Fatal(got)
	}
}

func TestColorRetryRecovers(t *testing.T) {
	events := []histLite{
		{Type: "TaskStateEntered", Entered: "Call"},
		{Type: "TaskFailed"},
		{Type: "TaskSucceeded"},
		{Type: "TaskStateExited", Exited: "Call"},
	}
	got := colorStates([]string{"Call"}, events, "SUCCEEDED")
	if got["Call"] != "succeeded" {
		t.Fatal(got)
	}
}

func TestColorFailPaintsOpenState(t *testing.T) {
	events := []histLite{
		{Type: "FailStateEntered", Entered: "Boom"},
		{Type: "ExecutionFailed"},
	}
	got := colorStates([]string{"Boom"}, events, "FAILED")
	if got["Boom"] != "failed" {
		t.Fatal(got)
	}
}

func TestAnnotateSkipsExecutionStep(t *testing.T) {
	prog := parseProgram(`{"StartAt":"A","States":{"A":{"Type":"Fail"}}}`)
	events := []histLite{
		{Type: "ExecutionStarted", HasTime: true, Time: 10},
		{Type: "FailStateEntered", Entered: "A", HasTime: true, Time: 11},
		{Type: "ExecutionFailed", HasTime: true, Time: 12},
	}
	ann := annotateHistory(prog, events)
	if ann[2].Step != "" || ann[2].Inner != "" {
		t.Fatalf("%+v", ann[2])
	}
	if ann[1].Step != "A" || ann[1].Inner != "A" {
		t.Fatalf("%+v", ann[1])
	}
	if ann[1].Delta < 0.9 || ann[1].Delta > 1.1 {
		t.Fatal(ann[1].Delta)
	}
	if matchesStep(ann[2], "A") {
		t.Fatal("execution event matched a step")
	}
	iter := histLite{Type: "MapIterationFailed", Step: "Map [0]", Inner: "Map"}
	if !matchesStep(iter, "Map") || matchesStep(iter, "Other") {
		t.Fatal(iter)
	}
}

func TestLayoutOrder(t *testing.T) {
	def := `{"StartAt":"A","States":{"Z":{"Type":"Pass","End":true},"A":{"Type":"Choice","Choices":[{"Variable":"$.n","NumericEquals":1,"Next":"B"}],"Default":"C"},"C":{"Type":"Pass","Next":"D"},"B":{"Type":"Pass","Next":"D"},"D":{"Type":"Succeed"}}}`
	g := layoutGraph(parseProgram(def), map[string]string{"B": "succeeded"}, "", 1)
	again := layoutGraph(parseProgram(def), nil, "", 1)
	if g == nil || again == nil || len(g.Nodes) != 5 || len(again.Nodes) != 5 {
		t.Fatal("layout")
	}
	for i := range g.Nodes {
		if g.Nodes[i].Name != again.Nodes[i].Name || g.Nodes[i].X != again.Nodes[i].X || g.Nodes[i].Y != again.Nodes[i].Y {
			t.Fatalf("unstable %s vs %s", g.Nodes[i].Name, again.Nodes[i].Name)
		}
	}
	want := []string{"A", "B", "C", "D", "Z"}
	for i, name := range want {
		if g.Nodes[i].Name != name {
			t.Fatalf("node %d %s", i, g.Nodes[i].Name)
		}
	}
	if yOf(t, g.Nodes[1]) >= yOf(t, g.Nodes[2]) {
		t.Fatal("choice should sit above default")
	}
	if xOf(t, g.Nodes[4]) <= xOf(t, g.Nodes[3]) {
		t.Fatal("unreached state should sit in a later column")
	}
	if g.Nodes[1].Fill != "fill-green-50" || g.Nodes[0].Fill != "fill-white" {
		t.Fatalf("paint %s %s", g.Nodes[1].Fill, g.Nodes[0].Fill)
	}
	caught := parseProgram(`{"StartAt":"A","States":{"A":{"Type":"Task","Resource":"arn:aws:states:::lambda:invoke","Catch":[{"ErrorEquals":["States.ALL"],"Next":"B"}],"Next":"C"},"B":{"Type":"Pass","End":true},"C":{"Type":"Succeed"}}}`)
	edges := layoutGraph(caught, nil, "A", 1)
	found := false
	for _, e := range edges.Edges {
		if strings.Contains(e.Class, "stroke-red-700") && strings.Contains(e.Class, "stroke-dasharray") {
			found = true
		}
	}
	if !found {
		t.Fatalf("catch edge %#v", edges.Edges)
	}
	if edges.Nodes[0].SW != "stroke-[2.5]" || edges.Nodes[0].Stroke != "stroke-slate-900" {
		t.Fatalf("selected %#v", edges.Nodes[0])
	}
}

func xOf(t *testing.T, n graphNode) float64 {
	t.Helper()
	v, err := strconv.ParseFloat(n.X, 64)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func yOf(t *testing.T, n graphNode) float64 {
	t.Helper()
	v, err := strconv.ParseFloat(n.Y, 64)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestDashboardExecution(t *testing.T) {
	srv := prodServer(t, ProfileSmithy)
	svc := srv.Handlers.(*Service)
	const def = `{"Comment":"Catch a failed branch.","StartAt":"CheckStock","States":{"CheckStock":{"Type":"Parallel","Branches":[{"StartAt":"Missing","States":{"Missing":{"Type":"Fail","Error":"InventoryMissing","Cause":"pen is unavailable"}}}],"Catch":[{"ErrorEquals":["InventoryMissing"],"Next":"Explain"}],"Next":"BothOk"},"BothOk":{"Type":"Pass","End":true},"Explain":{"Type":"Pass","Result":{"status":"needs-stock"},"End":true}}}`
	sm := createMachine(t, svc, "catch", def, roleARN)
	run := startExec(t, svc, *sm.StateMachineArn, "catch-run", `{"sku":"pen"}`)
	waitExec(t, svc, *run.ExecutionArn)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/dashboard?execution="+url.QueryEscape(*run.ExecutionArn), nil))
	if rec.Code != http.StatusOK {
		t.Fatal(rec.Body)
	}
	body := rec.Body.Bytes()
	assertNear(t, body, "CheckStock", "failed", "fill-red-50")
	assertNear(t, body, "Explain", "succeeded", "fill-green-50")
	assertNear(t, body, "BothOk", "idle", "fill-white")
	if !bytes.Contains(body, []byte("InventoryMissing")) {
		t.Fatal("missing failure from history")
	}
	if !bytes.Contains(body, []byte("Execution history")) || !bytes.Contains(body, []byte(`data-graph="run"`)) {
		t.Fatal("missing run graph or history")
	}
	if bytes.Contains(body, []byte(`data-graph="definition"`)) || bytes.Contains(body, []byte(`id="definition"`)) {
		t.Fatal("execution page repeats the definition")
	}
	if bytes.Contains(body, []byte("every 1s")) {
		t.Fatal("finished execution still polls")
	}
	rec = httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/dashboard?machine="+url.QueryEscape(*sm.StateMachineArn), nil))
	if rec.Code != http.StatusOK || !bytes.Contains(rec.Body.Bytes(), []byte(`data-graph="definition"`)) || !bytes.Contains(rec.Body.Bytes(), []byte("catch-run")) || !bytes.Contains(rec.Body.Bytes(), []byte("StartAt")) {
		t.Fatalf("machine definition %d %s", rec.Code, rec.Body)
	}
	assertNear(t, rec.Body.Bytes(), "CheckStock", "idle", "fill-white")
	express := gen.StateMachineTypeEXPRESS
	fast, err := svc.CreateStateMachine(context.Background(), &gen.CreateStateMachineInput{
		Name: sp("express"), Definition: sp(passDef), RoleArn: sp(roleARN), Type: &express,
	})
	if err != nil {
		t.Fatal(err)
	}
	rec = httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/dashboard?machine="+url.QueryEscape(*fast.StateMachineArn), nil))
	if rec.Code != 200 || !bytes.Contains(rec.Body.Bytes(), []byte("not supported")) {
		t.Fatalf("express list %d %s", rec.Code, rec.Body)
	}
}

func TestDashboardStop(t *testing.T) {
	srv := prodServer(t, ProfileSmithy)
	svc := srv.Handlers.(*Service)
	sm := createMachine(t, svc, "hold", `{"StartAt":"W","States":{"W":{"Type":"Wait","Seconds":30,"End":true}}}`, roleARN)
	run := startExec(t, svc, *sm.StateMachineArn, "hold-run", `{}`)
	page := "/dashboard?execution=" + url.QueryEscape(*run.ExecutionArn)
	body := waitPage(t, srv, page, "every 1s")
	if !bytes.Contains(body, []byte("RUNNING")) {
		t.Fatalf("not running\n%s", body)
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/dashboard/stop", strings.NewReader("executionArn="+url.QueryEscape(*run.ExecutionArn)))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("HX-Request", "true")
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.HasPrefix(rec.Header().Get("Content-Type"), "text/html") {
		t.Fatalf("hx stop %d %s %s", rec.Code, rec.Header().Get("Content-Type"), rec.Body)
	}
	if bytes.Contains(rec.Body.Bytes(), []byte("every 1s")) || !bytes.Contains(rec.Body.Bytes(), []byte("ABORTED")) {
		t.Fatalf("after stop\n%s", rec.Body)
	}
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/dashboard/stop", strings.NewReader("executionArn="+url.QueryEscape(*run.ExecutionArn)))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusSeeOther || !strings.Contains(rec.Header().Get("Location"), url.QueryEscape(*run.ExecutionArn)) {
		t.Fatalf("redirect %d %s", rec.Code, rec.Header().Get("Location"))
	}
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/dashboard/stop", strings.NewReader(""))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatal(rec.Code)
	}
}

func TestDashboardStepIO(t *testing.T) {
	srv := prodServer(t, ProfileSmithy)
	svc := srv.Handlers.(*Service)
	const def = `{"Comment":"Catch a failed branch.","StartAt":"CheckStock","States":{"CheckStock":{"Type":"Parallel","Branches":[{"StartAt":"Missing","States":{"Missing":{"Type":"Fail","Error":"InventoryMissing","Cause":"pen is unavailable"}}}],"Catch":[{"ErrorEquals":["InventoryMissing"],"Next":"Explain"}],"Next":"BothOk"},"BothOk":{"Type":"Pass","End":true},"Explain":{"Type":"Pass","Result":{"status":"needs-stock"},"End":true}}}`
	sm := createMachine(t, svc, "catch-io", def, roleARN)
	run := startExec(t, svc, *sm.StateMachineArn, "catch-io-run", `{"sku":"pen"}`)
	waitExec(t, svc, *run.ExecutionArn)
	base := "/dashboard?execution=" + url.QueryEscape(*run.ExecutionArn)

	body := dashGET(t, srv, base)
	input := elementText(t, body, "state-input")
	if !strings.Contains(input, "sku") || bytes.Contains(body, []byte(`id="io-note"`)) {
		t.Fatalf("execution input\n%s", input)
	}

	body = dashGET(t, srv, base+"&step=Explain")
	input = elementText(t, body, "state-input")
	output := elementText(t, body, "state-output")
	if !strings.Contains(input, "InventoryMissing") || strings.Contains(input, "sku") || !strings.Contains(output, "needs-stock") {
		t.Fatalf("explain input %s output %s", input, output)
	}
	if note := elementText(t, body, "io-note"); note != "Explain" {
		t.Fatalf("note %q", note)
	}

	body = dashGET(t, srv, base+"&step=BothOk")
	if elementText(t, body, "state-input") != "—" || elementText(t, body, "state-output") != "—" {
		t.Fatalf("untaken\n%s", body)
	}
	if note := elementText(t, body, "io-note"); note != "BothOk did not run" {
		t.Fatalf("note %q", note)
	}
	if bytes.Contains(body, []byte(`id="map-index"`)) {
		t.Fatal("untaken state has a map index")
	}
}

func TestDashboardMapIndex(t *testing.T) {
	srv := prodServer(t, ProfileSmithy)
	svc := srv.Handlers.(*Service)
	const def = `{"StartAt":"Prep","States":{"Prep":{"Type":"Pass","Next":"Outer"},"Outer":{"Type":"Map","ItemsPath":"$.items","MaxConcurrency":2,"Iterator":{"StartAt":"Inner","States":{"Inner":{"Type":"Map","ItemsPath":"$.inner","Iterator":{"StartAt":"P","States":{"P":{"Type":"Pass","End":true}}},"End":true}}},"End":true}}}`
	sm := createMachine(t, svc, "map-io", def, roleARN)
	run := startExec(t, svc, *sm.StateMachineArn, "map-io-run", `{"items":[{"inner":["pen"]},{"inner":["desk"]}]}`)
	ex := waitExec(t, svc, *run.ExecutionArn)
	var saw0, saw1 bool
	for _, ev := range ex.History {
		if ev.Type == "MapStateEntered" && ev.Name == "Outer" && ev.HasParentIndex {
			t.Fatalf("map enter stamped %+v", ev)
		}
		if ev.Type == "PassStateEntered" && ev.Name == "P" {
			if !ev.HasParentIndex {
				t.Fatalf("pass missing parent %+v", ev)
			}
			switch ev.ParentIndex {
			case 0:
				if !strings.Contains(ev.Input, "pen") || strings.Contains(ev.Input, "desk") {
					t.Fatalf("index 0 %+v", ev)
				}
				saw0 = true
			case 1:
				if !strings.Contains(ev.Input, "desk") || strings.Contains(ev.Input, "pen") {
					t.Fatalf("index 1 %+v", ev)
				}
				saw1 = true
			default:
				t.Fatalf("parent %d", ev.ParentIndex)
			}
		}
		if ev.Type == "MapIterationStarted" && ev.Name == "Inner" && ev.HasIndex && ev.Index == 0 {
			if !ev.HasParentIndex || (ev.ParentIndex != 0 && ev.ParentIndex != 1) {
				t.Fatalf("inner iteration lost the outer index %+v", ev)
			}
		}
	}
	if !saw0 || !saw1 {
		t.Fatalf("pass parents 0=%v 1=%v", saw0, saw1)
	}
	include := true
	hist, err := svc.GetExecutionHistory(context.Background(), &gen.GetExecutionHistoryInput{ExecutionArn: run.ExecutionArn, IncludeExecutionData: &include})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(hist)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, []byte("parentIndex")) || bytes.Contains(raw, []byte("ParentIndex")) {
		t.Fatal("history API leaked the parent index")
	}

	base := "/dashboard?execution=" + url.QueryEscape(*run.ExecutionArn)
	body := dashGET(t, srv, base+"&step=Outer&mi=0")
	input := elementText(t, body, "state-input")
	output := elementText(t, body, "state-output")
	if !strings.Contains(input, "pen") || strings.Contains(input, "desk") || !strings.Contains(output, "pen") || strings.Contains(output, "desk") {
		t.Fatalf("mi=0 input %s output %s", input, output)
	}
	if note := elementText(t, body, "io-note"); note != "Outer · index 0" {
		t.Fatalf("note %q", note)
	}
	if !bytes.Contains(body, []byte("Index 0")) || !bytes.Contains(body, []byte("Index 1")) {
		t.Fatal("missing index options")
	}
	if href := attrAfter(t, body, "map-index", "hx-get"); strings.Contains(href, "mi=") {
		t.Fatalf("index select repeats mi: %s", href)
	}
	if href := nodeHref(t, body, "Prep"); strings.Contains(href, "mi=") || !strings.Contains(href, "step=Prep") {
		t.Fatalf("prep link %s", href)
	}
	if !bytes.Contains(body, []byte("mi=0")) {
		t.Fatal("zoom or page url dropped mi")
	}

	body = dashGET(t, srv, base+"&step=Outer&mi=1")
	input = elementText(t, body, "state-input")
	output = elementText(t, body, "state-output")
	if !strings.Contains(input, "desk") || strings.Contains(input, "pen") || !strings.Contains(output, "desk") || strings.Contains(output, "pen") {
		t.Fatalf("mi=1 input %s output %s", input, output)
	}

	body = dashGET(t, srv, base+"&step=Outer")
	input = elementText(t, body, "state-input")
	if !strings.Contains(input, "pen") || !strings.Contains(input, "desk") {
		t.Fatalf("whole map input %s", input)
	}
	if bytes.Contains(body, []byte("mi=")) {
		t.Fatal("whole state echoed mi")
	}

	body = dashGET(t, srv, base+"&step=Outer&mi=9")
	input = elementText(t, body, "state-input")
	if !strings.Contains(input, "pen") || !strings.Contains(input, "desk") || bytes.Contains(body, []byte("mi=9")) {
		t.Fatalf("invalid index\n%s", input)
	}

	const failed = `{"StartAt":"M","States":{"M":{"Type":"Map","ItemsPath":"$.items","MaxConcurrency":2,"Iterator":{"StartAt":"C","States":{"C":{"Type":"Choice","Choices":[{"Variable":"$.bad","BooleanEquals":true,"Next":"F"}],"Default":"Ok"},"Ok":{"Type":"Pass","End":true},"F":{"Type":"Fail","Error":"Nope","Cause":"bad"}}},"End":true}}}`
	sm = createMachine(t, svc, "map-fail", failed, roleARN)
	run = startExec(t, svc, *sm.StateMachineArn, "map-fail-run", `{"items":[{"sku":"desk","bad":true}]}`)
	waitExec(t, svc, *run.ExecutionArn)
	base = "/dashboard?execution=" + url.QueryEscape(*run.ExecutionArn)
	body = dashGET(t, srv, base+"&step=M&mi=0")
	if elementText(t, body, "state-output") != "—" {
		t.Fatalf("failed iteration output %s", elementText(t, body, "state-output"))
	}
	input = elementText(t, body, "state-input")
	if !strings.Contains(input, "desk") || strings.Contains(input, "pen") {
		t.Fatalf("failed iteration input %s", input)
	}
	if !bytes.Contains(body, []byte("Index 0 · failed")) {
		t.Fatal("missing failed index label")
	}
}

func TestLatestVisit(t *testing.T) {
	events := []gen.HistoryEvent{
		enterEv("Loop", `{"n":1}`),
		exitEv("Loop", `{"n":1}`),
		enterEv("Loop", `{"n":2}`),
		enterEv("Loop", `{"n":"nested"}`),
		exitEv("Loop", `{"n":"nested"}`),
		exitEv("Loop", `{"n":2}`),
	}
	enter, exit, ok := latestVisit(events, "Loop")
	if !ok || enter != 2 || exit != 5 {
		t.Fatalf("visit %d %d %v", enter, exit, ok)
	}
	if got := eventInput(events[enter]); !strings.Contains(got, `"n": 2`) {
		t.Fatal(got)
	}
	if _, _, ok := latestVisit(events, "Missing"); ok {
		t.Fatal("missing state visited")
	}
	open := []gen.HistoryEvent{enterEv("Loop", `{"n":3}`)}
	enter, exit, ok = latestVisit(open, "Loop")
	if !ok || enter != 0 || exit != -1 {
		t.Fatalf("open %d %d %v", enter, exit, ok)
	}
}

func enterEv(name, input string) gen.HistoryEvent {
	return gen.HistoryEvent{StateEnteredEventDetails: &gen.StateEnteredEventDetails{Name: sp(name), Input: sp(input)}}
}

func exitEv(name, output string) gen.HistoryEvent {
	return gen.HistoryEvent{StateExitedEventDetails: &gen.StateExitedEventDetails{Name: sp(name), Output: sp(output)}}
}

func dashGET(t *testing.T, srv *Server, rawURL string) []byte {
	t.Helper()
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, rawURL, nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("%s %d %s", rawURL, rec.Code, rec.Body)
	}
	return rec.Body.Bytes()
}

func elementText(t *testing.T, body []byte, id string) string {
	t.Helper()
	needle := []byte(`id="` + id + `"`)
	i := bytes.Index(body, needle)
	if i < 0 {
		t.Fatalf("missing %s", id)
	}
	rest := body[i:]
	gt := bytes.IndexByte(rest, '>')
	end := bytes.Index(rest, []byte("</"))
	if gt < 0 || end < gt {
		t.Fatalf("bad %s", id)
	}
	return string(rest[gt+1 : end])
}

func attrAfter(t *testing.T, body []byte, id, attr string) string {
	t.Helper()
	needle := []byte(`id="` + id + `"`)
	i := bytes.Index(body, needle)
	if i < 0 {
		t.Fatalf("missing %s", id)
	}
	end := i + 800
	if end > len(body) {
		end = len(body)
	}
	window := body[i:end]
	key := []byte(attr + `="`)
	j := bytes.Index(window, key)
	if j < 0 {
		t.Fatalf("missing %s on %s", attr, id)
	}
	rest := window[j+len(key):]
	k := bytes.IndexByte(rest, '"')
	if k < 0 {
		t.Fatalf("open %s", attr)
	}
	return string(rest[:k])
}

func nodeHref(t *testing.T, body []byte, state string) string {
	t.Helper()
	needle := []byte(`data-state="` + state + `"`)
	i := bytes.Index(body, needle)
	if i < 0 {
		t.Fatalf("missing state %s", state)
	}
	start := i - 700
	if start < 0 {
		start = 0
	}
	window := body[start:i]
	key := []byte(`href="`)
	j := bytes.LastIndex(window, key)
	if j < 0 {
		t.Fatalf("no href for %s", state)
	}
	rest := window[j+len(key):]
	k := bytes.IndexByte(rest, '"')
	if k < 0 {
		t.Fatalf("open href for %s", state)
	}
	return string(rest[:k])
}

func assertNear(t *testing.T, body []byte, state, status, class string) {
	t.Helper()
	needle := []byte(`data-state="` + state + `"`)
	rest := body
	for {
		i := bytes.Index(rest, needle)
		if i < 0 {
			t.Fatalf("missing %s with status %s", state, status)
		}
		end := i + 1800
		if end > len(rest) {
			end = len(rest)
		}
		window := rest[i:end]
		if bytes.Contains(window, []byte(`data-status="`+status+`"`)) && bytes.Contains(window, []byte(class)) {
			return
		}
		rest = rest[i+len(needle):]
	}
}

func waitPage(t *testing.T, srv *Server, rawURL, want string) []byte {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	var last []byte
	for {
		rec := httptest.NewRecorder()
		srv.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, rawURL, nil))
		last = rec.Body.Bytes()
		if rec.Code == http.StatusOK && bytes.Contains(last, []byte(want)) {
			return last
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %q\n%s", want, last)
		}
		time.Sleep(15 * time.Millisecond)
	}
}
