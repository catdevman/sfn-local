package api

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"html/template"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/catdevman/sfn-local/internal/api/gen"
	"github.com/catdevman/sfn-local/internal/store"
)

//go:embed dashboard/htmx.min.js
var dashboardHTMX []byte

//go:embed dashboard/tailwind.css
var dashboardCSS []byte

//go:embed dashboard/templates.html
var dashboardTemplates string

// dashboardRequest reports whether this request is the embedded console
// rather than an AWS JSON call. POST /dashboard stays the API.
func dashboardRequest(r *http.Request) bool {
	switch r.URL.Path {
	case "/dashboard", "/dashboard/":
		return r.Method == http.MethodGet
	case "/dashboard/stop":
		return r.Method == http.MethodPost
	case "/dashboard/assets/htmx.min.js", "/dashboard/assets/tailwind.css":
		return r.Method == http.MethodGet
	default:
		return false
	}
}

func (s *Server) serveDashboard(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/dashboard/assets/htmx.min.js":
		writeAsset(w, "text/javascript; charset=utf-8", dashboardHTMX)
	case "/dashboard/assets/tailwind.css":
		writeAsset(w, "text/css; charset=utf-8", dashboardCSS)
	case "/dashboard/stop":
		s.stopFromDashboard(w, r)
	default:
		s.renderDashboard(w, r)
	}
}

func writeAsset(w http.ResponseWriter, contentType string, body []byte) {
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

func (s *Server) stopFromDashboard(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad form", http.StatusBadRequest)
		return
	}
	arn := r.Form.Get("executionArn")
	if arn == "" {
		http.Error(w, "missing execution", http.StatusBadRequest)
		return
	}
	if _, err := s.Handlers.StopExecution(r.Context(), &gen.StopExecutionInput{ExecutionArn: &arn}); err != nil {
		q := stopQuery(r, arn)
		q.Set("error", errText(err))
		s.renderDashboard(w, dashboardRequestWith(r, q))
		return
	}
	if r.Header.Get("HX-Request") != "true" {
		http.Redirect(w, r, executionURL(r.Form.Get("machine"), arn, r.Form.Get("step"), r.Form.Get("mi"), parseZoom(r.Form.Get("z")), "", ""), http.StatusSeeOther)
		return
	}
	s.renderDashboard(w, dashboardRequestWith(r, stopQuery(r, arn)))
}

func stopQuery(r *http.Request, arn string) url.Values {
	q := url.Values{"execution": {arn}, "part": {"main"}}
	for _, key := range []string{"machine", "step", "mi", "z"} {
		if v := r.Form.Get(key); v != "" {
			q.Set(key, v)
		}
	}
	return q
}

func dashboardRequestWith(r *http.Request, q url.Values) *http.Request {
	clone := r.Clone(r.Context())
	u := *r.URL
	u.RawQuery = q.Encode()
	clone.URL = &u
	return clone
}

func (s *Server) renderDashboard(w http.ResponseWriter, r *http.Request) {
	if s.Handlers == nil {
		http.Error(w, "dashboard is unavailable", http.StatusInternalServerError)
		return
	}
	view := s.loadDashboard(r.Context(), r)
	part := r.URL.Query().Get("part")
	name := "page"
	switch part {
	case "main":
		name = "fragment"
		view.OOB = true
	case "rows":
		name = "rows"
	case "live":
		name = "live"
	}
	if r.Header.Get("HX-Request") == "true" {
		trigger := r.Header.Get("HX-Trigger")
		switch part {
		case "rows":
			if trigger == "exec-filter" {
				w.Header().Set("HX-Replace-Url", view.pageURL())
			}
		case "live":
			switch trigger {
			case "live":
			case "event-filter":
				w.Header().Set("HX-Replace-Url", view.pageURL())
			default:
				w.Header().Set("HX-Push-Url", view.pageURL())
			}
		}
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
	if err := dashboardTmpl.ExecuteTemplate(w, name, view); err != nil {
		return
	}
}

func (s *Server) loadDashboard(ctx context.Context, r *http.Request) dashView {
	q := r.URL.Query()
	view := dashView{
		RefreshURL: "/dashboard",
		Part:       q.Get("part"),
	}
	if msg := q.Get("error"); msg != "" {
		view.Banner = msg
	}
	machines, err := s.allMachines(ctx)
	if err != nil {
		view.Kind = "error"
		view.Error = errText(err)
		return view
	}
	view.Machines = machines
	machine := q.Get("machine")
	execution := q.Get("execution")
	if execution != "" {
		s.fillExecution(ctx, &view, execution, q)
	} else {
		if machine == "" && len(machines) > 0 {
			machine = machines[0].ARN
		}
		if machine == "" {
			view.Kind = "empty"
			view.EmptyCopy = "Create one with the AWS CLI, then refresh."
		} else {
			s.fillExecutions(ctx, &view, machine, q.Get("q"))
		}
	}
	if view.Kind == "" {
		view.Kind = "empty"
		view.EmptyCopy = "Select a state machine."
	}
	view.Account = dashboardAccount(view)
	view.RefreshURL = view.pageURL()
	view.markActive()
	return view
}

func (s *Server) fillExecutions(ctx context.Context, view *dashView, machine, filter string) {
	view.Kind = "list"
	view.Machine = machine
	view.Filter = filter
	view.Title = machine
	for _, m := range view.Machines {
		if m.ARN == machine {
			view.Title = m.Name
			view.Sub = m.Type
		}
	}
	view.RowsURL = listURL(machine) + "&part=rows"
	if text, note := s.machineDefinition(ctx, machine); note != "" {
		view.PlainNote = note
	} else {
		setPlainGraph(view, text)
	}
	rows, err := s.allExecutions(ctx, machine)
	if err != nil {
		view.ListError = errText(err)
		return
	}
	view.Execs = filterExecs(rows, filter)
	view.NoMatch = len(rows) > 0 && len(view.Execs) == 0
	view.Empty = len(rows) == 0 && view.ListError == ""
	for _, row := range rows {
		if row.Status == "RUNNING" {
			view.Poll = true
			break
		}
	}
}

func (s *Server) fillExecution(ctx context.Context, view *dashView, arn string, q url.Values) {
	view.Kind = "detail"
	view.Execution = arn
	ex, err := s.Handlers.DescribeExecution(ctx, &gen.DescribeExecutionInput{ExecutionArn: &arn})
	if err != nil {
		view.Kind = "error"
		view.Error = errText(err)
		return
	}
	machine := valueOf(ex.StateMachineArn)
	if q.Get("machine") != "" {
		machine = q.Get("machine")
	}
	view.Machine = machine
	view.Title = valueOf(ex.Name)
	if view.Title == "" {
		view.Title = arn
	}
	status := ""
	if ex.Status != nil {
		status = string(*ex.Status)
	}
	view.Status = status
	view.Chip = chipClass(status)
	view.Meta = metaLine(ex)
	view.Input = prettyJSON(valueOf(ex.Input))
	if ex.Output != nil && *ex.Output != "" {
		view.Output = prettyJSON(*ex.Output)
	} else {
		view.Output = "—"
	}
	view.ShowStop = status == "RUNNING"
	view.BackURL = listPageURL(machine)
	view.BackFragment = listURL(machine) + "&part=main"
	view.ExecARN = arn
	view.Filter = q.Get("eq")

	defText := ""
	if sm, err := s.Handlers.DescribeStateMachineForExecution(ctx, &gen.DescribeStateMachineForExecutionInput{ExecutionArn: &arn}); err == nil && sm.Definition != nil {
		defText = *sm.Definition
	}
	events, err := s.allHistory(ctx, arn)
	if err != nil {
		view.Kind = "error"
		view.Error = errText(err)
		return
	}
	prog := parseProgram(defText)
	lites := make([]histLite, len(events))
	for i, ev := range events {
		lites[i] = liteOf(ev)
	}
	colors := map[string]string{}
	if prog != nil {
		names := make([]string, 0, len(prog.States))
		for name := range prog.States {
			names = append(names, name)
		}
		colors = colorStates(names, lites, status)
		view.Comment = prog.Comment
	}
	annotated := annotateHistory(prog, lites)
	view.FailureError, view.FailureCause = failureOf(ex, events)
	if prog == nil {
		if strings.TrimSpace(defText) == "" {
			view.GraphNote = "No definition."
		} else {
			view.GraphNote = "This definition has no state graph."
		}
	}
	z := parseZoom(q.Get("z"))
	view.Step = q.Get("step")
	parents := s.parentEvents(arn)
	focus := applyStepIO(view, events, parents, q.Get("mi"))
	view.Graph = layoutGraph(prog, colors, view.Step, z)
	if view.Graph == nil && view.GraphNote == "" {
		view.GraphNote = "This definition has no state graph."
	}
	if view.Graph != nil && z == 0 {
		z = view.Graph.Zoom
	}
	if z == 0 {
		z = 1
	}
	view.Zoom = z
	view.ZoomText = zoomParam(z)
	shown := make([]dashEvent, 0, len(events))
	for i := len(events) - 1; i >= 0; i-- {
		row := annotated[i]
		ev := events[i]
		if !matchesStep(row, view.Step) || !focus.allows(i, row.ID, parents) {
			continue
		}
		item := dashEvent{
			ID:     strconv.FormatInt(row.ID, 10),
			Type:   row.Type,
			Step:   row.Step,
			When:   fmtEpoch(row.Time),
			Delta:  fmtDelta(row.Delta),
			Error:  eventError(ev),
			Detail: revealEvent(ev),
		}
		blob := strings.ToLower(item.Detail + " " + item.Step + " " + item.Type + " " + item.ID)
		if view.Filter != "" && !strings.Contains(blob, strings.ToLower(view.Filter)) {
			continue
		}
		shown = append(shown, item)
	}
	view.Events = shown
	if len(shown) == len(events) {
		view.EventCount = strconv.Itoa(len(events)) + " events"
	} else {
		view.EventCount = strconv.Itoa(len(shown)) + " of " + strconv.Itoa(len(events)) + " events"
	}
	view.NoMatch = len(events) > 0 && len(shown) == 0
	view.Poll = status == "RUNNING"
	view.LiveURL = executionURL(machine, arn, view.Step, view.Index, z, "live", "")
	view.ZoomIn = executionURL(machine, arn, view.Step, view.Index, clampZoom(z*1.2), "live", "")
	view.ZoomOut = executionURL(machine, arn, view.Step, view.Index, clampZoom(z/1.2), "live", "")
	view.FitURL = executionURL(machine, arn, view.Step, view.Index, 0, "live", "")
	view.ClearURL = executionURL(machine, arn, "", "", z, "live", "")
	view.PushZoomIn = executionURL(machine, arn, view.Step, view.Index, clampZoom(z*1.2), "", view.Filter)
	view.PushZoomOut = executionURL(machine, arn, view.Step, view.Index, clampZoom(z/1.2), "", view.Filter)
	view.PushFit = executionURL(machine, arn, view.Step, view.Index, 0, "", view.Filter)
	view.PushClear = executionURL(machine, arn, "", "", z, "", view.Filter)
	view.IndexURL = executionURL(machine, arn, view.Step, "", z, "live", "")
	if view.Graph != nil {
		for i := range view.Graph.Nodes {
			nodeStep := view.Graph.Nodes[i].Name
			if nodeStep == view.Step {
				nodeStep = ""
			}
			view.Graph.Nodes[i].Href = executionURL(machine, arn, nodeStep, "", z, "", view.Filter)
			view.Graph.Nodes[i].Fragment = executionURL(machine, arn, nodeStep, "", z, "live", "")
		}
	}
}

func (s *Server) parentEvents(arn string) map[int64]store.HistoryEvent {
	svc, ok := s.Handlers.(*Service)
	if !ok || svc == nil || svc.Store == nil {
		return nil
	}
	ex, ok := svc.Store.Execution(arn)
	if !ok {
		return nil
	}
	out := make(map[int64]store.HistoryEvent, len(ex.History))
	for _, ev := range ex.History {
		out[ev.ID] = ev
	}
	return out
}

func (s *Server) allMachines(ctx context.Context) ([]dashMachine, error) {
	var out []dashMachine
	var token *string
	max := int32(1000)
	for page := 0; page < 40; page++ {
		res, err := s.Handlers.ListStateMachines(ctx, &gen.ListStateMachinesInput{MaxResults: &max, NextToken: token})
		if err != nil {
			return nil, err
		}
		if res != nil && res.StateMachines != nil {
			for _, m := range *res.StateMachines {
				arn := valueOf(m.StateMachineArn)
				typ := ""
				if m.Type != nil {
					typ = string(*m.Type)
				}
				out = append(out, dashMachine{
					Name:     valueOf(m.Name),
					ARN:      arn,
					Type:     typ,
					Href:     listPageURL(arn),
					Fragment: listURL(arn) + "&part=main",
				})
			}
		}
		if res == nil || res.NextToken == nil || *res.NextToken == "" {
			break
		}
		token = res.NextToken
	}
	return out, nil
}

func (s *Server) allExecutions(ctx context.Context, machine string) ([]dashExec, error) {
	var out []dashExec
	var token *string
	max := int32(1000)
	for page := 0; page < 40; page++ {
		res, err := s.Handlers.ListExecutions(ctx, &gen.ListExecutionsInput{
			StateMachineArn: &machine,
			MaxResults:      &max,
			NextToken:       token,
		})
		if err != nil {
			return nil, err
		}
		if res != nil && res.Executions != nil {
			for _, ex := range *res.Executions {
				status := ""
				if ex.Status != nil {
					status = string(*ex.Status)
				}
				arn := valueOf(ex.ExecutionArn)
				out = append(out, dashExec{
					Name:     valueOf(ex.Name),
					ARN:      arn,
					Status:   status,
					Chip:     chipClass(status),
					Started:  fmtStamp(ex.StartDate),
					Stopped:  fmtStamp(ex.StopDate),
					Duration: fmtDuration(ex.StartDate, ex.StopDate),
					Href:     executionURL(machine, arn, "", "", 1, "", ""),
					Fragment: executionURL(machine, arn, "", "", 1, "main", ""),
				})
			}
		}
		if res == nil || res.NextToken == nil || *res.NextToken == "" {
			break
		}
		token = res.NextToken
	}
	return out, nil
}

func (s *Server) allHistory(ctx context.Context, arn string) ([]gen.HistoryEvent, error) {
	var out []gen.HistoryEvent
	var token *string
	max := int32(1000)
	include := true
	reverse := false
	for page := 0; page < 40; page++ {
		res, err := s.Handlers.GetExecutionHistory(ctx, &gen.GetExecutionHistoryInput{
			ExecutionArn:         &arn,
			MaxResults:           &max,
			NextToken:            token,
			IncludeExecutionData: &include,
			ReverseOrder:         &reverse,
		})
		if err != nil {
			return nil, err
		}
		if res != nil && res.Events != nil {
			out = append(out, (*res.Events)...)
		}
		if res == nil || res.NextToken == nil || *res.NextToken == "" {
			break
		}
		token = res.NextToken
	}
	return out, nil
}

func filterExecs(rows []dashExec, q string) []dashExec {
	if q == "" {
		return rows
	}
	q = strings.ToLower(q)
	out := make([]dashExec, 0, len(rows))
	for _, row := range rows {
		if strings.Contains(strings.ToLower(row.Name), q) || strings.Contains(strings.ToLower(row.Status), q) {
			out = append(out, row)
		}
	}
	return out
}

func errText(err error) string {
	var apiErr gen.APIError
	if errors.As(err, &apiErr) {
		return apiErr.Error()
	}
	if err == nil {
		return ""
	}
	return err.Error()
}

func fmtStamp(ts *gen.Timestamp) string {
	if ts == nil {
		return "—"
	}
	return fmtEpoch(ts.Seconds())
}

func fmtEpoch(sec float64) string {
	if sec == 0 {
		return "—"
	}
	whole := int64(sec)
	frac := sec - float64(whole)
	if frac < 0 {
		frac = 0
	}
	return time.Unix(whole, int64(frac*1e9)).Local().Format("2006-01-02 15:04:05")
}

func fmtDuration(start, stop *gen.Timestamp) string {
	if start == nil {
		return "—"
	}
	end := float64(time.Now().UnixNano()) / 1e9
	if stop != nil {
		end = stop.Seconds()
	}
	ms := end - start.Seconds()
	if ms < 0 {
		ms = 0
	}
	if ms < 1 {
		return strconv.Itoa(int(ms*1000+0.5)) + " ms"
	}
	if ms < 60 {
		return strconv.FormatFloat(ms, 'f', 2, 64) + " s"
	}
	m := int(ms / 60)
	s := int(ms) - m*60
	if m < 60 {
		return strconv.Itoa(m) + "m " + strconv.Itoa(s) + "s"
	}
	h := m / 60
	return strconv.Itoa(h) + "h " + strconv.Itoa(m-h*60) + "m"
}

func fmtDelta(sec float64) string {
	if sec < 0.0005 {
		return ""
	}
	if sec < 1 {
		return "+" + strconv.Itoa(int(sec*1000+0.5)) + " ms"
	}
	if sec < 60 {
		return "+" + strconv.FormatFloat(sec, 'f', 2, 64) + " s"
	}
	m := int(sec / 60)
	s := int(sec) - m*60
	return "+" + strconv.Itoa(m) + "m " + strconv.Itoa(s) + "s"
}

func metaLine(ex *gen.DescribeExecutionOutput) string {
	parts := []string{}
	if ex.StartDate != nil {
		parts = append(parts, "Started "+fmtStamp(ex.StartDate))
	}
	if ex.StopDate != nil {
		parts = append(parts, "Stopped "+fmtStamp(ex.StopDate))
	}
	parts = append(parts, fmtDuration(ex.StartDate, ex.StopDate))
	if ex.RedriveCount != nil && *ex.RedriveCount > 0 {
		parts = append(parts, "redriven "+strconv.Itoa(int(*ex.RedriveCount)))
	}
	return strings.Join(parts, " · ")
}

func (s *Server) machineDefinition(ctx context.Context, machine string) (string, string) {
	if s.Handlers == nil {
		return "", "No definition."
	}
	sm, err := s.Handlers.DescribeStateMachine(ctx, &gen.DescribeStateMachineInput{StateMachineArn: &machine})
	if err != nil {
		return "", errText(err)
	}
	return valueOf(sm.Definition), ""
}

// setPlainGraph is the definition with no execution colors.
func setPlainGraph(view *dashView, defText string) {
	view.Definition = prettyJSON(defText)
	prog := parseProgram(defText)
	if prog == nil || len(prog.States) == 0 {
		if strings.TrimSpace(defText) == "" {
			view.PlainNote = "No definition."
		} else {
			view.PlainNote = "This definition has no state graph."
		}
		return
	}
	colors := make(map[string]string, len(prog.States))
	for name := range prog.States {
		colors[name] = "idle"
	}
	// Scale 1 matches the run graph. Fit grows toward 1100px and clips this column.
	g := layoutGraph(prog, colors, "", 1)
	if g == nil {
		view.PlainNote = "This definition has no state graph."
		return
	}
	g.Marker = "arrow-def"
	for i := range g.Nodes {
		g.Nodes[i].Kind = "definition"
		g.Nodes[i].Href = ""
		g.Nodes[i].Fragment = ""
	}
	view.Plain = g
}

func prettyJSON(text string) string {
	if text == "" {
		return ""
	}
	var v any
	if json.Unmarshal([]byte(text), &v) != nil {
		return text
	}
	out, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return text
	}
	return string(out)
}

func chipClass(status string) string {
	switch status {
	case "SUCCEEDED":
		return "bg-green-50 text-green-700"
	case "FAILED":
		return "bg-red-50 text-red-700"
	case "RUNNING":
		return "bg-sky-50 text-sky-700"
	case "TIMED_OUT", "PENDING_REDRIVE":
		return "bg-amber-50 text-amber-800"
	default:
		return "bg-slate-100 text-slate-600"
	}
}

func dashboardAccount(view dashView) string {
	arn := view.Machine
	if arn == "" && len(view.Machines) > 0 {
		arn = view.Machines[0].ARN
	}
	parts := strings.Split(arn, ":")
	if len(parts) < 5 || parts[3] == "" || parts[4] == "" {
		return ""
	}
	return parts[3] + " · " + parts[4]
}

func listPageURL(machine string) string {
	return "/dashboard?machine=" + url.QueryEscape(machine)
}

func listURL(machine string) string {
	return listPageURL(machine)
}

func executionURL(machine, execution, step, index string, zoom float64, part, eq string) string {
	q := url.Values{}
	if machine != "" {
		q.Set("machine", machine)
	}
	q.Set("execution", execution)
	if step != "" {
		q.Set("step", step)
	}
	if index != "" {
		q.Set("mi", index)
	}
	if z := zoomParam(zoom); z != "" {
		q.Set("z", z)
	}
	if eq != "" {
		q.Set("eq", eq)
	}
	if part != "" {
		q.Set("part", part)
	}
	return "/dashboard?" + q.Encode()
}

func zoomParam(z float64) string {
	if z == 0 {
		return "fit"
	}
	if z == 1 {
		return ""
	}
	return strconv.FormatFloat(z, 'f', -1, 64)
}

func parseZoom(s string) float64 {
	if s == "" {
		return 1
	}
	if s == "fit" {
		return 0
	}
	z, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 1
	}
	return clampZoom(z)
}

func clampZoom(z float64) float64 {
	if z < 0.35 {
		return 0.35
	}
	if z > 2.5 {
		return 2.5
	}
	return math.Round(z*100) / 100
}

func (v dashView) pageURL() string {
	switch v.Kind {
	case "detail":
		return executionURL(v.Machine, v.Execution, v.Step, v.Index, v.Zoom, "", v.Filter)
	case "list":
		if v.Filter != "" {
			return listPageURL(v.Machine) + "&q=" + url.QueryEscape(v.Filter)
		}
		return listPageURL(v.Machine)
	default:
		return "/dashboard"
	}
}

func (v *dashView) markActive() {
	for i := range v.Machines {
		v.Machines[i].Active = v.Machines[i].ARN == v.Machine
	}
}

var dashboardTmpl = template.Must(template.New("dashboard").Parse(dashboardTemplates))
