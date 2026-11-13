package api

import (
	"encoding/json"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/catdevman/sfn-local/internal/api/gen"
)

// View data for the dashboard templates. URLs are built in Go and escaped
// by html/template. Class strings that are assembled here also appear in
// templates.html so the Tailwind build keeps them.
type dashView struct {
	OOB          bool
	Part         string
	Kind         string
	Banner       string
	Error        string
	Account      string
	RefreshURL   string
	Machines     []dashMachine
	Machine      string
	Execution    string
	Filter       string
	Title        string
	Sub          string
	ListError    string
	RowsURL      string
	Execs        []dashExec
	NoMatch      bool
	Empty        bool
	Poll         bool
	EmptyCopy    string
	Status       string
	Chip         string
	Meta         string
	Input        string
	Output       string
	ShowStop     bool
	BackURL      string
	BackFragment string
	ExecARN      string
	Comment      string
	FailureError string
	FailureCause string
	Graph        *graphView
	GraphNote    string
	Plain        *graphView
	PlainNote    string
	Definition   string
	Events       []dashEvent
	EventCount   string
	Step         string
	Index        string
	IndexSet     bool
	Indexes      []dashIndex
	IndexURL     string
	IoNote       string
	Zoom         float64
	ZoomText     string
	LiveURL      string
	ZoomIn       string
	ZoomOut      string
	FitURL       string
	ClearURL     string
	PushZoomIn   string
	PushZoomOut  string
	PushFit      string
	PushClear    string
}

type dashMachine struct {
	Name     string
	ARN      string
	Type     string
	Href     string
	Fragment string
	Active   bool
}

type dashExec struct {
	Name     string
	ARN      string
	Status   string
	Chip     string
	Started  string
	Stopped  string
	Duration string
	Href     string
	Fragment string
}

type dashEvent struct {
	ID     string
	Type   string
	Step   string
	When   string
	Delta  string
	Error  string
	Detail string
}

type histLite struct {
	ID      int64
	Type    string
	Entered string
	Exited  string
	Iter    string
	Time    float64
	HasTime bool
	Delta   float64
	Step    string
	Inner   string
}

type program struct {
	StartAt string
	Comment string
	States  map[string]stateDef
}

type stateDef struct {
	Type          string            `json:"Type"`
	Next          string            `json:"Next"`
	Default       string            `json:"Default"`
	Resource      string            `json:"Resource"`
	End           bool              `json:"End"`
	Choices       []choiceEdge      `json:"Choices"`
	Catch         []choiceEdge      `json:"Catch"`
	Branches      []json.RawMessage `json:"Branches"`
	ItemProcessor json.RawMessage   `json:"ItemProcessor"`
	Iterator      json.RawMessage   `json:"Iterator"`
}

type choiceEdge struct {
	Next string `json:"Next"`
}

type graphView struct {
	Zoom   float64
	Marker string
	ViewW  string
	ViewH  string
	Width  string
	Height string
	Nodes  []graphNode
	Edges  []graphEdge
}

type graphNode struct {
	Name     string
	Short    string
	Type     string
	Sub      string
	Title    string
	Status   string
	X        string
	Y        string
	W        string
	H        string
	LabelX   string
	TypeY    string
	NameY    string
	SubY     string
	StartY   string
	Fill     string
	Stroke   string
	SW       string
	Extra    string
	Href     string
	Fragment string
	Kind     string
	Start    bool
}

type graphEdge struct {
	Path  string
	Class string
}

type laidEdge struct {
	from  string
	to    string
	kind  string
	slot  int
	slots int
	back  bool
}

func liteOf(ev gen.HistoryEvent) histLite {
	h := histLite{}
	if ev.Id != nil {
		h.ID = *ev.Id
	}
	if ev.Type != nil {
		h.Type = string(*ev.Type)
	}
	if ev.StateEnteredEventDetails != nil {
		h.Entered = valueOf(ev.StateEnteredEventDetails.Name)
	}
	if ev.StateExitedEventDetails != nil {
		h.Exited = valueOf(ev.StateExitedEventDetails.Name)
	}
	h.Iter = iterationStep(ev)
	if ev.Timestamp != nil {
		h.Time = ev.Timestamp.Seconds()
		h.HasTime = true
	}
	return h
}

func iterationStep(ev gen.HistoryEvent) string {
	for _, d := range []*gen.MapIterationEventDetails{
		ev.MapIterationStartedEventDetails,
		ev.MapIterationSucceededEventDetails,
		ev.MapIterationFailedEventDetails,
		ev.MapIterationAbortedEventDetails,
	} {
		if s := iterationName(d); s != "" {
			return s
		}
	}
	return ""
}

func iterationName(d *gen.MapIterationEventDetails) string {
	if d == nil || d.Name == nil || *d.Name == "" {
		return ""
	}
	if d.Index == nil {
		return *d.Name
	}
	return *d.Name + " [" + strconv.FormatInt(int64(*d.Index), 10) + "]"
}

func annotateHistory(prog *program, events []histLite) []histLite {
	top := map[string]bool{}
	if prog != nil {
		for name := range prog.States {
			top[name] = true
		}
	}
	var open []string
	var tops []string
	var prev float64
	prevSet := false
	out := make([]histLite, len(events))
	for i, ev := range events {
		row := ev
		if ev.Entered != "" {
			open = append(open, ev.Entered)
			if top[ev.Entered] {
				tops = append(tops, ev.Entered)
			}
		}
		if strings.HasPrefix(ev.Type, "Execution") {
			row.Step = ""
			row.Inner = ""
		} else {
			switch {
			case ev.Entered != "":
				row.Step = ev.Entered
			case ev.Exited != "":
				row.Step = ev.Exited
			case ev.Iter != "":
				row.Step = ev.Iter
			case len(open) > 0:
				row.Step = open[len(open)-1]
			}
			if len(tops) > 0 {
				row.Inner = tops[len(tops)-1]
			}
		}
		if ev.Exited != "" {
			open = removeLast(open, ev.Exited)
			if top[ev.Exited] {
				tops = removeLast(tops, ev.Exited)
			}
		}
		if ev.HasTime {
			if prevSet {
				row.Delta = ev.Time - prev
			}
			prev = ev.Time
			prevSet = true
		}
		out[i] = row
	}
	return out
}

func removeLast(list []string, name string) []string {
	for i := len(list) - 1; i >= 0; i-- {
		if list[i] == name {
			out := make([]string, 0, len(list)-1)
			out = append(out, list[:i]...)
			out = append(out, list[i+1:]...)
			return out
		}
	}
	return list
}

func colorStates(names []string, events []histLite, execStatus string) map[string]string {
	status := make(map[string]string, len(names))
	for _, name := range names {
		status[name] = "idle"
	}
	open := ""
	hasOpen := false
	recovered := map[string]bool{
		"TaskSucceeded":           true,
		"ParallelStateSucceeded":  true,
		"MapStateSucceeded":       true,
		"ActivitySucceeded":       true,
		"LambdaFunctionSucceeded": true,
	}
	for _, ev := range events {
		if ev.Entered != "" {
			if _, ok := status[ev.Entered]; ok {
				status[ev.Entered] = "running"
				open = ev.Entered
				hasOpen = true
			}
		}
		if ev.Exited != "" {
			if _, ok := status[ev.Exited]; ok {
				if status[ev.Exited] == "running" {
					status[ev.Exited] = "succeeded"
				}
				if hasOpen && open == ev.Exited {
					hasOpen = false
				}
			}
		}
		typ := ev.Type
		if !hasOpen || strings.HasPrefix(typ, "MapIteration") || strings.HasPrefix(typ, "Execution") {
			continue
		}
		switch {
		case recovered[typ]:
			status[open] = "succeeded"
		case strings.HasSuffix(typ, "Failed"):
			status[open] = "failed"
		case strings.HasSuffix(typ, "TimedOut"):
			status[open] = "timedout"
		case strings.HasSuffix(typ, "Aborted"):
			status[open] = "aborted"
		}
	}
	if hasOpen && status[open] == "running" {
		switch execStatus {
		case "FAILED":
			status[open] = "failed"
		case "TIMED_OUT":
			status[open] = "timedout"
		case "ABORTED":
			status[open] = "aborted"
		}
	}
	return status
}

func matchesStep(row histLite, step string) bool {
	if step == "" {
		return true
	}
	if strings.HasPrefix(row.Type, "Execution") {
		return false
	}
	if row.Step == step || row.Inner == step {
		return true
	}
	return strings.HasPrefix(row.Step, step+" [")
}

func failureOf(ex *gen.DescribeExecutionOutput, events []gen.HistoryEvent) (string, string) {
	if ex != nil && (valueOf(ex.Error) != "" || valueOf(ex.Cause) != "") {
		return valueOf(ex.Error), valueOf(ex.Cause)
	}
	for i := len(events) - 1; i >= 0; i-- {
		ev := events[i]
		var errS, cause string
		switch {
		case ev.ExecutionFailedEventDetails != nil:
			errS = valueOf(ev.ExecutionFailedEventDetails.Error)
			cause = valueOf(ev.ExecutionFailedEventDetails.Cause)
		case ev.ExecutionTimedOutEventDetails != nil:
			errS = valueOf(ev.ExecutionTimedOutEventDetails.Error)
			cause = valueOf(ev.ExecutionTimedOutEventDetails.Cause)
		case ev.ExecutionAbortedEventDetails != nil:
			errS = valueOf(ev.ExecutionAbortedEventDetails.Error)
			cause = valueOf(ev.ExecutionAbortedEventDetails.Cause)
		default:
			continue
		}
		if errS != "" || cause != "" {
			return errS, cause
		}
	}
	return "", ""
}

func eventError(ev gen.HistoryEvent) string {
	raw, err := json.Marshal(ev)
	if err != nil {
		return ""
	}
	var doc map[string]any
	if json.Unmarshal(raw, &doc) != nil {
		return ""
	}
	for _, v := range doc {
		obj, ok := v.(map[string]any)
		if !ok {
			continue
		}
		if s, ok := obj["error"].(string); ok && s != "" {
			return s
		}
	}
	return ""
}

func revealEvent(ev gen.HistoryEvent) string {
	raw, err := json.Marshal(ev)
	if err != nil {
		return ""
	}
	var doc map[string]any
	if json.Unmarshal(raw, &doc) != nil {
		return string(raw)
	}
	for key, v := range doc {
		obj, ok := v.(map[string]any)
		if !ok {
			continue
		}
		for ik, iv := range obj {
			s, ok := iv.(string)
			if !ok || s == "" {
				continue
			}
			var parsed any
			if json.Unmarshal([]byte(s), &parsed) != nil || parsed == nil {
				continue
			}
			switch parsed.(type) {
			case map[string]any, []any:
				obj[ik] = parsed
			}
		}
		doc[key] = obj
	}
	out, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return string(raw)
	}
	return string(out)
}

func parseProgram(text string) *program {
	if strings.TrimSpace(text) == "" {
		return nil
	}
	var raw struct {
		StartAt string                     `json:"StartAt"`
		Comment string                     `json:"Comment"`
		States  map[string]json.RawMessage `json:"States"`
	}
	if json.Unmarshal([]byte(text), &raw) != nil || raw.States == nil {
		return nil
	}
	p := &program{StartAt: raw.StartAt, Comment: raw.Comment, States: map[string]stateDef{}}
	for name, body := range raw.States {
		var st stateDef
		if json.Unmarshal(body, &st) != nil {
			continue
		}
		p.States[name] = st
	}
	return p
}

func stateNames(p *program) []string {
	if p == nil {
		return nil
	}
	names := make([]string, 0, len(p.States))
	for name := range p.States {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func layoutGraph(prog *program, colors map[string]string, step string, z float64) *graphView {
	if prog == nil || len(prog.States) == 0 {
		return nil
	}
	names := stateNames(prog)
	var edges []laidEdge
	for _, name := range names {
		for _, e := range edgesFrom(prog.States[name], prog.States) {
			edges = append(edges, laidEdge{from: name, to: e.to, kind: e.kind})
		}
	}
	rank := map[string]int{}
	order := bfsOrder(prog, rank)
	maxRank := -1
	for _, r := range rank {
		if r > maxRank {
			maxRank = r
		}
	}
	extra := maxRank + 1
	if extra < 0 {
		extra = 0
	}
	for _, name := range names {
		if _, ok := rank[name]; ok {
			continue
		}
		rank[name] = extra
		order = append(order, name)
		if rank[name] > maxRank {
			maxRank = rank[name]
		}
	}
	if maxRank < 0 {
		maxRank = 0
	}
	groups := map[string][]int{}
	for i := range edges {
		k := edges[i].from + "\n" + edges[i].to
		groups[k] = append(groups[k], i)
	}
	for _, idxs := range groups {
		for n, i := range idxs {
			edges[i].slot = n
			edges[i].slots = len(idxs)
			edges[i].back = rank[edges[i].to] <= rank[edges[i].from]
		}
	}
	const (
		nw   = 168.0
		nh   = 62.0
		gapX = 56.0
		gapY = 28.0
		pad  = 28.0
	)
	cols := map[int][]string{}
	var colKeys []int
	seenCol := map[int]bool{}
	for _, name := range order {
		r := rank[name]
		cols[r] = append(cols[r], name)
		if !seenCol[r] {
			seenCol[r] = true
			colKeys = append(colKeys, r)
		}
	}
	sort.Ints(colKeys)
	pos := map[string][2]float64{}
	maxRows := 1
	for _, r := range colKeys {
		if len(cols[r]) > maxRows {
			maxRows = len(cols[r])
		}
		for i, name := range cols[r] {
			pos[name] = [2]float64{pad + float64(r)*(nw+gapX), pad + float64(i)*(nh+gapY)}
		}
	}
	backs := 0
	for _, e := range edges {
		if e.back {
			backs++
		}
	}
	w := pad*2 + float64(maxRank+1)*nw + float64(maxRank)*gapX
	h := pad*2 + float64(maxRows)*nh + float64(maxRows-1)*gapY
	if backs > 0 {
		h += 36 + float64(backs)*8
	} else {
		h += 8
	}
	if w < 320 {
		w = 320
	}
	if h < 120 {
		h = 120
	}
	scale := z
	if scale == 0 {
		scale = fitScale(w)
	}
	g := &graphView{
		Zoom:   scale,
		Marker: "arrow",
		ViewW:  num(w),
		ViewH:  num(h),
		Width:  num(math.Round(w * scale)),
		Height: num(math.Round(h * scale)),
	}
	base := 0.0
	for _, p := range pos {
		if p[1]+nh > base {
			base = p[1] + nh
		}
	}
	for _, e := range edges {
		a, aok := pos[e.from]
		b, bok := pos[e.to]
		if !aok || !bok {
			continue
		}
		g.Edges = append(g.Edges, graphEdge{Path: edgePath(a, b, e, base, nw, nh), Class: edgeClass(e.kind)})
	}
	for _, r := range colKeys {
		for _, name := range cols[r] {
			p := pos[name]
			st := prog.States[name]
			status := colors[name]
			if status == "" {
				status = "idle"
			}
			fill, stroke, sw, extra := nodePaint(status, step == name)
			title := name
			if st.Type != "" {
				title += " (" + st.Type + ")"
			}
			if st.Resource != "" {
				title += "\n" + st.Resource
			}
			startY := p[1] - 8
			if startY < 12 {
				startY = 12
			}
			g.Nodes = append(g.Nodes, graphNode{
				Name:   name,
				Short:  short(name, 20),
				Type:   st.Type,
				Sub:    subtitle(st),
				Title:  title,
				Status: status,
				X:      num(p[0]),
				Y:      num(p[1]),
				W:      num(nw),
				H:      num(nh),
				LabelX: num(p[0] + 12),
				TypeY:  num(p[1] + 18),
				NameY:  num(p[1] + 38),
				SubY:   num(p[1] + 54),
				StartY: num(startY),
				Fill:   fill,
				Stroke: stroke,
				SW:     sw,
				Extra:  extra,
				Kind:   "run",
				Start:  name == prog.StartAt,
			})
		}
	}
	return g
}

type rawEdge struct {
	to   string
	kind string
}

func edgesFrom(st stateDef, states map[string]stateDef) []rawEdge {
	var out []rawEdge
	for _, c := range st.Choices {
		if c.Next != "" && hasState(states, c.Next) {
			out = append(out, rawEdge{to: c.Next, kind: "choice"})
		}
	}
	if st.Default != "" && hasState(states, st.Default) {
		out = append(out, rawEdge{to: st.Default, kind: "default"})
	}
	if st.Next != "" && hasState(states, st.Next) {
		out = append(out, rawEdge{to: st.Next, kind: "next"})
	}
	for _, c := range st.Catch {
		if c.Next != "" && hasState(states, c.Next) {
			out = append(out, rawEdge{to: c.Next, kind: "catch"})
		}
	}
	return out
}

func hasState(states map[string]stateDef, name string) bool {
	_, ok := states[name]
	return ok
}

func bfsOrder(prog *program, rank map[string]int) []string {
	if prog.StartAt == "" || !hasState(prog.States, prog.StartAt) {
		return nil
	}
	seen := map[string]bool{prog.StartAt: true}
	rank[prog.StartAt] = 0
	q := []string{prog.StartAt}
	var order []string
	for len(q) > 0 {
		n := q[0]
		q = q[1:]
		order = append(order, n)
		for _, e := range edgesFrom(prog.States[n], prog.States) {
			if seen[e.to] {
				continue
			}
			seen[e.to] = true
			rank[e.to] = rank[n] + 1
			q = append(q, e.to)
		}
	}
	return order
}

func edgePath(a, b [2]float64, e laidEdge, base, nw, nh float64) string {
	spread := (float64(e.slot) - float64(e.slots-1)/2) * 8
	if !e.back {
		x1 := a[0] + nw
		y1 := a[1] + nh/2 + spread
		x2 := b[0]
		y2 := b[1] + nh/2 + spread
		mx := (x1 + x2) / 2
		return "M " + num(x1) + " " + num(y1) + " C " + num(mx) + " " + num(y1) + ", " + num(mx) + " " + num(y2) + ", " + num(x2) + " " + num(y2)
	}
	low := base + 22 + float64(e.slot)*12
	sx := a[0] + nw/2
	sy := a[1] + nh
	tx := b[0] + nw/2
	ty := b[1] + nh
	return "M " + num(sx) + " " + num(sy) + " C " + num(sx) + " " + num(low) + ", " + num(tx) + " " + num(low) + ", " + num(tx) + " " + num(ty)
}

func nodePaint(status string, selected bool) (fill, stroke, width, extra string) {
	switch status {
	case "succeeded":
		fill, stroke = "fill-green-50", "stroke-green-700"
	case "failed":
		fill, stroke = "fill-red-50", "stroke-red-700"
	case "running":
		fill, stroke = "fill-sky-50", "stroke-sky-700"
		extra = "motion-safe:animate-pulse"
	case "timedout":
		fill, stroke = "fill-amber-50", "stroke-amber-700"
	case "aborted":
		fill, stroke = "fill-slate-100", "stroke-slate-500"
	default:
		fill, stroke = "fill-white", "stroke-slate-300"
	}
	width = "stroke-[1.5]"
	if selected {
		stroke = "stroke-slate-900"
		width = "stroke-[2.5]"
	}
	return fill, stroke, width, extra
}

func edgeClass(kind string) string {
	switch kind {
	case "catch":
		return "fill-none stroke-red-700 [stroke-dasharray:4_3]"
	case "default":
		return "fill-none stroke-slate-400 [stroke-dasharray:4_3]"
	default:
		return "fill-none stroke-slate-400"
	}
}

func subtitle(st stateDef) string {
	if st.Resource != "" {
		s := st.Resource
		if i := strings.LastIndex(s, ":"); i >= 0 {
			s = s[i+1:]
		}
		return short(s, 22)
	}
	if n := len(st.Branches); n > 0 {
		if n == 1 {
			return "1 branch"
		}
		return strconv.Itoa(n) + " branches"
	}
	if jsonPresent(st.ItemProcessor) || jsonPresent(st.Iterator) {
		return "inline"
	}
	return ""
}

func jsonPresent(raw json.RawMessage) bool {
	s := strings.TrimSpace(string(raw))
	return s != "" && s != "null"
}

func short(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	if n <= 1 {
		return "…"
	}
	return string(r[:n-1]) + "…"
}

func fitScale(width float64) float64 {
	if width <= 0 {
		return 1
	}
	z := 1100 / width
	if z > 1.5 {
		z = 1.5
	}
	return clampZoom(z)
}

func num(v float64) string {
	return strconv.FormatFloat(v, 'f', -1, 64)
}
