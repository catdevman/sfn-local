package api

import (
	"sort"
	"strconv"

	"github.com/catdevman/sfn-local/internal/api/gen"
	"github.com/catdevman/sfn-local/internal/store"
)

// dashIndex is one option in the Map iteration picker.
type dashIndex struct {
	Value    string
	Label    string
	Selected bool
}

// stepFocus keeps history rows to one iteration of the latest visit.
type stepFocus struct {
	lo, hi int
	index  int32
	active bool
}

func (f stepFocus) allows(i int, id int64, parents map[int64]store.HistoryEvent) bool {
	if !f.active {
		return true
	}
	if i < f.lo || i > f.hi {
		return false
	}
	ev, ok := parents[id]
	return ok && ev.HasParentIndex && ev.ParentIndex == f.index
}

type iterKind int

const (
	iterRunning iterKind = iota
	iterSucceeded
	iterFailed
	iterAborted
)

// applyStepIO replaces the execution document with the selected state's input
// and output. An mi query selects one iteration of the latest Map visit.
// An unknown index is ignored and is not written back into the page URL.
func applyStepIO(view *dashView, events []gen.HistoryEvent, parents map[int64]store.HistoryEvent, miRaw string) stepFocus {
	if view == nil || view.Step == "" {
		return stepFocus{}
	}
	enter, exit, ok := latestVisit(events, view.Step)
	if !ok {
		view.Input = "—"
		view.Output = "—"
		view.IoNote = view.Step + " did not run"
		return stepFocus{}
	}
	view.Input = eventInput(events[enter])
	if exit >= 0 {
		view.Output = eventOutput(events[exit])
	} else {
		view.Output = "—"
	}
	view.IoNote = view.Step
	hi := len(events) - 1
	if exit >= 0 {
		hi = exit
	}
	view.Indexes = mapIndexes(events, view.Step, enter, hi)
	if len(view.Indexes) == 0 {
		return stepFocus{}
	}
	idx, parsed := parseMapIndex(miRaw)
	if !parsed || !knownIndex(view.Indexes, idx) {
		return stepFocus{}
	}
	view.Index = strconv.FormatInt(int64(idx), 10)
	view.IndexSet = true
	for i := range view.Indexes {
		view.Indexes[i].Selected = view.Indexes[i].Value == view.Index
	}
	view.IoNote = view.Step + " · index " + view.Index
	view.Input, view.Output = iterationIO(events, parents, view.Step, idx, enter, hi)
	return stepFocus{lo: enter, hi: hi, index: idx, active: true}
}

func latestVisit(events []gen.HistoryEvent, name string) (enter, exit int, ok bool) {
	depth := 0
	start := -1
	lastStart, lastExit := -1, -1
	seen := false
	for i, ev := range events {
		if enteredName(ev) == name {
			if depth == 0 {
				start = i
			}
			depth++
		}
		if exitedName(ev) == name && depth > 0 {
			depth--
			if depth == 0 {
				lastStart = start
				lastExit = i
				seen = true
				start = -1
			}
		}
	}
	if depth > 0 && start >= 0 {
		return start, -1, true
	}
	if seen {
		return lastStart, lastExit, true
	}
	return -1, -1, false
}

func mapIndexes(events []gen.HistoryEvent, name string, lo, hi int) []dashIndex {
	status := map[int32]iterKind{}
	var order []int32
	for i := lo; i <= hi && i < len(events); i++ {
		n, idx, kind, ok := mapIter(events[i])
		if !ok || n != name {
			continue
		}
		if _, seen := status[idx]; !seen {
			order = append(order, idx)
		}
		status[idx] = kind
	}
	sort.Slice(order, func(a, b int) bool { return order[a] < order[b] })
	out := make([]dashIndex, 0, len(order))
	for _, idx := range order {
		value := strconv.FormatInt(int64(idx), 10)
		out = append(out, dashIndex{Value: value, Label: indexLabel(value, status[idx])})
	}
	return out
}

func indexLabel(value string, kind iterKind) string {
	base := "Index " + value
	switch kind {
	case iterFailed:
		return base + " · failed"
	case iterAborted:
		return base + " · aborted"
	case iterRunning:
		return base + " · running"
	default:
		return base
	}
}

func iterationIO(events []gen.HistoryEvent, parents map[int64]store.HistoryEvent, name string, idx int32, lo, hi int) (string, string) {
	var firstIn, lastOut string
	var haveIn, haveOut bool
	latest := iterRunning
	seen := false
	for i := lo; i <= hi && i < len(events); i++ {
		ev := events[i]
		if ev.Id == nil {
			continue
		}
		parent, ok := parents[*ev.Id]
		if !ok || !parent.HasParentIndex || parent.ParentIndex != idx {
			continue
		}
		if !haveIn {
			if text, ok := enteredPayload(ev); ok {
				firstIn = text
				haveIn = true
			}
		}
		if text, ok := exitedPayload(ev); ok {
			lastOut = text
			haveOut = true
		}
		if n, iter, kind, ok := mapIter(ev); ok && n == name && iter == idx {
			latest = kind
			seen = true
		}
	}
	in := "—"
	if haveIn {
		in = prettyJSON(firstIn)
	}
	out := "—"
	if seen && latest == iterSucceeded && haveOut {
		out = prettyJSON(lastOut)
	}
	return in, out
}

func parseMapIndex(raw string) (int32, bool) {
	if raw == "" {
		return 0, false
	}
	n, err := strconv.ParseInt(raw, 10, 32)
	if err != nil || n < 0 {
		return 0, false
	}
	return int32(n), true
}

func knownIndex(indexes []dashIndex, idx int32) bool {
	want := strconv.FormatInt(int64(idx), 10)
	for _, item := range indexes {
		if item.Value == want {
			return true
		}
	}
	return false
}

func mapIter(ev gen.HistoryEvent) (string, int32, iterKind, bool) {
	switch {
	case ev.MapIterationStartedEventDetails != nil:
		return iterDetail(ev.MapIterationStartedEventDetails, iterRunning)
	case ev.MapIterationSucceededEventDetails != nil:
		return iterDetail(ev.MapIterationSucceededEventDetails, iterSucceeded)
	case ev.MapIterationFailedEventDetails != nil:
		return iterDetail(ev.MapIterationFailedEventDetails, iterFailed)
	case ev.MapIterationAbortedEventDetails != nil:
		return iterDetail(ev.MapIterationAbortedEventDetails, iterAborted)
	default:
		return "", 0, 0, false
	}
}

func iterDetail(d *gen.MapIterationEventDetails, kind iterKind) (string, int32, iterKind, bool) {
	if d == nil || d.Name == nil || *d.Name == "" || d.Index == nil {
		return "", 0, 0, false
	}
	return *d.Name, *d.Index, kind, true
}

func enteredName(ev gen.HistoryEvent) string {
	if ev.StateEnteredEventDetails == nil {
		return ""
	}
	return valueOf(ev.StateEnteredEventDetails.Name)
}

func exitedName(ev gen.HistoryEvent) string {
	if ev.StateExitedEventDetails == nil {
		return ""
	}
	return valueOf(ev.StateExitedEventDetails.Name)
}

func enteredPayload(ev gen.HistoryEvent) (string, bool) {
	if ev.StateEnteredEventDetails == nil || ev.StateEnteredEventDetails.Input == nil || *ev.StateEnteredEventDetails.Input == "" {
		return "", false
	}
	return *ev.StateEnteredEventDetails.Input, true
}

func exitedPayload(ev gen.HistoryEvent) (string, bool) {
	if ev.StateExitedEventDetails == nil || ev.StateExitedEventDetails.Output == nil || *ev.StateExitedEventDetails.Output == "" {
		return "", false
	}
	return *ev.StateExitedEventDetails.Output, true
}

func eventInput(ev gen.HistoryEvent) string {
	text, ok := enteredPayload(ev)
	if !ok {
		return "—"
	}
	return prettyJSON(text)
}

func eventOutput(ev gen.HistoryEvent) string {
	text, ok := exitedPayload(ev)
	if !ok {
		return "—"
	}
	return prettyJSON(text)
}
