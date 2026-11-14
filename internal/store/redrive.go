package store

import (
	"context"
	"strings"
	"sync"
	"time"
)

// RedriveRefusal is why BeginRedrive left the execution unchanged.
type RedriveRefusal struct{ Reason string }

func (e RedriveRefusal) Error() string { return e.Reason }

// OpenState is the outermost state that was entered and not exited.
// The input is the input recorded on that state's entered event.
// An inner state with the same name does not pop the outer frame.
func OpenState(history []HistoryEvent) (name, input string, ok bool) {
	name, input, _, ok = OpenStateEvent(history)
	return name, input, ok
}

// OpenStateEvent is OpenState, and it also returns the entered event's id.
func OpenStateEvent(history []HistoryEvent) (name, input string, entered int64, ok bool) {
	type frame struct {
		name  string
		input string
		id    int64
	}
	var stack []frame
	for _, ev := range history {
		switch {
		case strings.HasSuffix(ev.Type, "StateEntered"):
			stack = append(stack, frame{name: ev.Name, input: ev.Input, id: ev.ID})
		case strings.HasSuffix(ev.Type, "StateExited") && len(stack) > 0 && stack[len(stack)-1].name == ev.Name:
			stack = stack[:len(stack)-1]
		}
	}
	if len(stack) == 0 {
		return "", "", 0, false
	}
	return stack[0].name, stack[0].input, stack[0].id, true
}

// redriveBlock is why an execution cannot be redriven, or "" when it can.
func redriveBlock(e Execution) string {
	switch {
	case e.Express:
		return "Execution is EXPRESS and cannot be redriven"
	case e.Status == ExecFailed || e.Status == ExecTimedOut || e.Status == ExecAborted:
	default:
		return "Execution is " + e.Status + " and cannot be redriven"
	}
	if len(e.History) >= MaxHistory-1 {
		return "Execution event history cannot fit a redrive"
	}
	if _, _, open := OpenState(e.History); !open {
		return "Execution has no state to redrive"
	}
	return ""
}

// RedriveStatus is DescribeExecution's redriveStatus and reason. An
// unsuccessful Map Run child is REDRIVABLE_BY_MAP_RUN, because only a redrive
// of its parent restarts it. The caller checks the state machine.
func RedriveStatus(e Execution) (status, reason string) {
	unsuccessful := e.Status == ExecFailed || e.Status == ExecTimedOut || e.Status == ExecAborted
	if e.MapRunARN != "" && unsuccessful {
		return "REDRIVABLE_BY_MAP_RUN", ""
	}
	if e.Status == ExecRunning || e.Status == ExecSucceeded {
		return "NOT_REDRIVABLE", "Execution is " + e.Status + " and cannot be redriven"
	}
	if reason := redriveBlock(e); reason != "" {
		return "NOT_REDRIVABLE", reason
	}
	return "REDRIVABLE", ""
}

// BeginRedrive reopens a FAILED, TIMED_OUT, or ABORTED STANDARD execution.
// It sets RUNNING before appending ExecutionRedriven, because Append refuses
// a row that is not RUNNING. A RUNNING retry whose client token matches the
// redrive in progress returns that redrive and a nil handle.
func (s *Store) BeginRedrive(arn string, when time.Time, token string, deadline time.Time) (Execution, *RunHandle, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.execs[arn]
	if !ok {
		return Execution{}, nil, ErrNotRunning
	}
	if rec.Status == ExecRunning && !rec.Express {
		if rec.RedriveDate != nil && token == rec.RedriveToken {
			return rec.snapshot(), nil, nil
		}
	}
	if reason := redriveBlock(rec.Execution); reason != "" {
		return Execution{}, nil, RedriveRefusal{Reason: reason}
	}
	prevStatus := rec.Status
	prevStopped := rec.Stopped
	prevOutput, prevHasOutput := rec.Output, rec.HasOutput
	prevError, prevCause := rec.Error, rec.Cause
	prevHasError, prevHasCause := rec.HasError, rec.HasCause
	prevCount := rec.RedriveCount
	prevDate := rec.RedriveDate
	prevToken := rec.RedriveToken
	prevDeadline := rec.Deadline
	prevDone := rec.done
	oldCancel := rec.cancel
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var once sync.Once
	rec.Status = ExecRunning
	rec.Stopped = nil
	rec.Output = ""
	rec.HasOutput = false
	rec.Error = ""
	rec.Cause = ""
	rec.HasError = false
	rec.HasCause = false
	rec.RedriveCount++
	date := when
	rec.RedriveDate = &date
	rec.RedriveToken = token
	rec.Deadline = deadline
	rec.cancel = cancel
	rec.done = done
	ev := HistoryEvent{Type: "ExecutionRedriven", Time: when, RedriveCount: rec.RedriveCount, HasRedrive: true}
	if err := s.appendLocked(arn, ev); err != nil {
		rec.Status = prevStatus
		rec.Stopped = prevStopped
		rec.Output, rec.HasOutput = prevOutput, prevHasOutput
		rec.Error, rec.Cause = prevError, prevCause
		rec.HasError, rec.HasCause = prevHasError, prevHasCause
		rec.RedriveCount = prevCount
		rec.RedriveDate = prevDate
		rec.RedriveToken = prevToken
		rec.Deadline = prevDeadline
		rec.cancel = oldCancel
		rec.done = prevDone
		cancel()
		return Execution{}, nil, err
	}
	if oldCancel != nil {
		oldCancel()
	}
	handle := &RunHandle{Ctx: ctx, Done: done, finish: func() {
		once.Do(func() { close(done) })
	}}
	return rec.snapshot(), handle, nil
}
