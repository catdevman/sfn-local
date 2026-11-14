package store

import (
	"context"
	"strings"
	"sync"
	"time"
)

// ErrNotRunning means a terminal write lost the race with another terminal write.
type errNotRunning struct{}

func (errNotRunning) Error() string { return "execution is not running" }

// ErrNotRunning is returned when the execution is already terminal or gone.
var ErrNotRunning error = errNotRunning{}

// ErrHistoryLimit means a non-terminal event would pass 24999 events.
type errHistoryLimit struct{}

func (errHistoryLimit) Error() string { return "history limit" }

// ErrHistoryLimit is returned by Append when only a terminal event may be added.
var ErrHistoryLimit error = errHistoryLimit{}

func isTerminalEvent(typ string) bool {
	switch typ {
	case "ExecutionSucceeded", "ExecutionFailed", "ExecutionAborted", "ExecutionTimedOut":
		return true
	default:
		return false
	}
}

// PutExecution inserts a RUNNING execution. A duplicate ARN returns the
// existing row and a nil handle; the caller must not start a second worker.
func (s *Store) PutExecution(ex Execution) (Execution, *RunHandle, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cur, ok := s.execs[ex.ARN]; ok {
		return cur.snapshot(), nil, true
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var once sync.Once
	rec := &execRec{
		Execution: ex.snapshot(),
		cancel:    cancel,
		done:      done,
	}
	rec.History = copyHistory(ex.History)
	s.execs[ex.ARN] = rec
	// Capture the channel locally. BeginRedrive replaces rec.done, and a late
	// Finish from this worker must not close the redrive's channel.
	handle := &RunHandle{Ctx: ctx, Done: done, finish: func() {
		once.Do(func() { close(done) })
	}}
	return rec.snapshot(), handle, false
}

// Execution returns a copy.
func (s *Store) Execution(arn string) (Execution, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.execs[arn]
	if !ok {
		return Execution{}, false
	}
	return rec.snapshot(), true
}

// Executions returns every execution, including express rows that have not
// been dropped yet.
func (s *Store) Executions(machineARN string) []Execution {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Execution
	for _, rec := range s.execs {
		if rec.StateMachineARN == machineARN {
			out = append(out, rec.snapshot())
		}
	}
	return out
}

// Finished is closed when the worker returns. A missing execution is already closed.
func (s *Store) Finished(arn string) <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.execs[arn]
	if !ok || rec.done == nil {
		ch := make(chan struct{})
		close(ch)
		return ch
	}
	return rec.done
}

// Cancel requests the worker to stop. StopExecution records the terminal event
// itself before cancelling.
func (s *Store) Cancel(arn string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if rec, ok := s.execs[arn]; ok && rec.cancel != nil {
		rec.cancel()
	}
}

// Append adds a non-terminal or terminal event. A non-terminal event is
// refused when 24999 events already exist.
//
// PreviousEventId follows AWS. ExecutionStarted and the event right after it
// have 0. An event with HasPrevious uses Previous. Otherwise an event points
// at the newest event on its chain, which SetChainLast can move. A Side event
// does not become its chain's newest event.
func (s *Store) Append(arn string, ev HistoryEvent) error {
	_, err := s.AppendID(arn, ev)
	return err
}

// AppendID is Append, and it returns the new event's id.
func (s *Store) AppendID(arn string, ev HistoryEvent) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.appendLocked(arn, ev); err != nil {
		return 0, err
	}
	h := s.execs[arn].History
	return h[len(h)-1].ID, nil
}

// SetChainLast makes id the newest event on chain, so the next event on that
// chain points at it.
func (s *Store) SetChainLast(arn, chain string, id int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if rec, ok := s.execs[arn]; ok {
		if rec.last == nil {
			rec.last = map[string]int64{}
		}
		rec.last[chain] = id
	}
}

// ChainLast is the newest event id on chain.
func (s *Store) ChainLast(arn, chain string) (int64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.execs[arn]
	if !ok {
		return 0, false
	}
	id, ok := rec.last[chain]
	return id, ok
}

// SetResult records the output of a finished Parallel branch or Map
// iteration.
func (s *Store) SetResult(arn, chain, output string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if rec, ok := s.execs[arn]; ok {
		if rec.results == nil {
			rec.results = map[string]string{}
		}
		rec.results[chain] = output
	}
}

// Result is a finished branch or iteration's output.
func (s *Store) Result(arn, chain string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.execs[arn]
	if !ok {
		return "", false
	}
	out, ok := rec.results[chain]
	return out, ok
}

// ClearResults forgets the results of every chain that starts with prefix,
// when a Parallel or Map state starts an attempt from scratch.
func (s *Store) ClearResults(arn, prefix string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if rec, ok := s.execs[arn]; ok {
		for chain := range rec.results {
			if strings.HasPrefix(chain, prefix) {
				delete(rec.results, chain)
			}
		}
	}
}

func (s *Store) appendLocked(arn string, ev HistoryEvent) error {
	rec, ok := s.execs[arn]
	if !ok || rec.Status != ExecRunning {
		return ErrNotRunning
	}
	n := len(rec.History)
	if n >= MaxHistory {
		return ErrHistoryLimit
	}
	if n >= MaxHistory-1 && !isTerminalEvent(ev.Type) {
		return ErrHistoryLimit
	}
	ev.ID = int64(n + 1)
	last, chained := rec.last[ev.Chain]
	switch {
	case ev.HasPrevious:
		ev.PreviousID = ev.Previous
	case ev.Chain == "" && n <= 1:
		ev.PreviousID = 0
	case chained:
		ev.PreviousID = last
	case n > 0:
		ev.PreviousID = rec.History[n-1].ID
	}
	if !ev.Side {
		if rec.last == nil {
			rec.last = map[string]int64{}
		}
		rec.last[ev.Chain] = ev.ID
	}
	rec.History = append(rec.History, ev)
	return nil
}

// Complete sets a terminal status and appends the terminal event when the
// execution is still RUNNING. The existing row is returned when it is already
// terminal.
func (s *Store) Complete(arn, status string, when time.Time, ev HistoryEvent, output string, hasOutput bool, errName, cause string, hasError, hasCause bool) (Execution, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.execs[arn]
	if !ok {
		return Execution{}, ErrNotRunning
	}
	if rec.Status != ExecRunning {
		return rec.snapshot(), ErrNotRunning
	}
	if err := s.appendLocked(arn, ev); err != nil {
		return Execution{}, err
	}
	rec.Status = status
	stopped := when
	rec.Stopped = &stopped
	if hasOutput {
		rec.Output = output
		rec.HasOutput = true
	}
	if hasError {
		rec.Error = errName
		rec.HasError = true
	}
	if hasCause {
		rec.Cause = cause
		rec.HasCause = true
	}
	return rec.snapshot(), nil
}

// Drop removes an execution and its history.
func (s *Store) Drop(arn string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dropTokensLocked(arn)
	delete(s.execs, arn)
}

// Running reports whether the execution exists and is RUNNING.
func (s *Store) Running(arn string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.execs[arn]
	return ok && rec.Status == ExecRunning
}

// Sweep drops terminal executions of DELETING machines and drops the machine
// when no STANDARD execution of it is RUNNING. Express rows are not in that
// scan, so an in-flight express execution does not keep the machine.
func (s *Store) Sweep() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for arn, m := range s.machines {
		if m.Status != StatusDeleting {
			continue
		}
		for exARN, rec := range s.execs {
			if rec.Machine() != arn || rec.Express {
				continue
			}
			if rec.Status != ExecRunning {
				s.dropTokensLocked(exARN)
				delete(s.execs, exARN)
			}
		}
		alive := false
		for _, rec := range s.execs {
			if rec.Machine() == arn && !rec.Express && rec.Status == ExecRunning {
				alive = true
				break
			}
		}
		if !alive {
			s.dropQualifiedLocked(arn)
			delete(s.machines, arn)
			delete(s.machineBy, m.Name)
			delete(s.tags, arn)
		}
	}
}
