package store

import (
	"sort"
	"time"
)

const (
	MapRunRunning   = "RUNNING"
	MapRunSucceeded = "SUCCEEDED"
	MapRunFailed    = "FAILED"
	MapRunAborted   = "ABORTED"
)

// MapRun is one Distributed Map run. Each attempt of the Map state, including
// a retry, is a new run. A parent redrive reuses the run.
type MapRun struct {
	ARN string
	// ExecutionARN is the parent execution.
	ExecutionARN string
	// StateMachineARN is the child machine: <parent>/<label>.
	StateMachineARN string
	Label           string
	Status          string
	Started         time.Time
	Stopped         *time.Time
	// MaxConcurrency 0 is unbounded. UpdateMapRun changes it while RUNNING.
	MaxConcurrency int32
	// TolerancePct and ToleranceCount are nil when the definition set neither.
	TolerancePct   *float32
	ToleranceCount *int64
	Items          MapRunCounts
	Executions     MapRunCounts
	// State is the Map state's name in the parent.
	State string
	// Inputs and Sizes are the child inputs and how many items each holds.
	// They are kept so a redrive runs the same items.
	Inputs []string
	Sizes  []int64
	// Children is the latest child ARN for each input. "" is never started.
	Children []string
	// WriteBucket and WritePrefix are the ResultWriter export target.
	WriteBucket  string
	WritePrefix  string
	RedriveCount int32
	RedriveDate  *time.Time
}

// MapRunCounts are the item or child execution counts of a Map Run.
type MapRunCounts struct {
	Pending        int64
	Running        int64
	Succeeded      int64
	Failed         int64
	TimedOut       int64
	Aborted        int64
	Total          int64
	ResultsWritten int64
	// PendingRedrive is unsuccessful children a redrive has not restarted yet.
	PendingRedrive int64
}

func (m MapRun) snapshot() MapRun {
	if m.Stopped != nil {
		s := *m.Stopped
		m.Stopped = &s
	}
	if m.TolerancePct != nil {
		p := *m.TolerancePct
		m.TolerancePct = &p
	}
	if m.ToleranceCount != nil {
		c := *m.ToleranceCount
		m.ToleranceCount = &c
	}
	if m.RedriveDate != nil {
		d := *m.RedriveDate
		m.RedriveDate = &d
	}
	// Inputs and Sizes never change after PutMapRun. Children does.
	m.Children = append([]string(nil), m.Children...)
	return m
}

// PutMapRun inserts a Map Run.
func (s *Store) PutMapRun(run MapRun) MapRun {
	s.mu.Lock()
	defer s.mu.Unlock()
	stored := run.snapshot()
	s.mapRuns[run.ARN] = &stored
	return stored.snapshot()
}

// MapRun returns a copy.
func (s *Store) MapRun(arn string) (MapRun, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	run, ok := s.mapRuns[arn]
	if !ok {
		return MapRun{}, false
	}
	return run.snapshot(), true
}

// MapRuns returns the runs a parent execution started, oldest first.
func (s *Store) MapRuns(executionARN string) []MapRun {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []MapRun
	for _, run := range s.mapRuns {
		if run.ExecutionARN == executionARN {
			out = append(out, run.snapshot())
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].Started.Equal(out[j].Started) {
			return out[i].Started.Before(out[j].Started)
		}
		return out[i].ARN < out[j].ARN
	})
	return out
}

// UpdateMapRun applies fn to the stored run and returns the result.
func (s *Store) UpdateMapRun(arn string, fn func(*MapRun)) (MapRun, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	run, ok := s.mapRuns[arn]
	if !ok {
		return MapRun{}, false
	}
	fn(run)
	return run.snapshot(), true
}

// MapRunExecutions returns the child executions of a Map Run.
func (s *Store) MapRunExecutions(mapRunARN string) []Execution {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Execution
	for _, rec := range s.execs {
		if rec.MapRunARN == mapRunARN {
			out = append(out, rec.snapshot())
		}
	}
	return out
}

// Machine is the state machine an execution belongs to. A Map Run child
// belongs to its parent's machine.
func (e Execution) Machine() string {
	if e.ParentMachineARN != "" {
		return e.ParentMachineARN
	}
	return e.StateMachineARN
}
