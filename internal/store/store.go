// Package store is the in-memory control plane: state machines, activities,
// tags, executions, and history. Nothing is written to disk.
package store

import (
	"context"
	"sync"
	"time"
)

const (
	StatusActive   = "ACTIVE"
	StatusDeleting = "DELETING"

	ExecRunning   = "RUNNING"
	ExecSucceeded = "SUCCEEDED"
	ExecFailed    = "FAILED"
	ExecTimedOut  = "TIMED_OUT"
	ExecAborted   = "ABORTED"

	TypeStandard = "STANDARD"
	TypeExpress  = "EXPRESS"

	// MaxHistory is the event cap. When 24999 events exist, only a terminal
	// event may still be appended.
	MaxHistory = 25000
)

// Tag is one resource tag.
type Tag struct {
	Key   string
	Value string
}

// Destination is one logging destination. The server does not open it.
type Destination struct {
	LogGroupArn string
}

// Logging is a stored logging configuration. A null level is OFF.
type Logging struct {
	Level                string
	IncludeExecutionData *bool
	Destinations         []Destination
}

// Machine is one state machine. Definition is the text the client submitted.
type Machine struct {
	ARN        string
	Name       string
	Definition string
	Type       string
	RoleArn    string
	Status     string
	Created    time.Time
	Updated    *time.Time
	Logging    *Logging
}

// Activity is one activity row. Phase 1 does not schedule tokens.
type Activity struct {
	ARN     string
	Name    string
	Created time.Time
}

// HistoryEvent is one history row. Id assignment belongs to Append.
type HistoryEvent struct {
	ID               int64
	PreviousID       int64
	Type             string
	Time             time.Time
	Name             string
	Input            string
	Output           string
	RoleArn          string
	Error            string
	Cause            string
	HasInput         bool
	HasOutput        bool
	HasRole          bool
	HasError         bool
	HasCause         bool
	Resource         string
	ResourceType     string
	Region           string
	Parameters       string
	TimeoutSeconds   int64
	HeartbeatSeconds int64
	HasResource      bool
	HasResourceType  bool
	HasRegion        bool
	HasParameters    bool
	HasTimeout       bool
	HasHeartbeat     bool
}

// Execution is one standard or express run. Definition is the snapshot taken
// at start. A later UpdateStateMachine does not change it.
type Execution struct {
	ARN             string
	Name            string
	StateMachineARN string
	Definition      string
	RoleArn         string
	Input           string
	Output          string
	HasOutput       bool
	Error           string
	Cause           string
	HasError        bool
	HasCause        bool
	Status          string
	Type            string
	Express         bool
	Started         time.Time
	Stopped         *time.Time
	Deadline        time.Time
	History         []HistoryEvent
	// MockTest is the suffix after one '#' on the start ARN. Empty means the
	// execution does not read the mock file.
	MockTest string
}

// ExistsError is a duplicate name whose definition differs.
type ExistsError struct {
	ARN string
}

func (e *ExistsError) Error() string { return "already exists: " + e.ARN }

// Terminal reports a status that is not RUNNING.
func Terminal(status string) bool { return status != "" && status != ExecRunning }

// Store is the process-wide table set. One mutex guards every map.
type Store struct {
	mu         sync.Mutex
	machines   map[string]*Machine
	machineBy  map[string]string
	activities map[string]*Activity
	activityBy map[string]string
	tags       map[string][]Tag
	execs      map[string]*execRec
}

type execRec struct {
	Execution
	cancel context.CancelFunc
	done   chan struct{}
	once   sync.Once
}

// RunHandle is the worker context and the channel closed when the worker returns.
type RunHandle struct {
	Ctx    context.Context
	Done   <-chan struct{}
	finish func()
}

// Finish closes Done. A second call does nothing.
func (h *RunHandle) Finish() {
	if h != nil && h.finish != nil {
		h.finish()
	}
}

// New returns an empty store.
func New() *Store {
	return &Store{
		machines:   map[string]*Machine{},
		machineBy:  map[string]string{},
		activities: map[string]*Activity{},
		activityBy: map[string]string{},
		tags:       map[string][]Tag{},
		execs:      map[string]*execRec{},
	}
}

func copyTags(tags []Tag) []Tag {
	if tags == nil {
		return nil
	}
	out := make([]Tag, len(tags))
	copy(out, tags)
	return out
}

func copyLogging(in *Logging) *Logging {
	if in == nil {
		return nil
	}
	out := *in
	if in.IncludeExecutionData != nil {
		v := *in.IncludeExecutionData
		out.IncludeExecutionData = &v
	}
	if in.Destinations != nil {
		out.Destinations = append([]Destination(nil), in.Destinations...)
	}
	return &out
}

func copyMachine(m *Machine) Machine {
	out := *m
	out.Logging = copyLogging(m.Logging)
	if m.Updated != nil {
		u := *m.Updated
		out.Updated = &u
	}
	return out
}

func copyHistory(in []HistoryEvent) []HistoryEvent {
	if in == nil {
		return nil
	}
	out := make([]HistoryEvent, len(in))
	copy(out, in)
	return out
}

func (e Execution) snapshot() Execution {
	e.History = copyHistory(e.History)
	if e.Stopped != nil {
		s := *e.Stopped
		e.Stopped = &s
	}
	return e
}
