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
// Revision changes when the mutable configuration is created or updated.
// A published version records the revision it snapped.
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
	Revision   string
}

// Activity is one activity row. Tokens for that ARN live in the token table.
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
	// Index and Length are Map iteration and MapStateStarted details.
	// HasIndex is set even when Index is 0 so the history API emits the number.
	Index     int32
	Length    int32
	HasIndex  bool
	HasLength bool
	// ParentIndex is the outermost Map iteration that produced this event.
	// HasParentIndex is set even when the index is 0. The history API does not copy it.
	ParentIndex    int32
	HasParentIndex bool
	// VersionARN and AliasARN are ExecutionStarted details for a qualified start.
	VersionARN string
	AliasARN   string
	HasVersion bool
	HasAlias   bool
	// Worker is activityStartedEventDetails.workerName.
	Worker    string
	HasWorker bool
	// RedriveCount is executionRedrivenEventDetails.redriveCount.
	// HasRedrive is set even when the count is 0 so the history API emits it.
	RedriveCount int32
	HasRedrive   bool
	// MapRunARN is the mapRunArn of mapRunStartedEventDetails and
	// mapRunRedrivenEventDetails. MapRunRedriven also uses RedriveCount.
	MapRunARN string
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
	// VersionARN and AliasARN are set when StartExecution used a qualified ARN.
	// StateMachineARN stays the unqualified machine ARN.
	VersionARN string
	AliasARN   string
	// RedriveCount is how many times RedriveExecution has resumed this run.
	// JSONPath context objects omit it. JSONata context includes it, including 0.
	RedriveCount int32
	// RedriveDate is the time of the latest accepted redrive. Nil means never.
	RedriveDate *time.Time
	// RedriveToken is the client token of the latest accepted redrive.
	// An empty token matches only while that redrive is still RUNNING.
	RedriveToken string
	// MapRunARN, Label, and ParentMachineARN are set on a Distributed Map
	// child. StateMachineARN is then <parent>/<label>, which is not a machine.
	MapRunARN        string
	Label            string
	ParentMachineARN string
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
	mu               sync.Mutex
	machines         map[string]*Machine
	machineBy        map[string]string
	activities       map[string]*Activity
	activityBy       map[string]string
	tags             map[string][]Tag
	execs            map[string]*execRec
	versions         map[string]*Version
	byMachineVersion map[string][]*Version
	nextVersion      map[string]int
	aliases          map[string]*Alias
	byMachineAlias   map[string][]*Alias
	aliasName        map[string]string
	tokens           map[string]*TaskToken
	tokensByExec     map[string]map[string]*TaskToken
	activityQ        map[string][]*TaskToken
	activityWait     map[string][]activityWaiter
	mapRuns          map[string]*MapRun
}

type execRec struct {
	Execution
	cancel context.CancelFunc
	done   chan struct{}
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
		machines:         map[string]*Machine{},
		machineBy:        map[string]string{},
		activities:       map[string]*Activity{},
		activityBy:       map[string]string{},
		tags:             map[string][]Tag{},
		execs:            map[string]*execRec{},
		versions:         map[string]*Version{},
		byMachineVersion: map[string][]*Version{},
		nextVersion:      map[string]int{},
		aliases:          map[string]*Alias{},
		byMachineAlias:   map[string][]*Alias{},
		aliasName:        map[string]string{},
		tokens:           map[string]*TaskToken{},
		tokensByExec:     map[string]map[string]*TaskToken{},
		activityQ:        map[string][]*TaskToken{},
		activityWait:     map[string][]activityWaiter{},
		mapRuns:          map[string]*MapRun{},
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
	if e.RedriveDate != nil {
		d := *e.RedriveDate
		e.RedriveDate = &d
	}
	return e
}
