package api

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/catdevman/sfn-local/internal/api/gen"
	"github.com/catdevman/sfn-local/internal/asl"
	"github.com/catdevman/sfn-local/internal/mock"
	"github.com/catdevman/sfn-local/internal/store"
)

func (s *Service) StartExecution(ctx context.Context, in *gen.StartExecutionInput) (*gen.StartExecutionOutput, error) {
	ex, handle, err := s.beginExecution(valueOf(in.StateMachineArn), valueOf(in.Name), in.Name != nil, valueOf(in.Input), in.Input != nil, false)
	if err != nil {
		return nil, err
	}
	if handle != nil {
		s.Runner.Start(handle, ex.ARN)
	}
	return &gen.StartExecutionOutput{ExecutionArn: sp(ex.ARN), StartDate: ts(ex.Started)}, nil
}

func (s *Service) StartSyncExecution(ctx context.Context, in *gen.StartSyncExecutionInput) (*gen.StartSyncExecutionOutput, error) {
	arn := valueOf(in.StateMachineArn)
	if strings.Contains(arn, "#") {
		return nil, s.unsupported("Unsupported Operation: 'Service integration mocking is not supported for the StartSyncExecution operation.'")
	}
	ex, handle, err := s.beginExecution(arn, valueOf(in.Name), in.Name != nil, valueOf(in.Input), in.Input != nil, true)
	if err != nil {
		return nil, err
	}
	if handle != nil {
		s.Runner.Start(handle, ex.ARN)
	}
	select {
	case <-s.Store.Finished(ex.ARN):
	case <-ctx.Done():
		s.Store.Cancel(ex.ARN)
		<-s.Store.Finished(ex.ARN)
	}
	finished, ok := s.Store.Execution(ex.ARN)
	if ok && finished.Express {
		s.Store.Drop(finished.ARN)
	}
	if !ok {
		finished = ex
	}
	return syncOutput(finished), nil
}

func (s *Service) DescribeExecution(_ context.Context, in *gen.DescribeExecutionInput) (*gen.DescribeExecutionOutput, error) {
	ex, err := s.standardExecution(valueOf(in.ExecutionArn))
	if err != nil {
		return nil, err
	}
	return describeExecution(ex), nil
}

func (s *Service) DescribeStateMachineForExecution(_ context.Context, in *gen.DescribeStateMachineForExecutionInput) (*gen.DescribeStateMachineForExecutionOutput, error) {
	ex, err := s.standardExecution(valueOf(in.ExecutionArn))
	if err != nil {
		return nil, err
	}
	m, ok := s.Store.Machine(ex.StateMachineARN)
	if !ok {
		return nil, modeled("StateMachineDoesNotExist", "State Machine Does Not Exist: '"+ex.StateMachineARN+"'")
	}
	out := &gen.DescribeStateMachineForExecutionOutput{
		StateMachineArn:      sp(m.ARN),
		Name:                 sp(m.Name),
		Definition:           sp(m.Definition),
		LoggingConfiguration: toGenLogging(m.Logging),
	}
	if m.RoleArn != "" {
		out.RoleArn = sp(m.RoleArn)
	}
	if m.Updated != nil {
		out.UpdateDate = ts(*m.Updated)
	}
	return out, nil
}

func (s *Service) GetExecutionHistory(_ context.Context, in *gen.GetExecutionHistoryInput) (*gen.GetExecutionHistoryOutput, error) {
	ex, err := s.standardExecution(valueOf(in.ExecutionArn))
	if err != nil {
		return nil, err
	}
	include := true
	if in.IncludeExecutionData != nil {
		include = *in.IncludeExecutionData
	}
	events := append([]store.HistoryEvent(nil), ex.History...)
	if in.ReverseOrder != nil && *in.ReverseOrder {
		for i, j := 0, len(events)-1; i < j; i, j = i+1, j-1 {
			events[i], events[j] = events[j], events[i]
		}
	}
	start, end, next, err := pageBounds(len(events), in.MaxResults, in.NextToken)
	if err != nil {
		return nil, err
	}
	list := make(gen.HistoryEventList, 0, end-start)
	for _, ev := range events[start:end] {
		list = append(list, toGenHistory(ev, include))
	}
	return &gen.GetExecutionHistoryOutput{Events: &list, NextToken: next}, nil
}

func (s *Service) ListExecutions(_ context.Context, in *gen.ListExecutionsInput) (*gen.ListExecutionsOutput, error) {
	if in == nil || in.StateMachineArn == nil || *in.StateMachineArn == "" {
		return nil, modeled("ValidationException", "1 validation error detected: Value null at 'stateMachineArn' failed to satisfy constraint: Member must not be null")
	}
	m, err := s.machineByARN(in.StateMachineArn)
	if err != nil {
		return nil, err
	}
	if m.Type == store.TypeExpress {
		return nil, &wireError{
			name:    "InvalidParameterValue",
			shape:   shapePrefix + "InvalidParameterValue",
			message: "This operation is not supported by this type of state machine",
			status:  400,
		}
	}
	rows := s.Store.Executions(m.ARN)
	if in.StatusFilter != nil {
		filtered := rows[:0]
		for _, row := range rows {
			if row.Status == string(*in.StatusFilter) {
				filtered = append(filtered, row)
			}
		}
		rows = filtered
	}
	sort.Slice(rows, func(i, j int) bool {
		if !rows[i].Started.Equal(rows[j].Started) {
			return rows[i].Started.After(rows[j].Started)
		}
		return rows[i].Name < rows[j].Name
	})
	start, end, next, err := pageBounds(len(rows), in.MaxResults, in.NextToken)
	if err != nil {
		return nil, err
	}
	list := make(gen.ExecutionList, 0, end-start)
	for _, row := range rows[start:end] {
		status := gen.ExecutionStatus(row.Status)
		item := gen.ExecutionListItem{
			ExecutionArn:    sp(row.ARN),
			StateMachineArn: sp(row.StateMachineARN),
			Name:            sp(row.Name),
			Status:          &status,
			StartDate:       ts(row.Started),
		}
		if row.Stopped != nil {
			item.StopDate = ts(*row.Stopped)
		}
		list = append(list, item)
	}
	return &gen.ListExecutionsOutput{Executions: &list, NextToken: next}, nil
}

func (s *Service) StopExecution(_ context.Context, in *gen.StopExecutionInput) (*gen.StopExecutionOutput, error) {
	ex, err := s.standardExecution(valueOf(in.ExecutionArn))
	if err != nil {
		return nil, err
	}
	if ex.Status != store.ExecRunning {
		when := ex.Started
		if ex.Stopped != nil {
			when = *ex.Stopped
		}
		return &gen.StopExecutionOutput{StopDate: ts(when)}, nil
	}
	when := s.now()
	ev := store.HistoryEvent{Type: "ExecutionAborted", Time: when}
	hasError := in.Error != nil
	hasCause := in.Cause != nil
	if hasError {
		ev.Error = *in.Error
		ev.HasError = true
	}
	if hasCause {
		ev.Cause = *in.Cause
		ev.HasCause = true
	}
	done, err := s.Store.Complete(ex.ARN, store.ExecAborted, when, ev, "", false, ev.Error, ev.Cause, hasError, hasCause)
	s.Store.Cancel(ex.ARN)
	if err != nil {
		if done.Stopped != nil {
			return &gen.StopExecutionOutput{StopDate: ts(*done.Stopped)}, nil
		}
		return &gen.StopExecutionOutput{StopDate: ts(when)}, nil
	}
	return &gen.StopExecutionOutput{StopDate: ts(*done.Stopped)}, nil
}

func (s *Service) beginExecution(machineARN, name string, nameSet bool, input string, inputSet, sync bool) (store.Execution, *store.RunHandle, error) {
	base := machineARN
	test := ""
	hashed := false
	if !sync {
		var err error
		base, test, err = SplitMockSuffix(machineARN)
		if err != nil {
			return store.Execution{}, nil, modeled("InvalidArn", "Invalid Arn: '"+machineARN+"'")
		}
		hashed = strings.Contains(machineARN, "#")
	}
	if !ValidStateMachineARN(base) {
		return store.Execution{}, nil, modeled("InvalidArn", "Invalid Arn: '"+base+"'")
	}
	m, ok := s.Store.Machine(base)
	if !ok {
		return store.Execution{}, nil, modeled("StateMachineDoesNotExist", "State Machine Does Not Exist: '"+base+"'")
	}
	if sync && m.Type != store.TypeExpress {
		return store.Execution{}, nil, modeled("StateMachineTypeNotSupported", "State Machine Type Not Supported: 'StartSyncExecution is not supported for STANDARD State Machines'")
	}
	if m.Status == store.StatusDeleting {
		return store.Execution{}, nil, modeled("StateMachineDeleting", "State Machine is being deleted: '"+m.ARN+"'")
	}
	if !nameSet {
		name = newExecutionName()
	} else if name == "" || NameRejected(name) {
		return store.Execution{}, nil, modeled("InvalidName", "Invalid Name: '"+name+"'")
	}
	if !inputSet {
		input = "{}"
	} else if err := checkExecutionInput(input); err != nil {
		return store.Execution{}, nil, err
	}
	prog, err := asl.Validate(m.Definition)
	if err != nil {
		return store.Execution{}, nil, modeled("InvalidDefinition", err.Error())
	}
	express := m.Type == store.TypeExpress
	arn := ExecutionARN(s.region(), s.account(), m.Name, name, express)
	if existing, found := s.Store.Execution(arn); found {
		if existing.Input == input && existing.Status == store.ExecRunning {
			return existing, nil, nil
		}
		return store.Execution{}, nil, modeled("ExecutionAlreadyExists", "Execution Already Exists: '"+existing.ARN+"'")
	}
	if hashed {
		var repo *mock.Repo
		if s.Runner != nil {
			repo = s.Runner.Mocks
		}
		if err := repo.Check(m.Name, test); err != nil {
			return store.Execution{}, nil, modeled("InvalidDefinition", err.Error())
		}
	}
	now := s.now()
	ex := store.Execution{
		ARN:             arn,
		Name:            name,
		StateMachineARN: m.ARN,
		Definition:      m.Definition,
		RoleArn:         m.RoleArn,
		Input:           input,
		Status:          store.ExecRunning,
		Type:            m.Type,
		Express:         express,
		Started:         now,
		Deadline:        executionDeadline(now, prog.TimeoutSeconds, express),
		MockTest:        test,
	}
	stored, handle, existed := s.Store.PutExecution(ex)
	if existed {
		if stored.Input == input && stored.Status == store.ExecRunning {
			return stored, nil, nil
		}
		return store.Execution{}, nil, modeled("ExecutionAlreadyExists", "Execution Already Exists: '"+stored.ARN+"'")
	}
	ev := store.HistoryEvent{Type: "ExecutionStarted", Time: now, Input: input, HasInput: true}
	if m.RoleArn != "" {
		ev.RoleArn = m.RoleArn
		ev.HasRole = true
	}
	if err := s.Store.Append(stored.ARN, ev); err != nil {
		s.Store.Drop(stored.ARN)
		handle.Finish()
		return store.Execution{}, nil, err
	}
	return stored, handle, nil
}

func (s *Service) standardExecution(arn string) (store.Execution, error) {
	switch ExecutionARNKind(arn) {
	case "placeholder":
		return store.Execution{}, modeled("InvalidArn", "Invalid Arn: '"+expressPlaceholderMessage+"'")
	case "standard":
		ex, ok := s.Store.Execution(arn)
		if !ok {
			return store.Execution{}, modeled("ExecutionDoesNotExist", "Execution Does Not Exist: '"+arn+"'")
		}
		return ex, nil
	default:
		return store.Execution{}, modeled("InvalidArn", "Invalid Arn: '"+arn+"'")
	}
}

func executionDeadline(now time.Time, timeoutSeconds int64, express bool) time.Time {
	limit := 365 * 24 * time.Hour
	if express {
		limit = 300 * time.Second
	}
	machine := time.Duration(timeoutSeconds) * time.Second
	if machine < limit {
		limit = machine
	}
	return now.Add(limit)
}

func checkExecutionInput(input string) error {
	if len(input) > 262144 {
		return modeled("ValidationException", "1 validation error detected: Value at 'input' failed to satisfy constraint: Member must have length less than or equal to 262144")
	}
	dec := json.NewDecoder(strings.NewReader(input))
	var v any
	if err := dec.Decode(&v); err != nil {
		return modeled("InvalidExecutionInput", "Invalid State Machine Execution Input: '"+err.Error()+"'")
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return modeled("InvalidExecutionInput", "Invalid State Machine Execution Input: 'trailing tokens'")
	}
	return nil
}

func newExecutionName() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

func describeExecution(ex store.Execution) *gen.DescribeExecutionOutput {
	status := gen.ExecutionStatus(ex.Status)
	included := true
	out := &gen.DescribeExecutionOutput{
		ExecutionArn:    sp(ex.ARN),
		StateMachineArn: sp(ex.StateMachineARN),
		Name:            sp(ex.Name),
		Status:          &status,
		StartDate:       ts(ex.Started),
		Input:           sp(ex.Input),
		InputDetails:    &gen.CloudWatchEventsExecutionDataDetails{Included: &included},
	}
	if ex.Stopped != nil {
		out.StopDate = ts(*ex.Stopped)
	}
	if ex.HasOutput {
		out.Output = sp(ex.Output)
		out.OutputDetails = &gen.CloudWatchEventsExecutionDataDetails{Included: &included}
	}
	return out
}

func syncOutput(ex store.Execution) *gen.StartSyncExecutionOutput {
	status := gen.SyncExecutionStatus(ex.Status)
	if ex.Status == store.ExecAborted {
		status = gen.SyncExecutionStatusFAILED
	}
	included := true
	out := &gen.StartSyncExecutionOutput{
		ExecutionArn:    sp(ex.ARN),
		StateMachineArn: sp(ex.StateMachineARN),
		Name:            sp(ex.Name),
		StartDate:       ts(ex.Started),
		Status:          &status,
		Input:           sp(ex.Input),
		InputDetails:    &gen.CloudWatchEventsExecutionDataDetails{Included: &included},
	}
	if ex.Stopped != nil {
		out.StopDate = ts(*ex.Stopped)
	}
	if ex.HasOutput {
		out.Output = sp(ex.Output)
		out.OutputDetails = &gen.CloudWatchEventsExecutionDataDetails{Included: &included}
	}
	if ex.HasError {
		out.Error = sp(ex.Error)
	}
	if ex.HasCause {
		out.Cause = sp(ex.Cause)
	}
	return out
}

func toGenHistory(ev store.HistoryEvent, include bool) gen.HistoryEvent {
	typ := gen.HistoryEventType(ev.Type)
	id := ev.ID
	prev := ev.PreviousID
	out := gen.HistoryEvent{
		Timestamp:       ts(ev.Time),
		Type:            &typ,
		Id:              &id,
		PreviousEventId: &prev,
	}
	switch ev.Type {
	case "ExecutionStarted":
		d := &gen.ExecutionStartedEventDetails{}
		if include && ev.HasInput {
			d.Input = sp(ev.Input)
		}
		if ev.HasRole {
			d.RoleArn = sp(ev.RoleArn)
		}
		out.ExecutionStartedEventDetails = d
	case "ExecutionSucceeded":
		d := &gen.ExecutionSucceededEventDetails{}
		if include && ev.HasOutput {
			d.Output = sp(ev.Output)
		}
		out.ExecutionSucceededEventDetails = d
	case "ExecutionFailed":
		d := &gen.ExecutionFailedEventDetails{}
		if ev.HasError {
			d.Error = sp(ev.Error)
		}
		if ev.HasCause {
			d.Cause = sp(ev.Cause)
		}
		out.ExecutionFailedEventDetails = d
	case "ExecutionAborted":
		d := &gen.ExecutionAbortedEventDetails{}
		if ev.HasError {
			d.Error = sp(ev.Error)
		}
		if ev.HasCause {
			d.Cause = sp(ev.Cause)
		}
		out.ExecutionAbortedEventDetails = d
	case "ExecutionTimedOut":
		d := &gen.ExecutionTimedOutEventDetails{}
		if ev.HasError {
			d.Error = sp(ev.Error)
		}
		if ev.HasCause {
			d.Cause = sp(ev.Cause)
		}
		out.ExecutionTimedOutEventDetails = d
	default:
		if strings.HasSuffix(ev.Type, "StateEntered") {
			d := &gen.StateEnteredEventDetails{Name: sp(ev.Name)}
			if include && ev.HasInput {
				d.Input = sp(ev.Input)
			}
			out.StateEnteredEventDetails = d
		}
		if strings.HasSuffix(ev.Type, "StateExited") {
			d := &gen.StateExitedEventDetails{Name: sp(ev.Name)}
			if include && ev.HasOutput {
				d.Output = sp(ev.Output)
			}
			out.StateExitedEventDetails = d
		}
	case "TaskScheduled":
		d := &gen.TaskScheduledEventDetails{}
		fillTaskResource(d, ev)
		if ev.HasRegion {
			d.Region = sp(ev.Region)
		}
		if include && ev.HasParameters {
			d.Parameters = sp(ev.Parameters)
		}
		if ev.HasTimeout {
			d.TimeoutInSeconds = i64p(ev.TimeoutSeconds)
		}
		if ev.HasHeartbeat {
			d.HeartbeatInSeconds = i64p(ev.HeartbeatSeconds)
		}
		out.TaskScheduledEventDetails = d
	case "TaskStarted":
		d := &gen.TaskStartedEventDetails{}
		if ev.HasResourceType {
			d.ResourceType = sp(ev.ResourceType)
		}
		if ev.HasResource {
			d.Resource = sp(ev.Resource)
		}
		out.TaskStartedEventDetails = d
	case "TaskSucceeded":
		d := &gen.TaskSucceededEventDetails{}
		if ev.HasResourceType {
			d.ResourceType = sp(ev.ResourceType)
		}
		if ev.HasResource {
			d.Resource = sp(ev.Resource)
		}
		if include && ev.HasOutput {
			d.Output = sp(ev.Output)
		}
		out.TaskSucceededEventDetails = d
	case "TaskFailed":
		out.TaskFailedEventDetails = taskFailedDetails(ev)
	case "TaskTimedOut":
		out.TaskTimedOutEventDetails = taskTimedOutDetails(ev)
	case "LambdaFunctionScheduled":
		d := &gen.LambdaFunctionScheduledEventDetails{}
		if ev.HasResource {
			d.Resource = sp(ev.Resource)
		}
		if include && ev.HasInput {
			d.Input = sp(ev.Input)
		}
		if ev.HasTimeout {
			d.TimeoutInSeconds = i64p(ev.TimeoutSeconds)
		}
		out.LambdaFunctionScheduledEventDetails = d
	case "LambdaFunctionSucceeded":
		d := &gen.LambdaFunctionSucceededEventDetails{}
		if include && ev.HasOutput {
			d.Output = sp(ev.Output)
		}
		out.LambdaFunctionSucceededEventDetails = d
	case "LambdaFunctionFailed":
		d := &gen.LambdaFunctionFailedEventDetails{}
		if ev.HasError {
			d.Error = sp(ev.Error)
		}
		if ev.HasCause {
			d.Cause = sp(ev.Cause)
		}
		out.LambdaFunctionFailedEventDetails = d
	case "LambdaFunctionTimedOut":
		d := &gen.LambdaFunctionTimedOutEventDetails{}
		if ev.HasError {
			d.Error = sp(ev.Error)
		}
		if ev.HasCause {
			d.Cause = sp(ev.Cause)
		}
		out.LambdaFunctionTimedOutEventDetails = d
	}
	return out
}

func fillTaskResource(d *gen.TaskScheduledEventDetails, ev store.HistoryEvent) {
	if ev.HasResourceType {
		d.ResourceType = sp(ev.ResourceType)
	}
	if ev.HasResource {
		d.Resource = sp(ev.Resource)
	}
}

func taskFailedDetails(ev store.HistoryEvent) *gen.TaskFailedEventDetails {
	d := &gen.TaskFailedEventDetails{}
	if ev.HasResourceType {
		d.ResourceType = sp(ev.ResourceType)
	}
	if ev.HasResource {
		d.Resource = sp(ev.Resource)
	}
	if ev.HasError {
		d.Error = sp(ev.Error)
	}
	if ev.HasCause {
		d.Cause = sp(ev.Cause)
	}
	return d
}

func taskTimedOutDetails(ev store.HistoryEvent) *gen.TaskTimedOutEventDetails {
	d := &gen.TaskTimedOutEventDetails{}
	if ev.HasResourceType {
		d.ResourceType = sp(ev.ResourceType)
	}
	if ev.HasResource {
		d.Resource = sp(ev.Resource)
	}
	if ev.HasError {
		d.Error = sp(ev.Error)
	}
	if ev.HasCause {
		d.Cause = sp(ev.Cause)
	}
	return d
}

func i64p(n int64) *int64 { return &n }
