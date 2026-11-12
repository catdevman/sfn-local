package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/catdevman/sfn-local/internal/api/gen"
	"github.com/catdevman/sfn-local/internal/asl"
	"github.com/catdevman/sfn-local/internal/exec"
)

const testStateLimit = 5 * time.Minute

// TestState runs one state through the execution handlers and stops before
// the next state. JSONata variables and stateConfiguration are rejected.
// Parallel, Map, and activity states return a modeled error and do not run.
func (s *Service) TestState(ctx context.Context, in *gen.TestStateInput) (*gen.TestStateOutput, error) {
	if in == nil || in.Definition == nil {
		return nil, modeled("ValidationException", "definition is required")
	}
	if in.Variables != nil {
		return nil, modeled("ValidationException", "variables are not implemented by this local server.")
	}
	if in.StateConfiguration != nil {
		return nil, modeled("ValidationException", "stateConfiguration is not implemented by this local server.")
	}
	if in.Context != nil && in.Mock == nil {
		return nil, modeled("ValidationException", "A context can only be specified if a mock is specified.")
	}
	if in.Context != nil {
		return nil, modeled("ValidationException", "context is not implemented by this local server.")
	}
	role := ""
	if in.RoleArn != nil {
		if !RoleARNValid(*in.RoleArn) {
			return nil, modeled("InvalidArn", "Invalid Role Arn: '"+*in.RoleArn+"'")
		}
		role = *in.RoleArn
	}
	input := "{}"
	if in.Input != nil {
		if err := checkExecutionInput(*in.Input); err != nil {
			return nil, err
		}
		input = *in.Input
	}
	stateName := ""
	if in.StateName != nil {
		stateName = *in.StateName
	}
	prog, st, text, err := testProgram(*in.Definition, stateName, in.StateName != nil)
	if err != nil {
		return nil, err
	}
	if st.Type == "Parallel" || st.Type == "Map" {
		return nil, modeled("ValidationException", "State type "+st.Type+" is not implemented by this local server.")
	}
	if activityResource(st.Resource) {
		return nil, modeled("ValidationException", "Activity tasks are not implemented by this local server.")
	}
	if in.Mock != nil && st.Type != "Task" {
		return nil, modeled("ValidationException", "A mock can only be specified for Task, Map, or Parallel states.")
	}
	mock, err := stateMockFrom(in.Mock)
	if err != nil {
		return nil, err
	}
	if s.Runner == nil || s.Runner.Store == nil {
		return nil, errors.New("no runner")
	}
	now := s.now()
	deadline := now.Add(testStateLimit)
	machineLimit := now.Add(time.Duration(prog.TimeoutSeconds) * time.Second)
	if machineLimit.Before(deadline) {
		deadline = machineLimit
	}
	res, err := s.Runner.RunState(ctx, exec.StateRun{
		Definition: text,
		Input:      input,
		RoleArn:    role,
		Region:     s.region(),
		Account:    s.account(),
		State:      st,
		Deadline:   deadline,
		Mock:       mock,
	})
	if err != nil {
		return nil, err
	}
	status := gen.TestExecutionStatus(res.Status)
	if !status.Valid() {
		return nil, fmt.Errorf("test state status %s", res.Status)
	}
	out := &gen.TestStateOutput{Status: &status}
	if res.HasOutput {
		out.Output = sp(res.Output)
	}
	if res.HasError {
		out.Error = sp(res.Error)
	}
	if res.HasCause {
		out.Cause = sp(res.Cause)
	}
	if res.Next != "" {
		out.NextState = sp(res.Next)
	}
	return out, nil
}

func testProgram(definition, stateName string, stateNameSet bool) (*asl.Program, *asl.State, string, error) {
	text := definition
	fields, peekErr := definitionObject(definition)
	if peekErr == nil && bareState(fields) {
		if stateNameSet {
			return nil, nil, "", modeled("InvalidDefinition", "Invalid State Machine Definition: 'stateName requires a state machine definition with StartAt and States'")
		}
		wrapped, err := wrapBareState("TestState", definition)
		if err != nil {
			return nil, nil, "", modeled("InvalidDefinition", err.Error())
		}
		text = wrapped
	}
	prog, err := asl.Validate(text)
	if err != nil {
		return nil, nil, "", modeled("InvalidDefinition", err.Error())
	}
	name := prog.StartAt
	if stateNameSet {
		name = stateName
	}
	st, ok := prog.States[name]
	if !ok {
		return nil, nil, "", modeled("InvalidDefinition", "Invalid State Machine Definition: 'State "+name+" does not exist'")
	}
	return prog, st, text, nil
}

func bareState(fields map[string]json.RawMessage) bool {
	if fields == nil {
		return false
	}
	_, hasStates := fields["States"]
	_, hasStart := fields["StartAt"]
	_, hasType := fields["Type"]
	return hasType && !hasStates && !hasStart
}

func definitionObject(definition string) (map[string]json.RawMessage, error) {
	dec := json.NewDecoder(strings.NewReader(definition))
	dec.UseNumber()
	var fields map[string]json.RawMessage
	if err := dec.Decode(&fields); err != nil {
		return nil, err
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return nil, errors.New("trailing tokens")
	}
	return fields, nil
}

func wrapBareState(name, state string) (string, error) {
	rawName, err := json.Marshal(name)
	if err != nil {
		return "", err
	}
	var buf strings.Builder
	buf.WriteString(`{"StartAt":`)
	buf.Write(rawName)
	buf.WriteString(`,"States":{`)
	buf.Write(rawName)
	buf.WriteByte(':')
	buf.WriteString(state)
	buf.WriteString(`}}`)
	return buf.String(), nil
}

func stateMockFrom(in *gen.MockInput) (*exec.StateMock, error) {
	if in == nil {
		return nil, nil
	}
	hasResult := in.Result != nil
	hasErr := in.ErrorOutput != nil
	if hasResult && hasErr {
		return nil, modeled("ValidationException", "mock result and errorOutput cannot both be set")
	}
	if !hasResult && !hasErr {
		return nil, modeled("ValidationException", "mock requires result or errorOutput")
	}
	m := &exec.StateMock{}
	if hasResult {
		if err := oneJSONValue(*in.Result); err != nil {
			return nil, modeled("ValidationException", "mock result must be JSON")
		}
		m.Result = json.RawMessage(*in.Result)
		m.HasResult = true
	}
	if hasErr {
		if in.ErrorOutput.Error == nil || *in.ErrorOutput.Error == "" {
			return nil, modeled("ValidationException", "mock errorOutput requires error")
		}
		m.HasThrow = true
		m.Error = *in.ErrorOutput.Error
		if in.ErrorOutput.Cause != nil {
			m.Cause = *in.ErrorOutput.Cause
		}
	}
	return m, nil
}

func oneJSONValue(text string) error {
	dec := json.NewDecoder(strings.NewReader(text))
	var v any
	if err := dec.Decode(&v); err != nil {
		return err
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return errors.New("trailing tokens")
	}
	return nil
}

// activityResource matches exec.isActivityARN. Activity tasks are refused
// before a state runs.
func activityResource(resource string) bool {
	parts := strings.Split(resource, ":")
	return len(parts) >= 7 && parts[0] == "arn" && parts[1] == "aws" && parts[2] == "states" && parts[5] == "activity" && parts[6] != ""
}
