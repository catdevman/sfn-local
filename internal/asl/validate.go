// Package asl validates a state machine against the generated field matrix.
// QueryLanguage selects JSONPath or JSONata. Omitted means JSONPath.
// Create-time messages for transitions, States.ALL, registered connectors,
// and EXPRESS resources live here. Logging configuration and tag length stay
// in the API.
package asl

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/catdevman/sfn-local/internal/asl/specgen"
)

const (
	codeSchema        = "SCHEMA_VALIDATION_FAILED"
	codeMissingTarget = "MISSING_TRANSITION_TARGET"
	codeMissingEnd    = "MISSING_END_STATE"
	defaultTimeout    = int64(99999999)
)

// activityARNPattern is the activity resource shape. It does not require Parameters.
var activityARNPattern = regexp.MustCompile(`^arn:aws:states:[a-z0-9-]+:[0-9]{12}:activity:.+$`)

// registeredConnectors are the arn:aws:states::: resources the spec names as
// known to the jar. Suffixes .sync, .sync:2, and .waitForTaskToken use the same base.
var registeredConnectors = map[string]bool{
	"arn:aws:states:::lambda:invoke":         true,
	"arn:aws:states:::sqs:sendMessage":       true,
	"arn:aws:states:::states:startExecution": true,
}

// PathKind is how a path field was written.
type PathKind int

const (
	PathDefault PathKind = iota
	PathIdentity
	PathNull
	PathOther
)

// Path is an InputPath, OutputPath, or ResultPath.
type Path struct {
	Kind PathKind
	Text string
}

// State is one state the runner can read. Nested machines are copied for
// Parallel branches and inline Map.
type State struct {
	Name                 string
	Type                 string
	Next                 string
	End                  bool
	InputPath            Path
	OutputPath           Path
	ResultPath           Path
	HasResult            bool
	Result               json.RawMessage
	HasParameters        bool
	HasAssign            bool
	Assign               json.RawMessage
	HasResultSelector    bool
	Parameters           json.RawMessage
	ResultSelector       json.RawMessage
	JSONata              bool
	QueryLanguage        string
	HasArguments         bool
	Arguments            json.RawMessage
	HasOutput            bool
	Output               json.RawMessage
	HasItems             bool
	Items                json.RawMessage
	SecondsExpr          string
	TimestampExpr        string
	TimeoutExpr          string
	HeartbeatExpr        string
	ErrorExpr            string
	CauseExpr            string
	MaxConcurrencyExpr   string
	Branches             []*Program
	ItemsPath            string
	MaxConcurrency       *int64
	HasItemSelector      bool
	ItemSelector         json.RawMessage
	Processor            *Program
	HasError             bool
	Error                string
	HasCause             bool
	Cause                string
	HasErrorPath         bool
	ErrorPath            string
	HasCausePath         bool
	CausePath            string
	Choices              json.RawMessage
	Default              string
	HasDefault           bool
	Seconds              *int64
	Timestamp            string
	HasTimestamp         bool
	SecondsPath          string
	TimestampPath        string
	Resource             string
	TimeoutSeconds       *int64
	TimeoutSecondsPath   string
	HeartbeatSeconds     *int64
	HeartbeatSecondsPath string
	Retry                []Retrier
	Catch                []Catcher
}

// Retrier is one Retry entry. Omitted numeric fields use the ASL defaults.
type Retrier struct {
	ErrorEquals     []string
	IntervalSeconds int64
	MaxAttempts     int64
	BackoffRate     float64
	MaxDelaySeconds int64
	JitterStrategy  string
}

// Catcher is one Catch entry. ResultPath defaults to $ when omitted.
// JSONata catchers use Output and Assign instead of ResultPath.
type Catcher struct {
	ErrorEquals []string
	Next        string
	ResultPath  Path
	HasAssign   bool
	Assign      json.RawMessage
	HasOutput   bool
	Output      json.RawMessage
}

// Program is a validated definition.
type Program struct {
	StartAt        string
	TimeoutSeconds int64
	QueryLanguage  string
	States         map[string]*State
}

// Validate parses a JSONPath definition. The error text is the InvalidDefinition message.
// EXPRESS resource bans are not applied. Use ValidateType when the machine type is known.
func Validate(definition string) (*Program, error) {
	return validateDefinition(definition, false)
}

// ValidateType is Validate, and for an EXPRESS machine it also rejects activity,
// .sync, and .waitForTaskToken resources.
func ValidateType(definition, machineType string) (*Program, error) {
	return validateDefinition(definition, machineType == "EXPRESS")
}

func validateDefinition(definition string, express bool) (*Program, error) {
	fields, err := decodeObject([]byte(definition))
	if err != nil {
		return nil, defErr(codeSchema, err.Error(), "/")
	}
	return validateMachine(fields, "/", true, express, langJSONPath)
}

func validateMachine(fields map[string]json.RawMessage, ptr string, top, express bool, machineLang string) (*Program, error) {
	if top {
		machineLang = langJSONPath
		if raw, ok := fields["QueryLanguage"]; ok {
			lang, err := readQueryLanguage(raw, join(ptr, "QueryLanguage"))
			if err != nil {
				return nil, err
			}
			machineLang = lang
		}
	}
	if machineLang == "" {
		machineLang = langJSONPath
	}
	allowed := map[string]bool{"Comment": true, "StartAt": true, "States": true}
	if top {
		allowed["TimeoutSeconds"] = true
		allowed["Version"] = true
		allowed["QueryLanguage"] = true
	}
	if err := unknown(fields, allowed, ptr); err != nil {
		return nil, err
	}
	startRaw, ok := fields["StartAt"]
	if !ok {
		return nil, defErr(codeSchema, "Missing required field 'StartAt'", ptr)
	}
	if isNull(startRaw) {
		return nil, defErr(codeMissingTarget, "Missing transition pointer", join(ptr, "StartAt"))
	}
	start, err := asString(startRaw)
	if err != nil || start == "" {
		return nil, defErr(codeSchema, "Field 'StartAt' must be a non-empty string", join(ptr, "StartAt"))
	}
	statesRaw, ok := fields["States"]
	if !ok {
		return nil, defErr(codeSchema, "Missing required field 'States'", ptr)
	}
	states, err := decodeObject(statesRaw)
	if err != nil || len(states) == 0 {
		return nil, defErr(codeSchema, "Field 'States' must be a non-empty object", join(ptr, "States"))
	}
	timeout := defaultTimeout
	if raw, ok := fields["TimeoutSeconds"]; ok {
		n, err := asInt(raw, 0, defaultTimeout)
		if err != nil {
			return nil, defErr(codeSchema, "Field 'TimeoutSeconds' must be an integer in 0..99999999", join(ptr, "TimeoutSeconds"))
		}
		timeout = n
	}
	if raw, ok := fields["Version"]; ok {
		v, err := asString(raw)
		if err != nil || v != "1.0" {
			return nil, defErr(codeSchema, "Field 'Version' must be \"1.0\"", join(ptr, "Version"))
		}
	}
	if raw, ok := fields["Comment"]; ok {
		if _, err := asString(raw); err != nil {
			return nil, defErr(codeSchema, "Field 'Comment' must be a string", join(ptr, "Comment"))
		}
	}
	prog := &Program{StartAt: start, TimeoutSeconds: timeout, QueryLanguage: machineLang, States: map[string]*State{}}
	for name, raw := range states {
		stPtr := join(ptr, "States") + "/" + escape(name)
		obj, err := decodeObject(raw)
		if err != nil {
			return nil, defErr(codeSchema, "Field must be an object", stPtr)
		}
		st, err := validateState(name, obj, stPtr, express, machineLang)
		if err != nil {
			return nil, err
		}
		prog.States[name] = st
	}
	if err := checkGraph(prog, ptr); err != nil {
		return nil, err
	}
	if top {
		if err := checkScopes(prog, nil); err != nil {
			return nil, err
		}
	}
	return prog, nil
}

func validateState(name string, fields map[string]json.RawMessage, ptr string, express bool, machineLang string) (*State, error) {
	typeRaw, ok := fields["Type"]
	if !ok {
		return nil, defErr(codeSchema, "Missing required field 'Type'", ptr)
	}
	typ, err := asString(typeRaw)
	if err != nil {
		return nil, defErr(codeSchema, "Field 'Type' must be a string", join(ptr, "Type"))
	}
	if !knownType(typ) {
		return nil, defErr(codeSchema, "Field 'Type' value '"+typ+"' is not supported", join(ptr, "Type"))
	}
	stateLang := machineLang
	if stateLang == "" {
		stateLang = langJSONPath
	}
	if raw, ok := fields["QueryLanguage"]; ok {
		stateLang, err = readQueryLanguage(raw, join(ptr, "QueryLanguage"))
		if err != nil {
			return nil, err
		}
	}
	jsonata := stateLang == langJSONata
	allowed := allowedFields(typ, jsonata)
	if err := unknown(fields, allowed, ptr); err != nil {
		return nil, err
	}
	matrix := jsonPathFields()
	if jsonata {
		matrix = jsonataFields()
	}
	for _, field := range matrix {
		if field.Name == "Type" {
			continue
		}
		if presence(field, typ) == specgen.PresenceRequired {
			if _, ok := fields[field.Name]; !ok {
				return nil, defErr(codeSchema, "Missing required field '"+field.Name+"'", ptr)
			}
		}
	}
	if err := checkTransition(typ, fields, ptr); err != nil {
		return nil, err
	}
	st := &State{Name: name, Type: typ, JSONata: jsonata, QueryLanguage: stateLang, InputPath: Path{Kind: PathDefault}, OutputPath: Path{Kind: PathDefault}, ResultPath: Path{Kind: PathDefault}}
	if raw, ok := fields["Next"]; ok {
		if isNull(raw) {
			return nil, defErr(codeMissingTarget, "Missing transition pointer", join(ptr, "Next"))
		}
		next, err := asString(raw)
		if err != nil || next == "" {
			return nil, defErr(codeSchema, "Field 'Next' must be a non-empty string", join(ptr, "Next"))
		}
		st.Next = next
	}
	if _, ok := fields["End"]; ok {
		st.End = true
	}
	if err := fillPaths(st, fields, ptr); err != nil {
		return nil, err
	}
	if raw, ok := fields["Result"]; ok {
		st.HasResult = true
		st.Result = append(json.RawMessage(nil), raw...)
	}
	if raw, ok := fields["Parameters"]; ok && !isNull(raw) {
		st.HasParameters = true
		st.Parameters = append(json.RawMessage(nil), raw...)
		if err := checkPayload(raw, join(ptr, "Parameters")); err != nil {
			return nil, err
		}
	}
	if raw, ok := fields["Arguments"]; ok && !isNull(raw) {
		st.HasArguments = true
		st.Arguments = append(json.RawMessage(nil), raw...)
		if err := scanRaw(raw, join(ptr, "Arguments"), false, false); err != nil {
			return nil, err
		}
	}
	if raw, ok := fields["Output"]; ok && !isNull(raw) {
		st.HasOutput = true
		st.Output = append(json.RawMessage(nil), raw...)
		if err := scanRaw(raw, join(ptr, "Output"), allowResult(st), false); err != nil {
			return nil, err
		}
	}
	if raw, ok := fields["Assign"]; ok {
		if err := checkAssign(raw, join(ptr, "Assign"), jsonata, allowResult(st), false); err != nil {
			return nil, err
		}
		st.HasAssign = true
		st.Assign = append(json.RawMessage(nil), raw...)
	}
	if raw, ok := fields["ResultSelector"]; ok {
		st.HasResultSelector = true
		st.ResultSelector = append(json.RawMessage(nil), raw...)
		if err := checkPayload(raw, join(ptr, "ResultSelector")); err != nil {
			return nil, err
		}
	}
	if raw, ok := fields["Error"]; ok {
		text, err := asString(raw)
		if err != nil {
			return nil, defErr(codeSchema, "Field 'Error' must be a string", join(ptr, "Error"))
		}
		if body, ok := fullExpr(text); ok && jsonata {
			if err := scanExpr(body, join(ptr, "Error"), false, false); err != nil {
				return nil, err
			}
			st.ErrorExpr = body
		} else {
			st.HasError = true
			st.Error = text
		}
	}
	if raw, ok := fields["Cause"]; ok {
		text, err := asString(raw)
		if err != nil {
			return nil, defErr(codeSchema, "Field 'Cause' must be a string", join(ptr, "Cause"))
		}
		if body, ok := fullExpr(text); ok && jsonata {
			if err := scanExpr(body, join(ptr, "Cause"), false, false); err != nil {
				return nil, err
			}
			st.CauseExpr = body
		} else {
			st.HasCause = true
			st.Cause = text
		}
	}
	if raw, ok := fields["ErrorPath"]; ok {
		text, err := asString(raw)
		if err != nil {
			return nil, defErr(codeSchema, "Field 'ErrorPath' must be a string", join(ptr, "ErrorPath"))
		}
		st.HasErrorPath = true
		st.ErrorPath = text
	}
	if raw, ok := fields["CausePath"]; ok {
		text, err := asString(raw)
		if err != nil {
			return nil, defErr(codeSchema, "Field 'CausePath' must be a string", join(ptr, "CausePath"))
		}
		st.HasCausePath = true
		st.CausePath = text
	}
	if err := checkTypeShape(st, fields, ptr, express, machineLang); err != nil {
		return nil, err
	}
	if err := captureRuntime(st, fields, ptr); err != nil {
		return nil, err
	}
	if err := checkTaskResource(st, express, ptr); err != nil {
		return nil, err
	}
	return st, nil
}

func captureRuntime(st *State, fields map[string]json.RawMessage, ptr string) error {
	if raw, ok := fields["Choices"]; ok {
		st.Choices = append(json.RawMessage(nil), raw...)
	}
	if raw, ok := fields["Default"]; ok {
		if isNull(raw) {
			return defErr(codeMissingTarget, "Missing transition pointer", join(ptr, "Default"))
		}
		text, err := asString(raw)
		if err != nil {
			return defErr(codeSchema, "Field 'Default' must be a string", join(ptr, "Default"))
		}
		st.Default = text
		st.HasDefault = true
	}
	if raw, ok := fields["Seconds"]; ok {
		if body, ok := expressionBody(raw); ok && st.JSONata {
			if err := scanExpr(body, join(ptr, "Seconds"), false, false); err != nil {
				return err
			}
			st.SecondsExpr = body
		} else {
			n, err := asInt(raw, 0, defaultTimeout)
			if err != nil {
				return defErr(codeSchema, "Field 'Seconds' must be an integer in 0..99999999", join(ptr, "Seconds"))
			}
			st.Seconds = &n
		}
	}
	if raw, ok := fields["Timestamp"]; ok {
		if body, ok := expressionBody(raw); ok && st.JSONata {
			if err := scanExpr(body, join(ptr, "Timestamp"), false, false); err != nil {
				return err
			}
			st.TimestampExpr = body
		} else {
			text, err := asString(raw)
			if err != nil {
				return defErr(codeSchema, "Field 'Timestamp' must be a string", join(ptr, "Timestamp"))
			}
			st.Timestamp = text
			st.HasTimestamp = true
		}
	}
	if raw, ok := fields["SecondsPath"]; ok {
		text, err := asString(raw)
		if err != nil {
			return defErr(codeSchema, "Field 'SecondsPath' must be a string", join(ptr, "SecondsPath"))
		}
		st.SecondsPath = text
	}
	if raw, ok := fields["TimestampPath"]; ok {
		text, err := asString(raw)
		if err != nil {
			return defErr(codeSchema, "Field 'TimestampPath' must be a string", join(ptr, "TimestampPath"))
		}
		st.TimestampPath = text
	}
	if raw, ok := fields["Resource"]; ok {
		text, err := asString(raw)
		if err != nil {
			return defErr(codeSchema, "Field 'Resource' must be a string", join(ptr, "Resource"))
		}
		st.Resource = text
	}
	if raw, ok := fields["TimeoutSeconds"]; ok {
		if body, ok := expressionBody(raw); ok && st.JSONata {
			if err := scanExpr(body, join(ptr, "TimeoutSeconds"), false, false); err != nil {
				return err
			}
			st.TimeoutExpr = body
		} else {
			n, err := asInt(raw, 0, defaultTimeout)
			if err != nil {
				return defErr(codeSchema, "Field 'TimeoutSeconds' must be an integer in 0..99999999", join(ptr, "TimeoutSeconds"))
			}
			st.TimeoutSeconds = &n
		}
	}
	if raw, ok := fields["TimeoutSecondsPath"]; ok {
		text, err := asString(raw)
		if err != nil {
			return defErr(codeSchema, "Field 'TimeoutSecondsPath' must be a string", join(ptr, "TimeoutSecondsPath"))
		}
		st.TimeoutSecondsPath = text
	}
	if raw, ok := fields["HeartbeatSeconds"]; ok {
		if body, ok := expressionBody(raw); ok && st.JSONata {
			if err := scanExpr(body, join(ptr, "HeartbeatSeconds"), false, false); err != nil {
				return err
			}
			st.HeartbeatExpr = body
		} else {
			n, err := asInt(raw, 0, defaultTimeout)
			if err != nil {
				return defErr(codeSchema, "Field 'HeartbeatSeconds' must be an integer in 0..99999999", join(ptr, "HeartbeatSeconds"))
			}
			st.HeartbeatSeconds = &n
		}
	}
	if raw, ok := fields["HeartbeatSecondsPath"]; ok {
		text, err := asString(raw)
		if err != nil {
			return defErr(codeSchema, "Field 'HeartbeatSecondsPath' must be a string", join(ptr, "HeartbeatSecondsPath"))
		}
		st.HeartbeatSecondsPath = text
	}
	if st.HeartbeatSeconds != nil && st.TimeoutSeconds != nil && *st.HeartbeatSeconds >= *st.TimeoutSeconds {
		return defErr(codeSchema, "HeartbeatSeconds must be less than TimeoutSeconds", join(ptr, "HeartbeatSeconds"))
	}
	if raw, ok := fields["Retry"]; ok {
		list, err := parseRetry(raw, join(ptr, "Retry"))
		if err != nil {
			return err
		}
		st.Retry = list
	}
	if raw, ok := fields["Catch"]; ok {
		list, err := parseCatch(raw, join(ptr, "Catch"), st.JSONata)
		if err != nil {
			return err
		}
		st.Catch = list
	}
	return nil
}

func checkPayload(raw json.RawMessage, ptr string) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return defErr(codeSchema, "Field must be JSON", ptr)
	}
	if _, err := dec.Token(); err != io.EOF {
		return defErr(codeSchema, "trailing tokens", ptr)
	}
	return checkTemplate(v, ptr)
}

func checkTemplate(v any, ptr string) error {
	switch node := v.(type) {
	case map[string]any:
		for key, child := range node {
			if !strings.HasSuffix(key, ".$") {
				continue
			}
			base := strings.TrimSuffix(key, ".$")
			if _, ok := node[base]; ok {
				return defErr(codeSchema, "Fields '"+base+"' and '"+key+"' are mutually exclusive", ptr)
			}
			if _, ok := child.(string); !ok {
				return defErr(codeSchema, "Field '"+key+"' must be a string", ptr)
			}
		}
		for key, child := range node {
			if strings.HasSuffix(key, ".$") {
				continue
			}
			if err := checkTemplate(child, join(ptr, key)); err != nil {
				return err
			}
		}
	case []any:
		for i, child := range node {
			if err := checkTemplate(child, fmt.Sprintf("%s[%d]", ptr, i)); err != nil {
				return err
			}
		}
	}
	return nil
}

func fillPaths(st *State, fields map[string]json.RawMessage, ptr string) error {
	var err error
	if raw, ok := fields["InputPath"]; ok {
		st.InputPath, err = parsePath(raw, join(ptr, "InputPath"))
		if err != nil {
			return err
		}
	}
	if raw, ok := fields["OutputPath"]; ok {
		st.OutputPath, err = parsePath(raw, join(ptr, "OutputPath"))
		if err != nil {
			return err
		}
	}
	if raw, ok := fields["ResultPath"]; ok {
		st.ResultPath, err = parsePath(raw, join(ptr, "ResultPath"))
		if err != nil {
			return err
		}
		if err := resultPathVariable(st.ResultPath, join(ptr, "ResultPath")); err != nil {
			return err
		}
	}
	return nil
}

func parsePath(raw json.RawMessage, ptr string) (Path, error) {
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return Path{Kind: PathNull}, nil
	}
	text, err := asString(raw)
	if err != nil {
		return Path{}, defErr(codeSchema, "Field must be a string or null", ptr)
	}
	if text == "$" {
		return Path{Kind: PathIdentity, Text: text}, nil
	}
	return Path{Kind: PathOther, Text: text}, nil
}

func checkTransition(typ string, fields map[string]json.RawMessage, ptr string) error {
	note := transitionNote(typ)
	_, hasNext := fields["Next"]
	endRaw, hasEnd := fields["End"]
	switch note {
	case specgen.PresenceAbsent:
		if hasNext {
			return defErr(codeSchema, "Field 'Next' is not supported", join(ptr, "Next"))
		}
		if hasEnd {
			return defErr(codeSchema, "Field 'End' is not supported", join(ptr, "End"))
		}
	case specgen.PresenceRequired:
		if hasNext == hasEnd {
			return defErr(codeSchema, "Exactly one of Next or End is required", ptr)
		}
		if hasEnd {
			var v bool
			if err := json.Unmarshal(endRaw, &v); err != nil || !v {
				return defErr(codeSchema, "Field 'End' must be true", join(ptr, "End"))
			}
		}
	}
	return nil
}

func checkTypeShape(st *State, fields map[string]json.RawMessage, ptr string, express bool, machineLang string) error {
	switch st.Type {
	case "Task":
		raw, ok := fields["Resource"]
		if !ok {
			return defErr(codeSchema, "Missing required field 'Resource'", ptr)
		}
		if _, err := asString(raw); err != nil {
			return defErr(codeSchema, "Field 'Resource' must be a string", join(ptr, "Resource"))
		}
		if err := atMostOne(fields, "TimeoutSeconds", "TimeoutSecondsPath", ptr); err != nil {
			return err
		}
		if err := atMostOne(fields, "HeartbeatSeconds", "HeartbeatSecondsPath", ptr); err != nil {
			return err
		}
	case "Wait":
		n := 0
		for _, key := range []string{"Seconds", "Timestamp", "SecondsPath", "TimestampPath"} {
			if _, ok := fields[key]; ok {
				n++
			}
		}
		if n != 1 {
			return defErr(codeSchema, "Exactly one of Seconds, Timestamp, SecondsPath, or TimestampPath is required", ptr)
		}
		if raw, ok := fields["Seconds"]; ok {
			if _, expr := expressionBody(raw); !(expr && st.JSONata) {
				if _, err := asInt(raw, 0, defaultTimeout); err != nil {
					return defErr(codeSchema, "Field 'Seconds' must be an integer in 0..99999999", join(ptr, "Seconds"))
				}
			}
		}
	case "Choice":
		raw, ok := fields["Choices"]
		if !ok {
			return defErr(codeSchema, "Missing required field 'Choices'", ptr)
		}
		if err := checkChoices(raw, join(ptr, "Choices"), st.JSONata); err != nil {
			return err
		}
		if raw, ok := fields["Default"]; ok && !isNull(raw) {
			text, err := asString(raw)
			if err != nil {
				return defErr(codeSchema, "Field 'Default' must be a string", join(ptr, "Default"))
			}
			if text == "" {
				return defErr(codeSchema, "Field 'Default' must be a non-empty string", join(ptr, "Default"))
			}
		}
	case "Fail":
		if err := atMostOne(fields, "Error", "ErrorPath", ptr); err != nil {
			return err
		}
		if err := atMostOne(fields, "Cause", "CausePath", ptr); err != nil {
			return err
		}
	case "Parallel":
		raw, ok := fields["Branches"]
		if !ok {
			return defErr(codeSchema, "Missing required field 'Branches'", ptr)
		}
		branches, err := checkBranches(raw, join(ptr, "Branches"), express, machineLang)
		if err != nil {
			return err
		}
		st.Branches = branches
	case "Map":
		if err := atMostOne(fields, "Parameters", "ItemSelector", ptr); err != nil {
			return err
		}
		_, it := fields["Iterator"]
		_, proc := fields["ItemProcessor"]
		if it == proc {
			return defErr(codeSchema, "Exactly one of Iterator or ItemProcessor is required", ptr)
		}
		if it {
			prog, err := checkSubmachine(fields["Iterator"], join(ptr, "Iterator"), express, machineLang)
			if err != nil {
				return err
			}
			st.Processor = prog
		}
		if proc {
			prog, err := checkProcessor(fields["ItemProcessor"], join(ptr, "ItemProcessor"), express, machineLang)
			if err != nil {
				return err
			}
			st.Processor = prog
		}
		if !st.JSONata {
			st.ItemsPath = "$"
			if raw, ok := fields["ItemsPath"]; ok {
				text, err := asString(raw)
				if err != nil || text == "" {
					return defErr(codeSchema, "Field 'ItemsPath' must be a non-empty string", join(ptr, "ItemsPath"))
				}
				st.ItemsPath = text
			}
		}
		if raw, ok := fields["Items"]; ok {
			if err := checkItems(raw, join(ptr, "Items")); err != nil {
				return err
			}
			st.HasItems = true
			st.Items = append(json.RawMessage(nil), raw...)
		}
		if raw, ok := fields["MaxConcurrency"]; ok {
			if body, ok := expressionBody(raw); ok && st.JSONata {
				if err := scanExpr(body, join(ptr, "MaxConcurrency"), false, false); err != nil {
					return err
				}
				st.MaxConcurrencyExpr = body
			} else {
				n, err := asInt(raw, 0, int64(^uint64(0)>>1))
				if err != nil {
					return defErr(codeSchema, "Field 'MaxConcurrency' must be an integer >= 0", join(ptr, "MaxConcurrency"))
				}
				st.MaxConcurrency = &n
			}
		}
		if raw, ok := fields["ItemSelector"]; ok && !isNull(raw) {
			st.HasItemSelector = true
			st.ItemSelector = append(json.RawMessage(nil), raw...)
			if st.JSONata {
				if err := checkItemSelectorJSONata(raw, join(ptr, "ItemSelector")); err != nil {
					return err
				}
			} else if err := checkPayload(raw, join(ptr, "ItemSelector")); err != nil {
				return err
			}
		}
	}
	if raw, ok := fields["Retry"]; ok {
		if err := checkRetry(raw, join(ptr, "Retry")); err != nil {
			return err
		}
	}
	if raw, ok := fields["Catch"]; ok {
		if err := checkCatch(raw, join(ptr, "Catch"), st.JSONata); err != nil {
			return err
		}
	}
	return nil
}

func checkBranches(raw json.RawMessage, ptr string, express bool, machineLang string) ([]*Program, error) {
	arr, err := decodeArray(raw)
	if err != nil || len(arr) == 0 {
		return nil, defErr(codeSchema, "Field 'Branches' must be a non-empty array", ptr)
	}
	out := make([]*Program, 0, len(arr))
	for i, item := range arr {
		prog, err := checkSubmachine(item, fmt.Sprintf("%s[%d]", ptr, i), express, machineLang)
		if err != nil {
			return nil, err
		}
		out = append(out, prog)
	}
	return out, nil
}

func checkSubmachine(raw json.RawMessage, ptr string, express bool, machineLang string) (*Program, error) {
	fields, err := decodeObject(raw)
	if err != nil {
		return nil, defErr(codeSchema, "Field must be an object", ptr)
	}
	// Branches and iterators only allow Comment, StartAt, and States.
	return validateMachine(fields, ptr, false, express, machineLang)
}

func checkProcessor(raw json.RawMessage, ptr string, express bool, machineLang string) (*Program, error) {
	fields, err := decodeObject(raw)
	if err != nil {
		return nil, defErr(codeSchema, "Field must be an object", ptr)
	}
	allowed := map[string]bool{"Comment": true, "StartAt": true, "States": true, "ProcessorConfig": true, "Retry": true}
	if err := unknown(fields, allowed, ptr); err != nil {
		return nil, err
	}
	if raw, ok := fields["ProcessorConfig"]; ok {
		cfg, err := decodeObject(raw)
		if err != nil {
			return nil, defErr(codeSchema, "Field 'ProcessorConfig' must be an object", join(ptr, "ProcessorConfig"))
		}
		if err := unknown(cfg, map[string]bool{"Mode": true}, join(ptr, "ProcessorConfig")); err != nil {
			return nil, err
		}
		mode, err := asString(cfg["Mode"])
		if err != nil || mode != "INLINE" {
			text := ""
			if err == nil {
				text = mode
			}
			return nil, defErr(codeSchema, "Field 'Mode' value '"+text+"' is not supported", join(ptr, "ProcessorConfig")+"/Mode")
		}
	}
	// Validate StartAt and States by reusing the branch rules. Drop the extra keys first.
	base := map[string]json.RawMessage{}
	for _, key := range []string{"Comment", "StartAt", "States"} {
		if raw, ok := fields[key]; ok {
			base[key] = raw
		}
	}
	prog, err := validateMachine(base, ptr, false, express, machineLang)
	if err != nil {
		return nil, err
	}
	if raw, ok := fields["Retry"]; ok {
		if err := checkRetry(raw, join(ptr, "Retry")); err != nil {
			return nil, err
		}
	}
	return prog, nil
}

func checkRetry(raw json.RawMessage, ptr string) error {
	_, err := parseRetry(raw, ptr)
	return err
}

func parseRetry(raw json.RawMessage, ptr string) ([]Retrier, error) {
	arr, err := decodeArray(raw)
	if err != nil {
		return nil, defErr(codeSchema, "Field 'Retry' must be an array", ptr)
	}
	allowed := map[string]bool{
		"ErrorEquals": true, "IntervalSeconds": true, "MaxAttempts": true,
		"BackoffRate": true, "MaxDelaySeconds": true, "JitterStrategy": true, "Comment": true,
	}
	out := make([]Retrier, 0, len(arr))
	for i, item := range arr {
		ip := fmt.Sprintf("%s[%d]", ptr, i)
		obj, err := decodeObject(item)
		if err != nil {
			return nil, defErr(codeSchema, "Field must be an object", ip)
		}
		if err := unknown(obj, allowed, ip); err != nil {
			return nil, err
		}
		if _, ok := obj["ErrorEquals"]; !ok {
			return nil, defErr(codeSchema, "Missing required field 'ErrorEquals'", ip)
		}
		names, err := stringList(obj["ErrorEquals"], join(ip, "ErrorEquals"))
		if err != nil {
			return nil, err
		}
		rt := Retrier{ErrorEquals: names, IntervalSeconds: 1, MaxAttempts: 3, BackoffRate: 2}
		if raw, ok := obj["IntervalSeconds"]; ok {
			n, err := asInt(raw, 0, defaultTimeout)
			if err != nil {
				return nil, defErr(codeSchema, "Field 'IntervalSeconds' must be an integer in 0..99999999", join(ip, "IntervalSeconds"))
			}
			rt.IntervalSeconds = n
		}
		if raw, ok := obj["MaxAttempts"]; ok {
			n, err := asInt(raw, 0, defaultTimeout)
			if err != nil {
				return nil, defErr(codeSchema, "Field 'MaxAttempts' must be an integer in 0..99999999", join(ip, "MaxAttempts"))
			}
			rt.MaxAttempts = n
		}
		if raw, ok := obj["BackoffRate"]; ok {
			n, err := asFloat(raw)
			if err != nil || math.IsNaN(n) || math.IsInf(n, 0) || n < 1 {
				return nil, defErr(codeSchema, "Field 'BackoffRate' must be a number >= 1", join(ip, "BackoffRate"))
			}
			rt.BackoffRate = n
		}
		if raw, ok := obj["MaxDelaySeconds"]; ok {
			n, err := asInt(raw, 1, 31622400)
			if err != nil {
				return nil, defErr(codeSchema, "Field 'MaxDelaySeconds' must be an integer in 1..31622400", join(ip, "MaxDelaySeconds"))
			}
			rt.MaxDelaySeconds = n
		}
		if raw, ok := obj["JitterStrategy"]; ok {
			v, err := asString(raw)
			if err != nil || (v != "NONE" && v != "FULL") {
				return nil, defErr(codeSchema, "Field 'JitterStrategy' must be NONE or FULL", join(ip, "JitterStrategy"))
			}
			rt.JitterStrategy = v
		}
		out = append(out, rt)
	}
	groups := make([][]string, len(out))
	for i, rt := range out {
		groups[i] = rt.ErrorEquals
	}
	if err := statesAllPlacement(groups, ptr); err != nil {
		return nil, err
	}
	return out, nil
}

func checkCatch(raw json.RawMessage, ptr string, jsonata bool) error {
	_, err := parseCatch(raw, ptr, jsonata)
	return err
}

func parseCatch(raw json.RawMessage, ptr string, jsonata bool) ([]Catcher, error) {
	arr, err := decodeArray(raw)
	if err != nil {
		return nil, defErr(codeSchema, "Field 'Catch' must be an array", ptr)
	}
	allowed := map[string]bool{"ErrorEquals": true, "Next": true, "Comment": true, "Assign": true}
	if jsonata {
		allowed["Output"] = true
	} else {
		allowed["ResultPath"] = true
	}
	out := make([]Catcher, 0, len(arr))
	for i, item := range arr {
		ip := fmt.Sprintf("%s[%d]", ptr, i)
		obj, err := decodeObject(item)
		if err != nil {
			return nil, defErr(codeSchema, "Field must be an object", ip)
		}
		if err := unknown(obj, allowed, ip); err != nil {
			return nil, err
		}
		if _, ok := obj["ErrorEquals"]; !ok {
			return nil, defErr(codeSchema, "Missing required field 'ErrorEquals'", ip)
		}
		names, err := stringList(obj["ErrorEquals"], join(ip, "ErrorEquals"))
		if err != nil {
			return nil, err
		}
		nextRaw, ok := obj["Next"]
		if !ok {
			return nil, defErr(codeSchema, "Missing required field 'Next'", ip)
		}
		if isNull(nextRaw) {
			return nil, defErr(codeMissingTarget, "Missing transition pointer", join(ip, "Next"))
		}
		next, err := asString(nextRaw)
		if err != nil {
			return nil, defErr(codeSchema, "Field 'Next' must be a string", join(ip, "Next"))
		}
		if next == "" {
			return nil, defErr(codeSchema, "Field 'Next' must be a non-empty string", join(ip, "Next"))
		}
		c := Catcher{ErrorEquals: names, Next: next, ResultPath: Path{Kind: PathDefault}}
		if raw, ok := obj["ResultPath"]; ok {
			path, err := parsePath(raw, join(ip, "ResultPath"))
			if err != nil {
				return nil, err
			}
			if err := resultPathVariable(path, join(ip, "ResultPath")); err != nil {
				return nil, err
			}
			c.ResultPath = path
		}
		if raw, ok := obj["Output"]; ok && !isNull(raw) {
			if err := scanRaw(raw, join(ip, "Output"), false, true); err != nil {
				return nil, err
			}
			c.HasOutput = true
			c.Output = append(json.RawMessage(nil), raw...)
		}
		if raw, ok := obj["Assign"]; ok {
			if err := checkAssign(raw, join(ip, "Assign"), jsonata, false, true); err != nil {
				return nil, err
			}
			c.HasAssign = true
			c.Assign = append(json.RawMessage(nil), raw...)
		}
		out = append(out, c)
	}
	groups := make([][]string, len(out))
	for i, c := range out {
		groups[i] = c.ErrorEquals
	}
	if err := statesAllPlacement(groups, ptr); err != nil {
		return nil, err
	}
	return out, nil
}

func checkChoices(raw json.RawMessage, ptr string, jsonata bool) error {
	arr, err := decodeArray(raw)
	if err != nil || len(arr) == 0 {
		return defErr(codeSchema, "Field 'Choices' must be a non-empty array", ptr)
	}
	for i, item := range arr {
		if err := checkRule(item, fmt.Sprintf("%s[%d]", ptr, i), true, jsonata); err != nil {
			return err
		}
	}
	return nil
}

func checkRule(raw json.RawMessage, ptr string, top, jsonata bool) error {
	obj, err := decodeObject(raw)
	if err != nil {
		return defErr(codeSchema, "Field must be an object", ptr)
	}
	if jsonata {
		return checkJSONataRule(obj, ptr, top)
	}
	allowed := map[string]bool{"Comment": true, "Variable": true, "Next": true, "And": true, "Or": true, "Not": true, "Assign": true}
	for name := range comparatorNames {
		allowed[name] = true
	}
	if err := unknown(obj, allowed, ptr); err != nil {
		return err
	}
	if !top {
		if _, ok := obj["Next"]; ok {
			return defErr(codeSchema, "Field 'Next' is not supported", join(ptr, "Next"))
		}
	}
	forms := 0
	if _, ok := obj["And"]; ok {
		forms++
	}
	if _, ok := obj["Or"]; ok {
		forms++
	}
	if _, ok := obj["Not"]; ok {
		forms++
	}
	comps := 0
	for name := range comparatorNames {
		if _, ok := obj[name]; ok {
			comps++
		}
	}
	if comps > 0 {
		forms++
	}
	if forms != 1 {
		return defErr(codeSchema, "A Choice rule needs exactly one comparator, And, Or, or Not", ptr)
	}
	if comps == 1 {
		if _, ok := obj["Variable"]; !ok {
			return defErr(codeSchema, "Missing required field 'Variable'", ptr)
		}
		if _, err := asString(obj["Variable"]); err != nil {
			return defErr(codeSchema, "Field 'Variable' must be a string", join(ptr, "Variable"))
		}
	}
	if top {
		raw, ok := obj["Next"]
		if !ok {
			return defErr(codeSchema, "Missing required field 'Next'", ptr)
		}
		if isNull(raw) {
			return defErr(codeMissingTarget, "Missing transition pointer", join(ptr, "Next"))
		}
		text, err := asString(raw)
		if err != nil {
			return defErr(codeSchema, "Field 'Next' must be a string", join(ptr, "Next"))
		}
		if text == "" {
			return defErr(codeSchema, "Field 'Next' must be a non-empty string", join(ptr, "Next"))
		}
	}
	if raw, ok := obj["Assign"]; ok {
		if !top {
			return defErr(codeSchema, "Field 'Assign' is not supported", join(ptr, "Assign"))
		}
		if err := checkAssign(raw, join(ptr, "Assign"), false, false, false); err != nil {
			return err
		}
	}
	if raw, ok := obj["And"]; ok {
		if err := checkRuleArray(raw, join(ptr, "And"), false); err != nil {
			return err
		}
	}
	if raw, ok := obj["Or"]; ok {
		if err := checkRuleArray(raw, join(ptr, "Or"), false); err != nil {
			return err
		}
	}
	if raw, ok := obj["Not"]; ok {
		if err := checkRule(raw, join(ptr, "Not"), false, false); err != nil {
			return err
		}
	}
	return nil
}

func checkJSONataRule(obj map[string]json.RawMessage, ptr string, top bool) error {
	allowed := map[string]bool{"Comment": true, "Condition": true, "Next": true, "Assign": true}
	if err := unknown(obj, allowed, ptr); err != nil {
		return err
	}
	raw, ok := obj["Condition"]
	if !ok {
		return defErr(codeSchema, "Missing required field 'Condition'", ptr)
	}
	body, ok := expressionBody(raw)
	if !ok {
		return defErr(codeSchema, "Field 'Condition' must be a JSONata expression", join(ptr, "Condition"))
	}
	if err := scanExpr(body, join(ptr, "Condition"), false, false); err != nil {
		return err
	}
	if !top {
		if _, ok := obj["Next"]; ok {
			return defErr(codeSchema, "Field 'Next' is not supported", join(ptr, "Next"))
		}
		if _, ok := obj["Assign"]; ok {
			return defErr(codeSchema, "Field 'Assign' is not supported", join(ptr, "Assign"))
		}
	}
	if top {
		next, ok := obj["Next"]
		if !ok {
			return defErr(codeSchema, "Missing required field 'Next'", ptr)
		}
		if isNull(next) {
			return defErr(codeMissingTarget, "Missing transition pointer", join(ptr, "Next"))
		}
		text, err := asString(next)
		if err != nil || text == "" {
			return defErr(codeSchema, "Field 'Next' must be a non-empty string", join(ptr, "Next"))
		}
		if raw, ok := obj["Assign"]; ok {
			if err := checkAssign(raw, join(ptr, "Assign"), true, false, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func checkRuleArray(raw json.RawMessage, ptr string, jsonata bool) error {
	arr, err := decodeArray(raw)
	if err != nil || len(arr) == 0 {
		return defErr(codeSchema, "Field must be a non-empty array", ptr)
	}
	for i, item := range arr {
		if err := checkRule(item, fmt.Sprintf("%s[%d]", ptr, i), false, jsonata); err != nil {
			return err
		}
	}
	return nil
}

func atMostOne(fields map[string]json.RawMessage, a, b, ptr string) error {
	_, ha := fields[a]
	_, hb := fields[b]
	if ha && hb {
		return defErr(codeSchema, "Fields '"+a+"' and '"+b+"' are mutually exclusive", ptr)
	}
	return nil
}

func unknown(fields map[string]json.RawMessage, allowed map[string]bool, ptr string) error {
	for name := range fields {
		if !allowed[name] {
			return defErr(codeSchema, "Field '"+name+"' is not supported", join(ptr, name))
		}
	}
	return nil
}

func knownType(typ string) bool {
	switch typ {
	case "Pass", "Task", "Choice", "Wait", "Succeed", "Fail", "Parallel", "Map":
		return true
	default:
		return false
	}
}

func allowedFields(typ string, jsonata bool) map[string]bool {
	out := map[string]bool{}
	fields := jsonPathFields()
	extras := extraFields[typ]
	if jsonata {
		fields = jsonataFields()
		extras = jsonataExtra[typ]
	}
	for _, field := range fields {
		if presence(field, typ) != specgen.PresenceAbsent {
			out[field.Name] = true
		}
	}
	for _, name := range extras {
		out[name] = true
	}
	out["QueryLanguage"] = true
	return out
}

func jsonataFields() []specgen.Field {
	for _, lang := range specgen.Languages {
		if lang.Name == "jsonata" {
			return lang.Fields
		}
	}
	return nil
}

var jsonataExtra = map[string][]string{
	"Pass":     {"Next", "End"},
	"Task":     {"Resource", "TimeoutSeconds", "HeartbeatSeconds", "Next", "End"},
	"Wait":     {"Seconds", "Timestamp", "Next", "End"},
	"Choice":   {"Choices", "Default"},
	"Succeed":  {},
	"Fail":     {"Error", "Cause"},
	"Parallel": {"Branches", "Next", "End"},
	"Map":      {"MaxConcurrency", "ItemSelector", "Iterator", "ItemProcessor", "Items", "Next", "End"},
}

var extraFields = map[string][]string{
	"Pass":     {"Result", "Next", "End"},
	"Task":     {"Resource", "TimeoutSeconds", "TimeoutSecondsPath", "HeartbeatSeconds", "HeartbeatSecondsPath", "Next", "End"},
	"Wait":     {"Seconds", "Timestamp", "SecondsPath", "TimestampPath", "Next", "End"},
	"Choice":   {"Choices", "Default"},
	"Succeed":  {},
	"Fail":     {"Error", "ErrorPath", "Cause", "CausePath"},
	"Parallel": {"Branches", "Next", "End"},
	"Map":      {"ItemsPath", "MaxConcurrency", "ItemSelector", "Iterator", "ItemProcessor", "Next", "End"},
}

func jsonPathFields() []specgen.Field {
	for _, lang := range specgen.Languages {
		if lang.Name == "jsonpath" {
			return lang.Fields
		}
	}
	return nil
}

func presence(field specgen.Field, typ string) specgen.Presence {
	switch typ {
	case "Task":
		return field.States.Task
	case "Parallel":
		return field.States.Parallel
	case "Map":
		return field.States.Map
	case "Pass":
		return field.States.Pass
	case "Wait":
		return field.States.Wait
	case "Choice":
		return field.States.Choice
	case "Succeed":
		return field.States.Succeed
	case "Fail":
		return field.States.Fail
	default:
		return specgen.PresenceAbsent
	}
}

func transitionNote(typ string) specgen.Presence {
	for _, lang := range specgen.Languages {
		if lang.Name != "jsonpath" {
			continue
		}
		for _, note := range lang.Notes {
			return presence(specgen.Field{States: note.States}, typ)
		}
	}
	return specgen.PresenceAbsent
}

func isNull(raw json.RawMessage) bool {
	return bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
}

func checkTaskResource(st *State, express bool, ptr string) error {
	if st == nil || st.Type != "Task" || st.Resource == "" {
		return nil
	}
	if express {
		switch {
		case activityARNPattern.MatchString(st.Resource):
			return defErr(codeSchema, "Express state machine does not support Activity ARN", join(ptr, "Resource"))
		case strings.HasSuffix(st.Resource, ".waitForTaskToken"):
			return defErr(codeSchema, "Express state machine does not support '.waitForTaskToken' service integration ", join(ptr, "Resource"))
		case strings.HasSuffix(st.Resource, ".sync") || strings.HasSuffix(st.Resource, ".sync:2"):
			return defErr(codeSchema, "Express state machine does not support '.sync' service integration", join(ptr, "Resource"))
		}
	}
	if registeredConnectors[connectorBase(st.Resource)] {
		if st.JSONata {
			if !st.HasArguments {
				return defErr(codeSchema, "Arguments field is required for resource ARN: "+st.Resource, join(ptr, "Resource"))
			}
		} else if !st.HasParameters {
			return defErr(codeSchema, "Parameters field is required for resource ARN: "+st.Resource, join(ptr, "Resource"))
		}
	}
	return nil
}

func connectorBase(resource string) string {
	switch {
	case strings.HasSuffix(resource, ".waitForTaskToken"):
		return strings.TrimSuffix(resource, ".waitForTaskToken")
	case strings.HasSuffix(resource, ".sync:2"):
		return strings.TrimSuffix(resource, ".sync:2")
	case strings.HasSuffix(resource, ".sync"):
		return strings.TrimSuffix(resource, ".sync")
	default:
		return resource
	}
}

type transitionEdge struct {
	name string
	ptr  string
}

func checkGraph(prog *Program, ptr string) error {
	if _, ok := prog.States[prog.StartAt]; !ok {
		return defErr(codeMissingTarget, "Missing 'Next' target: "+prog.StartAt, join(ptr, "StartAt"))
	}
	names := make([]string, 0, len(prog.States))
	for name := range prog.States {
		names = append(names, name)
	}
	sort.Strings(names)
	edgesByState := make(map[string][]transitionEdge, len(names))
	for _, name := range names {
		st := prog.States[name]
		stPtr := join(ptr, "States") + "/" + escape(name)
		edges, err := stateEdges(st, stPtr)
		if err != nil {
			return err
		}
		edgesByState[name] = edges
		for _, edge := range edges {
			if _, ok := prog.States[edge.name]; !ok {
				return defErr(codeMissingTarget, "Missing 'Next' target: "+edge.name, edge.ptr)
			}
		}
	}
	seen := map[string]bool{prog.StartAt: true}
	queue := []string{prog.StartAt}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, edge := range edgesByState[cur] {
			if seen[edge.name] {
				continue
			}
			seen[edge.name] = true
			queue = append(queue, edge.name)
		}
	}
	terminal := false
	for _, st := range prog.States {
		if st.Type == "Succeed" || st.Type == "Fail" || st.End {
			terminal = true
			break
		}
	}
	if !terminal {
		return defErr(codeMissingEnd, "Workflow has no terminal state", ptr)
	}
	for _, name := range names {
		if seen[name] {
			continue
		}
		return defErr(codeMissingTarget, `State "`+name+`" is not reachable.`, join(ptr, "States")+"/"+escape(name))
	}
	return nil
}

func stateEdges(st *State, stPtr string) ([]transitionEdge, error) {
	var out []transitionEdge
	if st.Next != "" {
		out = append(out, transitionEdge{name: st.Next, ptr: join(stPtr, "Next")})
	}
	if st.Type == "Choice" {
		nexts, err := choiceNexts(st.Choices)
		if err != nil {
			return nil, err
		}
		for i, next := range nexts {
			out = append(out, transitionEdge{name: next, ptr: fmt.Sprintf("%s/Choices[%d]/Next", stPtr, i)})
		}
		if st.HasDefault {
			out = append(out, transitionEdge{name: st.Default, ptr: join(stPtr, "Default")})
		}
	}
	for i, catcher := range st.Catch {
		out = append(out, transitionEdge{name: catcher.Next, ptr: fmt.Sprintf("%s/Catch[%d]/Next", stPtr, i)})
	}
	return out, nil
}

func choiceNexts(raw json.RawMessage) ([]string, error) {
	arr, err := decodeArray(raw)
	if err != nil {
		return nil, defErr(codeSchema, "Field 'Choices' must be a non-empty array", "/Choices")
	}
	out := make([]string, 0, len(arr))
	for i, item := range arr {
		obj, err := decodeObject(item)
		if err != nil {
			return nil, defErr(codeSchema, "Field must be an object", fmt.Sprintf("/Choices[%d]", i))
		}
		rawNext, ok := obj["Next"]
		if !ok {
			out = append(out, "")
			continue
		}
		next, err := asString(rawNext)
		if err != nil {
			return nil, defErr(codeSchema, "Field 'Next' must be a string", fmt.Sprintf("/Choices[%d]/Next", i))
		}
		out = append(out, next)
	}
	return out, nil
}

func statesAllPlacement(groups [][]string, ptr string) error {
	for i, names := range groups {
		if !hasStatesAll(names) {
			continue
		}
		if len(names) == 1 && i == len(groups)-1 {
			continue
		}
		return defErr(codeSchema, "States.ALL must appear alone and at end of list", fmt.Sprintf("%s[%d]", ptr, i))
	}
	return nil
}

func hasStatesAll(names []string) bool {
	for _, name := range names {
		if name == "States.ALL" {
			return true
		}
	}
	return false
}

// DefinitionError is one create-time diagnostic. Error text stays the
// InvalidDefinition message Create and Update already return.
type DefinitionError struct {
	Code     string
	Message  string
	Location string
}

func (e *DefinitionError) Error() string {
	ptr := "/"
	code, message := "", ""
	if e != nil {
		code, message = e.Code, e.Message
		if e.Location != "" {
			ptr = e.Location
		}
	}
	return fmt.Sprintf("Invalid State Machine Definition: '%s: %s at %s', ", code, message, ptr)
}

func defErr(code, message, ptr string) error {
	if ptr == "" {
		ptr = "/"
	}
	return &DefinitionError{Code: code, Message: message, Location: ptr}
}

func join(ptr, name string) string {
	if ptr == "/" {
		return "/" + escape(name)
	}
	return ptr + "/" + escape(name)
}

func escape(name string) string {
	name = strings.ReplaceAll(name, "~", "~0")
	return strings.ReplaceAll(name, "/", "~1")
}

func decodeObject(raw []byte) (map[string]json.RawMessage, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	delim, ok := tok.(json.Delim)
	if !ok || delim != '{' {
		return nil, fmt.Errorf("expected an object")
	}
	out := map[string]json.RawMessage{}
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return nil, err
		}
		key, ok := keyTok.(string)
		if !ok {
			return nil, fmt.Errorf("expected a field name")
		}
		if _, exists := out[key]; exists {
			return nil, fmt.Errorf("duplicate field '%s'", key)
		}
		var val json.RawMessage
		if err := dec.Decode(&val); err != nil {
			return nil, err
		}
		out[key] = val
	}
	tok, err = dec.Token()
	if err != nil {
		return nil, err
	}
	if delim, ok := tok.(json.Delim); !ok || delim != '}' {
		return nil, fmt.Errorf("expected end of object")
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, fmt.Errorf("trailing tokens")
	}
	return out, nil
}

func decodeArray(raw json.RawMessage) ([]json.RawMessage, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	delim, ok := tok.(json.Delim)
	if !ok || delim != '[' {
		return nil, fmt.Errorf("expected an array")
	}
	var out []json.RawMessage
	for dec.More() {
		var val json.RawMessage
		if err := dec.Decode(&val); err != nil {
			return nil, err
		}
		out = append(out, val)
	}
	if _, err := dec.Token(); err != nil {
		return nil, err
	}
	return out, nil
}

func asString(raw json.RawMessage) (string, error) {
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return "", err
	}
	return s, nil
}

func asFloat(raw json.RawMessage) (float64, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var n json.Number
	if err := dec.Decode(&n); err != nil {
		return 0, err
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return 0, fmt.Errorf("trailing")
	}
	return n.Float64()
}

func asInt(raw json.RawMessage, min, max int64) (int64, error) {
	text := strings.TrimSpace(string(raw))
	if text == "" || strings.ContainsAny(text, ".eE+") {
		return 0, fmt.Errorf("not an integer")
	}
	n, err := strconv.ParseInt(text, 10, 64)
	if err != nil || n < min || n > max {
		return 0, fmt.Errorf("out of range")
	}
	return n, nil
}

func stringArray(raw json.RawMessage, ptr string) error {
	_, err := stringList(raw, ptr)
	return err
}

func stringList(raw json.RawMessage, ptr string) ([]string, error) {
	arr, err := decodeArray(raw)
	if err != nil || len(arr) == 0 {
		return nil, defErr(codeSchema, "Field 'ErrorEquals' must be a non-empty array of strings", ptr)
	}
	out := make([]string, len(arr))
	for i, item := range arr {
		text, err := asString(item)
		if err != nil {
			return nil, defErr(codeSchema, "Field 'ErrorEquals' must be a non-empty array of strings", ptr)
		}
		out[i] = text
	}
	return out, nil
}

func checkJSON(raw json.RawMessage, ptr string) error {
	if !json.Valid(raw) {
		return defErr(codeSchema, "Field must be JSON", ptr)
	}
	return nil
}

var comparatorNames = map[string]bool{}

func init() {
	base := []string{
		"StringEquals", "StringLessThan", "StringGreaterThan", "StringLessThanEquals", "StringGreaterThanEquals",
		"NumericEquals", "NumericLessThan", "NumericGreaterThan", "NumericLessThanEquals", "NumericGreaterThanEquals",
		"BooleanEquals",
		"TimestampEquals", "TimestampLessThan", "TimestampGreaterThan", "TimestampLessThanEquals", "TimestampGreaterThanEquals",
		"StringMatches",
	}
	for _, name := range base {
		comparatorNames[name] = true
		comparatorNames[name+"Path"] = true
	}
	for _, name := range []string{"IsNull", "IsPresent", "IsNumeric", "IsString", "IsBoolean", "IsTimestamp"} {
		comparatorNames[name] = true
	}
}
