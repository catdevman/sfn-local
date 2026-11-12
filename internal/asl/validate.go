// Package asl validates a JSONPath state machine against the generated field
// matrix. QueryLanguage and JSONata-only fields fail. Phase 0b message parity
// (missing transition targets, States.ALL placement, express resource rules)
// is not this package.
package asl

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/catdevman/sfn-local/internal/asl/specgen"
)

const (
	codeSchema     = "SCHEMA_VALIDATION_FAILED"
	defaultTimeout = int64(99999999)
)

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

// State is one state the runner can read. Nested machines are validated and
// not copied onto this struct until a later phase executes them.
type State struct {
	Name              string
	Type              string
	Next              string
	End               bool
	InputPath         Path
	OutputPath        Path
	ResultPath        Path
	HasResult         bool
	Result            json.RawMessage
	HasParameters     bool
	HasAssign         bool
	HasResultSelector bool
	HasError          bool
	Error             string
	HasCause          bool
	Cause             string
	HasErrorPath      bool
	HasCausePath      bool
}

// Program is a validated definition.
type Program struct {
	StartAt        string
	TimeoutSeconds int64
	States         map[string]*State
}

// Validate parses a JSONPath definition. The error text is the InvalidDefinition message.
func Validate(definition string) (*Program, error) {
	fields, err := decodeObject([]byte(definition))
	if err != nil {
		return nil, defErr(codeSchema, err.Error(), "/")
	}
	return validateMachine(fields, "/", true)
}

func validateMachine(fields map[string]json.RawMessage, ptr string, top bool) (*Program, error) {
	allowed := map[string]bool{"Comment": true, "StartAt": true, "States": true}
	if top {
		allowed["TimeoutSeconds"] = true
		allowed["Version"] = true
	}
	if err := unknown(fields, allowed, ptr); err != nil {
		return nil, err
	}
	startRaw, ok := fields["StartAt"]
	if !ok {
		return nil, defErr(codeSchema, "Missing required field 'StartAt'", ptr)
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
	prog := &Program{StartAt: start, TimeoutSeconds: timeout, States: map[string]*State{}}
	for name, raw := range states {
		stPtr := join(ptr, "States") + "/" + escape(name)
		obj, err := decodeObject(raw)
		if err != nil {
			return nil, defErr(codeSchema, "Field must be an object", stPtr)
		}
		st, err := validateState(name, obj, stPtr)
		if err != nil {
			return nil, err
		}
		prog.States[name] = st
	}
	return prog, nil
}

func validateState(name string, fields map[string]json.RawMessage, ptr string) (*State, error) {
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
	allowed := allowedFields(typ)
	if err := unknown(fields, allowed, ptr); err != nil {
		return nil, err
	}
	for _, field := range jsonPathFields() {
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
	st := &State{Name: name, Type: typ, InputPath: Path{Kind: PathDefault}, OutputPath: Path{Kind: PathDefault}, ResultPath: Path{Kind: PathDefault}}
	if raw, ok := fields["Next"]; ok {
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
	if _, ok := fields["Parameters"]; ok {
		st.HasParameters = true
		if err := checkJSON(fields["Parameters"], join(ptr, "Parameters")); err != nil {
			return nil, err
		}
	}
	if _, ok := fields["Assign"]; ok {
		st.HasAssign = true
	}
	if _, ok := fields["ResultSelector"]; ok {
		st.HasResultSelector = true
	}
	if raw, ok := fields["Error"]; ok {
		text, err := asString(raw)
		if err != nil {
			return nil, defErr(codeSchema, "Field 'Error' must be a string", join(ptr, "Error"))
		}
		st.HasError = true
		st.Error = text
	}
	if raw, ok := fields["Cause"]; ok {
		text, err := asString(raw)
		if err != nil {
			return nil, defErr(codeSchema, "Field 'Cause' must be a string", join(ptr, "Cause"))
		}
		st.HasCause = true
		st.Cause = text
	}
	_, st.HasErrorPath = fields["ErrorPath"]
	_, st.HasCausePath = fields["CausePath"]
	if err := checkTypeShape(typ, fields, ptr); err != nil {
		return nil, err
	}
	return st, nil
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

func checkTypeShape(typ string, fields map[string]json.RawMessage, ptr string) error {
	switch typ {
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
			if _, err := asInt(raw, 0, defaultTimeout); err != nil {
				return defErr(codeSchema, "Field 'Seconds' must be an integer in 0..99999999", join(ptr, "Seconds"))
			}
		}
	case "Choice":
		raw, ok := fields["Choices"]
		if !ok {
			return defErr(codeSchema, "Missing required field 'Choices'", ptr)
		}
		if err := checkChoices(raw, join(ptr, "Choices")); err != nil {
			return err
		}
		if raw, ok := fields["Default"]; ok {
			if _, err := asString(raw); err != nil {
				return defErr(codeSchema, "Field 'Default' must be a string", join(ptr, "Default"))
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
		if err := checkBranches(raw, join(ptr, "Branches")); err != nil {
			return err
		}
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
			if err := checkSubmachine(fields["Iterator"], join(ptr, "Iterator"), false); err != nil {
				return err
			}
		}
		if proc {
			if err := checkProcessor(fields["ItemProcessor"], join(ptr, "ItemProcessor")); err != nil {
				return err
			}
		}
		if raw, ok := fields["MaxConcurrency"]; ok {
			if _, err := asInt(raw, 0, int64(^uint64(0)>>1)); err != nil {
				return defErr(codeSchema, "Field 'MaxConcurrency' must be an integer >= 0", join(ptr, "MaxConcurrency"))
			}
		}
	}
	if raw, ok := fields["Retry"]; ok {
		if err := checkRetry(raw, join(ptr, "Retry")); err != nil {
			return err
		}
	}
	if raw, ok := fields["Catch"]; ok {
		if err := checkCatch(raw, join(ptr, "Catch")); err != nil {
			return err
		}
	}
	return nil
}

func checkBranches(raw json.RawMessage, ptr string) error {
	arr, err := decodeArray(raw)
	if err != nil || len(arr) == 0 {
		return defErr(codeSchema, "Field 'Branches' must be a non-empty array", ptr)
	}
	for i, item := range arr {
		if err := checkSubmachine(item, fmt.Sprintf("%s[%d]", ptr, i), false); err != nil {
			return err
		}
	}
	return nil
}

func checkSubmachine(raw json.RawMessage, ptr string, processor bool) error {
	fields, err := decodeObject(raw)
	if err != nil {
		return defErr(codeSchema, "Field must be an object", ptr)
	}
	if _, err := validateMachine(fields, ptr, false); err != nil {
		if processor {
			return err
		}
		// validateMachine rejects ProcessorConfig and Retry. Iterators and
		// branches only allow Comment, StartAt, and States.
		return err
	}
	return nil
}

func checkProcessor(raw json.RawMessage, ptr string) error {
	fields, err := decodeObject(raw)
	if err != nil {
		return defErr(codeSchema, "Field must be an object", ptr)
	}
	allowed := map[string]bool{"Comment": true, "StartAt": true, "States": true, "ProcessorConfig": true, "Retry": true}
	if err := unknown(fields, allowed, ptr); err != nil {
		return err
	}
	if raw, ok := fields["ProcessorConfig"]; ok {
		cfg, err := decodeObject(raw)
		if err != nil {
			return defErr(codeSchema, "Field 'ProcessorConfig' must be an object", join(ptr, "ProcessorConfig"))
		}
		if err := unknown(cfg, map[string]bool{"Mode": true}, join(ptr, "ProcessorConfig")); err != nil {
			return err
		}
		mode, err := asString(cfg["Mode"])
		if err != nil || mode != "INLINE" {
			text := ""
			if err == nil {
				text = mode
			}
			return defErr(codeSchema, "Field 'Mode' value '"+text+"' is not supported", join(ptr, "ProcessorConfig")+"/Mode")
		}
	}
	// Validate StartAt and States by reusing the branch rules. Drop the extra keys first.
	base := map[string]json.RawMessage{}
	for _, key := range []string{"Comment", "StartAt", "States"} {
		if raw, ok := fields[key]; ok {
			base[key] = raw
		}
	}
	if _, err := validateMachine(base, ptr, false); err != nil {
		return err
	}
	if raw, ok := fields["Retry"]; ok {
		return checkRetry(raw, join(ptr, "Retry"))
	}
	return nil
}

func checkRetry(raw json.RawMessage, ptr string) error {
	arr, err := decodeArray(raw)
	if err != nil {
		return defErr(codeSchema, "Field 'Retry' must be an array", ptr)
	}
	allowed := map[string]bool{
		"ErrorEquals": true, "IntervalSeconds": true, "MaxAttempts": true,
		"BackoffRate": true, "MaxDelaySeconds": true, "JitterStrategy": true, "Comment": true,
	}
	for i, item := range arr {
		ip := fmt.Sprintf("%s[%d]", ptr, i)
		obj, err := decodeObject(item)
		if err != nil {
			return defErr(codeSchema, "Field must be an object", ip)
		}
		if err := unknown(obj, allowed, ip); err != nil {
			return err
		}
		if _, ok := obj["ErrorEquals"]; !ok {
			return defErr(codeSchema, "Missing required field 'ErrorEquals'", ip)
		}
		if err := stringArray(obj["ErrorEquals"], join(ip, "ErrorEquals")); err != nil {
			return err
		}
		if raw, ok := obj["IntervalSeconds"]; ok {
			if _, err := asInt(raw, 0, defaultTimeout); err != nil {
				return defErr(codeSchema, "Field 'IntervalSeconds' must be an integer in 0..99999999", join(ip, "IntervalSeconds"))
			}
		}
		if raw, ok := obj["MaxAttempts"]; ok {
			if _, err := asInt(raw, 0, defaultTimeout); err != nil {
				return defErr(codeSchema, "Field 'MaxAttempts' must be an integer in 0..99999999", join(ip, "MaxAttempts"))
			}
		}
		if raw, ok := obj["JitterStrategy"]; ok {
			v, err := asString(raw)
			if err != nil || (v != "NONE" && v != "FULL") {
				return defErr(codeSchema, "Field 'JitterStrategy' must be NONE or FULL", join(ip, "JitterStrategy"))
			}
		}
	}
	return nil
}

func checkCatch(raw json.RawMessage, ptr string) error {
	arr, err := decodeArray(raw)
	if err != nil {
		return defErr(codeSchema, "Field 'Catch' must be an array", ptr)
	}
	allowed := map[string]bool{"ErrorEquals": true, "Next": true, "ResultPath": true, "Comment": true, "Assign": true}
	for i, item := range arr {
		ip := fmt.Sprintf("%s[%d]", ptr, i)
		obj, err := decodeObject(item)
		if err != nil {
			return defErr(codeSchema, "Field must be an object", ip)
		}
		if err := unknown(obj, allowed, ip); err != nil {
			return err
		}
		if _, ok := obj["ErrorEquals"]; !ok {
			return defErr(codeSchema, "Missing required field 'ErrorEquals'", ip)
		}
		if err := stringArray(obj["ErrorEquals"], join(ip, "ErrorEquals")); err != nil {
			return err
		}
		if _, ok := obj["Next"]; !ok {
			return defErr(codeSchema, "Missing required field 'Next'", ip)
		}
		if _, err := asString(obj["Next"]); err != nil {
			return defErr(codeSchema, "Field 'Next' must be a string", join(ip, "Next"))
		}
	}
	return nil
}

func checkChoices(raw json.RawMessage, ptr string) error {
	arr, err := decodeArray(raw)
	if err != nil || len(arr) == 0 {
		return defErr(codeSchema, "Field 'Choices' must be a non-empty array", ptr)
	}
	for i, item := range arr {
		if err := checkRule(item, fmt.Sprintf("%s[%d]", ptr, i), true); err != nil {
			return err
		}
	}
	return nil
}

func checkRule(raw json.RawMessage, ptr string, top bool) error {
	obj, err := decodeObject(raw)
	if err != nil {
		return defErr(codeSchema, "Field must be an object", ptr)
	}
	allowed := map[string]bool{"Comment": true, "Variable": true, "Next": true, "And": true, "Or": true, "Not": true}
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
		if _, ok := obj["Next"]; !ok {
			return defErr(codeSchema, "Missing required field 'Next'", ptr)
		}
		if _, err := asString(obj["Next"]); err != nil {
			return defErr(codeSchema, "Field 'Next' must be a string", join(ptr, "Next"))
		}
	}
	if raw, ok := obj["And"]; ok {
		if err := checkRuleArray(raw, join(ptr, "And")); err != nil {
			return err
		}
	}
	if raw, ok := obj["Or"]; ok {
		if err := checkRuleArray(raw, join(ptr, "Or")); err != nil {
			return err
		}
	}
	if raw, ok := obj["Not"]; ok {
		if err := checkRule(raw, join(ptr, "Not"), false); err != nil {
			return err
		}
	}
	return nil
}

func checkRuleArray(raw json.RawMessage, ptr string) error {
	arr, err := decodeArray(raw)
	if err != nil || len(arr) == 0 {
		return defErr(codeSchema, "Field must be a non-empty array", ptr)
	}
	for i, item := range arr {
		if err := checkRule(item, fmt.Sprintf("%s[%d]", ptr, i), false); err != nil {
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

func allowedFields(typ string) map[string]bool {
	out := map[string]bool{}
	for _, field := range jsonPathFields() {
		if presence(field, typ) != specgen.PresenceAbsent {
			out[field.Name] = true
		}
	}
	for _, name := range extraFields[typ] {
		out[name] = true
	}
	return out
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

func defErr(code, message, ptr string) error {
	if ptr == "" {
		ptr = "/"
	}
	return fmt.Errorf("Invalid State Machine Definition: '%s: %s at %s', ", code, message, ptr)
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
	arr, err := decodeArray(raw)
	if err != nil || len(arr) == 0 {
		return defErr(codeSchema, "Field 'ErrorEquals' must be a non-empty array of strings", ptr)
	}
	for _, item := range arr {
		if _, err := asString(item); err != nil {
			return defErr(codeSchema, "Field 'ErrorEquals' must be a non-empty array of strings", ptr)
		}
	}
	return nil
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
