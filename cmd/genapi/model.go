package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"unicode"
)

const (
	protocolTrait         = "aws.protocols#awsJson1_0"
	traitRequired         = "smithy.api#required"
	traitLength           = "smithy.api#length"
	traitPattern          = "smithy.api#pattern"
	traitRange            = "smithy.api#range"
	traitDefault          = "smithy.api#default"
	traitEnumValue        = "smithy.api#enumValue"
	traitError            = "smithy.api#error"
	traitHTTPError        = "smithy.api#httpError"
	traitIdempotent       = "smithy.api#idempotent"
	traitSensitive        = "smithy.api#sensitive"
	traitIdempotencyToken = "smithy.api#idempotencyToken"
	traitPaginated        = "smithy.api#paginated"
)

var allowedKinds = map[string]bool{
	"service": true, "operation": true, "structure": true,
	"string": true, "boolean": true, "integer": true, "long": true,
	"float": true, "timestamp": true, "enum": true, "list": true, "map": true,
}

type model struct {
	Service *shape
	Shapes  map[string]*shape
	Order   []string
}

type shape struct {
	ID         string
	Name       string
	Kind       string
	Version    string
	Operations []string
	Members    []member
	ListOf     *member
	MapKey     *member
	MapValue   *member
	Input      string
	Output     string
	Errors     []string
	Traits     traits
}

type member struct {
	Name   string
	Target string
	Traits traits
}

type traits struct {
	Protocol         bool
	Required         bool
	Sensitive        bool
	Idempotent       bool
	IdempotencyToken bool
	HasPattern       bool
	Pattern          string
	HasLength        bool
	LengthMin        *int
	LengthMax        *int
	HasRangeMin      bool
	HasRangeMax      bool
	RangeMin         string
	RangeMax         string
	HasDefault       bool
	Default          json.RawMessage
	HasEnumValue     bool
	EnumValue        string
	ErrorFault       string
	HasHTTP          bool
	HTTPStatus       int
	Paginated        *pagination
}

type pagination struct {
	InputToken  string `json:"inputToken"`
	OutputToken string `json:"outputToken"`
	Items       string `json:"items"`
	PageSize    string `json:"pageSize"`
}

type orderedJSON struct {
	Keys []string
	Map  map[string]json.RawMessage
}

func (o *orderedJSON) UnmarshalJSON(data []byte) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	d, ok := tok.(json.Delim)
	if !ok || d != '{' {
		return fmt.Errorf("expected JSON object")
	}
	o.Map = make(map[string]json.RawMessage)
	for dec.More() {
		kt, err := dec.Token()
		if err != nil {
			return err
		}
		key, ok := kt.(string)
		if !ok {
			return fmt.Errorf("expected string key")
		}
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			return err
		}
		if _, exists := o.Map[key]; exists {
			return fmt.Errorf("duplicate key %s", key)
		}
		o.Keys = append(o.Keys, key)
		o.Map[key] = append(json.RawMessage(nil), raw...)
	}
	tok, err = dec.Token()
	if err != nil {
		return err
	}
	d, ok = tok.(json.Delim)
	if !ok || d != '}' {
		return fmt.Errorf("expected end of object")
	}
	return nil
}

type shapeRaw struct {
	Type       string      `json:"type"`
	Version    string      `json:"version"`
	Operations []targetRaw `json:"operations"`
	Members    orderedJSON `json:"members"`
	Member     *refRaw     `json:"member"`
	Key        *refRaw     `json:"key"`
	Value      *refRaw     `json:"value"`
	Input      *targetRaw  `json:"input"`
	Output     *targetRaw  `json:"output"`
	Errors     []targetRaw `json:"errors"`
	Traits     orderedJSON `json:"traits"`
}

type targetRaw struct {
	Target string `json:"target"`
}

type refRaw struct {
	Target string      `json:"target"`
	Traits orderedJSON `json:"traits"`
}

func loadModel(path string) (*model, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var top orderedJSON
	if err := json.Unmarshal(data, &top); err != nil {
		return nil, fmt.Errorf("parse smithy: %w", err)
	}
	rawShapes, ok := top.Map["shapes"]
	if !ok {
		return nil, fmt.Errorf("smithy model has no shapes")
	}
	var shapes orderedJSON
	if err := json.Unmarshal(rawShapes, &shapes); err != nil {
		return nil, fmt.Errorf("shapes: %w", err)
	}
	m := &model{Shapes: make(map[string]*shape, len(shapes.Keys))}
	for _, id := range shapes.Keys {
		sh, err := decodeShape(id, shapes.Map[id])
		if err != nil {
			return nil, err
		}
		if !allowedKinds[sh.Kind] {
			return nil, fmt.Errorf("unsupported shape kind %q for shape %s", sh.Kind, id)
		}
		m.Shapes[id] = sh
		m.Order = append(m.Order, id)
		if sh.Kind == "service" {
			if m.Service != nil {
				return nil, fmt.Errorf("multiple service shapes (%s and %s)", m.Service.ID, id)
			}
			m.Service = sh
		}
	}
	if m.Service == nil {
		return nil, fmt.Errorf("model has no service shape")
	}
	if !m.Service.Traits.Protocol {
		return nil, fmt.Errorf("service %s has no %s trait", m.Service.ID, protocolTrait)
	}
	if err := m.checkRefs(); err != nil {
		return nil, err
	}
	return m, nil
}

// present reports whether a trait key was stored. traits.present is filled
// by a side channel: we keep the raw keys on the shape during decode and
// fold them into typed fields. The helper below is replaced by a method
// that the decode path sets via HasProtocol.
func decodeShape(id string, raw json.RawMessage) (*shape, error) {
	var sr shapeRaw
	if err := json.Unmarshal(raw, &sr); err != nil {
		return nil, fmt.Errorf("%s: %w", id, err)
	}
	name, err := localName(id)
	if err != nil {
		return nil, err
	}
	if sr.Type == "" {
		return nil, fmt.Errorf("%s: missing type", id)
	}
	tr, err := parseTraits(id, sr.Traits)
	if err != nil {
		return nil, err
	}
	sh := &shape{
		ID:      id,
		Name:    name,
		Kind:    sr.Type,
		Version: sr.Version,
		Traits:  tr,
	}
	for _, op := range sr.Operations {
		if op.Target == "" {
			return nil, fmt.Errorf("%s: operation binding has no target", id)
		}
		sh.Operations = append(sh.Operations, op.Target)
	}
	for _, key := range sr.Members.Keys {
		mem, err := decodeMember(id+"."+key, key, sr.Members.Map[key])
		if err != nil {
			return nil, err
		}
		sh.Members = append(sh.Members, mem)
	}
	if sr.Member != nil {
		mem, err := refMember(id+".member", "", sr.Member)
		if err != nil {
			return nil, err
		}
		sh.ListOf = &mem
	}
	if sr.Key != nil {
		mem, err := refMember(id+".key", "", sr.Key)
		if err != nil {
			return nil, err
		}
		sh.MapKey = &mem
	}
	if sr.Value != nil {
		mem, err := refMember(id+".value", "", sr.Value)
		if err != nil {
			return nil, err
		}
		sh.MapValue = &mem
	}
	if sr.Input != nil {
		sh.Input = sr.Input.Target
	}
	if sr.Output != nil {
		sh.Output = sr.Output.Target
	}
	for _, e := range sr.Errors {
		if e.Target == "" {
			return nil, fmt.Errorf("%s: error binding has no target", id)
		}
		sh.Errors = append(sh.Errors, e.Target)
	}
	return sh, nil
}

func decodeMember(where, name string, raw json.RawMessage) (member, error) {
	var ref refRaw
	if err := json.Unmarshal(raw, &ref); err != nil {
		return member{}, fmt.Errorf("%s: %w", where, err)
	}
	return refMember(where, name, &ref)
}

func refMember(where, name string, ref *refRaw) (member, error) {
	if ref.Target == "" {
		return member{}, fmt.Errorf("%s: missing target", where)
	}
	tr, err := parseTraits(where, ref.Traits)
	if err != nil {
		return member{}, err
	}
	return member{Name: name, Target: ref.Target, Traits: tr}, nil
}

func parseTraits(where string, raw orderedJSON) (traits, error) {
	var t traits
	for _, key := range raw.Keys {
		val := raw.Map[key]
		switch key {
		case protocolTrait:
			t.Protocol = true
		case traitRequired:
			t.Required = true
		case traitSensitive:
			t.Sensitive = true
		case traitIdempotent:
			t.Idempotent = true
		case traitIdempotencyToken:
			t.IdempotencyToken = true
		case traitPattern:
			var s string
			if err := json.Unmarshal(val, &s); err != nil {
				return traits{}, fmt.Errorf("%s pattern: %w", where, err)
			}
			t.HasPattern = true
			t.Pattern = s
		case traitLength:
			var ln struct {
				Min *int `json:"min"`
				Max *int `json:"max"`
			}
			if err := json.Unmarshal(val, &ln); err != nil {
				return traits{}, fmt.Errorf("%s length: %w", where, err)
			}
			t.HasLength = true
			t.LengthMin = ln.Min
			t.LengthMax = ln.Max
		case traitRange:
			var rg struct {
				Min *json.RawMessage `json:"min"`
				Max *json.RawMessage `json:"max"`
			}
			if err := json.Unmarshal(val, &rg); err != nil {
				return traits{}, fmt.Errorf("%s range: %w", where, err)
			}
			if rg.Min != nil {
				t.HasRangeMin = true
				t.RangeMin = string(bytes.TrimSpace(*rg.Min))
			}
			if rg.Max != nil {
				t.HasRangeMax = true
				t.RangeMax = string(bytes.TrimSpace(*rg.Max))
			}
		case traitDefault:
			t.HasDefault = true
			t.Default = append(json.RawMessage(nil), bytes.TrimSpace(val)...)
		case traitEnumValue:
			var s string
			if err := json.Unmarshal(val, &s); err != nil {
				return traits{}, fmt.Errorf("%s enumValue: %w", where, err)
			}
			t.HasEnumValue = true
			t.EnumValue = s
		case traitError:
			var s string
			if err := json.Unmarshal(val, &s); err != nil {
				return traits{}, fmt.Errorf("%s error: %w", where, err)
			}
			if s != "client" && s != "server" {
				return traits{}, fmt.Errorf("%s error fault %q", where, s)
			}
			t.ErrorFault = s
		case traitHTTPError:
			code, err := parseHTTPError(val)
			if err != nil {
				return traits{}, fmt.Errorf("%s httpError: %w", where, err)
			}
			t.HasHTTP = true
			t.HTTPStatus = code
		case traitPaginated:
			var p pagination
			if err := json.Unmarshal(val, &p); err != nil {
				return traits{}, fmt.Errorf("%s paginated: %w", where, err)
			}
			t.Paginated = &p
		default:
			// Documentation, endpoint rules, and other traits are not part of the wire codec.
		}
	}
	return t, nil
}

func parseHTTPError(raw json.RawMessage) (int, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return 0, fmt.Errorf("empty")
	}
	if raw[0] != '{' {
		n, err := strconv.Atoi(string(raw))
		if err != nil {
			return 0, err
		}
		return n, nil
	}
	var obj struct {
		Code int `json:"code"`
	}
	if err := json.Unmarshal(raw, &obj); err != nil {
		return 0, err
	}
	if obj.Code == 0 {
		return 0, fmt.Errorf("missing code")
	}
	return obj.Code, nil
}

func (m *model) checkRefs() error {
	for _, id := range m.Service.Operations {
		sh := m.Shapes[id]
		if sh == nil {
			return fmt.Errorf("service operation %s is not in the model", id)
		}
		if sh.Kind != "operation" {
			return fmt.Errorf("%s is bound as an operation but is a %s", id, sh.Kind)
		}
		if sh.Input == "" || sh.Output == "" {
			return fmt.Errorf("operation %s needs an input and an output", id)
		}
		if err := m.mustKind(sh.Input, "structure"); err != nil {
			return fmt.Errorf("operation %s input: %w", sh.Name, err)
		}
		if err := m.mustKind(sh.Output, "structure"); err != nil {
			return fmt.Errorf("operation %s output: %w", sh.Name, err)
		}
		for _, eid := range sh.Errors {
			esh := m.Shapes[eid]
			if esh == nil {
				return fmt.Errorf("operation %s error %s is not in the model", sh.Name, eid)
			}
			if esh.Kind != "structure" || esh.Traits.ErrorFault == "" {
				return fmt.Errorf("operation %s error %s is not an error structure", sh.Name, eid)
			}
		}
	}
	for _, sh := range m.Shapes {
		switch sh.Kind {
		case "structure", "enum":
			for _, mem := range sh.Members {
				if !isIdent(mem.Name) {
					return fmt.Errorf("%s member %q is not an identifier", sh.ID, mem.Name)
				}
				if sh.Kind == "enum" {
					continue
				}
				if _, ok := m.Shapes[mem.Target]; !ok {
					return fmt.Errorf("%s member %s targets unknown shape %s", sh.ID, mem.Name, mem.Target)
				}
			}
		case "list":
			if sh.ListOf == nil {
				return fmt.Errorf("list %s has no member", sh.ID)
			}
			if _, ok := m.Shapes[sh.ListOf.Target]; !ok {
				return fmt.Errorf("list %s targets unknown shape %s", sh.ID, sh.ListOf.Target)
			}
		case "map":
			if sh.MapKey == nil || sh.MapValue == nil {
				return fmt.Errorf("map %s needs a key and a value", sh.ID)
			}
			key := m.Shapes[sh.MapKey.Target]
			if key == nil {
				return fmt.Errorf("map %s key targets unknown shape %s", sh.ID, sh.MapKey.Target)
			}
			if key.Kind != "string" && key.Kind != "enum" {
				return fmt.Errorf("map %s key %s is a %s, want string or enum", sh.ID, key.Name, key.Kind)
			}
			if _, ok := m.Shapes[sh.MapValue.Target]; !ok {
				return fmt.Errorf("map %s value targets unknown shape %s", sh.ID, sh.MapValue.Target)
			}
		}
	}
	return nil
}

func (m *model) mustKind(id, kind string) error {
	sh := m.Shapes[id]
	if sh == nil {
		return fmt.Errorf("%s is not in the model", id)
	}
	if sh.Kind != kind {
		return fmt.Errorf("%s is a %s, want %s", id, sh.Kind, kind)
	}
	return nil
}

func (m *model) shape(id string) *shape {
	sh := m.Shapes[id]
	if sh == nil {
		panic("missing shape " + id)
	}
	return sh
}

func mergeTraits(base, over traits) traits {
	out := base
	if over.Required {
		out.Required = true
	}
	if over.Sensitive {
		out.Sensitive = true
	}
	if over.IdempotencyToken {
		out.IdempotencyToken = true
	}
	if over.HasPattern {
		out.HasPattern = true
		out.Pattern = over.Pattern
	}
	if over.HasLength {
		out.HasLength = true
		out.LengthMin = over.LengthMin
		out.LengthMax = over.LengthMax
	}
	if over.HasRangeMin || over.HasRangeMax {
		out.HasRangeMin = over.HasRangeMin
		out.HasRangeMax = over.HasRangeMax
		out.RangeMin = over.RangeMin
		out.RangeMax = over.RangeMax
	}
	if over.HasDefault {
		out.HasDefault = true
		out.Default = over.Default
	}
	return out
}

func (sh *shape) httpStatus() int {
	if sh.Traits.HasHTTP {
		return sh.Traits.HTTPStatus
	}
	if sh.Traits.ErrorFault == "server" {
		return 500
	}
	return 400
}

func (sh *shape) enumValues() []string {
	out := make([]string, 0, len(sh.Members))
	for _, mem := range sh.Members {
		if mem.Traits.HasEnumValue {
			out = append(out, mem.Traits.EnumValue)
		} else {
			out = append(out, mem.Name)
		}
	}
	return out
}

func localName(id string) (string, error) {
	i := strings.LastIndex(id, "#")
	if i < 0 || i == len(id)-1 {
		return "", fmt.Errorf("shape id %q has no local name", id)
	}
	name := id[i+1:]
	if !isIdent(name) {
		return "", fmt.Errorf("shape name %q is not a Go identifier", name)
	}
	return name, nil
}

func isIdent(s string) bool {
	if s == "" {
		return false
	}
	for i, r := range s {
		if i == 0 {
			if !unicode.IsLetter(r) && r != '_' {
				return false
			}
			continue
		}
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' {
			return false
		}
	}
	return true
}

func exportName(name string) string {
	r, size := utf8Decode(name)
	return string(unicode.ToUpper(r)) + name[size:]
}

func utf8Decode(s string) (rune, int) {
	if s == "" {
		return 0, 0
	}
	return []rune(s)[0], len(string([]rune(s)[:1]))
}
