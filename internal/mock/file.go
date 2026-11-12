// Package mock loads a Step Functions Local mock file. The file is read when
// an execution asks for a test name, and again when its SHA-256 changes.
package mock

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

const maxBytes = 1_000_000

// FileError is a mock file the API reports as InvalidDefinition.
type FileError struct {
	Message string
}

func (e *FileError) Error() string { return e.Message }

func def(message string) error {
	return &FileError{Message: "Invalid State Machine Definition: '" + message + "'"}
}

// Repo reads one mock file. A nil Repo reports the file as missing.
type Repo struct {
	Path string

	mu   sync.Mutex
	sum  [32]byte
	have bool
	doc  *doc
}

// Check loads the file and reports a missing file, an unknown test, or an
// overlapping selector. machine is the state machine name. test may be empty
// when the ARN had a trailing '#'.
func (r *Repo) Check(machine, test string) error {
	if r == nil || r.Path == "" {
		return def("Mock file not found")
	}
	doc, err := r.load()
	if err != nil {
		return err
	}
	if _, ok := doc.tests[machine+"\x00"+test]; !ok {
		return def("Unknown mock test: " + test)
	}
	return nil
}

// StateNotMocked means this state is not in the test case and may call a resource.
var StateNotMocked = errors.New("state is not mocked")

// Attempt is one Return or Throw.
type Attempt struct {
	Return    json.RawMessage
	HasReturn bool
	Error     string
	Cause     string
	HasThrow  bool
}

// Mocked reports whether the test case names this state. A false result with
// a nil error means the execution may call the resource.
func (r *Repo) Mocked(machine, test, state string) (bool, error) {
	if test == "" {
		return false, nil
	}
	if r == nil || r.Path == "" {
		return false, def("Mock file not found")
	}
	doc, err := r.load()
	if err != nil {
		return false, err
	}
	states, ok := doc.tests[machine+"\x00"+test]
	if !ok {
		return false, def("Unknown mock test: " + test)
	}
	_, ok = states[state]
	return ok, nil
}

// Lookup selects the attempt for a state. attempt is zero on the first try.
func (r *Repo) Lookup(machine, test, state string, attempt int) (Attempt, error) {
	if test == "" || r == nil {
		return Attempt{}, StateNotMocked
	}
	if r.Path == "" {
		return Attempt{}, def("Mock file not found")
	}
	doc, err := r.load()
	if err != nil {
		return Attempt{}, err
	}
	states, ok := doc.tests[machine+"\x00"+test]
	if !ok {
		return Attempt{}, def("Unknown mock test: " + test)
	}
	response, ok := states[state]
	if !ok {
		return Attempt{}, StateNotMocked
	}
	got, ok := doc.responses[response].pick(attempt)
	if !ok {
		return Attempt{}, fmt.Errorf("no mock data was found for attempt %d of state %s", attempt, state)
	}
	if got.HasReturn == got.HasThrow {
		return Attempt{}, fmt.Errorf("mock response %s attempt %d must be Return or Throw", response, attempt)
	}
	if got.HasThrow && got.Error == "" {
		return Attempt{}, fmt.Errorf("mock response %s Throw is missing Error", response)
	}
	return got, nil
}

type doc struct {
	tests     map[string]map[string]string
	responses map[string]response
}

type response struct {
	spans []span
}

type span struct {
	start, end int
	attempt    Attempt
}

func (s response) pick(n int) (Attempt, bool) {
	for _, sp := range s.spans {
		if n >= sp.start && n <= sp.end {
			return sp.attempt, true
		}
	}
	return Attempt{}, false
}

func (r *Repo) load() (*doc, error) {
	raw, err := os.ReadFile(r.Path)
	if err != nil {
		return nil, def("Mock file not found: " + r.Path)
	}
	if len(raw) > maxBytes {
		return nil, def("Mock file " + r.Path + " exceeds 1000000 bytes")
	}
	sum := sha256.Sum256(raw)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.have && r.sum == sum && r.doc != nil {
		return r.doc, nil
	}
	parsed, err := parse(raw)
	if err != nil {
		r.have = false
		r.doc = nil
		return nil, err
	}
	r.sum = sum
	r.have = true
	r.doc = parsed
	return parsed, nil
}

func parse(raw []byte) (*doc, error) {
	var body struct {
		StateMachines map[string]struct {
			TestCases map[string]map[string]string `json:"TestCases"`
		} `json:"StateMachines"`
		MockedResponses map[string]map[string]json.RawMessage `json:"MockedResponses"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		return nil, def("Mock file is not JSON")
	}
	out := &doc{
		tests:     map[string]map[string]string{},
		responses: map[string]response{},
	}
	for name, resp := range body.MockedResponses {
		parsed, err := parseResponse(name, resp)
		if err != nil {
			return nil, err
		}
		out.responses[name] = parsed
	}
	for machine, sm := range body.StateMachines {
		for test, states := range sm.TestCases {
			if states == nil {
				states = map[string]string{}
			}
			for _, response := range states {
				if _, ok := out.responses[response]; !ok {
					return nil, def("Unknown mock response: " + response)
				}
			}
			out.tests[machine+"\x00"+test] = states
		}
	}
	return out, nil
}

var (
	attemptKey = regexp.MustCompile(`^(0|[1-9][0-9]{0,7})$`)
	rangeKey   = regexp.MustCompile(`^(0|[1-9][0-9]{0,7})-(0|[1-9][0-9]{0,7})$`)
)

func parseResponse(name string, raw map[string]json.RawMessage) (response, error) {
	var spans []span
	for key, body := range raw {
		start, end, err := selector(key)
		if err != nil {
			return response{}, def("Mock response " + name + " has invalid attempt key " + key)
		}
		for _, prev := range spans {
			if start <= prev.end && prev.start <= end {
				return response{}, def("Overlapping mock selectors in response " + name)
			}
		}
		att, err := parseAttempt(body)
		if err != nil {
			return response{}, def("Mock response " + name + " " + err.Error())
		}
		spans = append(spans, span{start: start, end: end, attempt: att})
	}
	return response{spans: spans}, nil
}

func selector(key string) (int, int, error) {
	if attemptKey.MatchString(key) {
		n, err := strconv.Atoi(key)
		return n, n, err
	}
	m := rangeKey.FindStringSubmatch(key)
	if m == nil {
		return 0, 0, fmt.Errorf("bad key")
	}
	start, err := strconv.Atoi(m[1])
	if err != nil {
		return 0, 0, err
	}
	end, err := strconv.Atoi(m[2])
	if err != nil || start > end {
		return 0, 0, fmt.Errorf("bad range")
	}
	return start, end, nil
}

func parseAttempt(raw json.RawMessage) (Attempt, error) {
	var body struct {
		Return json.RawMessage `json:"Return"`
		Throw  *struct {
			Error string `json:"Error"`
			Cause string `json:"Cause"`
		} `json:"Throw"`
	}
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&body); err != nil {
		return Attempt{}, err
	}
	att := Attempt{}
	if len(body.Return) > 0 && string(body.Return) != "null" {
		att.HasReturn = true
		att.Return = append(json.RawMessage(nil), body.Return...)
	}
	if body.Throw != nil {
		att.HasThrow = true
		att.Error = body.Throw.Error
		att.Cause = body.Throw.Cause
	}
	return att, nil
}
