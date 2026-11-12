// Package jsonpath evaluates the reference paths and payload templates the
// JSONPath state set uses. Paths are a single node: `$`, `$.a.b`, `$[0]`,
// or `$$.Execution.Id`. A `$name` variable reference fails closed.
package jsonpath

import (
	"errors"
	"strconv"
	"strings"

	"github.com/catdevman/sfn-local/internal/intrinsic"
)

// EvalError is a path or template failure the runner records on the execution.
type EvalError struct {
	Name  string
	Cause string
}

func (e *EvalError) Error() string {
	if e == nil {
		return ""
	}
	return e.Cause
}

// Runtime returns a States.Runtime failure.
func Runtime(cause string) error {
	return &EvalError{Name: "States.Runtime", Cause: cause}
}

// Conflict returns a States.ReferencePathConflict failure.
func Conflict(cause string) error {
	return &EvalError{Name: "States.ReferencePathConflict", Cause: cause}
}

// DataLimit returns a States.DataLimitExceeded failure.
func DataLimit(cause string) error {
	return &EvalError{Name: "States.DataLimitExceeded", Cause: cause}
}

type rootKind int

const (
	dataRoot rootKind = iota
	ctxRoot
)

type segKind int

const (
	segKey segKind = iota
	segIndex
)

type segment struct {
	kind  segKind
	key   string
	index int
}

// Get reads path. root is the `$` document and ctx is the `$$` document.
// found is false when a key or index is absent. A malformed path, a variable
// reference, or a descent into a value that is not a container returns an error.
func Get(path string, root, ctx any) (any, bool, error) {
	kind, segs, err := parse(path)
	if err != nil {
		return nil, false, err
	}
	cur := root
	if kind == ctxRoot {
		cur = ctx
	}
	for _, seg := range segs {
		next, ok, err := step(cur, seg)
		if err != nil || !ok {
			return nil, ok, err
		}
		cur = next
	}
	return cur, true, nil
}

// Set writes value at path in a copy of root. `$` replaces the document.
// The leaf is created when its parent exists. A missing intermediate or a
// parent of the wrong JSON type does not invent a container.
func Set(root any, path string, value any) (any, error) {
	kind, segs, err := parse(path)
	if err != nil {
		return nil, err
	}
	if kind == ctxRoot {
		return nil, Runtime("ResultPath cannot reference the context object")
	}
	if len(segs) == 0 {
		return value, nil
	}
	return write(clone(root), segs, value, path)
}

// Apply evaluates a payload template. A key ending in `.$` is a reference
// path or a States.* call.
func Apply(template, root, ctx any) (any, error) {
	switch node := template.(type) {
	case map[string]any:
		for key := range node {
			if !strings.HasSuffix(key, ".$") {
				continue
			}
			base := strings.TrimSuffix(key, ".$")
			if _, ok := node[base]; ok {
				return nil, Runtime("both " + base + " and " + key + " are set")
			}
		}
		out := make(map[string]any, len(node))
		for key, child := range node {
			if strings.HasSuffix(key, ".$") {
				text, ok := child.(string)
				if !ok {
					return nil, Runtime("path value for " + key + " must be a string")
				}
				val, err := Eval(text, root, ctx)
				if err != nil {
					return nil, err
				}
				out[strings.TrimSuffix(key, ".$")] = val
				continue
			}
			val, err := Apply(child, root, ctx)
			if err != nil {
				return nil, err
			}
			out[key] = val
		}
		return out, nil
	case []any:
		out := make([]any, len(node))
		for i, child := range node {
			val, err := Apply(child, root, ctx)
			if err != nil {
				return nil, err
			}
			out[i] = val
		}
		return out, nil
	default:
		return template, nil
	}
}

// Eval evaluates one payload-template expression.
func Eval(expr string, root, ctx any) (any, error) {
	expr = strings.TrimSpace(expr)
	if strings.HasPrefix(expr, "States.") {
		val, err := intrinsic.Eval(expr, func(path string) (any, error) {
			return Eval(path, root, ctx)
		})
		if err != nil {
			var ev *EvalError
			if errors.As(err, &ev) {
				return nil, err
			}
			return nil, Runtime(err.Error())
		}
		return val, nil
	}
	val, found, err := Get(expr, root, ctx)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, Runtime("path " + expr + " was not found")
	}
	return val, nil
}

func parse(path string) (rootKind, []segment, error) {
	if path == "" {
		return 0, nil, Runtime("path is empty")
	}
	if isVariable(path) {
		return 0, nil, Runtime("variable " + path + " is not enabled")
	}
	kind := dataRoot
	rest := path
	switch {
	case strings.HasPrefix(path, "$$"):
		kind = ctxRoot
		rest = path[2:]
	case strings.HasPrefix(path, "$"):
		rest = path[1:]
	default:
		return 0, nil, Runtime("path " + path + " must start with $. or $$")
	}
	var segs []segment
	for len(rest) > 0 {
		switch rest[0] {
		case '.':
			name, next, err := parseIdent(rest[1:], path)
			if err != nil {
				return 0, nil, err
			}
			segs = append(segs, segment{kind: segKey, key: name})
			rest = next
		case '[':
			seg, next, err := parseBracket(rest, path)
			if err != nil {
				return 0, nil, err
			}
			segs = append(segs, seg)
			rest = next
		default:
			return 0, nil, Runtime("invalid path " + path)
		}
	}
	return kind, segs, nil
}

func isVariable(path string) bool {
	if len(path) < 2 || path[0] != '$' {
		return false
	}
	if path[1] == '$' || path[1] == '.' || path[1] == '[' {
		return false
	}
	for i := 1; i < len(path); i++ {
		if i == 1 && !isIdentStart(path[i]) {
			return false
		}
		if i > 1 && !isIdentCont(path[i]) {
			return false
		}
	}
	return true
}

func parseIdent(rest, path string) (string, string, error) {
	if rest == "" || !isIdentStart(rest[0]) {
		return "", "", Runtime("invalid path " + path)
	}
	i := 1
	for i < len(rest) && isIdentCont(rest[i]) {
		i++
	}
	return rest[:i], rest[i:], nil
}

func parseBracket(rest, path string) (segment, string, error) {
	if len(rest) < 3 {
		return segment{}, "", Runtime("invalid path " + path)
	}
	if rest[1] == '\'' || rest[1] == '"' {
		quote := rest[1]
		var b strings.Builder
		i := 2
		for i < len(rest) {
			if rest[i] == '\\' && i+1 < len(rest) {
				b.WriteByte(rest[i+1])
				i += 2
				continue
			}
			if rest[i] == quote {
				break
			}
			b.WriteByte(rest[i])
			i++
		}
		if i >= len(rest) || rest[i] != quote || i+1 >= len(rest) || rest[i+1] != ']' {
			return segment{}, "", Runtime("invalid path " + path)
		}
		return segment{kind: segKey, key: b.String()}, rest[i+2:], nil
	}
	i := 1
	if rest[i] < '0' || rest[i] > '9' {
		return segment{}, "", Runtime("invalid path " + path)
	}
	j := i
	for j < len(rest) && rest[j] >= '0' && rest[j] <= '9' {
		j++
	}
	if j >= len(rest) || rest[j] != ']' {
		return segment{}, "", Runtime("invalid path " + path)
	}
	n, err := strconv.Atoi(rest[i:j])
	if err != nil {
		return segment{}, "", Runtime("invalid path " + path)
	}
	return segment{kind: segIndex, index: n}, rest[j+1:], nil
}

func step(cur any, seg segment) (any, bool, error) {
	switch seg.kind {
	case segKey:
		obj, ok := cur.(map[string]any)
		if !ok {
			return nil, false, Runtime("path could not be resolved")
		}
		val, ok := obj[seg.key]
		if !ok {
			return nil, false, nil
		}
		return val, true, nil
	case segIndex:
		arr, ok := cur.([]any)
		if !ok {
			return nil, false, Runtime("path could not be resolved")
		}
		if seg.index < 0 || seg.index >= len(arr) {
			return nil, false, nil
		}
		return arr[seg.index], true, nil
	default:
		return nil, false, Runtime("invalid path")
	}
}

func write(node any, segs []segment, value any, path string) (any, error) {
	seg := segs[0]
	last := len(segs) == 1
	switch seg.kind {
	case segKey:
		obj, ok := node.(map[string]any)
		if !ok {
			return nil, Conflict("Reference path conflict at " + path)
		}
		if last {
			obj[seg.key] = value
			return obj, nil
		}
		child, ok := obj[seg.key]
		if !ok {
			return nil, Runtime("could not apply ResultPath " + path)
		}
		updated, err := write(child, segs[1:], value, path)
		if err != nil {
			return nil, err
		}
		obj[seg.key] = updated
		return obj, nil
	case segIndex:
		arr, ok := node.([]any)
		if !ok || seg.index < 0 || seg.index >= len(arr) {
			return nil, Conflict("Reference path conflict at " + path)
		}
		if last {
			arr[seg.index] = value
			return arr, nil
		}
		updated, err := write(arr[seg.index], segs[1:], value, path)
		if err != nil {
			return nil, err
		}
		arr[seg.index] = updated
		return arr, nil
	default:
		return nil, Runtime("invalid path " + path)
	}
}

func clone(v any) any {
	switch node := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(node))
		for key, child := range node {
			out[key] = clone(child)
		}
		return out
	case []any:
		out := make([]any, len(node))
		for i, child := range node {
			out[i] = clone(child)
		}
		return out
	default:
		return v
	}
}

func isIdentStart(b byte) bool {
	return b == '_' || (b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z')
}

func isIdentCont(b byte) bool {
	return isIdentStart(b) || (b >= '0' && b <= '9')
}
