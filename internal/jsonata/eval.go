// Package jsonata evaluates one JSONata expression for a Step Functions state.
//
// The language AWS names is JSONata 2.0.6. The evaluator is
// github.com/recolabs/gnata, a pure-Go JSONata 2.x implementation. It is not
// the jsonata-js runtime and it does not embed a JavaScript interpreter.
// Expressions are still rejected at validation when they use the 2.1 operators
// "?:" and "??", which 2.0.6 does not have. $eval is replaced with a function
// that fails. No registered function reads the environment, opens a file, or
// fetches a URL.
package jsonata

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"math"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/recolabs/gnata"
)

const (
	// DefaultTimeout is the Step Functions evaluation cap.
	DefaultTimeout = time.Second
	// DefaultMemory is the approximate allocation budget for one evaluation.
	// The watchdog reads process-wide heap growth, so the cap is not a private
	// heap for the expression.
	DefaultMemory = 32 << 20
	// maxRange is how many numbers $range will build before reporting the
	// memory limit. It is not the intrinsic 1000-element cap.
	maxRange = 1_000_000
)

// Limits is the per-evaluation budget. Zero values use the defaults.
type Limits struct {
	Timeout     time.Duration
	MemoryBytes uint64
}

// EvalError is a JSONata failure. The runner records it as
// States.QueryEvaluationError.
type EvalError struct {
	Cause string
}

func (e *EvalError) Error() string {
	if e == nil {
		return ""
	}
	return e.Cause
}

// Expr evaluates one expression. Bindings are JSONata variables without the
// leading "$": $states is bindings["states"] and $x is bindings["x"]. input is
// the "$" document and is not flattened into the bindings.
type Expr interface {
	Eval(ctx context.Context, expression string, input any, bindings map[string]any, limits Limits) (any, error)
}

// New returns the process-wide evaluator. The custom-function environment is
// built once and shared. Each call compiles its own expression.
func New() Expr { return builtin{} }

type builtin struct{}

var sfEnv = gnata.NewCustomEnvironment(map[string]gnata.CustomFunc{
	"eval":      evalDisabled,
	"partition": fnPartition,
	"range":     fnRange,
	"hash":      fnHash,
	"random":    fnRandom,
	"uuid":      fnUUID,
	"parse":     fnParse,
})

func (builtin) Eval(ctx context.Context, expression string, input any, bindings map[string]any, limits Limits) (any, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	timeout := limits.Timeout
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	mem := limits.MemoryBytes
	if mem == 0 {
		mem = DefaultMemory
	}
	expr, err := gnata.Compile(expression)
	if err != nil {
		return nil, &EvalError{Cause: err.Error()}
	}
	input, err = prepareInput(input)
	if err != nil {
		return nil, err
	}
	bindings, err = prepareBindings(bindings)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var tripped atomic.Bool
	done := make(chan struct{})
	defer close(done)
	go watchMemory(done, cancel, &tripped, mem)

	var result any
	func() {
		defer func() {
			if rec := recover(); rec != nil {
				err = &EvalError{Cause: fmt.Sprintf("JSONata evaluation exceeded the stack limit (%v)", rec)}
			}
		}()
		result, err = expr.EvalWithCustomEnvironmentAndVars(ctx, input, sfEnv, bindings)
	}()
	if err != nil {
		var qe *EvalError
		if errors.As(err, &qe) {
			return nil, qe
		}
		if tripped.Load() {
			return nil, &EvalError{Cause: "JSONata evaluation exceeded the memory limit"}
		}
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return nil, &EvalError{Cause: "JSONata evaluation exceeded 1s"}
		}
		return nil, &EvalError{Cause: err.Error()}
	}
	if result == nil {
		return nil, &EvalError{Cause: "the JSONata expression returned undefined"}
	}
	if gnata.IsNull(result) {
		return nil, nil
	}
	return normalize(result)
}

// prepareInput turns runner numbers (int64 from JSON) into the float64 form
// the evaluator returns, so a variable and an input field compare as numbers.
func prepareInput(v any) (any, error) {
	if v == nil {
		return nil, nil
	}
	return normalize(v)
}

func prepareBindings(bindings map[string]any) (map[string]any, error) {
	if len(bindings) == 0 {
		return bindings, nil
	}
	out := make(map[string]any, len(bindings))
	for key, val := range bindings {
		next, err := prepareInput(val)
		if err != nil {
			return nil, err
		}
		out[key] = next
	}
	return out, nil
}

// watchMemory cancels the evaluation when the process heap grows by limit
// bytes after the call starts. The measurement is process-wide and sampled,
// so a busy neighbor can trip it and a short spike can be missed.
func watchMemory(done <-chan struct{}, cancel context.CancelFunc, tripped *atomic.Bool, limit uint64) {
	var base runtime.MemStats
	runtime.ReadMemStats(&base)
	start := base.Alloc
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-done:
			return
		case <-tick.C:
			var now runtime.MemStats
			runtime.ReadMemStats(&now)
			if now.Alloc > start && now.Alloc-start > limit {
				tripped.Store(true)
				cancel()
				return
			}
		}
	}
}

func normalize(v any) (any, error) {
	switch v.(type) {
	case string, bool, float64:
		return v, nil
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, &EvalError{Cause: err.Error()}
	}
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.UseNumber()
	var out any
	if err := dec.Decode(&out); err != nil {
		return nil, &EvalError{Cause: err.Error()}
	}
	return plainNumbers(out), nil
}

func plainNumbers(v any) any {
	switch n := v.(type) {
	case json.Number:
		if i, err := n.Int64(); err == nil {
			return float64(i)
		}
		f, err := n.Float64()
		if err != nil {
			return n.String()
		}
		return f
	case map[string]any:
		for k, child := range n {
			n[k] = plainNumbers(child)
		}
		return n
	case []any:
		for i, child := range n {
			n[i] = plainNumbers(child)
		}
		return n
	default:
		return v
	}
}

func evalDisabled(args []any, _ any) (any, error) {
	return nil, errors.New("$eval is not available")
}

func fnPartition(args []any, _ any) (any, error) {
	if len(args) != 2 {
		return nil, errors.New("$partition expects an array and a size")
	}
	arr, ok := asArray(args[0])
	if !ok {
		return nil, errors.New("$partition expects an array")
	}
	size, ok := floorInt(args[1])
	if !ok || size < 1 {
		return nil, errors.New("$partition size must be an integer >= 1")
	}
	out := make([]any, 0, (len(arr)+size-1)/size)
	for i := 0; i < len(arr); i += size {
		j := i + size
		if j > len(arr) {
			j = len(arr)
		}
		out = append(out, append([]any(nil), arr[i:j]...))
	}
	return out, nil
}

func fnRange(args []any, _ any) (any, error) {
	if len(args) != 3 {
		return nil, errors.New("$range expects start, end, and step")
	}
	start, ok1 := floorInt(args[0])
	end, ok2 := floorInt(args[1])
	step, ok3 := floorInt(args[2])
	if !ok1 || !ok2 || !ok3 {
		return nil, errors.New("$range arguments must be numbers")
	}
	if step == 0 {
		return nil, errors.New("$range step must not be 0")
	}
	if (step > 0 && start > end) || (step < 0 && start < end) {
		return []any{}, nil
	}
	n := (end-start)/step + 1
	if n < 0 {
		n = 0
	}
	if n > maxRange {
		return nil, errors.New("$range exceeds the memory limit")
	}
	out := make([]any, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, float64(start+i*step))
	}
	return out, nil
}

func fnHash(args []any, _ any) (any, error) {
	if len(args) != 2 {
		return nil, errors.New("$hash expects data and an algorithm")
	}
	data, ok := args[0].(string)
	if !ok {
		return nil, errors.New("$hash data must be a string")
	}
	algo, ok := args[1].(string)
	if !ok {
		return nil, errors.New("$hash algorithm must be a string")
	}
	var h hash.Hash
	switch algo {
	case "MD5":
		h = md5.New()
	case "SHA-1":
		h = sha1.New()
	case "SHA-256":
		h = sha256.New()
	case "SHA-384":
		h = sha512.New384()
	case "SHA-512":
		h = sha512.New()
	default:
		return nil, errors.New("$hash algorithm " + algo + " is not supported")
	}
	_, _ = io.WriteString(h, data)
	return hex.EncodeToString(h.Sum(nil)), nil
}

func fnRandom(args []any, _ any) (any, error) {
	switch len(args) {
	case 0:
		var b [8]byte
		if _, err := rand.Read(b[:]); err != nil {
			return nil, err
		}
		return unitFloat(binary.BigEndian.Uint64(b[:])), nil
	case 1:
		f, ok := asNumber(args[0])
		if !ok || math.IsNaN(f) || math.IsInf(f, 0) {
			return nil, errors.New("$random seed must be a number")
		}
		sum := sha256.Sum256([]byte(strconv.FormatFloat(f, 'f', -1, 64)))
		return unitFloat(binary.BigEndian.Uint64(sum[:8])), nil
	default:
		return nil, errors.New("$random accepts at most one argument")
	}
}

// SeededRandom is the value $random(seed) returns. Tests record it.
func SeededRandom(seed float64) float64 {
	sum := sha256.Sum256([]byte(strconv.FormatFloat(seed, 'f', -1, 64)))
	return unitFloat(binary.BigEndian.Uint64(sum[:8]))
}

func unitFloat(u uint64) float64 {
	return float64(u>>11) / (1 << 53)
}

func fnUUID(args []any, _ any) (any, error) {
	if len(args) != 0 {
		return nil, errors.New("$uuid accepts no arguments")
	}
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return nil, err
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}

func fnParse(args []any, _ any) (any, error) {
	if len(args) != 1 {
		return nil, errors.New("$parse expects one string")
	}
	text, ok := args[0].(string)
	if !ok {
		return nil, errors.New("$parse expects a string")
	}
	dec := json.NewDecoder(strings.NewReader(text))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, errors.New("$parse: invalid JSON")
	}
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, errors.New("$parse: trailing data")
	}
	return plainNumbers(v), nil
}

func asArray(v any) ([]any, bool) {
	arr, ok := v.([]any)
	return arr, ok
}

func asNumber(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	default:
		return 0, false
	}
}

func floorInt(v any) (int, bool) {
	f, ok := asNumber(v)
	if !ok || math.IsNaN(f) || math.IsInf(f, 0) {
		return 0, false
	}
	f = math.Floor(f)
	if f > math.MaxInt || f < math.MinInt {
		return 0, false
	}
	return int(f), true
}
