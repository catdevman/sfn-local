// Package intrinsic evaluates the eighteen States.* functions the JSONPath
// runner accepts. A bad call is reported as a plain error. The caller maps
// that to States.Runtime. There is no States.IntrinsicFailure.
package intrinsic

import (
	"bytes"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"math"
	"strings"
	"unicode/utf16"
	"unicode/utf8"
)

// Lookup resolves a JSONPath argument ($ or $$).
type Lookup func(path string) (any, error)

type callError struct{ msg string }

func (e *callError) Error() string {
	if e == nil {
		return ""
	}
	return e.msg
}

func fail(msg string) error { return &callError{msg: msg} }

// Eval evaluates one States.* call. lookup may be nil when no path is used.
func Eval(expr string, lookup Lookup) (any, error) {
	p := &parser{s: strings.TrimSpace(expr), lookup: lookup}
	val, err := p.parseCall()
	if err != nil {
		return nil, err
	}
	p.skip()
	if p.i != len(p.s) {
		return nil, fail("trailing input after intrinsic call")
	}
	return val, nil
}

type parser struct {
	s      string
	i      int
	lookup Lookup
}

func (p *parser) skip() {
	for p.i < len(p.s) && (p.s[p.i] == ' ' || p.s[p.i] == '\t' || p.s[p.i] == '\n' || p.s[p.i] == '\r') {
		p.i++
	}
}

func (p *parser) parseCall() (any, error) {
	p.skip()
	const prefix = "States."
	if !strings.HasPrefix(p.s[p.i:], prefix) {
		return nil, fail("intrinsic call must start with States.")
	}
	p.i += len(prefix)
	start := p.i
	for p.i < len(p.s) && isName(p.s[p.i]) {
		p.i++
	}
	name := p.s[start:p.i]
	if name == "" {
		return nil, fail("missing intrinsic name")
	}
	p.skip()
	if p.i >= len(p.s) || p.s[p.i] != '(' {
		return nil, fail("intrinsic call is missing '('")
	}
	p.i++
	var args []any
	p.skip()
	if p.i < len(p.s) && p.s[p.i] == ')' {
		p.i++
		return dispatch(name, args)
	}
	for {
		arg, err := p.parseArg()
		if err != nil {
			return nil, err
		}
		args = append(args, arg)
		p.skip()
		if p.i >= len(p.s) {
			return nil, fail("unclosed intrinsic call")
		}
		switch p.s[p.i] {
		case ',':
			p.i++
			p.skip()
			if p.i < len(p.s) && p.s[p.i] == ')' {
				return nil, fail("trailing comma in intrinsic call")
			}
		case ')':
			p.i++
			return dispatch(name, args)
		default:
			return nil, fail("expected ',' or ')' in intrinsic call")
		}
	}
}

func isName(c byte) bool {
	return c == '_' || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')
}

func (p *parser) parseArg() (any, error) {
	p.skip()
	if p.i >= len(p.s) {
		return nil, fail("missing intrinsic argument")
	}
	switch p.s[p.i] {
	case '\'':
		return p.parseSingle()
	case '"', '{', '[', 't', 'f', 'n', '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
		return p.parseJSON()
	}
	if strings.HasPrefix(p.s[p.i:], "States.") {
		return p.parseCall()
	}
	if p.s[p.i] == '$' {
		return p.parsePath()
	}
	return nil, fail("invalid intrinsic argument")
}

func (p *parser) parseSingle() (string, error) {
	p.i++
	var b strings.Builder
	for p.i < len(p.s) {
		c := p.s[p.i]
		if c == '\\' {
			p.i++
			if p.i >= len(p.s) {
				return "", fail("unterminated string")
			}
			b.WriteByte(p.s[p.i])
			p.i++
			continue
		}
		if c == '\'' {
			p.i++
			return b.String(), nil
		}
		b.WriteByte(c)
		p.i++
	}
	return "", fail("unterminated string")
}

func (p *parser) parseJSON() (any, error) {
	dec := json.NewDecoder(strings.NewReader(p.s[p.i:]))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, fail("invalid JSON argument")
	}
	p.i += int(dec.InputOffset())
	return norm(v), nil
}

func (p *parser) parsePath() (any, error) {
	start := p.i
	depth := 0
	var quote byte
	for p.i < len(p.s) {
		c := p.s[p.i]
		if quote != 0 {
			if c == '\\' && p.i+1 < len(p.s) {
				p.i += 2
				continue
			}
			if c == quote {
				quote = 0
			}
			p.i++
			continue
		}
		switch c {
		case '\'', '"':
			quote = c
			p.i++
		case '[':
			depth++
			p.i++
		case ']':
			if depth > 0 {
				depth--
			}
			p.i++
		case ',', ')':
			if depth == 0 {
				path := strings.TrimSpace(p.s[start:p.i])
				if p.lookup == nil {
					return nil, fail("path " + path + " cannot be resolved")
				}
				return p.lookup(path)
			}
			p.i++
		default:
			p.i++
		}
	}
	return nil, fail("unclosed path argument")
}

func norm(v any) any {
	switch x := v.(type) {
	case json.Number:
		if i, err := x.Int64(); err == nil {
			return i
		}
		f, err := x.Float64()
		if err != nil {
			return x.String()
		}
		return f
	case map[string]any:
		for k, child := range x {
			x[k] = norm(child)
		}
		return x
	case []any:
		for i, child := range x {
			x[i] = norm(child)
		}
		return x
	default:
		return v
	}
}

func dispatch(name string, args []any) (any, error) {
	switch name {
	case "Format":
		return fnFormat(args)
	case "StringToJson":
		return fnStringToJson(args)
	case "JsonToString":
		return fnJsonToString(args)
	case "Array":
		if args == nil {
			return []any{}, nil
		}
		return args, nil
	case "ArrayPartition":
		return fnArrayPartition(args)
	case "ArrayContains":
		return fnArrayContains(args)
	case "ArrayRange":
		return fnArrayRange(args)
	case "ArrayGetItem":
		return fnArrayGetItem(args)
	case "ArrayLength":
		return fnArrayLength(args)
	case "ArrayUnique":
		return fnArrayUnique(args)
	case "Base64Encode":
		return fnBase64(args, true)
	case "Base64Decode":
		return fnBase64(args, false)
	case "Hash":
		return fnHash(args)
	case "JsonMerge":
		return fnJsonMerge(args)
	case "MathAdd":
		return fnMathAdd(args)
	case "MathRandom":
		return fnMathRandom(args)
	case "StringSplit":
		return fnStringSplit(args)
	case "UUID":
		return fnUUID()
	default:
		return nil, fail("unknown intrinsic function States." + name)
	}
}

func fnFormat(args []any) (any, error) {
	if len(args) < 1 {
		return nil, fail("States.Format requires a template")
	}
	tmpl, ok := args[0].(string)
	if !ok {
		return nil, fail("States.Format template must be a string")
	}
	var b strings.Builder
	vi := 0
	vals := args[1:]
	for i := 0; i < len(tmpl); i++ {
		switch tmpl[i] {
		case '\\':
			if i+1 < len(tmpl) {
				b.WriteByte(tmpl[i+1])
				i++
			}
		case '}':
			return nil, fail("States.Format has a bare '}'")
		case '{':
			if i+1 >= len(tmpl) || tmpl[i+1] != '}' {
				return nil, fail("States.Format placeholder must be '{}'")
			}
			if vi >= len(vals) {
				return nil, fail("States.Format is missing a value")
			}
			text, err := formatValue(vals[vi])
			if err != nil {
				return nil, err
			}
			b.WriteString(text)
			vi++
			i++
		default:
			b.WriteByte(tmpl[i])
		}
	}
	if vi != len(vals) {
		return nil, fail("States.Format has too many values")
	}
	return b.String(), nil
}

func formatValue(v any) (string, error) {
	switch x := v.(type) {
	case string:
		return x, nil
	case bool:
		if x {
			return "true", nil
		}
		return "false", nil
	case nil:
		return "null", nil
	default:
		return compact(v)
	}
}

func fnStringToJson(args []any) (any, error) {
	if len(args) != 1 {
		return nil, fail("States.StringToJson requires one argument")
	}
	text, ok := args[0].(string)
	if !ok {
		return nil, fail("States.StringToJson requires a string")
	}
	dec := json.NewDecoder(strings.NewReader(text))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, fail("States.StringToJson input is not JSON")
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return nil, fail("States.StringToJson input has trailing tokens")
	}
	return norm(v), nil
}

func fnJsonToString(args []any) (any, error) {
	if len(args) != 1 {
		return nil, fail("States.JsonToString requires one argument")
	}
	return compact(args[0])
}

func fnArrayPartition(args []any) (any, error) {
	if len(args) != 2 {
		return nil, fail("States.ArrayPartition requires two arguments")
	}
	arr, ok := args[0].([]any)
	if !ok {
		return nil, fail("States.ArrayPartition requires an array")
	}
	chunk, ok := truncInt32(args[1])
	if !ok || chunk < 1 {
		return nil, fail("States.ArrayPartition chunk size must be an integer >= 1")
	}
	if len(arr) == 0 {
		return []any{}, nil
	}
	n := int(chunk)
	out := make([]any, 0, (len(arr)+n-1)/n)
	for i := 0; i < len(arr); i += n {
		end := i + n
		if end > len(arr) {
			end = len(arr)
		}
		part := make([]any, end-i)
		copy(part, arr[i:end])
		out = append(out, part)
	}
	return out, nil
}

func fnArrayContains(args []any) (any, error) {
	if len(args) != 2 {
		return nil, fail("States.ArrayContains requires two arguments")
	}
	arr, ok := args[0].([]any)
	if !ok {
		return nil, fail("States.ArrayContains requires an array")
	}
	wantNil := args[1] == nil
	want, wantOK := textForm(args[1])
	for _, el := range arr {
		if el == nil || wantNil {
			if el == nil && wantNil {
				return true, nil
			}
			continue
		}
		got, ok := textForm(el)
		if ok && wantOK && got == want {
			return true, nil
		}
	}
	return false, nil
}

func textForm(v any) (string, bool) {
	if v == nil {
		return "", false
	}
	if s, ok := v.(string); ok {
		return s, true
	}
	text, err := compact(v)
	if err != nil {
		return "", false
	}
	return text, true
}

func fnArrayRange(args []any) (any, error) {
	if len(args) != 3 {
		return nil, fail("States.ArrayRange requires three arguments")
	}
	start, ok1 := truncInt32(args[0])
	end, ok2 := truncInt32(args[1])
	step, ok3 := truncInt32(args[2])
	if !ok1 || !ok2 || !ok3 {
		return nil, fail("States.ArrayRange arguments must be inside signed 32-bit range")
	}
	if step == 0 {
		return nil, fail("States.ArrayRange step must not be 0")
	}
	// (end-start)/step is 32-bit integer division toward zero.
	quot := (end - start) / step
	sizef := math.Ceil(0.9999 + float64(quot))
	if sizef > 1000 {
		return nil, fail("States.ArrayRange exceeds 1000 elements")
	}
	size := int(sizef)
	if size <= 0 {
		return []any{}, nil
	}
	out := make([]any, size)
	for i := 0; i < size; i++ {
		out[i] = int64(start) + int64(i)*int64(step)
	}
	return out, nil
}

func fnArrayGetItem(args []any) (any, error) {
	if len(args) != 2 {
		return nil, fail("States.ArrayGetItem requires two arguments")
	}
	arr, ok := args[0].([]any)
	if !ok {
		return nil, fail("States.ArrayGetItem requires an array")
	}
	index, ok := truncInt32(args[1])
	if !ok {
		return nil, fail("States.ArrayGetItem index must be a 32-bit integer")
	}
	n := int(index)
	if n < 0 || n == len(arr) {
		return nil, fail("States.ArrayGetItem index is out of range")
	}
	if n > len(arr) {
		return nil, nil
	}
	return arr[n], nil
}

func fnArrayLength(args []any) (any, error) {
	if len(args) != 1 {
		return nil, fail("States.ArrayLength requires one argument")
	}
	arr, ok := args[0].([]any)
	if !ok {
		return nil, fail("States.ArrayLength requires an array")
	}
	return int64(len(arr)), nil
}

func fnArrayUnique(args []any) (any, error) {
	if len(args) != 1 {
		return nil, fail("States.ArrayUnique requires one argument")
	}
	arr, ok := args[0].([]any)
	if !ok {
		return nil, fail("States.ArrayUnique requires an array")
	}
	seen := map[string]bool{}
	out := make([]any, 0, len(arr))
	for _, el := range arr {
		key, err := compact(el)
		if err != nil {
			return nil, err
		}
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, el)
	}
	return out, nil
}

func fnBase64(args []any, encode bool) (any, error) {
	label := "States.Base64Decode"
	if encode {
		label = "States.Base64Encode"
	}
	if len(args) != 1 {
		return nil, fail(label + " requires one string")
	}
	text, ok := args[0].(string)
	if !ok {
		return nil, fail(label + " requires a string")
	}
	if utf16Len(text) > 10000 {
		return nil, fail(label + " exceeds 10000 UTF-16 code units")
	}
	if encode {
		return base64.StdEncoding.EncodeToString([]byte(text)), nil
	}
	raw, err := base64.StdEncoding.DecodeString(text)
	if err != nil || !utf8.Valid(raw) {
		return nil, fail(label + " input is not valid Base64")
	}
	return string(raw), nil
}

func fnHash(args []any) (any, error) {
	if len(args) != 2 {
		return nil, fail("States.Hash requires two arguments")
	}
	algo, ok := args[1].(string)
	if !ok {
		return nil, fail("States.Hash algorithm must be a string")
	}
	text, err := valueText(args[0])
	if err != nil {
		return nil, err
	}
	if utf16Len(text) > 10000 {
		return nil, fail("States.Hash data exceeds 10000 UTF-16 code units")
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
		return nil, fail("States.Hash algorithm is not supported")
	}
	_, _ = h.Write([]byte(text))
	return hex.EncodeToString(h.Sum(nil)), nil
}

func valueText(v any) (string, error) {
	if s, ok := v.(string); ok {
		return s, nil
	}
	return compact(v)
}

func fnJsonMerge(args []any) (any, error) {
	if len(args) != 3 {
		return nil, fail("States.JsonMerge requires three arguments")
	}
	flag, err := compact(args[2])
	if err != nil {
		return nil, err
	}
	if flag == "true" {
		return nil, fail("States.JsonMerge does not support deep merge")
	}
	left, ok1 := args[0].(map[string]any)
	right, ok2 := args[1].(map[string]any)
	if !ok1 || !ok2 {
		return nil, fail("States.JsonMerge requires two objects")
	}
	out := make(map[string]any, len(left)+len(right))
	for k, v := range left {
		out[k] = v
	}
	for k, v := range right {
		out[k] = v
	}
	return out, nil
}

func fnMathAdd(args []any) (any, error) {
	if len(args) != 2 {
		return nil, fail("States.MathAdd requires two numbers")
	}
	x, ok1 := asFloat(args[0])
	y, ok2 := asFloat(args[1])
	if !ok1 || !ok2 {
		return nil, fail("States.MathAdd requires two numbers")
	}
	if x < math.MinInt32 || x > math.MaxInt32 || y < math.MinInt32 || y > math.MaxInt32 {
		return nil, fail("States.MathAdd argument is outside signed 32-bit range")
	}
	a, ok1 := roundHalfPosInf(x)
	b, ok2 := roundHalfPosInf(y)
	if !ok1 || !ok2 {
		return nil, fail("States.MathAdd argument is outside signed 32-bit range")
	}
	sum := int64(a) + int64(b)
	if sum > math.MaxInt32 || sum < math.MinInt32 {
		return nil, fail("States.MathAdd sum overflows")
	}
	return sum, nil
}

func fnMathRandom(args []any) (any, error) {
	if len(args) != 2 && len(args) != 3 {
		return nil, fail("States.MathRandom requires two or three arguments")
	}
	min, ok1 := truncInt32(args[0])
	max, ok2 := truncInt32(args[1])
	if !ok1 || !ok2 || min >= max {
		return nil, fail("States.MathRandom bounds are invalid")
	}
	span := int64(max) - int64(min)
	if span <= 0 || span > math.MaxInt32 {
		return nil, fail("States.MathRandom bounds are invalid")
	}
	var seed int64
	if len(args) == 3 {
		s, ok := asInt64(args[2])
		if !ok {
			return nil, fail("States.MathRandom seed must be an integer")
		}
		seed = s
	} else {
		var buf [8]byte
		if _, err := rand.Read(buf[:]); err != nil {
			return nil, fail("States.MathRandom could not draw a seed")
		}
		seed = int64(binary.BigEndian.Uint64(buf[:]))
	}
	n := newJavaRandom(seed).nextInt(int32(span))
	return int64(n) + int64(min), nil
}

func fnStringSplit(args []any) (any, error) {
	if len(args) != 2 {
		return nil, fail("States.StringSplit requires two strings")
	}
	text, ok1 := args[0].(string)
	sep, ok2 := args[1].(string)
	if !ok1 || !ok2 {
		return nil, fail("States.StringSplit requires two strings")
	}
	set := map[rune]bool{}
	for _, r := range sep {
		set[r] = true
	}
	out := []any{}
	var b strings.Builder
	flush := func() {
		if b.Len() == 0 {
			return
		}
		out = append(out, b.String())
		b.Reset()
	}
	for _, r := range text {
		if set[r] {
			flush()
			continue
		}
		b.WriteRune(r)
	}
	flush()
	return out, nil
}

func fnUUID() (any, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return nil, fail("States.UUID could not draw random bytes")
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}

func truncInt32(v any) (int32, bool) {
	f, ok := asFloat(v)
	if !ok || f < math.MinInt32 || f > math.MaxInt32 {
		return 0, false
	}
	return int32(f), true
}

func asFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case float64:
		return n, true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	default:
		return 0, false
	}
}

func asInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case int:
		return int64(n), true
	case int32:
		return int64(n), true
	case int64:
		return n, true
	case float64:
		if math.IsNaN(n) || math.IsInf(n, 0) || n < math.MinInt64 || n > math.MaxInt64 {
			return 0, false
		}
		return int64(n), true
	default:
		return 0, false
	}
}

func roundHalfPosInf(f float64) (int32, bool) {
	r := math.Floor(f + 0.5)
	if r > math.MaxInt32 || r < math.MinInt32 {
		return 0, false
	}
	return int32(r), true
}

func utf16Len(s string) int {
	return len(utf16.Encode([]rune(s)))
}

func compact(v any) (string, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return "", fail("value is not JSON")
	}
	return strings.TrimSpace(buf.String()), nil
}
