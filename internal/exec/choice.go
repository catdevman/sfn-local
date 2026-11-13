package exec

import (
	"bytes"
	"encoding/json"
	"strings"
	"time"
	"unicode/utf16"

	"github.com/catdevman/sfn-local/internal/asl"
	"github.com/catdevman/sfn-local/internal/jsonpath"
)

type choiceRule struct {
	Next      string
	Condition string
	Variable  string
	Op        string
	Operand   json.RawMessage
	Assign    json.RawMessage
	And       []choiceRule
	Or        []choiceRule
	Not       *choiceRule
}

func choose(st *asl.State, root, ctx any, vars map[string]any) (string, json.RawMessage, bool, error) {
	rules, err := decodeRuleList(st.Choices)
	if err != nil {
		return "", nil, false, err
	}
	for _, rule := range rules {
		ok, err := evalRule(rule, root, ctx, vars)
		if err != nil {
			return "", nil, false, err
		}
		if ok {
			return rule.Next, rule.Assign, true, nil
		}
	}
	if st.HasDefault {
		return st.Default, nil, false, nil
	}
	return "", nil, false, jsonpath.Runtime("Failed to transition out of the state")
}

func evalRule(rule choiceRule, root, ctx any, vars map[string]any) (bool, error) {
	if rule.And != nil {
		for _, child := range rule.And {
			ok, err := evalRule(child, root, ctx, vars)
			if err != nil || !ok {
				return ok, err
			}
		}
		return true, nil
	}
	if rule.Or != nil {
		for _, child := range rule.Or {
			ok, err := evalRule(child, root, ctx, vars)
			if err != nil || ok {
				return ok, err
			}
		}
		return false, nil
	}
	if rule.Not != nil {
		ok, err := evalRule(*rule.Not, root, ctx, vars)
		if err != nil {
			return false, err
		}
		return !ok, nil
	}
	return evalCompare(rule, root, ctx, vars)
}

func evalCompare(rule choiceRule, root, ctx any, vars map[string]any) (bool, error) {
	op := rule.Op
	operand, err := decodeOperand(rule.Operand)
	if err != nil {
		return false, jsonpath.Runtime(op + " operand is not JSON")
	}
	if strings.HasSuffix(op, "Path") {
		text, ok := operand.(string)
		if !ok {
			return false, jsonpath.Runtime(op + " must be a path")
		}
		val, found, err := jsonpath.GetBound(text, root, ctx, vars)
		if err != nil {
			return false, err
		}
		if !found {
			return false, missingPath(text)
		}
		operand = val
		op = strings.TrimSuffix(op, "Path")
	}
	left, found, err := jsonpath.GetBound(rule.Variable, root, ctx, vars)
	if err != nil {
		return false, err
	}
	if strings.HasPrefix(op, "Is") {
		return evalIs(op, left, found, operand)
	}
	if !found {
		return false, missingPath(rule.Variable)
	}
	return compareValues(op, left, operand)
}

func evalIs(op string, left any, found bool, right any) (bool, error) {
	want, ok := right.(bool)
	if !ok {
		return false, jsonpath.Runtime(op + " operand must be a boolean")
	}
	if op == "IsPresent" {
		return found == want, nil
	}
	if !found {
		return false, jsonpath.Runtime("path was not found")
	}
	var pred bool
	switch op {
	case "IsNull":
		pred = left == nil
	case "IsNumeric":
		_, pred = asFloat(left)
	case "IsString":
		_, pred = left.(string)
	case "IsBoolean":
		_, pred = left.(bool)
	case "IsTimestamp":
		text, isString := left.(string)
		_, parsed := parseOffsetTime(text)
		pred = isString && parsed
	default:
		return false, jsonpath.Runtime("unknown comparator " + op)
	}
	return pred == want, nil
}

func compareValues(op string, left, right any) (bool, error) {
	switch op {
	case "StringEquals", "StringLessThan", "StringGreaterThan", "StringLessThanEquals", "StringGreaterThanEquals":
		return cmpString(op, left, right), nil
	case "StringMatches":
		pat, ok1 := right.(string)
		val, ok2 := left.(string)
		if !ok1 || !ok2 {
			return false, nil
		}
		return stringMatches(pat, val), nil
	case "NumericEquals", "NumericLessThan", "NumericGreaterThan", "NumericLessThanEquals", "NumericGreaterThanEquals":
		return cmpNumber(op, left, right), nil
	case "BooleanEquals":
		lb, ok1 := left.(bool)
		rb, ok2 := right.(bool)
		if !ok1 || !ok2 {
			return false, nil
		}
		return lb == rb, nil
	case "TimestampEquals", "TimestampLessThan", "TimestampGreaterThan", "TimestampLessThanEquals", "TimestampGreaterThanEquals":
		return cmpTimestamp(op, left, right), nil
	default:
		return false, jsonpath.Runtime("unknown comparator " + op)
	}
}

func cmpString(op string, left, right any) bool {
	ls, ok1 := left.(string)
	rs, ok2 := right.(string)
	if !ok1 || !ok2 {
		return false
	}
	c := cmpUTF16(ls, rs)
	switch op {
	case "StringEquals":
		return c == 0
	case "StringLessThan":
		return c < 0
	case "StringGreaterThan":
		return c > 0
	case "StringLessThanEquals":
		return c <= 0
	case "StringGreaterThanEquals":
		return c >= 0
	default:
		return false
	}
}

func cmpNumber(op string, left, right any) bool {
	lf, ok1 := asFloat(left)
	rf, ok2 := asFloat(right)
	if !ok1 || !ok2 {
		return false
	}
	switch op {
	case "NumericEquals":
		return lf == rf
	case "NumericLessThan":
		return lf < rf
	case "NumericGreaterThan":
		return lf > rf
	case "NumericLessThanEquals":
		return lf <= rf
	case "NumericGreaterThanEquals":
		return lf >= rf
	default:
		return false
	}
}

func cmpTimestamp(op string, left, right any) bool {
	ls, ok1 := left.(string)
	rs, ok2 := right.(string)
	if !ok1 || !ok2 {
		return false
	}
	lt, ok1 := parseOffsetTime(ls)
	rt, ok2 := parseOffsetTime(rs)
	if !ok1 || !ok2 {
		return false
	}
	switch op {
	case "TimestampEquals":
		return lt.Equal(rt)
	case "TimestampLessThan":
		return lt.Before(rt)
	case "TimestampGreaterThan":
		return lt.After(rt)
	case "TimestampLessThanEquals":
		return !lt.After(rt)
	case "TimestampGreaterThanEquals":
		return !lt.Before(rt)
	default:
		return false
	}
}

func asFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int64:
		return float64(n), true
	case int:
		return float64(n), true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	default:
		return 0, false
	}
}

func cmpUTF16(a, b string) int {
	ua := utf16.Encode([]rune(a))
	ub := utf16.Encode([]rune(b))
	n := len(ua)
	if len(ub) < n {
		n = len(ub)
	}
	for i := 0; i < n; i++ {
		if ua[i] != ub[i] {
			if ua[i] < ub[i] {
				return -1
			}
			return 1
		}
	}
	switch {
	case len(ua) < len(ub):
		return -1
	case len(ua) > len(ub):
		return 1
	default:
		return 0
	}
}

func stringMatches(pattern, value string) bool {
	return glob(utf16.Encode([]rune(pattern)), utf16.Encode([]rune(value)))
}

func glob(pat, val []uint16) bool {
	for {
		if len(pat) == 0 {
			return len(val) == 0
		}
		if pat[0] == '\\' && len(pat) >= 2 && pat[1] == '*' {
			if len(val) == 0 || val[0] != '*' {
				return false
			}
			pat = pat[2:]
			val = val[1:]
			continue
		}
		if pat[0] != '*' {
			if len(val) == 0 || pat[0] != val[0] {
				return false
			}
			pat = pat[1:]
			val = val[1:]
			continue
		}
		pat = pat[1:]
		for {
			if glob(pat, val) {
				return true
			}
			if len(val) == 0 {
				return false
			}
			val = val[1:]
		}
	}
}

func parseOffsetTime(s string) (time.Time, bool) {
	if s == "" {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

func decodeOperand(raw json.RawMessage) (any, error) {
	if len(raw) == 0 {
		return nil, jsonpath.Runtime("missing operand")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	return normalize(v), nil
}

func decodeRuleList(raw json.RawMessage) ([]choiceRule, error) {
	var arr []json.RawMessage
	if err := json.Unmarshal(raw, &arr); err != nil {
		return nil, jsonpath.Runtime("Choices must be an array")
	}
	out := make([]choiceRule, len(arr))
	for i, item := range arr {
		rule, err := decodeRule(item)
		if err != nil {
			return nil, err
		}
		out[i] = rule
	}
	return out, nil
}

func decodeRule(raw json.RawMessage) (choiceRule, error) {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return choiceRule{}, jsonpath.Runtime("Choice rule must be an object")
	}
	var rule choiceRule
	if raw, ok := obj["Next"]; ok {
		if err := json.Unmarshal(raw, &rule.Next); err != nil {
			return choiceRule{}, jsonpath.Runtime("Choice Next must be a string")
		}
	}
	if raw, ok := obj["Variable"]; ok {
		if err := json.Unmarshal(raw, &rule.Variable); err != nil {
			return choiceRule{}, jsonpath.Runtime("Choice Variable must be a string")
		}
	}
	if raw, ok := obj["Condition"]; ok {
		if err := json.Unmarshal(raw, &rule.Condition); err != nil {
			return choiceRule{}, jsonpath.Runtime("Choice Condition must be a string")
		}
	}
	if raw, ok := obj["Assign"]; ok {
		rule.Assign = append(json.RawMessage(nil), raw...)
	}
	for _, name := range comparatorOrder {
		if raw, ok := obj[name]; ok {
			rule.Op = name
			rule.Operand = append(json.RawMessage(nil), raw...)
			break
		}
	}
	if raw, ok := obj["And"]; ok {
		kids, err := decodeRuleList(raw)
		if err != nil {
			return choiceRule{}, err
		}
		rule.And = kids
	}
	if raw, ok := obj["Or"]; ok {
		kids, err := decodeRuleList(raw)
		if err != nil {
			return choiceRule{}, err
		}
		rule.Or = kids
	}
	if raw, ok := obj["Not"]; ok {
		child, err := decodeRule(raw)
		if err != nil {
			return choiceRule{}, err
		}
		rule.Not = &child
	}
	return rule, nil
}

var comparatorOrder = []string{
	"StringEquals", "StringLessThan", "StringGreaterThan", "StringLessThanEquals", "StringGreaterThanEquals",
	"NumericEquals", "NumericLessThan", "NumericGreaterThan", "NumericLessThanEquals", "NumericGreaterThanEquals",
	"BooleanEquals",
	"TimestampEquals", "TimestampLessThan", "TimestampGreaterThan", "TimestampLessThanEquals", "TimestampGreaterThanEquals",
	"StringMatches",
	"IsNull", "IsPresent", "IsNumeric", "IsString", "IsBoolean", "IsTimestamp",
}

func init() {
	base := append([]string(nil), comparatorOrder...)
	comparatorOrder = comparatorOrder[:0]
	for _, name := range base {
		comparatorOrder = append(comparatorOrder, name)
		if !strings.HasPrefix(name, "Is") {
			comparatorOrder = append(comparatorOrder, name+"Path")
		}
	}
}
