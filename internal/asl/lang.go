package asl

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/catdevman/sfn-local/internal/jsonpath"
)

const (
	langJSONPath = "JSONPath"
	langJSONata  = "JSONata"
)

var (
	directResult = regexp.MustCompile(`\$states\s*\.\s*result\b`)
	directError  = regexp.MustCompile(`\$states\s*\.\s*errorOutput\b`)
	evalCall     = regexp.MustCompile(`\$eval\b`)
)

func readQueryLanguage(raw json.RawMessage, ptr string) (string, error) {
	text, err := asString(raw)
	if err != nil || (text != langJSONPath && text != langJSONata) {
		return "", defErr(codeSchema, "Field 'QueryLanguage' must be JSONPath or JSONata", ptr)
	}
	return text, nil
}

func fullExpr(s string) (string, bool) {
	if len(s) < 4 || !strings.HasPrefix(s, "{%") || !strings.HasSuffix(s, "%}") {
		return "", false
	}
	return s[2 : len(s)-2], true
}

func checkVarName(name, ptr string) error {
	if name == "states" {
		return defErr(codeSchema, "Variable name 'states' is reserved", ptr)
	}
	runes := []rune(name)
	if len(runes) == 0 || len(runes) > 80 {
		return defErr(codeSchema, "Variable name '"+name+"' is invalid", ptr)
	}
	if !idStart(runes[0]) {
		return defErr(codeSchema, "Variable name '"+name+"' is invalid", ptr)
	}
	for _, r := range runes[1:] {
		if !idContinue(r) {
			return defErr(codeSchema, "Variable name '"+name+"' is invalid", ptr)
		}
	}
	return nil
}

func idStart(r rune) bool {
	return r != utf8.RuneError && (unicode.IsLetter(r) || unicode.Is(unicode.Nl, r))
}

func idContinue(r rune) bool {
	return idStart(r) || unicode.Is(unicode.Mn, r) || unicode.Is(unicode.Mc, r) || unicode.Is(unicode.Nd, r) || unicode.Is(unicode.Pc, r)
}

func allowResult(st *State) bool {
	return st != nil && (st.Type == "Task" || st.Type == "Map" || st.Type == "Parallel")
}

func checkAssign(raw json.RawMessage, ptr string, jsonata, allowResult, allowError bool) error {
	obj, err := decodeObject(raw)
	if err != nil {
		return defErr(codeSchema, "Field 'Assign' must be an object", ptr)
	}
	for key := range obj {
		name := key
		if jsonata {
			if strings.HasSuffix(key, ".$") {
				return defErr(codeSchema, "Field '"+key+"' is not supported", join(ptr, key))
			}
		} else if strings.HasSuffix(key, ".$") {
			name = strings.TrimSuffix(key, ".$")
		}
		if err := checkVarName(name, join(ptr, key)); err != nil {
			return err
		}
	}
	if !jsonata {
		return checkPayload(raw, ptr)
	}
	return scanRaw(raw, ptr, allowResult, allowError)
}

func scanRaw(raw json.RawMessage, ptr string, allowResult, allowError bool) error {
	v, err := decodeAny(raw)
	if err != nil {
		return defErr(codeSchema, "Field must be JSON", ptr)
	}
	return scanValue(v, ptr, allowResult, allowError)
}

func scanValue(v any, ptr string, allowResult, allowError bool) error {
	switch node := v.(type) {
	case map[string]any:
		for key, child := range node {
			if strings.HasSuffix(key, ".$") {
				return defErr(codeSchema, "Field '"+key+"' is not supported", join(ptr, key))
			}
			if err := scanValue(child, join(ptr, key), allowResult, allowError); err != nil {
				return err
			}
		}
	case []any:
		for i, child := range node {
			if err := scanValue(child, fmt.Sprintf("%s[%d]", ptr, i), allowResult, allowError); err != nil {
				return err
			}
		}
	case string:
		body, ok := fullExpr(node)
		if !ok {
			return nil
		}
		return scanExpr(body, ptr, allowResult, allowError)
	}
	return nil
}

func scanExpr(text, ptr string, allowResult, allowError bool) error {
	if evalCall.MatchString(text) {
		return defErr(codeSchema, "$eval is not available", ptr)
	}
	if err := rejectNewOperators(text, ptr); err != nil {
		return err
	}
	if !allowResult && directResult.MatchString(text) {
		return defErr(codeSchema, "$states.result is not allowed here", ptr)
	}
	if !allowError && directError.MatchString(text) {
		return defErr(codeSchema, "$states.errorOutput is not allowed here", ptr)
	}
	return nil
}

func rejectNewOperators(text, ptr string) error {
	inSingle, inDouble := false, false
	for i := 0; i < len(text); i++ {
		c := text[i]
		if inSingle {
			if c == '\\' && i+1 < len(text) {
				i++
				continue
			}
			if c == '\'' {
				inSingle = false
			}
			continue
		}
		if inDouble {
			if c == '\\' && i+1 < len(text) {
				i++
				continue
			}
			if c == '"' {
				inDouble = false
			}
			continue
		}
		switch c {
		case '\'':
			inSingle = true
		case '"':
			inDouble = true
		case '?':
			if i+1 < len(text) && (text[i+1] == ':' || text[i+1] == '?') {
				return defErr(codeSchema, "JSONata operator '"+text[i:i+2]+"' is not supported", ptr)
			}
		}
	}
	return nil
}

func checkItems(raw json.RawMessage, ptr string) error {
	v, err := decodeAny(raw)
	if err != nil {
		return defErr(codeSchema, "Field 'Items' must be an array or an expression", ptr)
	}
	switch node := v.(type) {
	case map[string]any:
		return defErr(codeSchema, "Field 'Items' must be an array or an expression", ptr)
	case string:
		body, ok := fullExpr(node)
		if !ok {
			return defErr(codeSchema, "Field 'Items' must be an array or an expression", ptr)
		}
		return scanExpr(body, ptr, false, false)
	case []any:
		return scanValue(node, ptr, false, false)
	default:
		return defErr(codeSchema, "Field 'Items' must be an array or an expression", ptr)
	}
}

func checkItemSelectorJSONata(raw json.RawMessage, ptr string) error {
	v, err := decodeAny(raw)
	if err != nil {
		return defErr(codeSchema, "Field 'ItemSelector' must be an object", ptr)
	}
	if _, ok := v.(map[string]any); !ok {
		return defErr(codeSchema, "Field 'ItemSelector' must be an object", ptr)
	}
	return checkSelectorStrings(v, ptr)
}

func checkSelectorStrings(v any, ptr string) error {
	switch node := v.(type) {
	case map[string]any:
		for key, child := range node {
			if strings.HasSuffix(key, ".$") {
				return defErr(codeSchema, "Field '"+key+"' is not supported", join(ptr, key))
			}
			if err := checkSelectorStrings(child, join(ptr, key)); err != nil {
				return err
			}
		}
	case []any:
		for i, child := range node {
			if err := checkSelectorStrings(child, fmt.Sprintf("%s[%d]", ptr, i)); err != nil {
				return err
			}
		}
	case string:
		body, ok := fullExpr(node)
		if !ok {
			return defErr(codeSchema, "ItemSelector string values must be JSONata expressions", ptr)
		}
		return scanExpr(body, ptr, false, false)
	}
	return nil
}

func decodeAny(raw json.RawMessage) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, fmt.Errorf("trailing")
	}
	return v, nil
}

func expressionBody(raw json.RawMessage) (string, bool) {
	text, err := asString(raw)
	if err != nil {
		return "", false
	}
	return fullExpr(text)
}

func checkScopes(prog *Program, outer map[string]bool) error {
	if prog == nil {
		return nil
	}
	local := map[string]bool{}
	for _, st := range prog.States {
		for _, name := range assignKeyNames(st.Assign) {
			local[name] = true
		}
		for _, name := range choiceAssignNames(st.Choices) {
			local[name] = true
		}
		for _, c := range st.Catch {
			for _, name := range assignKeyNames(c.Assign) {
				local[name] = true
			}
		}
	}
	conflicts := make([]string, 0)
	for name := range local {
		if outer[name] {
			conflicts = append(conflicts, name)
		}
	}
	sort.Strings(conflicts)
	if len(conflicts) > 0 {
		return defErr(codeSchema, "Variable '"+conflicts[0]+"' is already assigned in an outer scope", "/")
	}
	next := map[string]bool{}
	for name := range outer {
		next[name] = true
	}
	for name := range local {
		next[name] = true
	}
	names := make([]string, 0, len(prog.States))
	for name := range prog.States {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		st := prog.States[name]
		if st.Processor != nil {
			if err := checkScopes(st.Processor, next); err != nil {
				return err
			}
		}
		for _, branch := range st.Branches {
			if err := checkScopes(branch, next); err != nil {
				return err
			}
		}
	}
	return nil
}

func assignKeyNames(raw json.RawMessage) []string {
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil
	}
	obj, err := decodeObject(raw)
	if err != nil {
		return nil
	}
	names := make([]string, 0, len(obj))
	for key := range obj {
		name := key
		if strings.HasSuffix(key, ".$") {
			name = strings.TrimSuffix(key, ".$")
		}
		names = append(names, name)
	}
	return names
}

func choiceAssignNames(raw json.RawMessage) []string {
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil
	}
	var rules []map[string]json.RawMessage
	if err := json.Unmarshal(raw, &rules); err != nil {
		return nil
	}
	var names []string
	for _, rule := range rules {
		names = append(names, assignKeyNames(rule["Assign"])...)
	}
	return names
}

func resultPathVariable(path Path, ptr string) error {
	if path.Kind == PathOther && jsonpath.ReferencesVariable(path.Text) {
		return defErr(codeSchema, "ResultPath cannot reference a variable", ptr)
	}
	return nil
}
