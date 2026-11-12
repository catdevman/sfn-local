package main

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

// stateTypes is the only legal column set for a field table.
var stateTypes = []string{"Task", "Parallel", "Map", "Pass", "Wait", "Choice", "Succeed", "Fail"}

var knownCellClass = map[string]bool{
	"blank": true, "field": true, "required": true, "allowed": true, "empty": true,
}

type document struct {
	Languages []language
	AppendixA []string
	AppendixB []string
}

type language struct {
	Name   string
	Fields []field
	Notes  []note
}

type field struct {
	Name   string
	States stateCells
}

type note struct {
	Text   string
	States stateCells
}

type stateCells struct {
	Task     string
	Parallel string
	Map      string
	Pass     string
	Wait     string
	Choice   string
	Succeed  string
	Fail     string
}

type cell struct {
	tag   string
	class string
	text  string
}

type htmlTable struct {
	rows [][]cell
}

type event struct {
	kind  string
	id    string
	text  string
	table *htmlTable
}

func parseSpec(src string) (*document, error) {
	events, err := scanHTML(src)
	if err != nil {
		return nil, err
	}
	jsonata, err := tableAfter(events, "state-type-table-jsonata")
	if err != nil {
		return nil, fmt.Errorf("no field table for JSONata: %w", err)
	}
	jsonpath, err := tableAfter(events, "state-type-table-jsonpath")
	if err != nil {
		return nil, fmt.Errorf("no field table for JSONPath: %w", err)
	}
	if jsonata == nil || jsonpath == nil {
		return nil, fmt.Errorf("no field table")
	}
	fieldTables := map[*htmlTable]bool{jsonata: true, jsonpath: true}
	for _, ev := range events {
		if ev.kind != "table" || ev.table == nil {
			continue
		}
		if !tableHasClass(ev.table, "field") {
			continue
		}
		if !fieldTables[ev.table] {
			return nil, fmt.Errorf("unexpected field table")
		}
	}
	ja, err := parseFieldTable("jsonata", jsonata)
	if err != nil {
		return nil, fmt.Errorf("JSONata field table: %w", err)
	}
	jp, err := parseFieldTable("jsonpath", jsonpath)
	if err != nil {
		return nil, fmt.Errorf("JSONPath field table: %w", err)
	}
	codes, err := appendixA(events)
	if err != nil {
		return nil, err
	}
	intrinsics, err := appendixB(events)
	if err != nil {
		return nil, err
	}
	return &document{
		Languages: []language{ja, jp},
		AppendixA: codes,
		AppendixB: intrinsics,
	}, nil
}

func tableAfter(events []event, id string) (*htmlTable, error) {
	for i, ev := range events {
		if ev.kind == "h3" && ev.id == id {
			for _, next := range events[i+1:] {
				if next.kind == "table" {
					return next.table, nil
				}
				if next.kind == "h2" || next.kind == "h3" {
					break
				}
			}
			return nil, fmt.Errorf("heading %s is not followed by a table", id)
		}
	}
	return nil, fmt.Errorf("heading %s is missing", id)
}

func tableHasClass(t *htmlTable, class string) bool {
	for _, row := range t.rows {
		for _, c := range row {
			if c.class == class {
				return true
			}
		}
	}
	return false
}

func parseFieldTable(name string, t *htmlTable) (language, error) {
	if err := checkCellClasses(t); err != nil {
		return language{}, err
	}
	header := -1
	for i, row := range t.rows {
		texts := thTexts(row)
		exact, related := stateHeader(texts)
		if exact {
			header = i
			break
		}
		if related {
			return language{}, fmt.Errorf("state-type columns other than %s, found %s", strings.Join(stateTypes, ", "), strings.Join(texts, ", "))
		}
	}
	if header < 0 {
		return language{}, fmt.Errorf("no field table: state-type columns not found")
	}
	lang := language{Name: name}
	seen := map[string]bool{}
	for _, row := range t.rows[header+1:] {
		if len(row) != len(stateTypes)+1 {
			return language{}, fmt.Errorf("field row has %d cells, want %d", len(row), len(stateTypes)+1)
		}
		label := row[0]
		if label.class != "field" {
			return language{}, fmt.Errorf("field label has class %q", label.class)
		}
		cells, err := rowStates(row[1:])
		if err != nil {
			return language{}, err
		}
		names, isNote := splitLabel(label.text)
		if isNote {
			lang.Notes = append(lang.Notes, note{Text: label.text, States: cells})
			continue
		}
		for _, n := range names {
			if seen[n] {
				return language{}, fmt.Errorf("duplicate field %s", n)
			}
			seen[n] = true
			lang.Fields = append(lang.Fields, field{Name: n, States: cells})
		}
	}
	if len(lang.Fields) == 0 {
		return language{}, fmt.Errorf("no field table: no fields")
	}
	return lang, nil
}

func checkCellClasses(t *htmlTable) error {
	for _, row := range t.rows {
		for _, c := range row {
			if c.tag == "th" && (c.class == "" || c.class == "blank") {
				continue
			}
			if !knownCellClass[c.class] {
				got := c.class
				if got == "" {
					got = "(none)"
				}
				return fmt.Errorf("unknown cell class %q", got)
			}
		}
	}
	return nil
}

func thTexts(row []cell) []string {
	var out []string
	for _, c := range row {
		if c.tag == "th" {
			out = append(out, c.text)
		}
	}
	return out
}

func stateHeader(texts []string) (exact, related bool) {
	if len(texts) == len(stateTypes) {
		exact = true
		for i, name := range stateTypes {
			if texts[i] != name {
				exact = false
				break
			}
		}
	}
	known := map[string]bool{}
	for _, name := range stateTypes {
		known[name] = true
	}
	for _, text := range texts {
		if known[text] {
			related = true
			break
		}
	}
	return exact, related
}

func rowStates(cells []cell) (stateCells, error) {
	if len(cells) != len(stateTypes) {
		return stateCells{}, fmt.Errorf("state cells %d, want %d", len(cells), len(stateTypes))
	}
	vals := make([]string, len(cells))
	for i, c := range cells {
		switch c.class {
		case "required", "allowed":
			vals[i] = c.class
		case "empty":
			vals[i] = "absent"
		default:
			return stateCells{}, fmt.Errorf("state cell class %q", c.class)
		}
	}
	return stateCells{
		Task: vals[0], Parallel: vals[1], Map: vals[2], Pass: vals[3],
		Wait: vals[4], Choice: vals[5], Succeed: vals[6], Fail: vals[7],
	}, nil
}

func splitLabel(label string) (names []string, isNote bool) {
	parts := strings.Split(label, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" || !isFieldIdent(part) {
			return nil, true
		}
		names = append(names, part)
	}
	if len(names) == 0 {
		return nil, true
	}
	return names, false
}

func isFieldIdent(s string) bool {
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

func appendixA(events []event) ([]string, error) {
	table, err := tableAfter(events, "appendix-a")
	if err != nil || table == nil {
		return nil, fmt.Errorf("appendix A: %w", err)
	}
	var codes []string
	for i, row := range table.rows {
		if len(row) == 0 {
			continue
		}
		text := row[0].text
		if i == 0 && strings.EqualFold(text, "Code") {
			continue
		}
		if text == "" {
			return nil, fmt.Errorf("appendix A has an empty code")
		}
		codes = append(codes, text)
	}
	if len(codes) == 0 {
		return nil, fmt.Errorf("appendix A has no error codes")
	}
	return codes, nil
}

func appendixB(events []event) ([]string, error) {
	start := -1
	for i, ev := range events {
		if ev.kind == "h3" && ev.id == "appendix-b" {
			start = i
			break
		}
	}
	if start < 0 {
		return nil, fmt.Errorf("appendix B heading is missing")
	}
	var names []string
	for _, ev := range events[start+1:] {
		if ev.kind == "h2" || ev.kind == "h3" {
			break
		}
		if ev.kind != "h4" {
			continue
		}
		if ev.text == "" {
			return nil, fmt.Errorf("appendix B has an empty intrinsic name")
		}
		names = append(names, ev.text)
	}
	if len(names) == 0 {
		return nil, fmt.Errorf("appendix B has no intrinsic names")
	}
	return names, nil
}

func scanHTML(src string) ([]event, error) {
	var events []event
	var table *htmlTable
	var row []cell
	var cur *cell
	var heading *event
	skip := ""
	i := 0
	for i < len(src) {
		if skip != "" {
			end := strings.Index(strings.ToLower(src[i:]), "</"+skip)
			if end < 0 {
				return nil, fmt.Errorf("unclosed <%s>", skip)
			}
			i += end
			skip = ""
			continue
		}
		if strings.HasPrefix(src[i:], "<!--") {
			end := strings.Index(src[i+4:], "-->")
			if end < 0 {
				return nil, fmt.Errorf("unclosed comment")
			}
			i += 4 + end + 3
			continue
		}
		if src[i] != '<' {
			if cur != nil || heading != nil {
				j := strings.IndexByte(src[i:], '<')
				if j < 0 {
					j = len(src) - i
				}
				chunk := decodeEntities(src[i : i+j])
				if cur != nil {
					cur.text += chunk
				}
				if heading != nil {
					heading.text += chunk
				}
				i += j
				continue
			}
			i++
			continue
		}
		if strings.HasPrefix(src[i:], "</") {
			j := strings.IndexByte(src[i:], '>')
			if j < 0 {
				return nil, fmt.Errorf("unclosed end tag")
			}
			name := tagName(src[i+2 : i+j])
			switch name {
			case "td", "th":
				if cur != nil {
					cur.text = normSpace(cur.text)
					cur = nil
				}
			case "tr":
				if table != nil && row != nil {
					table.rows = append(table.rows, row)
				}
				row = nil
			case "table":
				if table != nil {
					events = append(events, event{kind: "table", table: table})
				}
				table = nil
			case "h2", "h3", "h4":
				if heading != nil && heading.kind == name {
					heading.text = normSpace(heading.text)
					events = append(events, *heading)
					heading = nil
				}
			}
			i += j + 1
			continue
		}
		j := strings.IndexByte(src[i:], '>')
		if j < 0 {
			return nil, fmt.Errorf("unclosed tag")
		}
		raw := src[i+1 : i+j]
		selfClose := strings.HasSuffix(raw, "/")
		if selfClose {
			raw = strings.TrimSuffix(raw, "/")
		}
		name := tagName(raw)
		attrs := raw[len(name):]
		i += j + 1
		switch name {
		case "script", "style":
			if !selfClose {
				skip = name
			}
		case "table":
			table = &htmlTable{}
		case "tr":
			if table != nil {
				row = []cell{}
			}
		case "td", "th":
			if row != nil {
				row = append(row, cell{tag: name, class: attr(attrs, "class")})
				cur = &row[len(row)-1]
			}
		case "h2", "h3", "h4":
			heading = &event{kind: name, id: attr(attrs, "id")}
		}
	}
	return events, nil
}

func tagName(raw string) string {
	raw = strings.TrimSpace(raw)
	n := 0
	for n < len(raw) {
		r, size := utf8.DecodeRuneInString(raw[n:])
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			n += size
			continue
		}
		break
	}
	return strings.ToLower(raw[:n])
}

func attr(raw, name string) string {
	s := raw
	for len(s) > 0 {
		s = strings.TrimLeftFunc(s, unicode.IsSpace)
		if s == "" {
			break
		}
		eq := strings.IndexAny(s, " \t\r\n=")
		if eq < 0 {
			break
		}
		key := strings.ToLower(s[:eq])
		rest := strings.TrimLeftFunc(s[eq:], unicode.IsSpace)
		if !strings.HasPrefix(rest, "=") {
			s = s[eq:]
			continue
		}
		rest = strings.TrimLeftFunc(rest[1:], unicode.IsSpace)
		if rest == "" {
			break
		}
		var val string
		switch rest[0] {
		case '"', '\'':
			q := rest[0]
			end := strings.IndexByte(rest[1:], q)
			if end < 0 {
				return ""
			}
			val = rest[1 : 1+end]
			s = rest[1+end+1:]
		default:
			end := strings.IndexFunc(rest, unicode.IsSpace)
			if end < 0 {
				val = rest
				s = ""
			} else {
				val = rest[:end]
				s = rest[end:]
			}
		}
		if key == name {
			return val
		}
	}
	return ""
}

func normSpace(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

func decodeEntities(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] != '&' {
			b.WriteByte(s[i])
			continue
		}
		semi := strings.IndexByte(s[i:], ';')
		if semi < 0 || semi > 12 {
			b.WriteByte(s[i])
			continue
		}
		ent := s[i+1 : i+semi]
		switch ent {
		case "quot":
			b.WriteByte('"')
		case "amp":
			b.WriteByte('&')
		case "lt":
			b.WriteByte('<')
		case "gt":
			b.WriteByte('>')
		case "nbsp":
			b.WriteByte(' ')
		case "apos":
			b.WriteByte('\'')
		default:
			if strings.HasPrefix(ent, "#") {
				r, ok := numericEntity(ent[1:])
				if ok {
					b.WriteRune(r)
					i += semi
					continue
				}
			}
			b.WriteByte(s[i])
			continue
		}
		i += semi
	}
	return b.String()
}

func numericEntity(s string) (rune, bool) {
	base := 10
	if strings.HasPrefix(s, "x") || strings.HasPrefix(s, "X") {
		base = 16
		s = s[1:]
	}
	if s == "" {
		return 0, false
	}
	var n int32
	for _, r := range s {
		var d int32
		switch {
		case r >= '0' && r <= '9':
			d = r - '0'
		case base == 16 && r >= 'a' && r <= 'f':
			d = r - 'a' + 10
		case base == 16 && r >= 'A' && r <= 'F':
			d = r - 'A' + 10
		default:
			return 0, false
		}
		n = n*int32(base) + d
	}
	return rune(n), true
}
