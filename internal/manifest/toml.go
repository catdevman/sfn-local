// Package manifest parses the small TOML subset used by the classification files.
// It accepts [[tables]], string keys, comments, and blank lines.
package manifest

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

// Table is one [[name]] block, in file order.
type Table struct {
	Name   string
	Values map[string]string
}

// Parse reads classification TOML. A key before the first table is an error.
func Parse(src string) ([]Table, error) {
	var out []Table
	var cur *Table
	lineNo := 0
	for _, line := range strings.Split(src, "\n") {
		lineNo++
		trim := strings.TrimSpace(line)
		if trim == "" || strings.HasPrefix(trim, "#") {
			continue
		}
		if strings.HasPrefix(trim, "[[") && strings.HasSuffix(trim, "]]") {
			name := strings.TrimSpace(trim[2 : len(trim)-2])
			if name == "" || strings.ContainsAny(name, "[]#") {
				return nil, fmt.Errorf("line %d: invalid table %q", lineNo, trim)
			}
			out = append(out, Table{Name: name, Values: map[string]string{}})
			cur = &out[len(out)-1]
			continue
		}
		if cur == nil {
			return nil, fmt.Errorf("line %d: key outside a table", lineNo)
		}
		key, value, err := parseKV(trim)
		if err != nil {
			return nil, fmt.Errorf("line %d: %w", lineNo, err)
		}
		if _, exists := cur.Values[key]; exists {
			return nil, fmt.Errorf("line %d: duplicate key %s", lineNo, key)
		}
		cur.Values[key] = value
	}
	return out, nil
}

func parseKV(line string) (string, string, error) {
	eq := strings.IndexByte(line, '=')
	if eq < 0 {
		return "", "", fmt.Errorf("expected key = value")
	}
	key := strings.TrimSpace(line[:eq])
	if key == "" || strings.ContainsAny(key, " \t.#[]") {
		return "", "", fmt.Errorf("invalid key %q", key)
	}
	rest := strings.TrimSpace(line[eq+1:])
	if rest == "" || rest[0] != '"' {
		return "", "", fmt.Errorf("value for %s must be a quoted string", key)
	}
	val, n, err := unquote(rest)
	if err != nil {
		return "", "", err
	}
	tail := strings.TrimSpace(rest[n:])
	if tail != "" && !strings.HasPrefix(tail, "#") {
		return "", "", fmt.Errorf("trailing data after %s", key)
	}
	return key, val, nil
}

func unquote(s string) (string, int, error) {
	if len(s) < 2 || s[0] != '"' {
		return "", 0, fmt.Errorf("string must start with a quote")
	}
	var b strings.Builder
	for i := 1; i < len(s); i++ {
		c := s[i]
		if c == '"' {
			return b.String(), i + 1, nil
		}
		if c != '\\' {
			b.WriteByte(c)
			continue
		}
		if i+1 >= len(s) {
			return "", 0, fmt.Errorf("truncated escape")
		}
		i++
		switch s[i] {
		case '\\', '"':
			b.WriteByte(s[i])
		case 'n':
			b.WriteByte('\n')
		case 't':
			b.WriteByte('\t')
		case 'u':
			if i+4 >= len(s) {
				return "", 0, fmt.Errorf("short unicode escape")
			}
			r, err := decodeHex(s[i+1 : i+5])
			if err != nil {
				return "", 0, err
			}
			if !utf8.ValidRune(rune(r)) {
				return "", 0, fmt.Errorf("invalid unicode escape")
			}
			b.WriteRune(rune(r))
			i += 4
		default:
			return "", 0, fmt.Errorf("unknown escape \\%c", s[i])
		}
	}
	return "", 0, fmt.Errorf("unterminated string")
}

func decodeHex(s string) (int32, error) {
	var n int32
	for _, c := range s {
		var d int32
		switch {
		case c >= '0' && c <= '9':
			d = c - '0'
		case c >= 'a' && c <= 'f':
			d = c - 'a' + 10
		case c >= 'A' && c <= 'F':
			d = c - 'A' + 10
		default:
			return 0, fmt.Errorf("invalid hex %q", s)
		}
		n = n*16 + d
	}
	return n, nil
}
