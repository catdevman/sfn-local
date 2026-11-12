package api

import (
	"fmt"

	"github.com/catdevman/sfn-local/internal/manifest"
)

// Surface is the operation classification loaded from surface.toml.
type Surface struct {
	ops     map[string]OpRow
	members map[string]string
	errors  map[string]string
}

// OpRow is one operation's classification.
type OpRow struct {
	Status  string
	Error   string
	Message string
}

// ParseSurface reads surface.toml. A deferred operation needs an error name
// and a message. Status is implemented or deferred.
func ParseSurface(src string) (*Surface, error) {
	tables, err := manifest.Parse(src)
	if err != nil {
		return nil, err
	}
	s := &Surface{
		ops:     map[string]OpRow{},
		members: map[string]string{},
		errors:  map[string]string{},
	}
	for _, table := range tables {
		switch table.Name {
		case "operation":
			row, err := opRow(table.Values)
			if err != nil {
				return nil, err
			}
			if _, ok := s.ops[row.Name]; ok {
				return nil, fmt.Errorf("duplicate operation %s", row.Name)
			}
			s.ops[row.Name] = row.OpRow
		case "member":
			key, status, err := memberRow(table.Values)
			if err != nil {
				return nil, err
			}
			if _, ok := s.members[key]; ok {
				return nil, fmt.Errorf("duplicate member %s", key)
			}
			s.members[key] = status
		case "error":
			name, status, err := errorRow(table.Values)
			if err != nil {
				return nil, err
			}
			if _, ok := s.errors[name]; ok {
				return nil, fmt.Errorf("duplicate error %s", name)
			}
			s.errors[name] = status
		default:
			return nil, fmt.Errorf("unknown surface table %s", table.Name)
		}
	}
	return s, nil
}

type namedOp struct {
	Name string
	OpRow
}

func opRow(v map[string]string) (namedOp, error) {
	name := v["name"]
	status := v["status"]
	if name == "" || !knownStatus(status) {
		return namedOp{}, fmt.Errorf("operation %q has status %q", name, status)
	}
	row := OpRow{Status: status, Error: v["error"], Message: v["message"]}
	if status == "deferred" && (row.Error == "" || row.Message == "") {
		return namedOp{}, fmt.Errorf("deferred operation %s needs error and message", name)
	}
	return namedOp{Name: name, OpRow: row}, nil
}

func memberRow(v map[string]string) (string, string, error) {
	op, dir, name, status := v["operation"], v["direction"], v["name"], v["status"]
	if op == "" || name == "" || (dir != "input" && dir != "output") || !knownStatus(status) {
		return "", "", fmt.Errorf("member %s %s %s has status %q", op, dir, name, status)
	}
	return op + "\x00" + dir + "\x00" + name, status, nil
}

func errorRow(v map[string]string) (string, string, error) {
	name, status := v["name"], v["status"]
	if name == "" || !knownStatus(status) {
		return "", "", fmt.Errorf("error %q has status %q", name, status)
	}
	return name, status, nil
}

func knownStatus(status string) bool {
	return status == "implemented" || status == "deferred"
}

// Operation returns the classification for one modeled operation.
func (s *Surface) Operation(name string) (OpRow, bool) {
	row, ok := s.ops[name]
	return row, ok
}

// Operations returns a copy of the operation rows.
func (s *Surface) Operations() map[string]OpRow {
	out := make(map[string]OpRow, len(s.ops))
	for k, v := range s.ops {
		out[k] = v
	}
	return out
}

// MemberStatus returns the classification for an input or output member.
func (s *Surface) MemberStatus(operation, direction, name string) (string, bool) {
	status, ok := s.members[operation+"\x00"+direction+"\x00"+name]
	return status, ok
}

// ErrorStatus returns the classification for a modeled error name.
func (s *Surface) ErrorStatus(name string) (string, bool) {
	status, ok := s.errors[name]
	return status, ok
}

// Counts returns the number of operation, member, and error rows.
func (s *Surface) Counts() (operations, members, errors int) {
	return len(s.ops), len(s.members), len(s.errors)
}
