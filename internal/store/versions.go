package store

import (
	"sort"
	"strconv"
	"time"
)

const (
	maxVersions = 1000
	maxAliases  = 100
)

// Version is an immutable snapshot of one machine revision.
type Version struct {
	ARN         string
	MachineARN  string
	Number      int
	Definition  string
	Type        string
	RoleArn     string
	Logging     *Logging
	Revision    string
	Description string
	Created     time.Time
}

// Route is one alias weight. Weights for an alias sum to 100.
type Route struct {
	VersionARN string
	Weight     int32
}

// Alias points at one or two versions of one machine.
type Alias struct {
	ARN         string
	Name        string
	MachineARN  string
	Description string
	Routes      []Route
	Created     time.Time
	Updated     *time.Time
}

// MissingError is an unknown machine, version, or alias ARN.
type MissingError struct{ ARN string }

func (e *MissingError) Error() string { return "missing: " + e.ARN }

// DeletingError is a machine whose status is DELETING.
type DeletingError struct{ ARN string }

func (e *DeletingError) Error() string { return "deleting: " + e.ARN }

// RevisionMismatch means PublishStateMachineVersion saw a stale revision id.
type RevisionMismatch struct{ Current string }

func (e *RevisionMismatch) Error() string { return "revision mismatch" }

// LimitError is the version or alias quota.
type LimitError struct{ Resource string }

func (e *LimitError) Error() string { return "limit: " + e.Resource }

// AliasExistsError is a duplicate alias name on one machine.
type AliasExistsError struct{ ARN string }

func (e *AliasExistsError) Error() string { return "alias exists: " + e.ARN }

// VersionInUseError means an alias still routes to the version.
type VersionInUseError struct{ AliasARN string }

func (e *VersionInUseError) Error() string { return "version in use: " + e.AliasARN }

// PublishVersion snaps the current revision. A live version of that revision
// is returned again. wantRevision, when set, must equal the current revision.
func (s *Store) PublishVersion(machineARN, wantRevision, description string, when time.Time) (Version, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.machines[machineARN]
	if !ok {
		return Version{}, &MissingError{ARN: machineARN}
	}
	if m.Status == StatusDeleting {
		return Version{}, &DeletingError{ARN: machineARN}
	}
	if wantRevision != "" && wantRevision != m.Revision {
		return Version{}, &RevisionMismatch{Current: m.Revision}
	}
	for _, v := range s.byMachineVersion[machineARN] {
		if v.Revision == m.Revision {
			return copyVersion(v), nil
		}
	}
	if len(s.byMachineVersion[machineARN]) >= maxVersions {
		return Version{}, &LimitError{Resource: "versions"}
	}
	n := s.nextVersion[machineARN] + 1
	s.nextVersion[machineARN] = n
	v := &Version{
		ARN:         m.ARN + ":" + strconv.Itoa(n),
		MachineARN:  m.ARN,
		Number:      n,
		Definition:  m.Definition,
		Type:        m.Type,
		RoleArn:     m.RoleArn,
		Logging:     copyLogging(m.Logging),
		Revision:    m.Revision,
		Description: description,
		Created:     when,
	}
	s.versions[v.ARN] = v
	s.byMachineVersion[machineARN] = append(s.byMachineVersion[machineARN], v)
	return copyVersion(v), nil
}

// Version returns a copy by version ARN.
func (s *Store) Version(arn string) (Version, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.versions[arn]
	if !ok {
		return Version{}, false
	}
	return copyVersion(v), true
}

// Versions returns copies in version-number order.
func (s *Store) Versions(machineARN string) []Version {
	s.mu.Lock()
	defer s.mu.Unlock()
	src := s.byMachineVersion[machineARN]
	out := make([]Version, len(src))
	for i, v := range src {
		out[i] = copyVersion(v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Number < out[j].Number })
	return out
}

// DeleteVersion removes a version that no alias routes to.
func (s *Store) DeleteVersion(arn string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.versions[arn]
	if !ok {
		return &MissingError{ARN: arn}
	}
	for _, a := range s.byMachineAlias[v.MachineARN] {
		for _, route := range a.Routes {
			if route.VersionARN == arn {
				return &VersionInUseError{AliasARN: a.ARN}
			}
		}
	}
	delete(s.versions, arn)
	kept := s.byMachineVersion[v.MachineARN][:0]
	for _, item := range s.byMachineVersion[v.MachineARN] {
		if item.ARN != arn {
			kept = append(kept, item)
		}
	}
	s.byMachineVersion[v.MachineARN] = kept
	return nil
}

// CreateAlias inserts an alias for machineARN. routes were already checked
// to sum to 100. This rechecks that each version still belongs to the machine.
func (s *Store) CreateAlias(machineARN, name, description string, routes []Route, when time.Time) (Alias, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.routesLocked(machineARN, routes); err != nil {
		return Alias{}, err
	}
	key := aliasKey(machineARN, name)
	if arn, ok := s.aliasName[key]; ok {
		return Alias{}, &AliasExistsError{ARN: arn}
	}
	if len(s.byMachineAlias[machineARN]) >= maxAliases {
		return Alias{}, &LimitError{Resource: "aliases"}
	}
	a := &Alias{
		ARN:         machineARN + ":" + name,
		Name:        name,
		MachineARN:  machineARN,
		Description: description,
		Routes:      copyRoutes(routes),
		Created:     when,
	}
	s.aliases[a.ARN] = a
	s.byMachineAlias[machineARN] = append(s.byMachineAlias[machineARN], a)
	s.aliasName[key] = a.ARN
	return copyAlias(a), nil
}

// Alias returns a copy by alias ARN.
func (s *Store) Alias(arn string) (Alias, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.aliases[arn]
	if !ok {
		return Alias{}, false
	}
	return copyAlias(a), true
}

// Aliases returns copies in name order.
func (s *Store) Aliases(machineARN string) []Alias {
	s.mu.Lock()
	defer s.mu.Unlock()
	src := s.byMachineAlias[machineARN]
	out := make([]Alias, len(src))
	for i, a := range src {
		out[i] = copyAlias(a)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// UpdateAlias replaces description when description is non-nil, and replaces
// routes when routesSet is true.
func (s *Store) UpdateAlias(arn string, description *string, routes []Route, routesSet bool, when time.Time) (Alias, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.aliases[arn]
	if !ok {
		return Alias{}, &MissingError{ARN: arn}
	}
	if routesSet {
		if err := s.routesLocked(a.MachineARN, routes); err != nil {
			return Alias{}, err
		}
		a.Routes = copyRoutes(routes)
	}
	if description != nil {
		a.Description = *description
	}
	updated := when
	a.Updated = &updated
	return copyAlias(a), nil
}

// DeleteAlias removes an alias.
func (s *Store) DeleteAlias(arn string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.aliases[arn]
	if !ok {
		return &MissingError{ARN: arn}
	}
	delete(s.aliases, arn)
	delete(s.aliasName, aliasKey(a.MachineARN, a.Name))
	kept := s.byMachineAlias[a.MachineARN][:0]
	for _, item := range s.byMachineAlias[a.MachineARN] {
		if item.ARN != arn {
			kept = append(kept, item)
		}
	}
	s.byMachineAlias[a.MachineARN] = kept
	return nil
}

func (s *Store) routesLocked(machineARN string, routes []Route) error {
	m, ok := s.machines[machineARN]
	if !ok {
		return &MissingError{ARN: machineARN}
	}
	if m.Status == StatusDeleting {
		return &DeletingError{ARN: machineARN}
	}
	for _, route := range routes {
		v, ok := s.versions[route.VersionARN]
		if !ok {
			return &MissingError{ARN: route.VersionARN}
		}
		if v.MachineARN != machineARN {
			return &MissingError{ARN: route.VersionARN}
		}
	}
	return nil
}

func (s *Store) dropQualifiedLocked(machineARN string) {
	for _, v := range s.byMachineVersion[machineARN] {
		delete(s.versions, v.ARN)
	}
	delete(s.byMachineVersion, machineARN)
	delete(s.nextVersion, machineARN)
	for _, a := range s.byMachineAlias[machineARN] {
		delete(s.aliases, a.ARN)
		delete(s.aliasName, aliasKey(machineARN, a.Name))
	}
	delete(s.byMachineAlias, machineARN)
}

func aliasKey(machineARN, name string) string {
	return machineARN + "\n" + name
}

func copyVersion(v *Version) Version {
	out := *v
	out.Logging = copyLogging(v.Logging)
	return out
}

func copyRoutes(in []Route) []Route {
	if in == nil {
		return nil
	}
	out := make([]Route, len(in))
	copy(out, in)
	return out
}

func copyAlias(a *Alias) Alias {
	out := *a
	out.Routes = copyRoutes(a.Routes)
	if a.Updated != nil {
		u := *a.Updated
		out.Updated = &u
	}
	return out
}
