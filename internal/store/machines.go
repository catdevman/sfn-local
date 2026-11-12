package store

import "time"

// PutMachine inserts a machine or, when the name exists with the same
// definition, returns that row without changing its status. A different
// definition is ExistsError. tags replaces the tag list when non-nil.
func (s *Store) PutMachine(m Machine, tags []Tag) (Machine, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if arn, ok := s.machineBy[m.Name]; ok {
		cur := s.machines[arn]
		if cur.Definition != m.Definition {
			return Machine{}, &ExistsError{ARN: cur.ARN}
		}
		if tags != nil {
			s.tags[cur.ARN] = copyTags(tags)
		}
		return copyMachine(cur), nil
	}
	stored := m
	stored.Logging = copyLogging(m.Logging)
	s.machines[m.ARN] = &stored
	s.machineBy[m.Name] = m.ARN
	if tags != nil {
		s.tags[m.ARN] = copyTags(tags)
	}
	return copyMachine(&stored), nil
}

// Machine returns a copy by ARN.
func (s *Store) Machine(arn string) (Machine, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.machines[arn]
	if !ok {
		return Machine{}, false
	}
	return copyMachine(m), true
}

// Machines returns copies sorted by the caller.
func (s *Store) Machines() []Machine {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Machine, 0, len(s.machines))
	for _, m := range s.machines {
		out = append(out, copyMachine(m))
	}
	return out
}

// UpdateMachine applies the provided fields. nil definition or role leaves
// the stored value. loggingSet replaces logging, including with nil.
func (s *Store) UpdateMachine(arn string, definition, role *string, logging *Logging, loggingSet bool, when time.Time) (Machine, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.machines[arn]
	if !ok {
		return Machine{}, false
	}
	if definition != nil {
		m.Definition = *definition
	}
	if role != nil {
		m.RoleArn = *role
	}
	if loggingSet {
		m.Logging = copyLogging(logging)
	}
	updated := when
	m.Updated = &updated
	return copyMachine(m), true
}

// MarkDeleting sets status to DELETING. An unknown ARN returns false.
func (s *Store) MarkDeleting(arn string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.machines[arn]
	if !ok {
		return false
	}
	m.Status = StatusDeleting
	return true
}

// PutActivity inserts an activity. A duplicate name is ExistsError.
func (s *Store) PutActivity(a Activity, tags []Tag) (Activity, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if arn, ok := s.activityBy[a.Name]; ok {
		return Activity{}, &ExistsError{ARN: arn}
	}
	stored := a
	s.activities[a.ARN] = &stored
	s.activityBy[a.Name] = a.ARN
	if tags != nil {
		s.tags[a.ARN] = copyTags(tags)
	}
	return stored, nil
}

// Activity returns a copy by ARN.
func (s *Store) Activity(arn string) (Activity, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.activities[arn]
	if !ok {
		return Activity{}, false
	}
	return *a, true
}

// Activities returns every activity row.
func (s *Store) Activities() []Activity {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Activity, 0, len(s.activities))
	for _, a := range s.activities {
		out = append(out, *a)
	}
	return out
}

// DeleteActivity removes the row and its tags.
func (s *Store) DeleteActivity(arn string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.activities[arn]
	if !ok {
		return false
	}
	delete(s.activities, arn)
	delete(s.activityBy, a.Name)
	delete(s.tags, arn)
	return true
}

// Tags returns a copy of the tag list. The second result is false when the
// resource ARN is not a known machine or activity.
func (s *Store) Tags(arn string) ([]Tag, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.knownResource(arn) {
		return nil, false
	}
	return copyTags(s.tags[arn]), true
}

// TagMergeUnknown means the resource ARN is not a machine or an activity.
// TagMergeLimit means the merge would exceed 50 tags. The list is unchanged.
const (
	TagMergeOK = iota
	TagMergeUnknown
	TagMergeLimit
)

// AddTags merges tags by key. An existing key updates its value and does not
// increase the count.
func (s *Store) AddTags(arn string, tags []Tag) (int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.knownResource(arn) {
		return 0, TagMergeUnknown
	}
	cur := append([]Tag(nil), s.tags[arn]...)
	index := map[string]int{}
	for i, tag := range cur {
		index[tag.Key] = i
	}
	for _, tag := range tags {
		if i, ok := index[tag.Key]; ok {
			cur[i] = tag
			continue
		}
		index[tag.Key] = len(cur)
		cur = append(cur, tag)
	}
	if len(cur) > 50 {
		return len(s.tags[arn]), TagMergeLimit
	}
	s.tags[arn] = cur
	return len(cur), TagMergeOK
}

// RemoveTags deletes keys. Unknown keys are ignored. Unknown resources return false.
func (s *Store) RemoveTags(arn string, keys []string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.knownResource(arn) {
		return false
	}
	drop := map[string]bool{}
	for _, k := range keys {
		drop[k] = true
	}
	cur := s.tags[arn]
	out := cur[:0:0]
	for _, tag := range cur {
		if !drop[tag.Key] {
			out = append(out, tag)
		}
	}
	if out == nil {
		out = []Tag{}
	}
	s.tags[arn] = out
	return true
}

func (s *Store) knownResource(arn string) bool {
	_, m := s.machines[arn]
	_, a := s.activities[arn]
	return m || a
}
