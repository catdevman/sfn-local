package api

import (
	"context"

	"github.com/catdevman/sfn-local/internal/api/gen"
	"github.com/catdevman/sfn-local/internal/asl"
	"github.com/catdevman/sfn-local/internal/store"
)

func (s *Service) CreateStateMachine(_ context.Context, in *gen.CreateStateMachineInput) (*gen.CreateStateMachineOutput, error) {
	name := ""
	if in != nil && in.Name != nil {
		name = *in.Name
	}
	if name == "" || NameRejected(name) {
		return nil, modeled("InvalidName", "Invalid Name: '"+name+"'")
	}
	if err := rejectEncryptionTracing(in.EncryptionConfiguration != nil, in.TracingConfiguration != nil); err != nil {
		return nil, err
	}
	if err := rejectVersionDescription(in.Publish, in.VersionDescription); err != nil {
		return nil, err
	}
	role, err := s.roleForCreate(in.RoleArn)
	if err != nil {
		return nil, err
	}
	typ := store.TypeStandard
	if in.Type != nil && string(*in.Type) != "" {
		typ = string(*in.Type)
	}
	logging, err := normalizeLogging(in.LoggingConfiguration, typ == store.TypeExpress)
	if err != nil {
		return nil, err
	}
	if _, err := asl.ValidateType(*in.Definition, typ); err != nil {
		return nil, modeled("InvalidDefinition", err.Error())
	}
	tags, err := tagsFrom(in.Tags)
	if err != nil {
		return nil, err
	}
	machine := store.Machine{
		ARN:        StateMachineARN(s.region(), s.account(), name),
		Name:       name,
		Definition: *in.Definition,
		Type:       typ,
		RoleArn:    role,
		Status:     store.StatusActive,
		Created:    s.now(),
		Logging:    logging,
	}
	stored, err := s.Store.PutMachine(machine, tags)
	if err != nil {
		if exists, ok := err.(*store.ExistsError); ok {
			return nil, modeled("StateMachineAlreadyExists", "State Machine Already Exists: '"+exists.ARN+"'")
		}
		return nil, err
	}
	out := &gen.CreateStateMachineOutput{
		StateMachineArn: sp(stored.ARN),
		CreationDate:    ts(stored.Created),
	}
	if in.Publish != nil && *in.Publish {
		v, err := s.publishVersion(stored.ARN, "", valueOf(in.VersionDescription))
		if err != nil {
			return nil, err
		}
		out.StateMachineVersionArn = sp(v.ARN)
	}
	return out, nil
}

func (s *Service) DescribeStateMachine(_ context.Context, in *gen.DescribeStateMachineInput) (*gen.DescribeStateMachineOutput, error) {
	text := valueOf(in.StateMachineArn)
	base, qual, ok := splitQualifiedARN(text)
	if !ok {
		return nil, modeled("InvalidArn", "Invalid Arn: '"+text+"'")
	}
	switch qualifierKind(qual) {
	case "machine":
		m, err := s.machineByARN(&base)
		if err != nil {
			return nil, err
		}
		out := machineDescription(m)
		if s.Profile != ProfileSFNLocal {
			out.LoggingConfiguration, out.EncryptionConfiguration, out.TracingConfiguration = awsConfiguration(out.LoggingConfiguration)
			if m.Revision == m.FirstRevision {
				out.RevisionId = nil
			}
		}
		return out, nil
	case "version":
		out, err := s.describeVersion(text)
		if err == nil && s.Profile != ProfileSFNLocal {
			out.LoggingConfiguration, out.EncryptionConfiguration, out.TracingConfiguration = awsConfiguration(out.LoggingConfiguration)
			if v, ok := s.Store.Version(text); ok {
				if m, found := s.Store.Machine(v.MachineARN); found && v.Revision == m.FirstRevision {
					out.RevisionId = nil
				}
			}
		}
		return out, err
	case "alias":
		return nil, validationReason("DescribeStateMachine does not accept an alias ARN", gen.ValidationExceptionReasonAPI_DOES_NOT_SUPPORT_LABELED_ARNS)
	default:
		return nil, modeled("InvalidArn", "Invalid Arn: '"+text+"'")
	}
}

func (s *Service) describeVersion(arn string) (*gen.DescribeStateMachineOutput, error) {
	v, ok := s.Store.Version(arn)
	if !ok {
		return nil, modeled("StateMachineDoesNotExist", "State Machine Does Not Exist: '"+arn+"'")
	}
	m, found := s.Store.Machine(v.MachineARN)
	if !found {
		return nil, modeled("StateMachineDoesNotExist", "State Machine Does Not Exist: '"+v.MachineARN+"'")
	}
	status := gen.StateMachineStatus(m.Status)
	typ := gen.StateMachineType(v.Type)
	out := &gen.DescribeStateMachineOutput{
		Name:                 sp(m.Name),
		Status:               &status,
		Definition:           sp(v.Definition),
		CreationDate:         ts(v.Created),
		StateMachineArn:      sp(v.ARN),
		Type:                 &typ,
		LoggingConfiguration: toGenLogging(v.Logging),
		RevisionId:           sp(v.Revision),
	}
	if v.RoleArn != "" {
		out.RoleArn = sp(v.RoleArn)
	}
	if v.Description != "" {
		out.Description = sp(v.Description)
	}
	return out, nil
}

func (s *Service) ListStateMachines(_ context.Context, in *gen.ListStateMachinesInput) (*gen.ListStateMachinesOutput, error) {
	rows := s.Store.Machines()
	sortMachines(rows)
	var max *int32
	var token *string
	if in != nil {
		max, token = in.MaxResults, in.NextToken
	}
	start, end, next, err := pageBounds(len(rows), max, token)
	if err != nil {
		return nil, err
	}
	list := make(gen.StateMachineList, 0, end-start)
	for _, m := range rows[start:end] {
		typ := gen.StateMachineType(m.Type)
		item := gen.StateMachineListItem{
			StateMachineArn: sp(m.ARN),
			Name:            sp(m.Name),
			Type:            &typ,
			CreationDate:    ts(m.Created),
		}
		list = append(list, item)
	}
	return &gen.ListStateMachinesOutput{StateMachines: &list, NextToken: next}, nil
}

func (s *Service) UpdateStateMachine(_ context.Context, in *gen.UpdateStateMachineInput) (*gen.UpdateStateMachineOutput, error) {
	if err := rejectEncryptionTracing(in.EncryptionConfiguration != nil, in.TracingConfiguration != nil); err != nil {
		return nil, err
	}
	if err := rejectVersionDescription(in.Publish, in.VersionDescription); err != nil {
		return nil, err
	}
	publish := in.Publish != nil && *in.Publish
	if in.Definition == nil && in.RoleArn == nil && in.LoggingConfiguration == nil && !publish {
		return nil, modeled("ValidationException", "At least one of definition, roleArn, or loggingConfiguration is required")
	}
	m, err := s.machineByARN(in.StateMachineArn)
	if err != nil {
		return nil, err
	}
	if in.Definition != nil {
		if _, err := asl.ValidateType(*in.Definition, m.Type); err != nil {
			return nil, modeled("InvalidDefinition", err.Error())
		}
	}
	if in.RoleArn != nil && !RoleARNValid(*in.RoleArn) {
		return nil, modeled("InvalidArn", "Invalid Role Arn: '"+*in.RoleArn+"'")
	}
	logging, err := normalizeLogging(in.LoggingConfiguration, in.LoggingConfiguration != nil)
	if err != nil {
		return nil, err
	}
	when := s.now()
	updated := m
	changed := in.Definition != nil || in.RoleArn != nil || in.LoggingConfiguration != nil
	if changed {
		var ok bool
		updated, ok = s.Store.UpdateMachine(m.ARN, in.Definition, in.RoleArn, logging, in.LoggingConfiguration != nil, when)
		if !ok {
			return nil, modeled("StateMachineDoesNotExist", "State Machine Does Not Exist: '"+m.ARN+"'")
		}
	} else if m.Updated != nil {
		when = *m.Updated
	} else {
		when = m.Created
	}
	out := &gen.UpdateStateMachineOutput{UpdateDate: ts(when), RevisionId: sp(updated.Revision)}
	if !publish {
		return out, nil
	}
	v, err := s.publishVersion(updated.ARN, "", valueOf(in.VersionDescription))
	if err != nil {
		return nil, err
	}
	out.StateMachineVersionArn = sp(v.ARN)
	return out, nil
}

func (s *Service) DeleteStateMachine(_ context.Context, in *gen.DeleteStateMachineInput) (*gen.DeleteStateMachineOutput, error) {
	arn := ""
	if in != nil && in.StateMachineArn != nil {
		arn = *in.StateMachineArn
	}
	if !ValidStateMachineARN(arn) {
		if s.Profile == ProfileSFNLocal {
			return &gen.DeleteStateMachineOutput{}, nil
		}
		return nil, modeled("InvalidArn", "Invalid Arn: '"+arn+"'")
	}
	if !s.Store.MarkDeleting(arn) {
		if s.Profile == ProfileSFNLocal {
			return &gen.DeleteStateMachineOutput{}, nil
		}
		return nil, modeled("StateMachineDoesNotExist", "State Machine Does Not Exist: '"+arn+"'")
	}
	return &gen.DeleteStateMachineOutput{}, nil
}

func rejectEncryptionTracing(encryption, tracing bool) error {
	switch {
	case encryption:
		return modeled("InvalidEncryptionConfiguration", "encryptionConfiguration is not supported by this local server.")
	case tracing:
		return modeled("InvalidTracingConfiguration", "tracingConfiguration is not supported by this local server.")
	default:
		return nil
	}
}

func (s *Service) roleForCreate(role *string) (string, error) {
	if role == nil {
		if s.Profile != ProfileSFNLocal {
			return "", modeled("ValidationException", "roleArn is required")
		}
		s.logf("roleArn is absent; continuing\n")
		return "", nil
	}
	if !RoleARNValid(*role) {
		return "", modeled("InvalidArn", "Invalid Role Arn: '"+*role+"'")
	}
	return *role, nil
}

func (s *Service) machineByARN(arn *string) (store.Machine, error) {
	text := ""
	if arn != nil {
		text = *arn
	}
	if !ValidStateMachineARN(text) {
		return store.Machine{}, modeled("InvalidArn", "Invalid Arn: '"+text+"'")
	}
	m, ok := s.Store.Machine(text)
	if !ok {
		return store.Machine{}, modeled("StateMachineDoesNotExist", "State Machine Does Not Exist: '"+text+"'")
	}
	return m, nil
}

func machineDescription(m store.Machine) *gen.DescribeStateMachineOutput {
	status := gen.StateMachineStatus(m.Status)
	typ := gen.StateMachineType(m.Type)
	out := &gen.DescribeStateMachineOutput{
		Name:                 sp(m.Name),
		Status:               &status,
		Definition:           sp(m.Definition),
		CreationDate:         ts(m.Created),
		StateMachineArn:      sp(m.ARN),
		Type:                 &typ,
		LoggingConfiguration: toGenLogging(m.Logging),
	}
	if m.RoleArn != "" {
		out.RoleArn = sp(m.RoleArn)
	}
	if m.Revision != "" {
		out.RevisionId = sp(m.Revision)
	}
	return out
}

func (s *Service) account() string {
	if s.Account == "" {
		return "123456789012"
	}
	return s.Account
}

func (s *Service) region() string {
	if s.Region == "" {
		return "us-east-1"
	}
	return s.Region
}

// awsConfiguration fills in the settings AWS reports for a machine that set
// none: logging OFF without execution data, an AWS-owned key, and tracing
// off. This server does not encrypt or trace.
func awsConfiguration(logging *gen.LoggingConfiguration) (*gen.LoggingConfiguration, *gen.EncryptionConfiguration, *gen.TracingConfiguration) {
	if logging == nil {
		off := gen.LogLevelOFF
		no := false
		logging = &gen.LoggingConfiguration{Level: &off, IncludeExecutionData: &no}
	}
	owned := gen.EncryptionTypeAWS_OWNED_KEY
	disabled := false
	return logging, &gen.EncryptionConfiguration{Type: &owned}, &gen.TracingConfiguration{Enabled: &disabled}
}
