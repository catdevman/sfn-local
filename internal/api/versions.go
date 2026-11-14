package api

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"

	"github.com/catdevman/sfn-local/internal/api/gen"
	"github.com/catdevman/sfn-local/internal/asl"
	"github.com/catdevman/sfn-local/internal/store"
)

// rollAlias returns an integer in [0, 100). AliasRoll overrides it in tests.
func (s *Service) rollAlias() int {
	if s != nil && s.AliasRoll != nil {
		n := s.AliasRoll()
		if n < 0 {
			return 0
		}
		if n > 99 {
			return 99
		}
		return n
	}
	var b [1]byte
	_, _ = rand.Read(b[:])
	return int(b[0] % 100)
}

type startRef struct {
	machine    store.Machine
	definition string
	role       string
	versionARN string
	aliasARN   string
}

func (s *Service) resolveStart(arn string) (startRef, error) {
	base, qual, ok := splitQualifiedARN(arn)
	if !ok {
		return startRef{}, modeled("InvalidArn", "Invalid Arn: '"+arn+"'")
	}
	switch qualifierKind(qual) {
	case "machine":
		m, err := s.machineByARN(&base)
		if err != nil {
			return startRef{}, err
		}
		return startRef{machine: m, definition: m.Definition, role: m.RoleArn}, nil
	case "version":
		return s.refVersion(arn, "")
	case "alias":
		a, found := s.Store.Alias(arn)
		if !found {
			return startRef{}, modeled("StateMachineDoesNotExist", "State Machine Does Not Exist: '"+arn+"'")
		}
		return s.refVersion(pickVersion(a.Routes, s.rollAlias()), a.ARN)
	default:
		return startRef{}, modeled("InvalidArn", "Invalid Arn: '"+arn+"'")
	}
}

func (s *Service) refVersion(versionARN, aliasARN string) (startRef, error) {
	v, ok := s.Store.Version(versionARN)
	if !ok {
		return startRef{}, modeled("StateMachineDoesNotExist", "State Machine Does Not Exist: '"+versionARN+"'")
	}
	m, found := s.Store.Machine(v.MachineARN)
	if !found {
		return startRef{}, modeled("StateMachineDoesNotExist", "State Machine Does Not Exist: '"+v.MachineARN+"'")
	}
	return startRef{
		machine:    m,
		definition: v.Definition,
		role:       v.RoleArn,
		versionARN: v.ARN,
		aliasARN:   aliasARN,
	}, nil
}

func pickVersion(routes []store.Route, roll int) string {
	if len(routes) == 0 {
		return ""
	}
	if roll < 0 {
		roll = 0
	}
	if roll > 99 {
		roll = 99
	}
	acc := 0
	for _, route := range routes {
		acc += int(route.Weight)
		if roll < acc {
			return route.VersionARN
		}
	}
	return routes[len(routes)-1].VersionARN
}

func (s *Service) listedMachine(arn string) (store.Machine, string, string, error) {
	base, qual, ok := splitQualifiedARN(arn)
	if !ok {
		return store.Machine{}, "", "", modeled("InvalidArn", "Invalid Arn: '"+arn+"'")
	}
	switch qualifierKind(qual) {
	case "machine":
		m, err := s.machineByARN(&base)
		return m, "", "", err
	case "version":
		v, found := s.Store.Version(arn)
		if !found {
			return store.Machine{}, "", "", modeled("StateMachineDoesNotExist", "State Machine Does Not Exist: '"+arn+"'")
		}
		m, found := s.Store.Machine(v.MachineARN)
		if !found {
			return store.Machine{}, "", "", modeled("StateMachineDoesNotExist", "State Machine Does Not Exist: '"+v.MachineARN+"'")
		}
		return m, v.ARN, "", nil
	case "alias":
		a, found := s.Store.Alias(arn)
		if !found {
			return store.Machine{}, "", "", modeled("StateMachineDoesNotExist", "State Machine Does Not Exist: '"+arn+"'")
		}
		m, found := s.Store.Machine(a.MachineARN)
		if !found {
			return store.Machine{}, "", "", modeled("StateMachineDoesNotExist", "State Machine Does Not Exist: '"+a.MachineARN+"'")
		}
		return m, "", a.ARN, nil
	default:
		return store.Machine{}, "", "", modeled("InvalidArn", "Invalid Arn: '"+arn+"'")
	}
}

func (s *Service) unqualifiedMachine(arn string) (store.Machine, error) {
	base, qual, ok := splitQualifiedARN(arn)
	if !ok {
		return store.Machine{}, modeled("InvalidArn", "Invalid Arn: '"+arn+"'")
	}
	if qual != "" {
		return store.Machine{}, validationReason("This operation does not accept a version or alias ARN", gen.ValidationExceptionReasonAPI_DOES_NOT_SUPPORT_LABELED_ARNS)
	}
	return s.machineByARN(&base)
}

func (s *Service) publishVersion(machineARN, wantRevision, description string) (store.Version, error) {
	v, err := s.Store.PublishVersion(machineARN, wantRevision, description, s.now())
	if err == nil {
		return v, nil
	}
	return store.Version{}, versionErr(err)
}

func versionErr(err error) error {
	switch e := err.(type) {
	case *store.MissingError:
		return modeled("StateMachineDoesNotExist", "State Machine Does Not Exist: '"+e.ARN+"'")
	case *store.DeletingError:
		return modeled("StateMachineDeleting", "State Machine is being deleted: '"+e.ARN+"'")
	case *store.RevisionMismatch:
		return modeled("ConflictException", "Revision Id does not match the current revision '"+e.Current+"'")
	case *store.LimitError:
		return modeled("ServiceQuotaExceededException", "You have reached the maximum number of "+e.Resource+".")
	case *store.AliasExistsError:
		return modeled("ConflictException", "State Machine Alias Already Exists: '"+e.ARN+"'")
	case *store.VersionInUseError:
		return modeled("ConflictException", "State machine version is referenced by alias '"+e.AliasARN+"'")
	default:
		return err
	}
}

func (s *Service) PublishStateMachineVersion(_ context.Context, in *gen.PublishStateMachineVersionInput) (*gen.PublishStateMachineVersionOutput, error) {
	m, err := s.unqualifiedMachine(valueOf(in.StateMachineArn))
	if err != nil {
		return nil, err
	}
	v, err := s.publishVersion(m.ARN, valueOf(in.RevisionId), valueOf(in.Description))
	if err != nil {
		return nil, err
	}
	return &gen.PublishStateMachineVersionOutput{
		CreationDate:           ts(v.Created),
		StateMachineVersionArn: sp(v.ARN),
	}, nil
}

func (s *Service) ListStateMachineVersions(_ context.Context, in *gen.ListStateMachineVersionsInput) (*gen.ListStateMachineVersionsOutput, error) {
	m, err := s.unqualifiedMachine(valueOf(in.StateMachineArn))
	if err != nil {
		return nil, err
	}
	rows := s.Store.Versions(m.ARN)
	var max *int32
	var token *string
	if in != nil {
		max, token = in.MaxResults, in.NextToken
	}
	start, end, next, err := pageBounds(len(rows), max, token)
	if err != nil {
		return nil, err
	}
	list := make(gen.StateMachineVersionList, 0, end-start)
	for _, v := range rows[start:end] {
		list = append(list, gen.StateMachineVersionListItem{
			StateMachineVersionArn: sp(v.ARN),
			CreationDate:           ts(v.Created),
		})
	}
	return &gen.ListStateMachineVersionsOutput{StateMachineVersions: &list, NextToken: next}, nil
}

func (s *Service) DeleteStateMachineVersion(_ context.Context, in *gen.DeleteStateMachineVersionInput) (*gen.DeleteStateMachineVersionOutput, error) {
	arn := valueOf(in.StateMachineVersionArn)
	if _, qual, ok := splitQualifiedARN(arn); !ok || qualifierKind(qual) != "version" {
		return nil, modeled("InvalidArn", "Invalid Arn: '"+arn+"'")
	}
	if err := s.Store.DeleteVersion(arn); err != nil {
		return nil, versionErr(err)
	}
	return &gen.DeleteStateMachineVersionOutput{}, nil
}

func (s *Service) CreateStateMachineAlias(_ context.Context, in *gen.CreateStateMachineAliasInput) (*gen.CreateStateMachineAliasOutput, error) {
	if in == nil || in.RoutingConfiguration == nil {
		return nil, modeled("ValidationException", "routingConfiguration is required")
	}
	name := valueOf(in.Name)
	if qualifierKind(name) != "alias" {
		return nil, modeled("ValidationException", "Invalid alias name: '"+name+"'")
	}
	routes, machineARN, err := s.checkRoutes(*in.RoutingConfiguration)
	if err != nil {
		return nil, err
	}
	a, err := s.Store.CreateAlias(machineARN, name, valueOf(in.Description), routes, s.now())
	if err != nil {
		return nil, versionErr(err)
	}
	return &gen.CreateStateMachineAliasOutput{
		StateMachineAliasArn: sp(a.ARN),
		CreationDate:         ts(a.Created),
	}, nil
}

func (s *Service) DescribeStateMachineAlias(_ context.Context, in *gen.DescribeStateMachineAliasInput) (*gen.DescribeStateMachineAliasOutput, error) {
	arn := valueOf(in.StateMachineAliasArn)
	if _, qual, ok := splitQualifiedARN(arn); !ok || qualifierKind(qual) != "alias" {
		return nil, modeled("InvalidArn", "Invalid Arn: '"+arn+"'")
	}
	a, ok := s.Store.Alias(arn)
	if !ok {
		return nil, modeled("StateMachineDoesNotExist", "State Machine Does Not Exist: '"+arn+"'")
	}
	out := &gen.DescribeStateMachineAliasOutput{
		StateMachineAliasArn: sp(a.ARN),
		Name:                 sp(a.Name),
		RoutingConfiguration: toGenRoutes(a.Routes),
		CreationDate:         ts(a.Created),
	}
	if a.Description != "" {
		out.Description = sp(a.Description)
	}
	if a.Updated != nil {
		out.UpdateDate = ts(*a.Updated)
	}
	return out, nil
}

func (s *Service) ListStateMachineAliases(_ context.Context, in *gen.ListStateMachineAliasesInput) (*gen.ListStateMachineAliasesOutput, error) {
	m, err := s.unqualifiedMachine(valueOf(in.StateMachineArn))
	if err != nil {
		return nil, err
	}
	rows := s.Store.Aliases(m.ARN)
	var max *int32
	var token *string
	if in != nil {
		max, token = in.MaxResults, in.NextToken
	}
	start, end, next, err := pageBounds(len(rows), max, token)
	if err != nil {
		return nil, err
	}
	list := make(gen.StateMachineAliasList, 0, end-start)
	for _, a := range rows[start:end] {
		list = append(list, gen.StateMachineAliasListItem{
			StateMachineAliasArn: sp(a.ARN),
			CreationDate:         ts(a.Created),
		})
	}
	return &gen.ListStateMachineAliasesOutput{StateMachineAliases: &list, NextToken: next}, nil
}

func (s *Service) UpdateStateMachineAlias(_ context.Context, in *gen.UpdateStateMachineAliasInput) (*gen.UpdateStateMachineAliasOutput, error) {
	arn := valueOf(in.StateMachineAliasArn)
	if _, qual, ok := splitQualifiedARN(arn); !ok || qualifierKind(qual) != "alias" {
		return nil, modeled("InvalidArn", "Invalid Arn: '"+arn+"'")
	}
	if in.Description == nil && in.RoutingConfiguration == nil {
		return nil, modeled("ValidationException", "At least one of description or routingConfiguration is required")
	}
	var routes []store.Route
	if in.RoutingConfiguration != nil {
		checked, _, err := s.checkRoutes(*in.RoutingConfiguration)
		if err != nil {
			return nil, err
		}
		routes = checked
	}
	a, err := s.Store.UpdateAlias(arn, in.Description, routes, in.RoutingConfiguration != nil, s.now())
	if err != nil {
		return nil, versionErr(err)
	}
	when := a.Created
	if a.Updated != nil {
		when = *a.Updated
	}
	return &gen.UpdateStateMachineAliasOutput{UpdateDate: ts(when)}, nil
}

func (s *Service) DeleteStateMachineAlias(_ context.Context, in *gen.DeleteStateMachineAliasInput) (*gen.DeleteStateMachineAliasOutput, error) {
	arn := valueOf(in.StateMachineAliasArn)
	if _, qual, ok := splitQualifiedARN(arn); !ok || qualifierKind(qual) != "alias" {
		return nil, modeled("InvalidArn", "Invalid Arn: '"+arn+"'")
	}
	if err := s.Store.DeleteAlias(arn); err != nil {
		return nil, versionErr(err)
	}
	return &gen.DeleteStateMachineAliasOutput{}, nil
}

func (s *Service) ValidateStateMachineDefinition(_ context.Context, in *gen.ValidateStateMachineDefinitionInput) (*gen.ValidateStateMachineDefinitionOutput, error) {
	if in == nil || in.Definition == nil {
		return nil, modeled("ValidationException", "1 validation error detected: Value null at 'definition' failed to satisfy constraint: Member must not be null")
	}
	typ := store.TypeStandard
	if in.Type != nil && string(*in.Type) != "" {
		if !in.Type.Valid() {
			return nil, modeled("ValidationException", "type must be STANDARD or EXPRESS")
		}
		typ = string(*in.Type)
	}
	if in.Severity != nil && !in.Severity.Valid() {
		return nil, modeled("ValidationException", "severity must be ERROR or WARNING")
	}
	_, err := asl.ValidateType(*in.Definition, typ)
	var found gen.ValidateStateMachineDefinitionDiagnosticList
	failed := false
	if err != nil {
		failed = true
		code, message, loc := "SCHEMA_VALIDATION_FAILED", err.Error(), "/"
		var de *asl.DefinitionError
		if errors.As(err, &de) {
			code, message, loc = de.Code, de.Message, de.Location
		}
		sev := gen.ValidateStateMachineDefinitionSeverityERROR
		found = append(found, gen.ValidateStateMachineDefinitionDiagnostic{
			Severity: &sev,
			Code:     sp(code),
			Message:  sp(message),
			Location: sp(loc),
		})
		// AWS also reports a definition with no terminal state, after a
		// missing transition target.
		if code == "MISSING_TRANSITION_TARGET" && !hasTerminalState(*in.Definition) {
			found = append(found, gen.ValidateStateMachineDefinitionDiagnostic{
				Severity: &sev,
				Code:     sp("MISSING_END_STATE"),
				Message:  sp("Workflow has no terminal state"),
			})
		}
	}
	if in.Severity != nil && *in.Severity == gen.ValidateStateMachineDefinitionSeverityERROR {
		kept := found[:0]
		for _, d := range found {
			if d.Severity != nil && *d.Severity == gen.ValidateStateMachineDefinitionSeverityERROR {
				kept = append(kept, d)
			}
		}
		found = kept
	}
	limit := 100
	if in.MaxResults != nil {
		if *in.MaxResults < 0 || *in.MaxResults > 100 {
			return nil, modeled("ValidationException", "maxResults must be between 0 and 100")
		}
		limit = int(*in.MaxResults)
	}
	truncated := false
	if len(found) > limit {
		found = found[:limit]
		truncated = true
	}
	if found == nil {
		found = gen.ValidateStateMachineDefinitionDiagnosticList{}
	}
	result := gen.ValidateStateMachineDefinitionResultCodeOK
	if failed {
		result = gen.ValidateStateMachineDefinitionResultCodeFAIL
	}
	return &gen.ValidateStateMachineDefinitionOutput{
		Result:      &result,
		Diagnostics: &found,
		Truncated:   &truncated,
	}, nil
}

func (s *Service) checkRoutes(list gen.RoutingConfigurationList) ([]store.Route, string, error) {
	if len(list) < 1 || len(list) > 2 {
		return nil, "", routingErr("routingConfiguration must contain 1 or 2 versions")
	}
	var sum int32
	var machine string
	seen := map[string]bool{}
	out := make([]store.Route, 0, len(list))
	for _, item := range list {
		if item.StateMachineVersionArn == nil || item.Weight == nil {
			return nil, "", routingErr("stateMachineVersionArn and weight are required")
		}
		arn := *item.StateMachineVersionArn
		if seen[arn] {
			return nil, "", routingErr("routingConfiguration repeats a version ARN")
		}
		seen[arn] = true
		if *item.Weight < 0 || *item.Weight > 100 {
			return nil, "", routingErr("weight must be between 0 and 100")
		}
		v, ok := s.Store.Version(arn)
		if !ok {
			return nil, "", modeled("StateMachineDoesNotExist", "State Machine Does Not Exist: '"+arn+"'")
		}
		if machine == "" {
			machine = v.MachineARN
		} else if machine != v.MachineARN {
			return nil, "", routingErr("versions must belong to the same state machine")
		}
		sum += *item.Weight
		out = append(out, store.Route{VersionARN: arn, Weight: *item.Weight})
	}
	if sum != 100 {
		return nil, "", routingErr("weights must sum to 100")
	}
	m, ok := s.Store.Machine(machine)
	if !ok {
		return nil, "", modeled("StateMachineDoesNotExist", "State Machine Does Not Exist: '"+machine+"'")
	}
	if m.Status == store.StatusDeleting {
		return nil, "", modeled("StateMachineDeleting", "State Machine is being deleted: '"+machine+"'")
	}
	return out, machine, nil
}

func routingErr(message string) error {
	return validationReason(message, gen.ValidationExceptionReasonINVALID_ROUTING_CONFIGURATION)
}

func validationReason(message string, reason gen.ValidationExceptionReason) error {
	m := message
	r := reason
	return &gen.ValidationException{Message: &m, Reason: &r}
}

func toGenRoutes(routes []store.Route) *gen.RoutingConfigurationList {
	list := make(gen.RoutingConfigurationList, 0, len(routes))
	for _, route := range routes {
		arn, weight := route.VersionARN, route.Weight
		list = append(list, gen.RoutingConfigurationListItem{
			StateMachineVersionArn: &arn,
			Weight:                 &weight,
		})
	}
	return &list
}

func rejectVersionDescription(publish *bool, desc *string) error {
	if desc != nil && (publish == nil || !*publish) {
		return modeled("ValidationException", "versionDescription requires publish to be true")
	}
	return nil
}

// hasTerminalState reports whether a top-level state ends the workflow:
// End true, or a Succeed or Fail state.
func hasTerminalState(definition string) bool {
	var doc struct {
		States map[string]struct {
			Type string
			End  bool
		}
	}
	if json.Unmarshal([]byte(definition), &doc) != nil {
		return true
	}
	for _, st := range doc.States {
		if st.End || st.Type == "Succeed" || st.Type == "Fail" {
			return true
		}
	}
	return false
}
