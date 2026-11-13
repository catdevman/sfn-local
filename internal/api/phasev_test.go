package api

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/catdevman/sfn-local/internal/api/gen"
	"github.com/catdevman/sfn-local/internal/store"
)

func TestValidateStateMachineDefinition(t *testing.T) {
	s := newService()
	okOut, err := s.ValidateStateMachineDefinition(context.Background(), &gen.ValidateStateMachineDefinitionInput{
		Definition: sp(passDef),
	})
	if err != nil || okOut.Result == nil || *okOut.Result != gen.ValidateStateMachineDefinitionResultCodeOK || okOut.Diagnostics == nil || len(*okOut.Diagnostics) != 0 || okOut.Truncated == nil || *okOut.Truncated {
		t.Fatalf("%+v %v", okOut, err)
	}
	bad, err := s.ValidateStateMachineDefinition(context.Background(), &gen.ValidateStateMachineDefinitionInput{
		Definition: sp(`{"StartAt":"P","States":{"P":{"Type":"Pass","Nope":1,"End":true}}}`),
	})
	if err != nil || *bad.Result != gen.ValidateStateMachineDefinitionResultCodeFAIL || len(*bad.Diagnostics) != 1 {
		t.Fatalf("%+v %v", bad, err)
	}
	diag := (*bad.Diagnostics)[0]
	if diag.Code == nil || *diag.Code != "SCHEMA_VALIDATION_FAILED" || diag.Message == nil || !strings.Contains(*diag.Message, "Nope") || diag.Location == nil || *diag.Severity != gen.ValidateStateMachineDefinitionSeverityERROR {
		t.Fatalf("%+v", diag)
	}
	zero := int32(0)
	trunc, err := s.ValidateStateMachineDefinition(context.Background(), &gen.ValidateStateMachineDefinitionInput{
		Definition: sp(`not-json`), MaxResults: &zero,
	})
	if err != nil || *trunc.Result != gen.ValidateStateMachineDefinitionResultCodeFAIL || len(*trunc.Diagnostics) != 0 || trunc.Truncated == nil || !*trunc.Truncated {
		t.Fatalf("%+v %v", trunc, err)
	}
	warn := gen.ValidateStateMachineDefinitionSeverityWARNING
	still, err := s.ValidateStateMachineDefinition(context.Background(), &gen.ValidateStateMachineDefinitionInput{
		Definition: sp(`not-json`), Severity: &warn,
	})
	if err != nil || *still.Result != gen.ValidateStateMachineDefinitionResultCodeFAIL || len(*still.Diagnostics) != 1 {
		t.Fatalf("WARNING filter drops errors %+v %v", still, err)
	}
	activity := `{"StartAt":"A","States":{"A":{"Type":"Task","Resource":"arn:aws:states:us-east-1:123456789012:activity:work","End":true}}}`
	standard := gen.StateMachineTypeSTANDARD
	express := gen.StateMachineTypeEXPRESS
	std, err := s.ValidateStateMachineDefinition(context.Background(), &gen.ValidateStateMachineDefinitionInput{
		Definition: sp(activity), Type: &standard,
	})
	if err != nil || *std.Result != gen.ValidateStateMachineDefinitionResultCodeOK {
		t.Fatalf("standard activity %+v %v", std, err)
	}
	ex, err := s.ValidateStateMachineDefinition(context.Background(), &gen.ValidateStateMachineDefinitionInput{
		Definition: sp(activity), Type: &express,
	})
	if err != nil || *ex.Result != gen.ValidateStateMachineDefinitionResultCodeFAIL || !strings.Contains(*(*ex.Diagnostics)[0].Message, "Activity ARN") {
		t.Fatalf("express activity %+v %v", ex, err)
	}
}

func TestVersionsAliasesAndStart(t *testing.T) {
	s := newService()
	pub := true
	def1 := `{"StartAt":"P","States":{"P":{"Type":"Pass","Result":{"v":1},"End":true}}}`
	def2 := `{"StartAt":"P","States":{"P":{"Type":"Pass","Result":{"v":2},"End":true}}}`
	created, err := s.CreateStateMachine(context.Background(), &gen.CreateStateMachineInput{
		Name: sp("roll"), Definition: sp(def1), RoleArn: sp(roleARN), Publish: &pub, VersionDescription: sp("first"),
	})
	if err != nil || created.StateMachineVersionArn == nil || !strings.HasSuffix(*created.StateMachineVersionArn, ":1") {
		t.Fatal(err, created)
	}
	again, err := s.CreateStateMachine(context.Background(), &gen.CreateStateMachineInput{
		Name: sp("roll"), Definition: sp(def1), RoleArn: sp(roleARN), Publish: &pub, VersionDescription: sp("ignored"),
	})
	if err != nil || *again.StateMachineVersionArn != *created.StateMachineVersionArn {
		t.Fatal(err, again)
	}
	live, err := s.DescribeStateMachine(context.Background(), &gen.DescribeStateMachineInput{StateMachineArn: created.StateMachineArn})
	if err != nil || live.RevisionId == nil {
		t.Fatal(err, live)
	}
	ver, err := s.DescribeStateMachine(context.Background(), &gen.DescribeStateMachineInput{StateMachineArn: created.StateMachineVersionArn})
	if err != nil || *ver.Definition != def1 || ver.Description == nil || *ver.Description != "first" || *ver.StateMachineArn != *created.StateMachineVersionArn || *ver.RevisionId != *live.RevisionId {
		t.Fatalf("%+v", ver)
	}
	_, err = s.DescribeStateMachine(context.Background(), &gen.DescribeStateMachineInput{StateMachineArn: sp(*created.StateMachineArn + ":prod")})
	var reason *gen.ValidationException
	if !errors.As(err, &reason) || reason.Reason == nil || *reason.Reason != gen.ValidationExceptionReasonAPI_DOES_NOT_SUPPORT_LABELED_ARNS {
		t.Fatal(err)
	}
	stale := *live.RevisionId
	updated, err := s.UpdateStateMachine(context.Background(), &gen.UpdateStateMachineInput{
		StateMachineArn: created.StateMachineArn, Definition: sp(def2), Publish: &pub, VersionDescription: sp("second"), RoleArn: sp(rolePath),
	})
	if err != nil || updated.StateMachineVersionArn == nil || !strings.HasSuffix(*updated.StateMachineVersionArn, ":2") || updated.RevisionId == nil || *updated.RevisionId == stale {
		t.Fatal(err, updated)
	}
	conflict, err := s.PublishStateMachineVersion(context.Background(), &gen.PublishStateMachineVersionInput{
		StateMachineArn: created.StateMachineArn, RevisionId: sp(stale),
	})
	if conflict != nil || mustAPI(t, err).ErrorName() != "ConflictException" {
		t.Fatal(err, conflict)
	}
	same, err := s.PublishStateMachineVersion(context.Background(), &gen.PublishStateMachineVersionInput{
		StateMachineArn: created.StateMachineArn, Description: sp("nope"),
	})
	if err != nil || *same.StateMachineVersionArn != *updated.StateMachineVersionArn {
		t.Fatal(err, same)
	}
	old, err := s.DescribeStateMachine(context.Background(), &gen.DescribeStateMachineInput{StateMachineArn: created.StateMachineVersionArn})
	if err != nil || *old.Definition != def1 || *old.RoleArn != roleARN || *old.Description != "first" {
		t.Fatalf("version moved %+v", old)
	}
	one := waitExec(t, s, *startExec(t, s, *created.StateMachineVersionArn, "v1", `{}`).ExecutionArn)
	if one.Status != store.ExecSucceeded || one.Output != `{"v":1}` || one.VersionARN != *created.StateMachineVersionArn || one.AliasARN != "" {
		t.Fatalf("version start %+v", one)
	}
	cur := waitExec(t, s, *startExec(t, s, *created.StateMachineArn, "cur", `{}`).ExecutionArn)
	if cur.Output != `{"v":2}` || cur.VersionARN != "" {
		t.Fatalf("current start %+v", cur)
	}
	w100 := int32(100)
	alias, err := s.CreateStateMachineAlias(context.Background(), &gen.CreateStateMachineAliasInput{
		Name: sp("prod"), Description: sp("live"),
		RoutingConfiguration: &gen.RoutingConfigurationList{{StateMachineVersionArn: created.StateMachineVersionArn, Weight: &w100}},
	})
	if err != nil || !strings.HasSuffix(*alias.StateMachineAliasArn, ":prod") {
		t.Fatal(err, alias)
	}
	via := waitExec(t, s, *startExec(t, s, *alias.StateMachineAliasArn, "alias", `{}`).ExecutionArn)
	if via.Output != `{"v":1}` || via.AliasARN != *alias.StateMachineAliasArn || via.VersionARN != *created.StateMachineVersionArn {
		t.Fatalf("alias start %+v", via)
	}
	described, err := s.DescribeExecution(context.Background(), &gen.DescribeExecutionInput{ExecutionArn: sp(via.ARN)})
	if err != nil || described.StateMachineVersionArn == nil || *described.StateMachineVersionArn != via.VersionARN || described.StateMachineAliasArn == nil || *described.StateMachineAliasArn != via.AliasARN {
		t.Fatalf("%+v %v", described, err)
	}
	if via.History[0].VersionARN != via.VersionARN || via.History[0].AliasARN != via.AliasARN {
		t.Fatalf("history %+v", via.History[0])
	}
	forExec, err := s.DescribeStateMachineForExecution(context.Background(), &gen.DescribeStateMachineForExecutionInput{ExecutionArn: sp(via.ARN)})
	if err != nil || *forExec.Definition != def1 || *forExec.StateMachineArn != *created.StateMachineVersionArn {
		t.Fatalf("%+v %v", forExec, err)
	}
	w50 := int32(50)
	rolls := []int{0, 99}
	n := 0
	s.AliasRoll = func() int {
		v := rolls[n]
		n++
		return v
	}
	split, err := s.CreateStateMachineAlias(context.Background(), &gen.CreateStateMachineAliasInput{
		Name: sp("split"),
		RoutingConfiguration: &gen.RoutingConfigurationList{
			{StateMachineVersionArn: created.StateMachineVersionArn, Weight: &w50},
			{StateMachineVersionArn: updated.StateMachineVersionArn, Weight: &w50},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	first := waitExec(t, s, *startExec(t, s, *split.StateMachineAliasArn, "a", `{}`).ExecutionArn)
	second := waitExec(t, s, *startExec(t, s, *split.StateMachineAliasArn, "b", `{}`).ExecutionArn)
	if first.Output != `{"v":1}` || second.Output != `{"v":2}` {
		t.Fatalf("%s %s", first.Output, second.Output)
	}
	s.AliasRoll = nil
	if _, err := s.DeleteStateMachineVersion(context.Background(), &gen.DeleteStateMachineVersionInput{StateMachineVersionArn: created.StateMachineVersionArn}); mustAPI(t, err).ErrorName() != "ConflictException" {
		t.Fatal(err)
	}
	if _, err := s.DeleteStateMachineAlias(context.Background(), &gen.DeleteStateMachineAliasInput{StateMachineAliasArn: alias.StateMachineAliasArn}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DeleteStateMachineAlias(context.Background(), &gen.DeleteStateMachineAliasInput{StateMachineAliasArn: split.StateMachineAliasArn}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DeleteStateMachineVersion(context.Background(), &gen.DeleteStateMachineVersionInput{StateMachineVersionArn: created.StateMachineVersionArn}); err != nil {
		t.Fatal(err)
	}
	def3 := `{"StartAt":"P","States":{"P":{"Type":"Pass","Result":{"v":3},"End":true}}}`
	if _, err := s.UpdateStateMachine(context.Background(), &gen.UpdateStateMachineInput{
		StateMachineArn: created.StateMachineArn, Definition: sp(def3),
	}); err != nil {
		t.Fatal(err)
	}
	republish, err := s.PublishStateMachineVersion(context.Background(), &gen.PublishStateMachineVersionInput{StateMachineArn: created.StateMachineArn})
	if err != nil || !strings.HasSuffix(*republish.StateMachineVersionArn, ":3") {
		t.Fatal(err, republish)
	}
	listed, err := s.ListStateMachineVersions(context.Background(), &gen.ListStateMachineVersionsInput{StateMachineArn: created.StateMachineArn})
	if err != nil || len(*listed.StateMachineVersions) != 2 {
		t.Fatal(err, listed)
	}
	onePage := int32(1)
	page, err := s.ListStateMachineVersions(context.Background(), &gen.ListStateMachineVersionsInput{StateMachineArn: created.StateMachineArn, MaxResults: &onePage})
	if err != nil || len(*page.StateMachineVersions) != 1 || page.NextToken == nil || *page.NextToken != "1" {
		t.Fatalf("%+v %v", page, err)
	}
	_, err = s.CreateStateMachineAlias(context.Background(), &gen.CreateStateMachineAliasInput{
		Name:                 sp("123"),
		RoutingConfiguration: &gen.RoutingConfigurationList{{StateMachineVersionArn: updated.StateMachineVersionArn, Weight: &w100}},
	})
	if mustAPI(t, err).ErrorName() != "ValidationException" {
		t.Fatal(err)
	}
	w40 := int32(40)
	_, err = s.CreateStateMachineAlias(context.Background(), &gen.CreateStateMachineAliasInput{
		Name:                 sp("bad-weight"),
		RoutingConfiguration: &gen.RoutingConfigurationList{{StateMachineVersionArn: updated.StateMachineVersionArn, Weight: &w40}},
	})
	if !errors.As(err, &reason) || reason.Reason == nil || *reason.Reason != gen.ValidationExceptionReasonINVALID_ROUTING_CONFIGURATION {
		t.Fatal(err)
	}
	other, err := s.CreateStateMachine(context.Background(), &gen.CreateStateMachineInput{
		Name: sp("other"), Definition: sp(def2), RoleArn: sp(roleARN), Publish: &pub,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.CreateStateMachineAlias(context.Background(), &gen.CreateStateMachineAliasInput{
		Name: sp("cross"),
		RoutingConfiguration: &gen.RoutingConfigurationList{
			{StateMachineVersionArn: updated.StateMachineVersionArn, Weight: &w50},
			{StateMachineVersionArn: other.StateMachineVersionArn, Weight: &w50},
		},
	})
	if !errors.As(err, &reason) || *reason.Reason != gen.ValidationExceptionReasonINVALID_ROUTING_CONFIGURATION {
		t.Fatal(err)
	}
	kept, err := s.CreateStateMachineAlias(context.Background(), &gen.CreateStateMachineAliasInput{
		Name: sp("zeta"), RoutingConfiguration: &gen.RoutingConfigurationList{{StateMachineVersionArn: updated.StateMachineVersionArn, Weight: &w100}},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.CreateStateMachineAlias(context.Background(), &gen.CreateStateMachineAliasInput{
		Name: sp("alpha"), RoutingConfiguration: &gen.RoutingConfigurationList{{StateMachineVersionArn: updated.StateMachineVersionArn, Weight: &w100}},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.CreateStateMachineAlias(context.Background(), &gen.CreateStateMachineAliasInput{
		Name: sp("zeta"), RoutingConfiguration: &gen.RoutingConfigurationList{{StateMachineVersionArn: updated.StateMachineVersionArn, Weight: &w100}},
	})
	if mustAPI(t, err).ErrorName() != "ConflictException" {
		t.Fatal(err)
	}
	note := "renamed"
	if _, err := s.UpdateStateMachineAlias(context.Background(), &gen.UpdateStateMachineAliasInput{
		StateMachineAliasArn: kept.StateMachineAliasArn, Description: sp(note),
		RoutingConfiguration: &gen.RoutingConfigurationList{{StateMachineVersionArn: republish.StateMachineVersionArn, Weight: &w100}},
	}); err != nil {
		t.Fatal(err)
	}
	got, err := s.DescribeStateMachineAlias(context.Background(), &gen.DescribeStateMachineAliasInput{StateMachineAliasArn: kept.StateMachineAliasArn})
	if err != nil || got.Description == nil || *got.Description != note || got.UpdateDate == nil || len(*got.RoutingConfiguration) != 1 || *(*got.RoutingConfiguration)[0].StateMachineVersionArn != *republish.StateMachineVersionArn {
		t.Fatalf("%+v %v", got, err)
	}
	names, err := s.ListStateMachineAliases(context.Background(), &gen.ListStateMachineAliasesInput{StateMachineArn: created.StateMachineArn})
	if err != nil || len(*names.StateMachineAliases) != 2 || !strings.HasSuffix(*(*names.StateMachineAliases)[0].StateMachineAliasArn, ":alpha") {
		t.Fatalf("%+v %v", names, err)
	}
	aliasRuns := waitExec(t, s, *startExec(t, s, *kept.StateMachineAliasArn, "moved", `{}`).ExecutionArn)
	listedRuns, err := s.ListExecutions(context.Background(), &gen.ListExecutionsInput{StateMachineArn: kept.StateMachineAliasArn})
	if err != nil || listedRuns.Executions == nil || len(*listedRuns.Executions) != 1 || *(*listedRuns.Executions)[0].ExecutionArn != aliasRuns.ARN {
		t.Fatalf("%+v %v", listedRuns, err)
	}
	kms := gen.EncryptionTypeCUSTOMER_MANAGED_KMS_KEY
	key := "arn:aws:kms:us-east-1:123456789012:key/abc"
	_, err = s.CreateStateMachine(context.Background(), &gen.CreateStateMachineInput{
		Name: sp("kms"), Definition: sp(passDef), RoleArn: sp(roleARN),
		EncryptionConfiguration: &gen.EncryptionConfiguration{Type: &kms, KmsKeyId: sp(key)},
	})
	if mustAPI(t, err).ErrorName() != "InvalidEncryptionConfiguration" {
		t.Fatal(err)
	}
	enabled := true
	_, err = s.UpdateStateMachine(context.Background(), &gen.UpdateStateMachineInput{
		StateMachineArn: created.StateMachineArn, TracingConfiguration: &gen.TracingConfiguration{Enabled: &enabled},
	})
	if mustAPI(t, err).ErrorName() != "InvalidTracingConfiguration" {
		t.Fatal(err)
	}

	if _, err := s.DeleteStateMachine(context.Background(), &gen.DeleteStateMachineInput{StateMachineArn: other.StateMachineArn}); err != nil {
		t.Fatal(err)
	}
	s.Store.Sweep()
	if _, err := s.DescribeStateMachine(context.Background(), &gen.DescribeStateMachineInput{StateMachineArn: other.StateMachineVersionArn}); mustAPI(t, err).ErrorName() != "StateMachineDoesNotExist" {
		t.Fatal(err)
	}
}

func TestExpressVersionStart(t *testing.T) {
	s := newService()
	pub := true
	express := gen.StateMachineTypeEXPRESS
	okDef := `{"StartAt":"P","States":{"P":{"Type":"Pass","Result":{"ok":true},"End":true}}}`
	badDef := `{"StartAt":"F","States":{"F":{"Type":"Fail","Error":"Boom","Cause":"no"}}}`
	created, err := s.CreateStateMachine(context.Background(), &gen.CreateStateMachineInput{
		Name: sp("fast"), Definition: sp(okDef), RoleArn: sp(roleARN), Type: &express, Publish: &pub,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.UpdateStateMachine(context.Background(), &gen.UpdateStateMachineInput{
		StateMachineArn: created.StateMachineArn, Definition: sp(badDef),
	}); err != nil {
		t.Fatal(err)
	}
	out, err := s.StartSyncExecution(context.Background(), &gen.StartSyncExecutionInput{
		StateMachineArn: created.StateMachineVersionArn, Name: sp("kept"), Input: sp(`{}`),
	})
	if err != nil || out.Status == nil || *out.Status != gen.SyncExecutionStatusSUCCEEDED || out.Output == nil || *out.Output != `{"ok":true}` {
		t.Fatalf("%+v %v", out, err)
	}
	failed, err := s.StartSyncExecution(context.Background(), &gen.StartSyncExecutionInput{
		StateMachineArn: created.StateMachineArn, Name: sp("now"), Input: sp(`{}`),
	})
	if err != nil || failed.Status == nil || *failed.Status != gen.SyncExecutionStatusFAILED || failed.Error == nil || *failed.Error != "Boom" {
		t.Fatalf("%+v %v", failed, err)
	}
}
