package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/catdevman/sfn-local/internal/api/gen"
	"github.com/catdevman/sfn-local/internal/exec"
	"github.com/catdevman/sfn-local/internal/store"
)

const (
	passDef  = `{"StartAt":"P","States":{"P":{"Type":"Pass","End":true}}}`
	roleARN  = "arn:aws:iam::123456789012:role/local"
	rolePath = "arn:aws:iam::123456789012:role/service-role/MyRole"
)

func newService() *Service {
	st := store.New()
	return &Service{
		Store: st,
		Runner: &exec.Runner{
			Store:   st,
			Region:  "us-east-1",
			Account: "123456789012",
		},
		Account: "123456789012",
		Region:  "us-east-1",
		Profile: ProfileSmithy,
		Log:     io.Discard,
	}
}

func createMachine(t *testing.T, s *Service, name, def, role string) *gen.CreateStateMachineOutput {
	t.Helper()
	out, err := s.CreateStateMachine(context.Background(), &gen.CreateStateMachineInput{
		Name: sp(name), Definition: sp(def), RoleArn: sp(role),
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func mustAPI(t *testing.T, err error) gen.APIError {
	t.Helper()
	if err == nil {
		t.Fatal("expected an error")
	}
	var api gen.APIError
	if !errors.As(err, &api) {
		t.Fatalf("%T %v", err, err)
	}
	return api
}

func TestCreateDescribeListUpdateDelete(t *testing.T) {
	s := newService()
	out := createMachine(t, s, "rebuild-check", passDef, rolePath)
	raw, err := json.Marshal(out)
	if err != nil {
		t.Fatal(err)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var body map[string]any
	if err := dec.Decode(&body); err != nil {
		t.Fatal(err)
	}
	arn, _ := body["stateMachineArn"].(string)
	if !strings.Contains(arn, ":stateMachine:rebuild-check") {
		t.Fatal(arn)
	}
	if _, ok := body["stateMachineVersionArn"]; ok {
		t.Fatal("version arn")
	}
	num, ok := body["creationDate"].(json.Number)
	if !ok || strings.ContainsAny(num.String(), "eE") {
		t.Fatalf("creationDate %v", body["creationDate"])
	}
	again := createMachine(t, s, "rebuild-check", passDef, roleARN)
	if again.CreationDate.Seconds() != out.CreationDate.Seconds() || *again.StateMachineArn != arn {
		t.Fatal("idempotent create changed the row")
	}
	_, err = s.CreateStateMachine(context.Background(), &gen.CreateStateMachineInput{
		Name: sp("rebuild-check"), Definition: sp(`{"StartAt":"S","States":{"S":{"Type":"Succeed"}}}`), RoleArn: sp(roleARN),
	})
	api := mustAPI(t, err)
	if api.ErrorName() != "StateMachineAlreadyExists" || !strings.Contains(api.Error(), arn) {
		t.Fatal(api)
	}
	_, err = s.CreateStateMachine(context.Background(), &gen.CreateStateMachineInput{
		Name: sp("has space"), Definition: sp(passDef), RoleArn: sp(roleARN),
	})
	if mustAPI(t, err).ErrorName() != "InvalidName" {
		t.Fatal(err)
	}
	_, err = s.CreateStateMachine(context.Background(), &gen.CreateStateMachineInput{
		Name: sp("bad"), Definition: sp(`{"StartAt":"P","States":{"P":{"Type":"Pass","Nope":1,"End":true}}}`), RoleArn: sp(roleARN),
	})
	if api = mustAPI(t, err); api.ErrorName() != "InvalidDefinition" || !strings.Contains(api.Error(), "SCHEMA_VALIDATION_FAILED") || !strings.Contains(api.Error(), "Nope") {
		t.Fatal(api)
	}
	created, err := s.CreateStateMachine(context.Background(), &gen.CreateStateMachineInput{
		Name: sp("q"), Definition: sp(`{"StartAt":"P","QueryLanguage":"JSONata","States":{"P":{"Type":"Pass","End":true}}}`), RoleArn: sp(roleARN),
	})
	if err != nil || created.StateMachineArn == nil || !strings.Contains(*created.StateMachineArn, ":stateMachine:q") {
		t.Fatal(err, created)
	}

	described, err := s.DescribeStateMachine(context.Background(), &gen.DescribeStateMachineInput{StateMachineArn: sp(arn)})
	if err != nil {
		t.Fatal(err)
	}
	descRaw, _ := json.Marshal(described)
	var desc map[string]any
	if err := json.Unmarshal(descRaw, &desc); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"name", "status", "definition", "creationDate", "roleArn", "stateMachineArn", "type", "loggingConfiguration", "encryptionConfiguration", "tracingConfiguration"} {
		if desc[key] == nil {
			t.Fatalf("missing %s in %s", key, descRaw)
		}
	}
	// AWS shows a revision only once the machine has been updated.
	if desc["revisionId"] != nil {
		t.Fatalf("new machine has a revision: %s", descRaw)
	}
	if desc["status"] != "ACTIVE" || desc["type"] != "STANDARD" || desc["roleArn"] != rolePath {
		t.Fatalf("%s", descRaw)
	}
	stored, _ := s.Store.Machine(*created.StateMachineArn)
	firstRev := stored.Revision
	if firstRev == "" {
		t.Fatal("revisionId")
	}
	if logging, _ := desc["loggingConfiguration"].(map[string]any); logging["level"] != "OFF" || logging["includeExecutionData"] != false {
		t.Fatalf("default logging %s", descRaw)
	}

	createMachine(t, s, "other", passDef, roleARN)
	listed, err := s.ListStateMachines(context.Background(), &gen.ListStateMachinesInput{})
	if err != nil || listed.StateMachines == nil || len(*listed.StateMachines) != 3 {
		t.Fatal(err, listed)
	}
	if *(*listed.StateMachines)[0].Name != "other" || *(*listed.StateMachines)[1].Name != "q" || *(*listed.StateMachines)[2].Name != "rebuild-check" {
		t.Fatal("sort")
	}
	zero := int32(0)
	page, err := s.ListStateMachines(context.Background(), &gen.ListStateMachinesInput{MaxResults: &zero})
	if err != nil || len(*page.StateMachines) != 0 || page.NextToken == nil || *page.NextToken != "0" {
		t.Fatalf("%+v %v", page, err)
	}
	bad := "01"
	_, err = s.ListStateMachines(context.Background(), &gen.ListStateMachinesInput{NextToken: &bad})
	if mustAPI(t, err).ErrorName() != "InvalidToken" {
		t.Fatal(err)
	}
	start, end, next, err := pageBounds(150, nil, nil)
	if err != nil || start != 0 || end != 100 || next == nil || *next != "100" {
		t.Fatalf("%d %d %v %v", start, end, next, err)
	}

	updated, err := s.UpdateStateMachine(context.Background(), &gen.UpdateStateMachineInput{
		StateMachineArn: sp(arn), Definition: sp(passDef),
	})
	if err != nil || updated.UpdateDate == nil {
		t.Fatal(err)
	}
	_, err = s.UpdateStateMachine(context.Background(), &gen.UpdateStateMachineInput{StateMachineArn: sp(arn)})
	if mustAPI(t, err).ErrorName() != "ValidationException" {
		t.Fatal(err)
	}
	_, err = s.UpdateStateMachine(context.Background(), &gen.UpdateStateMachineInput{
		StateMachineArn: sp(arn), Definition: sp(`{"StartAt":"P","States":{"P":{"Nope":true}}}`),
	})
	if mustAPI(t, err).ErrorName() != "InvalidDefinition" {
		t.Fatal(err)
	}
	_, err = s.UpdateStateMachine(context.Background(), &gen.UpdateStateMachineInput{
		StateMachineArn: sp(arn), RoleArn: sp(roleARN),
	})
	if err != nil {
		t.Fatal(err)
	}
	described, err = s.DescribeStateMachine(context.Background(), &gen.DescribeStateMachineInput{StateMachineArn: sp(arn)})
	if err != nil || *described.RoleArn != roleARN || *described.Definition != passDef {
		t.Fatal(described, err)
	}
	if described.RevisionId == nil || *described.RevisionId == firstRev {
		t.Fatal("revision did not change", described.RevisionId)
	}

	if _, err := s.DeleteStateMachine(context.Background(), &gen.DeleteStateMachineInput{StateMachineArn: sp(arn)}); err != nil {
		t.Fatal(err)
	}
	described, err = s.DescribeStateMachine(context.Background(), &gen.DescribeStateMachineInput{StateMachineArn: sp(arn)})
	if err != nil || *described.Status != gen.StateMachineStatusDELETING {
		t.Fatal(described, err)
	}
	same := createMachine(t, s, "rebuild-check", passDef, roleARN)
	if *same.StateMachineArn != arn {
		t.Fatal(*same.StateMachineArn)
	}
	described, err = s.DescribeStateMachine(context.Background(), &gen.DescribeStateMachineInput{StateMachineArn: sp(arn)})
	if err != nil || *described.Status != gen.StateMachineStatusDELETING {
		t.Fatal("create reset status", described, err)
	}
	s.Store.Sweep()
	_, err = s.DescribeStateMachine(context.Background(), &gen.DescribeStateMachineInput{StateMachineArn: sp(arn)})
	if mustAPI(t, err).ErrorName() != "StateMachineDoesNotExist" {
		t.Fatal(err)
	}
}

func TestMachineExtrasLoggingAndRoleProfile(t *testing.T) {
	s := newService()
	enc := gen.EncryptionTypeAWS_OWNED_KEY
	_, err := s.CreateStateMachine(context.Background(), &gen.CreateStateMachineInput{
		Name: sp("e"), Definition: sp(passDef), RoleArn: sp(roleARN),
		EncryptionConfiguration: &gen.EncryptionConfiguration{Type: &enc},
	})
	if api := mustAPI(t, err); api.ErrorName() != "InvalidEncryptionConfiguration" || !strings.Contains(api.Error(), "encryptionConfiguration") {
		t.Fatal(api)
	}
	enabled := false
	_, err = s.CreateStateMachine(context.Background(), &gen.CreateStateMachineInput{
		Name: sp("t"), Definition: sp(passDef), RoleArn: sp(roleARN),
		TracingConfiguration: &gen.TracingConfiguration{Enabled: &enabled},
	})
	if api := mustAPI(t, err); api.ErrorName() != "InvalidTracingConfiguration" || !strings.Contains(api.Error(), "tracingConfiguration") {
		t.Fatal(api)
	}
	pub := false
	unpublished, err := s.CreateStateMachine(context.Background(), &gen.CreateStateMachineInput{
		Name: sp("p"), Definition: sp(passDef), RoleArn: sp(roleARN), Publish: &pub,
	})
	if err != nil || unpublished.StateMachineVersionArn != nil {
		t.Fatal(err, unpublished)
	}
	_, err = s.CreateStateMachine(context.Background(), &gen.CreateStateMachineInput{
		Name: sp("v"), Definition: sp(passDef), RoleArn: sp(roleARN), VersionDescription: sp("note"),
	})
	if api := mustAPI(t, err); api.ErrorName() != "ValidationException" || !strings.Contains(api.Error(), "versionDescription") {
		t.Fatal(api)
	}
	_, err = s.CreateStateMachine(context.Background(), &gen.CreateStateMachineInput{
		Name: sp("role"), Definition: sp(passDef), RoleArn: sp("arn:aws:iam::123456789012:role"),
	})
	if api := mustAPI(t, err); api.ErrorName() != "InvalidArn" || !strings.Contains(api.Error(), "Invalid Role Arn") {
		t.Fatal(api)
	}

	srv := prodServer(t, ProfileSmithy)
	code, raw := callRaw(t, srv, "AWSStepFunctions.CreateStateMachine", `{"name":"omit","definition":`+jsonString(passDef)+`}`)
	if code != 400 || !strings.Contains(string(raw), "Member must not be null") || strings.Contains(string(raw), "swf.service") {
		t.Fatalf("%d %s", code, raw)
	}
	var log bytes.Buffer
	compat := prodServer(t, ProfileSFNLocal)
	compat.Handlers.(*Service).Log = &log
	code, raw = callRaw(t, compat, "AWSStepFunctions.CreateStateMachine", `{"name":"omit","definition":`+jsonString(passDef)+`}`)
	if code != 200 || !strings.Contains(log.String(), "roleArn is absent; continuing") {
		t.Fatalf("%d %s log %s", code, raw, log.String())
	}
	code, raw = callRaw(t, compat, "AWSStepFunctions.DeleteStateMachine", `{"stateMachineArn":"nope"}`)
	if code != 200 || strings.TrimSpace(string(raw)) != "{}" {
		t.Fatalf("compat delete %d %s", code, raw)
	}
	missing := "arn:aws:states:us-east-1:123456789012:stateMachine:missing"
	got := call(t, srv, "AWSStepFunctions.DeleteStateMachine", `{"stateMachineArn":"`+missing+`"}`, contentTypeJSON10)
	if got.Code() != "StateMachineDoesNotExist" {
		t.Fatal(got)
	}
	got = call(t, srv, "AWSStepFunctions.DeleteStateMachine", `{"stateMachineArn":"nope"}`, contentTypeJSON10)
	if got.Code() != "InvalidArn" {
		t.Fatal(got)
	}

	levelNull := `{"name":"logged","definition":` + jsonString(passDef) + `,"roleArn":"` + roleARN + `","loggingConfiguration":{"level":null,"includeExecutionData":true,"destinations":[{"cloudWatchLogsLogGroup":{"logGroupArn":"arn:aws:logs:us-east-1:123456789012:log-group:g"}}]}}`
	code, raw = callRaw(t, srv, "AWSStepFunctions.CreateStateMachine", levelNull)
	if code != 200 {
		t.Fatalf("log create %d %s", code, raw)
	}
	var created struct {
		StateMachineArn string `json:"stateMachineArn"`
	}
	if err := json.Unmarshal(raw, &created); err != nil {
		t.Fatal(err)
	}
	code, raw = callRaw(t, srv, "AWSStepFunctions.DescribeStateMachine", `{"stateMachineArn":"`+created.StateMachineArn+`"}`)
	if code != 200 || !strings.Contains(string(raw), `"level":"OFF"`) || !strings.Contains(string(raw), `"includeExecutionData":true`) || !strings.Contains(string(raw), "log-group:g") {
		t.Fatalf("describe log %d %s", code, raw)
	}
}

func TestActivitiesAndTags(t *testing.T) {
	s := newService()
	out, err := s.CreateActivity(context.Background(), &gen.CreateActivityInput{Name: sp("work")})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.CreateActivity(context.Background(), &gen.CreateActivityInput{Name: sp("work")})
	if api := mustAPI(t, err); api.ErrorName() != "ActivityAlreadyExists" || !strings.Contains(api.Error(), *out.ActivityArn) {
		t.Fatal(api)
	}
	enc := gen.EncryptionTypeAWS_OWNED_KEY
	_, err = s.CreateActivity(context.Background(), &gen.CreateActivityInput{
		Name: sp("enc"), EncryptionConfiguration: &gen.EncryptionConfiguration{Type: &enc},
	})
	if mustAPI(t, err).ErrorName() != "InvalidEncryptionConfiguration" {
		t.Fatal(err)
	}
	if _, err := s.CreateActivity(context.Background(), &gen.CreateActivityInput{Name: sp("beta")}); err != nil {
		t.Fatal(err)
	}
	listed, err := s.ListActivities(context.Background(), &gen.ListActivitiesInput{})
	if err != nil || *(*listed.Activities)[0].Name != "beta" || *(*listed.Activities)[1].Name != "work" {
		t.Fatal(listed, err)
	}
	described, err := s.DescribeActivity(context.Background(), &gen.DescribeActivityInput{ActivityArn: out.ActivityArn})
	if err != nil || *described.Name != "work" {
		t.Fatal(described, err)
	}
	_, err = s.DescribeActivity(context.Background(), &gen.DescribeActivityInput{ActivityArn: sp("not-an-arn")})
	if mustAPI(t, err).ErrorName() != "InvalidArn" {
		t.Fatal(err)
	}
	missing := ActivityARN("us-east-1", "123456789012", "missing")
	_, err = s.DeleteActivity(context.Background(), &gen.DeleteActivityInput{ActivityArn: sp(missing)})
	if mustAPI(t, err).ErrorName() != "ActivityDoesNotExist" {
		t.Fatal(err)
	}
	if _, err := s.DeleteActivity(context.Background(), &gen.DeleteActivityInput{ActivityArn: out.ActivityArn}); err != nil {
		t.Fatal(err)
	}
	_, err = s.DescribeActivity(context.Background(), &gen.DescribeActivityInput{ActivityArn: out.ActivityArn})
	if mustAPI(t, err).ErrorName() != "ActivityDoesNotExist" {
		t.Fatal(err)
	}

	machine := createMachine(t, s, "tagged", passDef, roleARN)
	tags := gen.TagList{{Key: sp("env"), Value: sp("dev")}, {Key: sp("env"), Value: sp("prod")}}
	if _, err := s.TagResource(context.Background(), &gen.TagResourceInput{ResourceArn: machine.StateMachineArn, Tags: &tags}); err != nil {
		t.Fatal(err)
	}
	got, err := s.ListTagsForResource(context.Background(), &gen.ListTagsForResourceInput{ResourceArn: machine.StateMachineArn})
	if err != nil || len(*got.Tags) != 1 || *(*got.Tags)[0].Value != "prod" {
		t.Fatal(got, err)
	}
	badKey := gen.TagList{{Key: sp("bad#key"), Value: sp("x")}}
	_, err = s.TagResource(context.Background(), &gen.TagResourceInput{ResourceArn: machine.StateMachineArn, Tags: &badKey})
	if mustAPI(t, err).ErrorName() != "ValidationException" {
		t.Fatal(err)
	}
	drop := gen.TagKeyList{"env"}
	if _, err := s.UntagResource(context.Background(), &gen.UntagResourceInput{ResourceArn: machine.StateMachineArn, TagKeys: &drop}); err != nil {
		t.Fatal(err)
	}
	many := make(gen.TagList, 50)
	for i := range many {
		key := "k" + string(rune('a'+i/26)) + string(rune('a'+i%26))
		many[i] = gen.Tag{Key: sp(key), Value: sp("")}
	}
	if _, err := s.TagResource(context.Background(), &gen.TagResourceInput{ResourceArn: machine.StateMachineArn, Tags: &many}); err != nil {
		t.Fatal(err)
	}
	extra := gen.TagList{{Key: sp("overflow"), Value: sp("1")}}
	_, err = s.TagResource(context.Background(), &gen.TagResourceInput{ResourceArn: machine.StateMachineArn, Tags: &extra})
	if api := mustAPI(t, err); api.ErrorName() != "TooManyTags" || !strings.Contains(api.Error(), "Too many tags provided.") {
		t.Fatal(api)
	}
	update := gen.TagList{{Key: many[0].Key, Value: sp("kept")}}
	if _, err := s.TagResource(context.Background(), &gen.TagResourceInput{ResourceArn: machine.StateMachineArn, Tags: &update}); err != nil {
		t.Fatal(err)
	}
	keys := gen.TagKeyList{*many[0].Key}
	if _, err := s.UntagResource(context.Background(), &gen.UntagResourceInput{ResourceArn: machine.StateMachineArn, TagKeys: &keys}); err != nil {
		t.Fatal(err)
	}
	unknown := StateMachineARN("us-east-1", "123456789012", "missing")
	_, err = s.ListTagsForResource(context.Background(), &gen.ListTagsForResourceInput{ResourceArn: sp(unknown)})
	if api := mustAPI(t, err); api.ErrorName() != "ResourceNotFound" || !strings.Contains(api.Error(), unknown) {
		t.Fatal(api)
	}
	execARN := ExecutionARN("us-east-1", "123456789012", "tagged", "run", false)
	_, err = s.TagResource(context.Background(), &gen.TagResourceInput{ResourceArn: sp(execARN), Tags: &tags})
	if mustAPI(t, err).ErrorName() != "InvalidArn" {
		t.Fatal(err)
	}
}
