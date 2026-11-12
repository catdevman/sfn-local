package api

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/catdevman/sfn-local/internal/api/gen"
	"github.com/catdevman/sfn-local/internal/store"
)

func TestExpressResourceBansAndParameters(t *testing.T) {
	s := newService()
	express := gen.StateMachineTypeEXPRESS
	activity := `{"StartAt":"T","States":{"T":{"Type":"Task","Resource":"arn:aws:states:us-east-1:123456789012:activity:work","End":true}}}`
	_, err := s.CreateStateMachine(context.Background(), &gen.CreateStateMachineInput{
		Name: sp("act-ex"), Definition: sp(activity), RoleArn: sp(roleARN), Type: &express,
	})
	if api := mustAPI(t, err); api.ErrorName() != "InvalidDefinition" || !strings.Contains(api.Error(), "Express state machine does not support Activity ARN") {
		t.Fatal(api)
	}
	callback := `{"StartAt":"T","States":{"T":{"Type":"Task","Resource":"arn:aws:states:::sqs:sendMessage.waitForTaskToken","Parameters":{},"End":true}}}`
	_, err = s.CreateStateMachine(context.Background(), &gen.CreateStateMachineInput{
		Name: sp("cb-ex"), Definition: sp(callback), RoleArn: sp(roleARN), Type: &express,
	})
	if api := mustAPI(t, err); api.ErrorName() != "InvalidDefinition" || !strings.Contains(api.Error(), "'.waitForTaskToken' service integration ") {
		t.Fatal(api)
	}
	sync := `{"StartAt":"T","States":{"T":{"Type":"Task","Resource":"arn:aws:states:::lambda:invoke.sync","End":true}}}`
	_, err = s.CreateStateMachine(context.Background(), &gen.CreateStateMachineInput{
		Name: sp("sync-ex"), Definition: sp(sync), RoleArn: sp(roleARN), Type: &express,
	})
	if api := mustAPI(t, err); api.ErrorName() != "InvalidDefinition" || !strings.Contains(api.Error(), "'.sync' service integration") || strings.Contains(api.Error(), "Parameters field is required") {
		t.Fatal(api)
	}

	stdActivity := createMachine(t, s, "act-std", activity, roleARN)
	arun := startExec(t, s, *stdActivity.StateMachineArn, "a", `{}`)
	aex := waitExec(t, s, *arun.ExecutionArn)
	if aex.Status != store.ExecFailed || countEvents(aex, "ActivityScheduled") != 0 {
		t.Fatalf("%s %s %v", aex.Error, aex.Cause, typesOf(aex))
	}
	stdSync := createMachine(t, s, "sync-std", `{"StartAt":"T","States":{"T":{"Type":"Task","Resource":"arn:aws:states:::sqs:sendMessage.sync","Parameters":{},"End":true}}}`, roleARN)
	srun := startExec(t, s, *stdSync.StateMachineArn, "s", `{}`)
	sex := waitExec(t, s, *srun.ExecutionArn)
	if sex.Status != store.ExecFailed || !strings.Contains(sex.Cause, "arn:aws:states:::sqs:sendMessage.sync") || !strings.Contains(sex.Cause, "unsupported resource") {
		t.Fatalf("%s %s", sex.Error, sex.Cause)
	}
	sns := createMachine(t, s, "sns", `{"StartAt":"T","States":{"T":{"Type":"Task","Resource":"arn:aws:states:::sns:publish","End":true}}}`, roleARN)
	nrun := startExec(t, s, *sns.StateMachineArn, "n", `{}`)
	nex := waitExec(t, s, *nrun.ExecutionArn)
	if nex.Status != store.ExecFailed || !strings.Contains(nex.Cause, "arn:aws:states:::sns:publish") || !strings.Contains(nex.Cause, "unsupported resource") {
		t.Fatalf("%s %s", nex.Error, nex.Cause)
	}

	created := createMachine(t, s, "reval", passDef, roleARN)
	_, err = s.UpdateStateMachine(context.Background(), &gen.UpdateStateMachineInput{
		StateMachineArn: created.StateMachineArn,
		Definition:      sp(activity),
	})
	if err != nil {
		t.Fatal(err)
	}
	exCreated, err := s.CreateStateMachine(context.Background(), &gen.CreateStateMachineInput{
		Name: sp("reval-ex"), Definition: sp(passDef), RoleArn: sp(roleARN), Type: &express,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.UpdateStateMachine(context.Background(), &gen.UpdateStateMachineInput{
		StateMachineArn: exCreated.StateMachineArn,
		Definition:      sp(activity),
	})
	if api := mustAPI(t, err); api.ErrorName() != "InvalidDefinition" || !strings.Contains(api.Error(), "Express state machine does not support Activity ARN") {
		t.Fatal(api)
	}
}

func TestLoggingDestinationRules(t *testing.T) {
	srv := prodServer(t, ProfileSmithy)
	good := "arn:aws:logs:us-east-1:123456789012:log-group:g"
	one := `{"level":"ALL","destinations":[{"cloudWatchLogsLogGroup":{"logGroupArn":"` + good + `"}}]}`
	two := `{"level":"OFF","destinations":[{"cloudWatchLogsLogGroup":{"logGroupArn":"` + good + `"}},{"cloudWatchLogsLogGroup":{"logGroupArn":"` + good + `"}}]}`
	code, raw := callRaw(t, srv, "AWSStepFunctions.CreateStateMachine", logBody("std-all", "", `{"level":"ALL"}`))
	if code != 200 {
		t.Fatalf("standard create does not enforce destinations: %d %s", code, raw)
	}
	code, raw = callRaw(t, srv, "AWSStepFunctions.CreateStateMachine", logBody("std-two", "", two))
	if code != 200 {
		t.Fatalf("standard create allows two destinations: %d %s", code, raw)
	}
	code, raw = callRaw(t, srv, "AWSStepFunctions.CreateStateMachine", logBody("ex-off", "EXPRESS", `{"level":"OFF"}`))
	if code != 200 {
		t.Fatalf("express OFF: %d %s", code, raw)
	}
	var created struct {
		StateMachineArn string `json:"stateMachineArn"`
	}
	if err := json.Unmarshal(raw, &created); err != nil {
		t.Fatal(err)
	}
	code, raw = callRaw(t, srv, "AWSStepFunctions.DescribeStateMachine", `{"stateMachineArn":"`+created.StateMachineArn+`"}`)
	if code != 200 || !strings.Contains(string(raw), `"level":"OFF"`) {
		t.Fatalf("describe %d %s", code, raw)
	}
	code, raw = callRaw(t, srv, "AWSStepFunctions.CreateStateMachine", logBody("ex-null", "EXPRESS", `{"level":null}`))
	if code != 200 {
		t.Fatalf("express null level: %d %s", code, raw)
	}
	code, raw = callRaw(t, srv, "AWSStepFunctions.CreateStateMachine", logBody("ex-all", "EXPRESS", `{"level":"ALL"}`))
	assertLogErr(t, code, raw, "Must specify exactly one Log Destination")
	code, raw = callRaw(t, srv, "AWSStepFunctions.CreateStateMachine", logBody("ex-two", "EXPRESS", two))
	assertLogErr(t, code, raw, "Must specify exactly one Log Destination")
	if strings.Contains(string(raw), "CloudWatch") {
		t.Fatalf("count wins over ARN: %s", raw)
	}
	code, raw = callRaw(t, srv, "AWSStepFunctions.CreateStateMachine", logBody("ex-bad", "EXPRESS", `{"level":"OFF","destinations":[{"cloudWatchLogsLogGroup":{"logGroupArn":"nope"}}]}`))
	assertLogErr(t, code, raw, "Must set valid CloudWatch Log Group ARN")
	code, raw = callRaw(t, srv, "AWSStepFunctions.CreateStateMachine", logBody("ex-empty", "EXPRESS", `{"level":"ALL","destinations":[{}]}`))
	assertLogErr(t, code, raw, "Must set valid CloudWatch Log Group ARN")
	code, raw = callRaw(t, srv, "AWSStepFunctions.CreateStateMachine", logBody("ex-good", "EXPRESS", one))
	if code != 200 {
		t.Fatalf("express good: %d %s", code, raw)
	}

	code, raw = callRaw(t, srv, "AWSStepFunctions.CreateStateMachine", logBody("upd", "", ""))
	if code != 200 || json.Unmarshal(raw, &created) != nil {
		t.Fatalf("create %d %s", code, raw)
	}
	code, raw = callRaw(t, srv, "AWSStepFunctions.UpdateStateMachine", `{"stateMachineArn":"`+created.StateMachineArn+`","definition":`+jsonString(passDef)+`}`)
	if code != 200 {
		t.Fatalf("definition update: %d %s", code, raw)
	}
	code, raw = callRaw(t, srv, "AWSStepFunctions.UpdateStateMachine", `{"stateMachineArn":"`+created.StateMachineArn+`","loggingConfiguration":{"level":"ERROR"}}`)
	assertLogErr(t, code, raw, "Must specify exactly one Log Destination")
	code, raw = callRaw(t, srv, "AWSStepFunctions.UpdateStateMachine", `{"stateMachineArn":"`+created.StateMachineArn+`","loggingConfiguration":{"level":"OFF"}}`)
	if code != 200 {
		t.Fatalf("update OFF: %d %s", code, raw)
	}
	code, raw = callRaw(t, srv, "AWSStepFunctions.UpdateStateMachine", `{"stateMachineArn":"`+created.StateMachineArn+`","loggingConfiguration":`+two+`}`)
	assertLogErr(t, code, raw, "Must specify exactly one Log Destination")
	code, raw = callRaw(t, srv, "AWSStepFunctions.UpdateStateMachine", `{"stateMachineArn":"`+created.StateMachineArn+`","loggingConfiguration":`+one+`}`)
	if code != 200 {
		t.Fatalf("update one: %d %s", code, raw)
	}

	for _, profile := range []Profile{ProfileSmithy, ProfileSFNLocal} {
		ps := prodServer(t, profile)
		code, raw = callRaw(t, ps, "AWSStepFunctions.CreateStateMachine", logBody("loud", "", `{"level":"LOUD"}`))
		typ, msg := wireBody(t, raw)
		if code != 400 || typ != "com.amazonaws.sfn#InvalidLoggingConfiguration" || msg != "Invalid Logging Configuration: 'LOUD'" {
			t.Fatalf("%s %d %s %s", profile, code, typ, msg)
		}
		if strings.Contains(typ, "swf.service") || strings.Contains(string(raw), "coral") || strings.Contains(msg, "ValidationException") {
			t.Fatalf("%s", raw)
		}
	}
}

func TestCompatTagLengthSentence(t *testing.T) {
	longKey := strings.Repeat("k", 129)
	longVal := strings.Repeat("v", 257)
	longRole := "arn:aws:iam::123456789012:role/" + strings.Repeat("r", 240)
	for _, profile := range []Profile{ProfileSmithy, ProfileSFNLocal} {
		srv := prodServer(t, profile)
		code, raw := callRaw(t, srv, "AWSStepFunctions.CreateStateMachine", logBody("tags", "", ""))
		if code != 200 {
			t.Fatalf("create %d %s", code, raw)
		}
		var created struct {
			StateMachineArn string `json:"stateMachineArn"`
		}
		if err := json.Unmarshal(raw, &created); err != nil {
			t.Fatal(err)
		}
		code, raw = callRaw(t, srv, "AWSStepFunctions.TagResource", `{"resourceArn":"`+created.StateMachineArn+`","tags":[{"key":"`+longKey+`","value":"x"}]}`)
		typ, msg := wireBody(t, raw)
		if code != 400 || typ != "com.amazonaws.sfn#ValidationException" {
			t.Fatalf("%s %d %s %s", profile, code, typ, msg)
		}
		if strings.Contains(typ, "swf.service") || strings.Contains(string(raw), "coral.validate") || strings.Contains(string(raw), "services.stepfunctions.model") {
			t.Fatalf("%s", raw)
		}
		if profile == ProfileSFNLocal {
			if !strings.Contains(msg, "at 'tags[0].key' failed to satisfy constraint: Member must have length less than or equal to 80") || strings.Contains(msg, "128") {
				t.Fatalf("%s", msg)
			}
		} else if !strings.Contains(msg, "less than or equal to 128") {
			t.Fatalf("%s", msg)
		}
		code, raw = callRaw(t, srv, "AWSStepFunctions.TagResource", `{"resourceArn":"`+created.StateMachineArn+`","tags":[{"key":"env","value":"`+longVal+`"}]}`)
		_, msg = wireBody(t, raw)
		if profile == ProfileSFNLocal {
			if !strings.Contains(msg, "Member must have length less than or equal to 80") || strings.Contains(msg, "256") {
				t.Fatalf("%s", msg)
			}
		} else if !strings.Contains(msg, "less than or equal to 256") {
			t.Fatalf("%s", msg)
		}
		code, raw = callRaw(t, srv, "AWSStepFunctions.TagResource", `{"resourceArn":"`+created.StateMachineArn+`","tags":[{"key":"","value":"x"}]}`)
		_, msg = wireBody(t, raw)
		if profile == ProfileSFNLocal {
			if !strings.Contains(msg, "at 'tags[0].key'") || !strings.Contains(msg, "Member must have length less than or equal to 80") {
				t.Fatalf("%s", msg)
			}
		} else if !strings.Contains(msg, "greater than or equal to 1") || !strings.Contains(msg, "128") {
			t.Fatalf("%s", msg)
		}
	}
	compat := prodServer(t, ProfileSFNLocal)
	code, raw := callRaw(t, compat, "AWSStepFunctions.CreateStateMachine", `{"name":"rolelen","definition":`+jsonString(passDef)+`,"roleArn":"`+longRole+`"}`)
	_, msg := wireBody(t, raw)
	if code != 400 || !strings.Contains(msg, "roleArn") || !strings.Contains(msg, "256") || strings.Contains(msg, "equal to 80") {
		t.Fatalf("%d %s", code, msg)
	}
	code, raw = callRaw(t, compat, "AWSStepFunctions.CreateStateMachine", `{"name":"ctag","definition":`+jsonString(passDef)+`,"roleArn":"`+roleARN+`","tags":[{"key":"`+longKey+`","value":"x"}]}`)
	_, msg = wireBody(t, raw)
	if code != 400 || !strings.Contains(msg, "Member must have length less than or equal to 80") || strings.Contains(msg, "128") {
		t.Fatalf("%d %s", code, msg)
	}
}

func logBody(name, typ, logging string) string {
	body := `{"name":"` + name + `","definition":` + jsonString(passDef) + `,"roleArn":"` + roleARN + `"`
	if typ != "" {
		body += `,"type":"` + typ + `"`
	}
	if logging != "" {
		body += `,"loggingConfiguration":` + logging
	}
	return body + `}`
}

func assertLogErr(t *testing.T, code int, raw []byte, sentence string) {
	t.Helper()
	typ, msg := wireBody(t, raw)
	if code != 400 || typ != "com.amazonaws.sfn#InvalidLoggingConfiguration" || msg != "Invalid Logging Configuration: '"+sentence+"'" {
		t.Fatalf("%d %s %s", code, typ, msg)
	}
}

func wireBody(t *testing.T, raw []byte) (string, string) {
	t.Helper()
	var body struct {
		Type    string `json:"__type"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatal(err)
	}
	return body.Type, body.Message
}
