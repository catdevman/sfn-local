package asl

import (
	"strings"
	"testing"
)

func TestValidatePassAndRejectedFields(t *testing.T) {
	prog, err := Validate(`{"StartAt":"P","States":{"P":{"Type":"Pass","Result":{"ok":true},"End":true}}}`)
	if err != nil {
		t.Fatal(err)
	}
	if prog.StartAt != "P" || prog.TimeoutSeconds != 99999999 || !prog.States["P"].HasResult || !prog.States["P"].End {
		t.Fatalf("%+v", prog)
	}
	okLang, err := Validate(`{"StartAt":"P","QueryLanguage":"JSONata","States":{"P":{"Type":"Pass","End":true}}}`)
	if err != nil || !okLang.States["P"].JSONata || okLang.QueryLanguage != "JSONata" {
		t.Fatal(err)
	}
	for _, def := range []string{
		`{"StartAt":"P","States":{"P":{"Type":"Pass","Output":"ok","End":true}}}`,
		`{"StartAt":"P","States":{"P":{"Type":"Pass","Arguments":{},"End":true}}}`,
		`{"StartAt":"P","States":{"P":{"Type":"Pass","Nope":1,"End":true}}}`,
		`{"StartAt":"P","States":{"P":{"Type":"Pass","Next":"Q","End":true}}}`,
		`{"StartAt":"P","States":{"P":{"Type":"Pass","End":false}}}`,
		`{"StartAt":"S","States":{"S":{"Type":"Succeed","Next":"P"}}}`,
		`{"StartAt":"P","States":{}}`,
		`{"StartAt":"P","Version":"1.1","States":{"P":{"Type":"Pass","End":true}}}`,
		`{"StartAt":"P","TimeoutSeconds":100000000,"States":{"P":{"Type":"Pass","End":true}}}`,
		`{"StartAt":"P","States":{"P":{"Type":"Pass","End":true}}}{}`,
		`{"StartAt":"P","StartAt":"Q","States":{"P":{"Type":"Pass","End":true}}}`,
	} {
		_, err := Validate(def)
		if err == nil || !strings.Contains(err.Error(), "SCHEMA_VALIDATION_FAILED") {
			t.Fatalf("%s -> %v", def, err)
		}
	}
	_, err = Validate(`{"StartAt":"P","States":{"P":{"Type":"Pass","Nope":1,"End":true}}}`)
	if err == nil || !strings.Contains(err.Error(), "Nope") {
		t.Fatal(err)
	}
}

func TestValidateOtherStateTypes(t *testing.T) {
	ok := []string{
		`{"StartAt":"S","States":{"S":{"Type":"Succeed"}}}`,
		`{"StartAt":"F","States":{"F":{"Type":"Fail","Error":"Boom","Cause":"no"}}}`,
		`{"StartAt":"C","States":{"C":{"Type":"Choice","Choices":[{"Variable":"$.n","NumericEquals":1,"Next":"S"}],"Default":"S"},"S":{"Type":"Succeed"}}}`,
		`{"StartAt":"W","States":{"W":{"Type":"Wait","Seconds":1,"End":true}}}`,
		`{"StartAt":"T","States":{"T":{"Type":"Task","Resource":"arn:aws:lambda:us-east-1:123456789012:function:f","End":true}}}`,
		`{"StartAt":"M","States":{"M":{"Type":"Map","End":true,"Iterator":{"StartAt":"P","States":{"P":{"Type":"Pass","End":true}}}}}}`,
		`{"StartAt":"M","States":{"M":{"Type":"Map","End":true,"ItemProcessor":{"ProcessorConfig":{"Mode":"INLINE"},"StartAt":"P","States":{"P":{"Type":"Pass","End":true}}}}}}`,
		`{"StartAt":"P","States":{"P":{"Type":"Parallel","End":true,"Branches":[{"StartAt":"S","States":{"S":{"Type":"Succeed"}}}]}}}`,
	}
	for _, def := range ok {
		if _, err := Validate(def); err != nil {
			t.Fatalf("%s: %v", def, err)
		}
	}
	_, err := Validate(`{"StartAt":"M","States":{"M":{"Type":"Map","End":true,"ItemProcessor":{"ProcessorConfig":{"Mode":"DISTRIBUTED"},"StartAt":"P","States":{"P":{"Type":"Pass","End":true}}}}}}`)
	if err == nil || !strings.Contains(err.Error(), "DISTRIBUTED") {
		t.Fatal(err)
	}
	_, err = Validate(`{"StartAt":"P","States":{"P":{"Type":"Pass","Parameters":{"a":1,"a.$":"$.x"},"End":true}}}`)
	if err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatal(err)
	}
	_, err = Validate(`{"StartAt":"M","States":{"M":{"Type":"Map","End":true}}}`)
	if err == nil || !strings.Contains(err.Error(), "Iterator") {
		t.Fatal(err)
	}
	prog, err := Validate(`{"StartAt":"T","States":{"T":{"Type":"Task","Resource":"arn:aws:lambda:us-east-1:123456789012:function:f","Retry":[{"ErrorEquals":["E"]}],"Catch":[{"ErrorEquals":["E"],"Next":"T"}],"End":true}}}`)
	if err != nil {
		t.Fatal(err)
	}
	rt := prog.States["T"].Retry[0]
	if rt.IntervalSeconds != 1 || rt.MaxAttempts != 3 || rt.BackoffRate != 2 || rt.MaxDelaySeconds != 0 || rt.JitterStrategy != "" {
		t.Fatalf("%+v", rt)
	}
	if prog.States["T"].Catch[0].ResultPath.Kind != PathDefault || prog.States["T"].Catch[0].Next != "T" {
		t.Fatalf("%+v", prog.States["T"].Catch[0])
	}
	for _, def := range []string{
		`{"StartAt":"T","States":{"T":{"Type":"Task","Resource":"r","Retry":[{"ErrorEquals":["E"],"MaxDelaySeconds":0}],"End":true}}}`,
		`{"StartAt":"T","States":{"T":{"Type":"Task","Resource":"r","Retry":[{"ErrorEquals":["E"],"BackoffRate":0.5}],"End":true}}}`,
	} {
		if _, err := Validate(def); err == nil || !strings.Contains(err.Error(), "SCHEMA_VALIDATION_FAILED") {
			t.Fatalf("%s -> %v", def, err)
		}
	}
}

func TestPhase0bValidatorMessages(t *testing.T) {
	task := func(resource, extra string) string {
		return `{"StartAt":"T","States":{"T":{"Type":"Task","Resource":"` + resource + `"` + extra + `,"End":true}}}`
	}
	activity := "arn:aws:states:us-east-1:123456789012:activity:work"
	if _, err := Validate(task(activity, "")); err != nil {
		t.Fatal(err)
	}
	if _, err := ValidateType(task(activity, ""), "STANDARD"); err != nil {
		t.Fatal(err)
	}
	assertInvalid(t, task(activity, ""), "EXPRESS", "SCHEMA_VALIDATION_FAILED", "Express state machine does not support Activity ARN")
	assertInvalid(t, task("arn:aws:states:::lambda:invoke.sync", ""), "EXPRESS", "SCHEMA_VALIDATION_FAILED", "'.sync' service integration at")
	assertInvalid(t, task("arn:aws:states:::lambda:invoke.sync:2", ""), "EXPRESS", "SCHEMA_VALIDATION_FAILED", "'.sync' service integration at")
	_, err := ValidateType(task("arn:aws:states:::sqs:sendMessage.waitForTaskToken", ""), "EXPRESS")
	if err == nil || !strings.Contains(err.Error(), "'.waitForTaskToken' service integration  at") || strings.Contains(err.Error(), "Parameters field is required") {
		t.Fatal(err)
	}

	lambdaInvoke := "arn:aws:states:::lambda:invoke"
	assertInvalid(t, task(lambdaInvoke, ""), "", "SCHEMA_VALIDATION_FAILED", "Parameters field is required for resource ARN: "+lambdaInvoke)
	assertInvalid(t, task(lambdaInvoke, `,"Parameters":null`), "", "SCHEMA_VALIDATION_FAILED", "Parameters field is required for resource ARN: "+lambdaInvoke)
	if _, err := Validate(task(lambdaInvoke, `,"Parameters":{}`)); err != nil {
		t.Fatal(err)
	}
	fullSync := "arn:aws:states:::sqs:sendMessage.sync"
	assertInvalid(t, task(fullSync, ""), "STANDARD", "SCHEMA_VALIDATION_FAILED", "Parameters field is required for resource ARN: "+fullSync)
	for _, resource := range []string{
		"arn:aws:lambda:us-east-1:123456789012:function:f",
		activity,
		"arn:aws:states:::sns:publish",
		"arn:aws:states:::not-a-service:op",
		"arn:aws:states:::aws-sdk:sqs:sendMessage",
	} {
		if _, err := Validate(task(resource, "")); err != nil {
			t.Fatalf("%s: %v", resource, err)
		}
	}

	nested := `{"StartAt":"P","States":{"P":{"Type":"Parallel","End":true,"Branches":[{"StartAt":"T","States":{"T":{"Type":"Task","Resource":"` + activity + `","End":true}}}]}}}`
	if _, err := Validate(nested); err != nil {
		t.Fatal(err)
	}
	assertInvalid(t, nested, "EXPRESS", "SCHEMA_VALIDATION_FAILED", "Express state machine does not support Activity ARN")
	sameName := `{"StartAt":"P","States":{"P":{"Type":"Parallel","End":true,"Branches":[{"StartAt":"S","States":{"S":{"Type":"Succeed"}}},{"StartAt":"S","States":{"S":{"Type":"Succeed"}}}]}}}`
	if _, err := Validate(sameName); err != nil {
		t.Fatal(err)
	}
	assertInvalid(t, `{"StartAt":"P","States":{"P":{"Type":"Parallel","End":true,"Branches":[{"StartAt":"A","States":{"A":{"Type":"Pass","Next":"P"}}}]}}}`, "", "MISSING_TRANSITION_TARGET", "Missing 'Next' target: P")

	assertInvalid(t, `{"StartAt":"Gone","States":{"P":{"Type":"Pass","End":true}}}`, "", "MISSING_TRANSITION_TARGET", "Missing 'Next' target: Gone")
	assertInvalid(t, `{"StartAt":null,"States":{"P":{"Type":"Pass","End":true}}}`, "", "MISSING_TRANSITION_TARGET", "Missing transition pointer")
	assertInvalid(t, `{"StartAt":"P","States":{"P":{"Type":"Pass","Next":null}}}`, "", "MISSING_TRANSITION_TARGET", "Missing transition pointer")
	assertInvalid(t, `{"StartAt":"C","States":{"C":{"Type":"Choice","Choices":[{"Variable":"$.n","NumericEquals":1,"Next":null}],"Default":"S"},"S":{"Type":"Succeed"}}}`, "", "MISSING_TRANSITION_TARGET", "Missing transition pointer")
	assertInvalid(t, `{"StartAt":"C","States":{"C":{"Type":"Choice","Choices":[{"Variable":"$.n","NumericEquals":1,"Next":"S"}],"Default":null},"S":{"Type":"Succeed"}}}`, "", "MISSING_TRANSITION_TARGET", "Missing transition pointer")
	assertInvalid(t, `{"StartAt":"T","States":{"T":{"Type":"Task","Resource":"r","Catch":[{"ErrorEquals":["E"],"Next":null}],"End":true}}}`, "", "MISSING_TRANSITION_TARGET", "Missing transition pointer")
	assertInvalid(t, `{"StartAt":"C","States":{"C":{"Type":"Choice","Choices":[{"Variable":"$.n","NumericEquals":1,"Next":"Gone"}],"Default":"S"},"S":{"Type":"Succeed"}}}`, "", "MISSING_TRANSITION_TARGET", "Missing 'Next' target: Gone")
	assertInvalid(t, `{"StartAt":"C","States":{"C":{"Type":"Choice","Choices":[{"Variable":"$.n","NumericEquals":1,"Next":"S"}],"Default":"Gone"},"S":{"Type":"Succeed"}}}`, "", "MISSING_TRANSITION_TARGET", "Missing 'Next' target: Gone")
	assertInvalid(t, `{"StartAt":"T","States":{"T":{"Type":"Task","Resource":"r","Catch":[{"ErrorEquals":["E"],"Next":"Later"}],"End":true}}}`, "", "MISSING_TRANSITION_TARGET", "Missing 'Next' target: Later")
	assertInvalid(t, `{"StartAt":"A","States":{"A":{"Type":"Pass","Next":"Gone"},"B":{"Type":"Succeed"}}}`, "", "MISSING_TRANSITION_TARGET", "Missing 'Next' target: Gone")
	assertInvalid(t, `{"StartAt":"P","States":{"P":{"Type":"Pass","Next":"P"}}}`, "", "MISSING_END_STATE", "Workflow has no terminal state")
	assertInvalid(t, `{"StartAt":"A","States":{"A":{"Type":"Pass","Next":"A"},"B":{"Type":"Pass","Next":"B"}}}`, "", "MISSING_END_STATE", "Workflow has no terminal state")
	_, err = Validate(`{"StartAt":"A","States":{"A":{"Type":"Pass","Next":"A"},"B":{"Type":"Succeed"}}}`)
	if err == nil || !strings.Contains(err.Error(), `State "B" is not reachable.`) || strings.Contains(err.Error(), "MISSING_END_STATE") {
		t.Fatal(err)
	}
	_, err = Validate(`{"StartAt":"A","States":{"A":{"Type":"Succeed"},"C":{"Type":"Pass","End":true},"B":{"Type":"Fail"}}}`)
	if err == nil || !strings.Contains(err.Error(), `State "B" is not reachable.`) || !strings.Contains(err.Error(), "at /States/B") {
		t.Fatal(err)
	}

	retryFixture := `{"StartAt":"T","States":{"T":{"Type":"Task","Resource":"arn:aws:lambda:us-east-1:123456789012:function:f","Retry":[{"ErrorEquals":["States.ALL"],"MaxAttempts":1},{"ErrorEquals":["States.Timeout"],"MaxAttempts":1}],"End":true}}}`
	_, err = Validate(retryFixture)
	if err == nil || !strings.Contains(err.Error(), "SCHEMA_VALIDATION_FAILED") || !strings.Contains(err.Error(), "States.ALL must appear alone and at end of list") || !strings.Contains(err.Error(), "Retry[0]") {
		t.Fatal(err)
	}
	assertInvalid(t, `{"StartAt":"T","States":{"T":{"Type":"Task","Resource":"r","Retry":[{"ErrorEquals":["States.ALL","States.Timeout"],"MaxAttempts":1}],"End":true}}}`, "", "SCHEMA_VALIDATION_FAILED", "States.ALL must appear alone and at end of list")
	assertInvalid(t, `{"StartAt":"T","States":{"T":{"Type":"Task","Resource":"r","Catch":[{"ErrorEquals":["States.ALL"],"Next":"T"},{"ErrorEquals":["States.Timeout"],"Next":"T"}],"End":true}}}`, "", "SCHEMA_VALIDATION_FAILED", "States.ALL must appear alone and at end of list")
	validAll := `{"StartAt":"T","States":{"T":{"Type":"Task","Resource":"r","Retry":[{"ErrorEquals":["States.Timeout"],"MaxAttempts":1},{"ErrorEquals":["States.ALL"],"MaxAttempts":1}],"Catch":[{"ErrorEquals":["States.ALL"],"Next":"T"}],"End":true}}}`
	if _, err := Validate(validAll); err != nil {
		t.Fatal(err)
	}
}

func TestJSONataValidation(t *testing.T) {
	ok := []string{
		`{"QueryLanguage":"JSONata","StartAt":"P","States":{"P":{"Type":"Pass","Assign":{"x":3,"a":6},"Output":"{% $states.input %}","End":true}}}`,
		`{"QueryLanguage":"JSONata","StartAt":"S","States":{"S":{"Type":"Succeed","Output":{"lastName":"{% $states.input.customer.lastName %}"}}}}`,
		`{"QueryLanguage":"JSONata","StartAt":"C","States":{"C":{"Type":"Choice","Choices":[{"Condition":"{% $states.input.score > 90 %}","Assign":{"outputValue":"high score"},"Next":"P"}],"Assign":{"outputValue":"default path taken"},"Default":"P"},"P":{"Type":"Pass","End":true}}}`,
		`{"QueryLanguage":"JSONata","StartAt":"M","States":{"M":{"Type":"Map","Items":[1,"{% 2 %}",3],"Iterator":{"StartAt":"P","States":{"P":{"Type":"Pass","End":true}}},"End":true}}}`,
		`{"StartAt":"P","States":{"P":{"Type":"Pass","Assign":{"saved.$":"$.id"},"End":true}}}`,
		`{"QueryLanguage":"JSONPath","StartAt":"P","States":{"P":{"Type":"Pass","QueryLanguage":"JSONata","End":true}}}`,
	}
	for _, def := range ok {
		if _, err := Validate(def); err != nil {
			t.Fatalf("%s: %v", def, err)
		}
	}
	bad := []struct{ def, part string }{
		{`{"QueryLanguage":"JSONata","StartAt":"P","States":{"P":{"Type":"Pass","Result":1,"End":true}}}`, "Result"},
		{`{"QueryLanguage":"JSONata","StartAt":"P","States":{"P":{"Type":"Pass","Output":"{% $states.result %}","End":true}}}`, "$states.result"},
		{`{"QueryLanguage":"JSONata","StartAt":"P","States":{"P":{"Type":"Pass","Output":"{% $eval(1) %}","End":true}}}`, "$eval"},
		{`{"QueryLanguage":"JSONata","StartAt":"P","States":{"P":{"Type":"Pass","Output":"{% a ?: 1 %}","End":true}}}`, "?:"},
		{`{"QueryLanguage":"JSONata","StartAt":"P","States":{"P":{"Type":"Pass","Assign":{"_x":1},"End":true}}}`, "invalid"},
		{`{"QueryLanguage":"JSONata","StartAt":"P","States":{"P":{"Type":"Pass","Assign":{"states":1},"End":true}}}`, "reserved"},
		{`{"QueryLanguage":"JSONata","StartAt":"P","States":{"P":{"Type":"Pass","Assign":{"x.y":1},"End":true}}}`, "invalid"},
		{`{"QueryLanguage":"JSONata","StartAt":"M","States":{"M":{"Type":"Map","Items":{"a":1},"Iterator":{"StartAt":"P","States":{"P":{"Type":"Pass","End":true}}},"End":true}}}`, "Items"},
		{`{"QueryLanguage":"JSONata","StartAt":"M","States":{"M":{"Type":"Map","Parameters":{},"Iterator":{"StartAt":"P","States":{"P":{"Type":"Pass","End":true}}},"End":true}}}`, "Parameters"},
		{`{"QueryLanguage":"JSONata","StartAt":"T","States":{"T":{"Type":"Task","Resource":"arn:aws:states:::lambda:invoke","End":true}}}`, "Arguments"},
		{`{"StartAt":"P","States":{"P":{"Type":"Pass","ResultPath":"$saved","End":true}}}`, "variable"},
		{`{"QueryLanguage":"JSONata","StartAt":"M","States":{"M":{"Type":"Map","Assign":{"caught":1},"Iterator":{"StartAt":"P","States":{"P":{"Type":"Pass","Assign":{"caught":2},"End":true}}},"End":true}}}`, "outer scope"},
		{`{"StartAt":"I","States":{"I":{"Type":"Map","Iterator":{"QueryLanguage":"JSONata","StartAt":"P","States":{"P":{"Type":"Pass","End":true}}},"End":true}}}`, "QueryLanguage"},
		{`{"QueryLanguage":"Nope","StartAt":"P","States":{"P":{"Type":"Pass","End":true}}}`, "QueryLanguage"},
	}
	for _, tc := range bad {
		_, err := Validate(tc.def)
		if err == nil || !strings.Contains(err.Error(), "SCHEMA_VALIDATION_FAILED") || !strings.Contains(err.Error(), tc.part) {
			t.Fatalf("%s -> %v", tc.def, err)
		}
	}
	// A quoted ?: inside an expression is still the 2.0.6 language.
	if _, err := Validate(`{"QueryLanguage":"JSONata","StartAt":"P","States":{"P":{"Type":"Pass","Output":"{% '?:' %}","End":true}}}`); err != nil {
		t.Fatal(err)
	}
}

func assertInvalid(t *testing.T, def, machineType string, parts ...string) {
	t.Helper()
	var err error
	if machineType == "" {
		_, err = Validate(def)
	} else {
		_, err = ValidateType(def, machineType)
	}
	if err == nil {
		t.Fatalf("expected error for %s", def)
	}
	for _, part := range parts {
		if !strings.Contains(err.Error(), part) {
			t.Fatalf("missing %q in %v", part, err)
		}
	}
}
