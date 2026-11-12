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
	for _, def := range []string{
		`{"StartAt":"P","QueryLanguage":"JSONata","States":{"P":{"Type":"Pass","End":true}}}`,
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
