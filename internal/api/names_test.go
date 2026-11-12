package api

import "testing"

func TestNamesAndARNs(t *testing.T) {
	for _, name := range []string{"a b", "a#b", "a\"b", "a/b", "line\n", "café "} {
		if !NameRejected(name) {
			t.Fatalf("accepted %q", name)
		}
	}
	for _, name := range []string{"rebuild-check", "café", "A1"} {
		if NameRejected(name) {
			t.Fatalf("rejected %q", name)
		}
	}
	if !RoleARNValid("arn:aws:iam::123456789012:role/local") {
		t.Fatal("role/local")
	}
	if !RoleARNValid("arn:aws:iam::123456789012:role/service-role/MyRole") {
		t.Fatal("service-role path")
	}
	if RoleARNValid("arn:aws:iam::123456789012:role") || RoleARNValid("arn:aws:iam::123:role/local") {
		t.Fatal("invalid role accepted")
	}
	sm := StateMachineARN("us-east-1", "123456789012", "rebuild-check")
	if !ValidStateMachineARN(sm) || ValidStateMachineARN("arn:aws:states:us-east-1:123:stateMachine:rebuild-check") {
		t.Fatal(sm)
	}
	act := ActivityARN("us-east-1", "123456789012", "work")
	if !ValidActivityARN(act) {
		t.Fatal(act)
	}
	base, testName, err := SplitMockSuffix(sm + "#Case")
	if err != nil || base != sm || testName != "Case" {
		t.Fatalf("%s %s %v", base, testName, err)
	}
	if _, _, err := SplitMockSuffix(sm + "#A#B"); err == nil {
		t.Fatal("two hashes")
	}
	if got, _, err := SplitMockSuffix(sm); err != nil || got != sm {
		t.Fatal(got, err)
	}
	std := ExecutionARN("us-east-1", "123456789012", "rebuild-check", "run1", false)
	exp := ExecutionARN("us-east-1", "123456789012", "rebuild-check", "run1", true)
	if ExecutionARNKind(std) != "standard" {
		t.Fatal(std)
	}
	if ExecutionARNKind(exp) != "express" {
		t.Fatal(exp)
	}
	placeholder := "arn:aws:states:us-east-1:123456789012:express:stateMachineName:run1"
	if ExecutionARNKind(placeholder) != "placeholder" {
		t.Fatal(placeholder)
	}
	if ExecutionARNKind("nope") != "invalid" {
		t.Fatal("invalid")
	}
}
