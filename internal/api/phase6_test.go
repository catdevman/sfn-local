package api

import (
	"context"
	"strings"
	"testing"

	"github.com/catdevman/sfn-local/internal/api/gen"
	"github.com/catdevman/sfn-local/internal/store"
)

const workedJSONata = `{
  "QueryLanguage": "JSONata",
  "Comment": "Assign snapshot. Output must not read names this state assigns.",
  "StartAt": "Store",
  "States": {
    "Store": {
      "Type": "Pass",
      "Assign": {"x": 3, "a": 6},
      "Output": "{% $states.input %}",
      "Next": "Swap"
    },
    "Swap": {
      "Type": "Pass",
      "Assign": {"x": "{% $a %}", "nextX": "{% $x %}"},
      "Output": {"xBefore": "{% $x %}", "a": "{% $a %}", "seen": "{% $states.input.seen %}"},
      "Next": "After"
    },
    "After": {
      "Type": "Pass",
      "Output": {"x": "{% $x %}", "nextX": "{% $nextX %}", "a": "{% $a %}"},
      "End": true
    }
  }
}`

func TestPhase6JSONataAssign(t *testing.T) {
	s := newService()
	ex := runMachine(t, s, "worked", workedJSONata, "run", `{"seen":true}`)
	if ex.Status != store.ExecSucceeded {
		t.Fatalf("%s %s %s", ex.Status, ex.Error, ex.Cause)
	}
	jsonEqual(t, ex.Output, `{"x":6,"nextX":3,"a":6}`)
	assertTypes(t, ex, "ExecutionStarted", "PassStateEntered", "PassStateExited", "PassStateEntered", "PassStateExited", "PassStateEntered", "PassStateExited", "ExecutionSucceeded")

	negative := strings.Replace(workedJSONata, `"Output": {"xBefore": "{% $x %}", "a": "{% $a %}", "seen": "{% $states.input.seen %}"}`, `"Output": "{% $nextX %}"`, 1)
	bad := runMachine(t, s, "negative", negative, "run", `{"seen":true}`)
	if bad.Status != store.ExecFailed || bad.Error != "States.QueryEvaluationError" || !strings.Contains(bad.Cause, "undefined") {
		t.Fatalf("%s %s %s", bad.Status, bad.Error, bad.Cause)
	}
	assertTypes(t, bad, "ExecutionStarted", "PassStateEntered", "PassStateExited", "PassStateEntered", "ExecutionFailed")
	if countEvents(bad, "PassStateExited") != 1 {
		t.Fatal(typesOf(bad))
	}
}

func TestPhase6SucceedOutputAndContext(t *testing.T) {
	s := newService()
	def := `{
	  "QueryLanguage": "JSONata",
	  "StartAt": "S",
	  "States": {
	    "S": {
	      "Type": "Succeed",
	      "Output": {"lastName": "{% 'Last=>' & $states.input.customer.lastName %}", "orderValue": "{% $states.input.order.total %}"}
	    }
	  }
	}`
	ex := runMachine(t, s, "succeed", def, "run", `{"customer":{"firstName":"Martha","lastName":"Rivera"},"order":{"items":7,"total":27.91}}`)
	if ex.Status != store.ExecSucceeded {
		t.Fatalf("%s %s %s", ex.Status, ex.Error, ex.Cause)
	}
	jsonEqual(t, ex.Output, `{"lastName":"Last=>Rivera","orderValue":27.91}`)

	count := `{"QueryLanguage":"JSONata","StartAt":"P","States":{"P":{"Type":"Pass","Output":"{% $states.context.Execution.RedriveCount %}","End":true}}}`
	ex = runMachine(t, s, "count", count, "run", `{}`)
	if ex.Status != store.ExecSucceeded || ex.Output != "0" {
		t.Fatalf("redrive %s %s %s", ex.Status, ex.Output, ex.Cause)
	}
}

func TestPhase6OmittedOutput(t *testing.T) {
	s := newService()
	pass := `{"QueryLanguage":"JSONata","StartAt":"P","States":{"P":{"Type":"Pass","End":true}}}`
	ex := runMachine(t, s, "pass", pass, "run", `{"a":1}`)
	jsonEqual(t, ex.Output, `{"a":1}`)

	choice := `{"QueryLanguage":"JSONata","StartAt":"C","States":{"C":{"Type":"Choice","Choices":[{"Condition":"{% true %}","Next":"S"}],"Default":"S"},"S":{"Type":"Succeed"}}}`
	ex = runMachine(t, s, "choice", choice, "run", `{"a":1}`)
	jsonEqual(t, ex.Output, `{"a":1}`)

	wait := `{"QueryLanguage":"JSONata","StartAt":"W","States":{"W":{"Type":"Wait","Seconds":0,"Next":"S"},"S":{"Type":"Succeed"}}}`
	ex = runMachine(t, s, "wait", wait, "run", `{"a":1}`)
	jsonEqual(t, ex.Output, `{"a":1}`)

	succeed := `{"QueryLanguage":"JSONata","StartAt":"S","States":{"S":{"Type":"Succeed"}}}`
	ex = runMachine(t, s, "succeed-omit", succeed, "run", `{"a":1}`)
	jsonEqual(t, ex.Output, `{"a":1}`)

	s.Runner.Mocks = writeMock(t, `{
	  "StateMachines": {
	    "task-out": {"TestCases": {"T": {"Call": "Ok"}}},
	    "task-catch": {"TestCases": {"T": {"Call": "Bad"}}}
	  },
	  "MockedResponses": {
	    "Ok": {"0": {"Return": {"ok": true}}},
	    "Bad": {"0": {"Throw": {"Error": "CustomError", "Cause": "once"}}}
	  }
	}`)
	task := `{"QueryLanguage":"JSONata","StartAt":"Call","States":{"Call":{"Type":"Task","Resource":"` + lambdaARN + `","End":true}}}`
	sm := createMachine(t, s, "task-out", task, roleARN)
	run := startExec(t, s, *sm.StateMachineArn+"#T", "run", `{"a":1}`)
	ex = waitExec(t, s, *run.ExecutionArn)
	if ex.Status != store.ExecSucceeded {
		t.Fatalf("task %s %s %s", ex.Status, ex.Error, ex.Cause)
	}
	jsonEqual(t, ex.Output, `{"ok":true}`)

	caught := `{"QueryLanguage":"JSONata","StartAt":"Call","States":{"Call":{"Type":"Task","Resource":"` + lambdaARN + `","Catch":[{"ErrorEquals":["States.ALL"],"Next":"Done"}],"End":true},"Done":{"Type":"Pass","End":true}}}`
	csm := createMachine(t, s, "task-catch", caught, roleARN)
	crun := startExec(t, s, *csm.StateMachineArn+"#T", "run", `{}`)
	ex = waitExec(t, s, *crun.ExecutionArn)
	if ex.Status != store.ExecSucceeded {
		t.Fatalf("catch %s %s %s", ex.Status, ex.Error, ex.Cause)
	}
	jsonEqual(t, ex.Output, `{"Error":"CustomError","Cause":"once"}`)
}

func TestPhase6JSONPathVariables(t *testing.T) {
	s := newService()
	def := `{
	  "StartAt": "P",
	  "States": {
	    "P": {"Type": "Pass", "Parameters": {"id.$": "$.order.id"}, "Assign": {"saved.$": "$.id"}, "Next": "C"},
	    "C": {"Type": "Choice", "Choices": [{"Variable": "$saved", "StringEquals": "A", "Next": "Fmt"}], "Default": "No"},
	    "Fmt": {"Type": "Pass", "Parameters": {"label.$": "States.Format('id={}', $saved)"}, "End": true},
	    "No": {"Type": "Fail", "Error": "No", "Cause": "no"}
	  }
	}`
	ex := runMachine(t, s, "vars", def, "run", `{"order":{"id":"A"}}`)
	if ex.Status != store.ExecSucceeded {
		t.Fatalf("%s %s %s", ex.Status, ex.Error, ex.Cause)
	}
	jsonEqual(t, ex.Output, `{"label":"id=A"}`)

	_, err := s.CreateStateMachine(context.Background(), &gen.CreateStateMachineInput{
		Name: sp("bad-path"), Definition: sp(`{"StartAt":"P","States":{"P":{"Type":"Pass","ResultPath":"$saved","End":true}}}`), RoleArn: sp(roleARN),
	})
	if api := mustAPI(t, err); api.ErrorName() != "InvalidDefinition" || !strings.Contains(api.Error(), "ResultPath cannot reference a variable") {
		t.Fatal(err)
	}
}

func TestPhase6ParallelAndChoiceScopes(t *testing.T) {
	s := newService()
	hidden := `{
	  "QueryLanguage": "JSONata",
	  "StartAt": "P",
	  "States": {
	    "P": {
	      "Type": "Parallel",
	      "Branches": [
	        {"StartAt": "A", "States": {"A": {"Type": "Pass", "Assign": {"branchOnly": 1}, "Output": 1, "End": true}}},
	        {"StartAt": "B", "States": {"B": {"Type": "Pass", "Assign": {"branchOnly": 2}, "Output": 2, "End": true}}}
	      ],
	      "Next": "After"
	    },
	    "After": {"Type": "Pass", "Output": "{% $branchOnly %}", "End": true}
	  }
	}`
	ex := runMachine(t, s, "hidden", hidden, "run", `{}`)
	if ex.Status != store.ExecFailed || ex.Error != "States.QueryEvaluationError" {
		t.Fatalf("hidden %s %s %s", ex.Status, ex.Error, ex.Cause)
	}

	shown := `{
	  "QueryLanguage": "JSONata",
	  "StartAt": "P",
	  "States": {
	    "P": {
	      "Type": "Parallel",
	      "Branches": [
	        {"StartAt": "A", "States": {"A": {"Type": "Pass", "Assign": {"branchOnly": 1}, "Output": 1, "End": true}}},
	        {"StartAt": "B", "States": {"B": {"Type": "Pass", "Assign": {"branchOnly": 2}, "Output": 2, "End": true}}}
	      ],
	      "Output": "{% $states.result %}",
	      "End": true
	    }
	  }
	}`
	ex = runMachine(t, s, "shown", shown, "run", `{}`)
	if ex.Status != store.ExecSucceeded {
		t.Fatalf("shown %s %s %s", ex.Status, ex.Error, ex.Cause)
	}
	jsonEqual(t, ex.Output, `[1,2]`)

	choice := `{
	  "QueryLanguage": "JSONata",
	  "StartAt": "C",
	  "States": {
	    "C": {
	      "Type": "Choice",
	      "Assign": {"outputValue": "default path taken"},
	      "Choices": [{"Condition": "{% $states.input.score > 90 %}", "Assign": {"outputValue": "high score"}, "Next": "P"}],
	      "Default": "P"
	    },
	    "P": {"Type": "Pass", "Output": "{% $outputValue %}", "End": true}
	  }
	}`
	ex = runMachine(t, s, "high", choice, "run", `{"score":95}`)
	if ex.Status != store.ExecSucceeded || ex.Output != `"high score"` {
		t.Fatalf("high %s %s %s", ex.Status, ex.Output, ex.Cause)
	}
	ex = runMachine(t, s, "low", choice, "run", `{"score":50}`)
	if ex.Status != store.ExecSucceeded || ex.Output != `"default path taken"` {
		t.Fatalf("low %s %s %s", ex.Status, ex.Output, ex.Cause)
	}
}

func TestPhase6MapItemsAndCatch(t *testing.T) {
	s := newService()
	items := `{
	  "QueryLanguage": "JSONata",
	  "StartAt": "M",
	  "States": {
	    "M": {
	      "Type": "Map",
	      "Items": [1, "{% 2 %}", 3],
	      "Iterator": {"StartAt": "P", "States": {"P": {"Type": "Pass", "Output": "{% $states.input %}", "End": true}}},
	      "End": true
	    }
	  }
	}`
	ex := runMachine(t, s, "items", items, "run", `{}`)
	if ex.Status != store.ExecSucceeded {
		t.Fatalf("items %s %s %s", ex.Status, ex.Error, ex.Cause)
	}
	jsonEqual(t, ex.Output, `[1,2,3]`)

	obj := `{"QueryLanguage":"JSONata","StartAt":"M","States":{"M":{"Type":"Map","Items":"{% {'a': 1} %}","Iterator":{"StartAt":"P","States":{"P":{"Type":"Pass","End":true}}},"End":true}}}`
	ex = runMachine(t, s, "obj", obj, "run", `{}`)
	if ex.Status != store.ExecFailed || ex.Error != "States.QueryEvaluationError" || !strings.Contains(ex.Cause, "Items must be an array") {
		t.Fatalf("object %s %s %s", ex.Status, ex.Error, ex.Cause)
	}

	caught := `{
	  "QueryLanguage": "JSONata",
	  "StartAt": "M",
	  "States": {
	    "M": {
	      "Type": "Map",
	      "Items": [1],
	      "Iterator": {"StartAt": "F", "States": {"F": {"Type": "Fail", "Error": "Boom", "Cause": "no"}}},
	      "Catch": [{"ErrorEquals": ["States.ALL"], "Assign": {"caught": "{% $states.errorOutput.Error %}"}, "Next": "P"}],
	      "End": true
	    },
	    "P": {"Type": "Pass", "Output": "{% $caught %}", "End": true}
	  }
	}`
	ex = runMachine(t, s, "caught", caught, "run", `{}`)
	if ex.Status != store.ExecSucceeded || ex.Output != `"Boom"` {
		t.Fatalf("caught %s %s %s", ex.Status, ex.Output, ex.Cause)
	}

	reuse := `{
	  "QueryLanguage": "JSONata",
	  "StartAt": "M",
	  "States": {
	    "M": {
	      "Type": "Map",
	      "Items": [1],
	      "Iterator": {"StartAt": "P", "States": {"P": {"Type": "Pass", "Assign": {"caught": 2}, "End": true}}},
	      "Catch": [{"ErrorEquals": ["States.ALL"], "Assign": {"caught": "{% $states.errorOutput.Error %}"}, "Next": "P"}],
	      "End": true
	    },
	    "P": {"Type": "Pass", "End": true}
	  }
	}`
	_, err := s.CreateStateMachine(context.Background(), &gen.CreateStateMachineInput{
		Name: sp("reuse"), Definition: sp(reuse), RoleArn: sp(roleARN),
	})
	if api := mustAPI(t, err); api.ErrorName() != "InvalidDefinition" || !strings.Contains(api.Error(), "caught") {
		t.Fatal(err)
	}
}

func TestPhase6ArgumentsAndLimits(t *testing.T) {
	s := newService()
	uncaught := `{"QueryLanguage":"JSONata","StartAt":"T","States":{"T":{"Type":"Task","Resource":"` + lambdaARN + `","Arguments":"{% $nope %}","End":true}}}`
	ex := runMachine(t, s, "args", uncaught, "run", `{}`)
	if ex.Status != store.ExecFailed || ex.Error != "States.QueryEvaluationError" || !strings.Contains(ex.Cause, "undefined") {
		t.Fatalf("args %s %s %s", ex.Status, ex.Error, ex.Cause)
	}
	assertTypes(t, ex, "ExecutionStarted", "TaskStateEntered", "ExecutionFailed")
	if countEvents(ex, "TaskStateExited") != 0 || countEvents(ex, "LambdaFunctionScheduled") != 0 {
		t.Fatal(typesOf(ex))
	}

	caught := `{"QueryLanguage":"JSONata","StartAt":"T","States":{"T":{"Type":"Task","Resource":"` + lambdaARN + `","Arguments":"{% $lookup($states, \"result\") %}","Catch":[{"ErrorEquals":["States.ALL"],"Output":"{% $states.errorOutput.Error %}","Next":"P"}],"End":true},"P":{"Type":"Pass","End":true}}}`
	ex = runMachine(t, s, "dyn", caught, "run", `{}`)
	if ex.Status != store.ExecSucceeded || ex.Output != `"States.QueryEvaluationError"` {
		t.Fatalf("dynamic %s %s %s", ex.Status, ex.Output, ex.Cause)
	}
	// In JSONata 2.0.6 a non-numeric bracket is a predicate, not a key lookup.
	// $states["result"] keeps the object. $lookup of an absent key is undefined.
	pred := `{"QueryLanguage":"JSONata","StartAt":"P","States":{"P":{"Type":"Pass","Output":"{% $states[\"result\"].input %}","End":true}}}`
	ex = runMachine(t, s, "pred", pred, "run", `{"n":1}`)
	if ex.Status != store.ExecSucceeded {
		t.Fatalf("predicate %s %s %s", ex.Status, ex.Error, ex.Cause)
	}
	jsonEqual(t, ex.Output, `{"n":1}`)

	big := strings.Repeat("a", 262145)
	limit := `{"QueryLanguage":"JSONata","StartAt":"P","States":{"P":{"Type":"Pass","Assign":{"big":"` + big + `"},"End":true}}}`
	ex = runMachine(t, s, "limit", limit, "run", `{}`)
	if ex.Status != store.ExecFailed || ex.Error != "States.QueryEvaluationError" || ex.Cause != "variable value exceeded the 262144 byte limit" {
		t.Fatalf("limit %s %s %s", ex.Status, ex.Error, ex.Cause)
	}
}
