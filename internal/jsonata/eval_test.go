package jsonata

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"regexp"
	"testing"
)

// randomSeed1 is $random(1) from this engine: SHA-256 of the decimal seed,
// first 8 bytes, (u>>11)/(1<<53). It is not States.MathRandom.
const randomSeed1 = 0.42002406436877837

func TestFunctionsAndUndefined(t *testing.T) {
	e := New()
	ctx := context.Background()
	cases := []struct {
		expr string
		want string
	}{
		{`$partition([1,2,3,4,5], 2)`, `[[1,2],[3,4],[5]]`},
		{`$partition([1,2,3], 2.9)`, `[[1,2],[3]]`},
		{`$range(0,10,2)`, `[0,2,4,6,8,10]`},
		{`$range(5,1,-2)`, `[5,3,1]`},
		{`$range(5,1,1)`, `[]`},
		{`$hash("ab", "MD5")`, `"187ef4436122d1cc2f40dc2b92f0eba0"`},
		{`$parse('{"a":1}')`, `{"a":1}`},
	}
	for _, tc := range cases {
		got, err := e.Eval(ctx, tc.expr, nil, nil, Limits{})
		if err != nil {
			t.Fatalf("%s: %v", tc.expr, err)
		}
		raw, _ := json.Marshal(got)
		if string(raw) != tc.want {
			t.Fatalf("%s: %s", tc.expr, raw)
		}
	}
	if _, err := e.Eval(ctx, `$range(0, 1, 0)`, nil, nil, Limits{}); err == nil {
		t.Fatal("step 0")
	}
	if _, err := e.Eval(ctx, `$partition([1], 0.5)`, nil, nil, Limits{}); err == nil {
		t.Fatal("size < 1")
	}
	if _, err := e.Eval(ctx, `$parse('{"a":1} trailing')`, nil, nil, Limits{}); err == nil {
		t.Fatal("trailing")
	}
	if _, err := e.Eval(ctx, `$parse('nope')`, nil, nil, Limits{}); err == nil {
		t.Fatal("invalid json")
	}
	id, err := e.Eval(ctx, `$uuid()`, nil, nil, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if !regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`).MatchString(id.(string)) {
		t.Fatal(id)
	}
	if _, err := e.Eval(ctx, `$uuid(1)`, nil, nil, Limits{}); err == nil {
		t.Fatal("uuid args")
	}
	n, err := e.Eval(ctx, `$random()`, nil, nil, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	f := n.(float64)
	if f < 0 || f >= 1 || math.IsNaN(f) {
		t.Fatal(f)
	}
	a, err := e.Eval(ctx, `$random(1)`, nil, nil, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	b, err := e.Eval(ctx, `$random(1)`, nil, nil, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if a.(float64) != b.(float64) || a.(float64) != randomSeed1 || a.(float64) != SeededRandom(1) {
		t.Fatalf("seeded %v want %v", a, randomSeed1)
	}
}

func TestAssignSnapshotAndAbsentEval(t *testing.T) {
	e := New()
	ctx := context.Background()
	bindings := map[string]any{"x": 3, "a": 6, "states": map[string]any{"input": map[string]any{"seen": true}}}
	got, err := e.Eval(ctx, `{"x": $a, "nextX": $x}`, nil, bindings, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(got)
	if string(raw) != `{"nextX":3,"x":6}` && string(raw) != `{"x":6,"nextX":3}` {
		t.Fatal(string(raw))
	}
	obj := got.(map[string]any)
	if obj["x"] != float64(6) || obj["nextX"] != float64(3) {
		t.Fatalf("%#v", obj)
	}
	seen, err := e.Eval(ctx, `$states.input`, nil, bindings, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	raw, _ = json.Marshal(seen)
	if string(raw) != `{"seen":true}` {
		t.Fatal(string(raw))
	}
	_, err = e.Eval(ctx, `$nextX`, nil, bindings, Limits{})
	var qe *EvalError
	if !errors.As(err, &qe) || qe.Cause == "" {
		t.Fatalf("negative $nextX: %v", err)
	}
	_, err = e.Eval(ctx, `$eval("1")`, nil, bindings, Limits{})
	if err == nil || !errors.As(err, &qe) {
		t.Fatalf("absent $eval: %v", err)
	}
	null, err := e.Eval(ctx, `null`, nil, nil, Limits{})
	if err != nil || null != nil {
		t.Fatalf("json null %#v %v", null, err)
	}
	field, err := e.Eval(ctx, `a`, map[string]any{"a": nil}, nil, Limits{})
	if err != nil || field != nil {
		t.Fatalf("null field %#v %v", field, err)
	}
	_, err = e.Eval(ctx, `b`, map[string]any{"a": nil}, nil, Limits{})
	if !errors.As(err, &qe) {
		t.Fatalf("missing field %v", err)
	}
	stopped, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = e.Eval(stopped, `1+1`, nil, nil, Limits{})
	if !errors.As(err, &qe) || qe.Cause != "JSONata evaluation exceeded 1s" {
		t.Fatalf("timeout cause %v", err)
	}
	// A variable must not hide an input field of the same name.
	score, err := e.Eval(ctx, `score`, map[string]any{"score": 95.0}, map[string]any{"score": 1}, Limits{})
	if err != nil || score != float64(95) {
		t.Fatalf("field %v %v", score, err)
	}
	variable, err := e.Eval(ctx, `$score`, map[string]any{"score": 95.0}, map[string]any{"score": 1}, Limits{})
	if err != nil || variable != float64(1) {
		t.Fatalf("var %v %v", variable, err)
	}
}
