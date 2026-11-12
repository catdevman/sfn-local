package jsonpath

import "testing"

func TestGetAndSet(t *testing.T) {
	root := map[string]any{"user": map[string]any{"name": "ada"}, "items": []any{int64(1), int64(2)}}
	ctx := map[string]any{"Execution": map[string]any{"Name": "run1"}}

	got, found, err := Get("$.user.name", root, ctx)
	if err != nil || !found || got != "ada" {
		t.Fatalf("%v %v %v", got, found, err)
	}
	got, found, err = Get("$$.Execution.Name", root, ctx)
	if err != nil || !found || got != "run1" {
		t.Fatalf("context %v %v %v", got, found, err)
	}
	_, found, err = Get("$.missing", root, ctx)
	if err != nil || found {
		t.Fatalf("missing %v %v", found, err)
	}
	_, found, err = Get("$.items[0]", root, ctx)
	if err != nil || !found {
		t.Fatal(err, found)
	}
	if _, _, err := Get("$saved", root, ctx); err == nil || !contains(err.Error(), "not enabled") {
		t.Fatal(err)
	}
	if _, _, err := Get("name", root, ctx); err == nil || !contains(err.Error(), "$.") {
		t.Fatal(err)
	}
	if _, _, err := Get("$.user.name.extra", root, nil); err == nil {
		t.Fatal("descent into a string")
	}

	updated, err := Set(root, "$.user.title", "eng")
	if err != nil {
		t.Fatal(err)
	}
	title, _, _ := Get("$.user.title", updated, nil)
	if title != "eng" {
		t.Fatal(title)
	}
	if _, ok := root["user"].(map[string]any)["title"]; ok {
		t.Fatal("Set mutated the input")
	}
	updated, err = Set(map[string]any{"a": map[string]any{}}, "$.a.b", map[string]any{"ok": true})
	if err != nil {
		t.Fatal(err)
	}
	b, found, _ := Get("$.a.b", updated, nil)
	if !found || b.(map[string]any)["ok"] != true {
		t.Fatal(updated)
	}
	if _, err := Set(map[string]any{"a": int64(1)}, "$.a.b", true); err == nil || !conflict(err) {
		t.Fatal(err)
	}
	if _, err := Set(map[string]any{}, "$.a.b", true); err == nil || conflict(err) {
		t.Fatal(err)
	}
}

func TestApply(t *testing.T) {
	root := map[string]any{"user": map[string]any{"name": "ada"}}
	tmpl := map[string]any{"name.$": "$.user.name", "static": int64(1), "tags": []any{"x"}}
	got, err := Apply(tmpl, root, nil)
	if err != nil {
		t.Fatal(err)
	}
	obj := got.(map[string]any)
	if obj["name"] != "ada" || obj["static"] != int64(1) {
		t.Fatal(obj)
	}
	if _, err := Apply(map[string]any{"a": int64(1), "a.$": "$.user.name"}, root, nil); err == nil {
		t.Fatal("both keys")
	}
	formatted, err := Apply(map[string]any{"a.$": "States.Format('{}', $.user.name)"}, root, nil)
	if err != nil {
		t.Fatal(err)
	}
	if formatted.(map[string]any)["a"] != "ada" {
		t.Fatal(formatted)
	}
	if _, err := Apply(map[string]any{"a.$": "States.Nope()"}, root, nil); err == nil || !contains(err.Error(), "intrinsic") {
		t.Fatal(err)
	}
	if _, err := Apply(map[string]any{"a.$": "$.missing"}, root, nil); err == nil || !contains(err.Error(), "not found") {
		t.Fatal(err)
	}
}

func contains(s, part string) bool {
	return len(s) >= len(part) && (s == part || len(part) == 0 || indexOf(s, part) >= 0)
}

func indexOf(s, part string) int {
	for i := 0; i+len(part) <= len(s); i++ {
		if s[i:i+len(part)] == part {
			return i
		}
	}
	return -1
}

func conflict(err error) bool {
	ev, ok := err.(*EvalError)
	return ok && ev.Name == "States.ReferencePathConflict"
}
