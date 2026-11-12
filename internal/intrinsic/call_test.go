package intrinsic

import (
	"encoding/json"
	"regexp"
	"testing"
)

func TestIntrinsicFixtures(t *testing.T) {
	must(t, `States.Format('id={}', 'A')`, "id=A")
	must(t, `States.Format('\\{\\}')`, "{}")
	must(t, `States.Format('a\\')`, "a")
	mustFail(t, `States.Format('{}')`)
	mustFail(t, `States.Format('{')`)
	mustJSON(t, `States.StringToJson('{"a":1}')`, `{"a":1}`)
	must(t, `States.JsonToString({"a":1})`, `{"a":1}`)
	mustJSON(t, `States.Array(1, "a", true)`, `[1,"a",true]`)
	mustJSON(t, `States.Array()`, `[]`)
	mustJSON(t, `States.ArrayPartition([1,2,3,4,5], 2)`, `[[1,2],[3,4],[5]]`)
	mustFail(t, `States.ArrayPartition([1], 0)`)
	must(t, `States.ArrayContains([1, "b"], "1")`, true)
	must(t, `States.ArrayContains([1, 2], 3)`, false)
	mustJSON(t, `States.ArrayRange(0, 10, 3)`, `[0,3,6,9]`)
	mustJSON(t, `States.ArrayRange(5, 1, 1)`, `[]`)
	mustJSON(t, `States.ArrayRange(5, 1, -1)`, `[5,4,3,2,1]`)
	mustFail(t, `States.ArrayRange(0, 1, 0)`)
	must(t, `States.ArrayGetItem([10, 20, 30], 1)`, int64(20))
	must(t, `States.ArrayGetItem([10, 20, 30], 5)`, nil)
	mustFail(t, `States.ArrayGetItem([10, 20, 30], 3)`)
	mustFail(t, `States.ArrayGetItem([10, 20, 30], -1)`)
	must(t, `States.ArrayLength([1, 2, 3])`, int64(3))
	must(t, `States.ArrayLength([])`, int64(0))
	mustJSON(t, `States.ArrayUnique(["b", "a", "b"])`, `["b","a"]`)
	must(t, `States.Base64Encode("ab")`, "YWI=")
	must(t, `States.Base64Decode("YWI=")`, "ab")
	must(t, `States.Hash("ab", "MD5")`, "187ef4436122d1cc2f40dc2b92f0eba0")
	mustJSON(t, `States.JsonMerge({"a":1,"b":2}, {"b":3,"c":4}, false)`, `{"a":1,"b":3,"c":4}`)
	mustFail(t, `States.JsonMerge({"a":1}, {"b":2}, true)`)
	must(t, `States.MathAdd(1.5, 2.5)`, int64(5))
	mustFail(t, `States.MathAdd(2147483647, 1)`)
	mustFail(t, `States.MathAdd(2147483647.5, 0)`)
	must(t, `States.MathRandom(0, 10, 1)`, int64(5))
	mustJSON(t, `States.StringSplit("a--b", "-")`, `["a","b"]`)
	mustJSON(t, `States.StringSplit("a-b", "-x")`, `["a","b"]`)
	got, err := Eval(`States.UUID()`, nil)
	if err != nil {
		t.Fatal(err)
	}
	text, _ := got.(string)
	if !regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`).MatchString(text) {
		t.Fatal(text)
	}
	if _, err := Eval(`States.Nope()`, nil); err == nil || !contains(err.Error(), "intrinsic") {
		t.Fatal(err)
	}
	if _, err := Eval(`States.Format('{}', $.missing)`, func(string) (any, error) {
		return nil, errPath("not found")
	}); err == nil || err.Error() != "not found" {
		t.Fatal(err)
	}
}

func TestMathRandomSeed(t *testing.T) {
	if got := newJavaRandom(1).nextInt(10); got != 5 {
		t.Fatal(got)
	}
}

type errPath string

func (e errPath) Error() string { return string(e) }

func must(t *testing.T, expr string, want any) {
	t.Helper()
	got, err := Eval(expr, nil)
	if err != nil {
		t.Fatalf("%s: %v", expr, err)
	}
	if !same(got, want) {
		t.Fatalf("%s: %#v want %#v", expr, got, want)
	}
}

func mustJSON(t *testing.T, expr, want string) {
	t.Helper()
	got, err := Eval(expr, nil)
	if err != nil {
		t.Fatalf("%s: %v", expr, err)
	}
	raw, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != want {
		t.Fatalf("%s: %s want %s", expr, raw, want)
	}
}

func mustFail(t *testing.T, expr string) {
	t.Helper()
	if _, err := Eval(expr, nil); err == nil {
		t.Fatalf("%s succeeded", expr)
	}
}

func same(got, want any) bool {
	if want == nil {
		return got == nil
	}
	return got == want
}

func contains(s, part string) bool {
	return len(s) >= len(part) && (s == part || indexOf(s, part) >= 0)
}

func indexOf(s, part string) int {
	for i := 0; i+len(part) <= len(s); i++ {
		if s[i:i+len(part)] == part {
			return i
		}
	}
	return -1
}
