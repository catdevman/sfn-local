package exec

import (
	"reflect"
	"testing"
)

func TestCSVRecords(t *testing.T) {
	for name, tc := range map[string]struct {
		text  string
		delim rune
		want  [][]string
	}{
		"plain":            {"a,b\n1,2\n", ',', [][]string{{"a", "b"}, {"1", "2"}}},
		"crlf and blank":   {"a,b\r\n\r\n1,2", ',', [][]string{{"a", "b"}, {"1", "2"}}},
		"quoted delimiter": {"\"x,y\",z\n", ',', [][]string{{"x,y", "z"}}},
		"quoted newline":   {"\"x\ny\",z", ',', [][]string{{"x\ny", "z"}}},
		"doubled quote":    {"\"say \"\"hi\"\"\",1", ',', [][]string{{"say \"hi\"", "1"}}},
		"backslash":        {"C:\\\\Program Files\\\\App.exe,6534512", ',', [][]string{{"C:\\Program Files\\App.exe", "6534512"}}},
		"escaped delim":    {"a\\,b,c", ',', [][]string{{"a,b", "c"}}},
		"dropped escape":   {"a\\qb,c", ',', [][]string{{"aqb", "c"}}},
		"pipe":             {"a|b,c", '|', [][]string{{"a", "b,c"}}},
		"empty fields":     {",,\n", ',', [][]string{{"", "", ""}}},
	} {
		if got := csvRecords(tc.text, tc.delim); !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%s: %q", name, got)
		}
	}
}

func TestCSVItems(t *testing.T) {
	items, err := csvItems([]byte("id,name,qty\n1,pen\n2,desk,3,extra\n"), ',', true, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := []any{
		map[string]any{"id": "1", "name": "pen", "qty": ""},
		map[string]any{"id": "2", "name": "desk", "qty": "3"},
	}
	if !reflect.DeepEqual(items, want) {
		t.Fatalf("%v", items)
	}
	given, err := csvItems([]byte("1,307\n"), ',', false, []string{"userId", "movieId"})
	if err != nil || !reflect.DeepEqual(given, []any{map[string]any{"userId": "1", "movieId": "307"}}) {
		t.Fatalf("given %v %v", given, err)
	}
}

func TestJSONItems(t *testing.T) {
	body := []byte(`{"data":{"a/b":[{"id":1},{"id":2}],"list":[[1,2],[3]]}}`)
	items, err := jsonItems(body, "/data/a~1b")
	if err != nil || len(items) != 2 {
		t.Fatalf("%v %v", items, err)
	}
	items, err = jsonItems(body, "/data/list/1")
	if err != nil || len(items) != 1 {
		t.Fatalf("index %v %v", items, err)
	}
	if _, err := jsonItems(body, ""); err == nil {
		t.Fatal("object root without a pointer")
	}
	if _, err := jsonItems(body, "/data/nope"); err == nil {
		t.Fatal("missing pointer")
	}
	lines, err := jsonlItems([]byte("{\"a\":1}\n\n[2]\n"))
	if err != nil || len(lines) != 2 {
		t.Fatalf("jsonl %v %v", lines, err)
	}
	if _, err := jsonlItems([]byte("{\"a\":1}\nnope\n")); err == nil || err.Error() != "line 2 is not valid JSON" {
		t.Fatalf("bad line %v", err)
	}
}
