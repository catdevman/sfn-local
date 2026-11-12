package main

import (
	"os"
	"strings"
	"testing"

	"github.com/catdevman/sfn-local/internal/manifest"
)

func TestParseRealSpec(t *testing.T) {
	root, err := manifest.ModuleRoot()
	if err != nil {
		t.Fatal(err)
	}
	doc, err := parseFile(root + "/aws-spec/asl/spec.html")
	if err != nil {
		t.Fatal(err)
	}
	if len(doc.Languages) != 2 || doc.Languages[0].Name != "jsonata" || doc.Languages[1].Name != "jsonpath" {
		t.Fatalf("languages %+v", names(doc))
	}
	if len(doc.Languages[0].Fields) == 0 || len(doc.Languages[1].Fields) == 0 {
		t.Fatal("genasl did not find both field tables")
	}
	if len(doc.AppendixA) != 14 || len(doc.AppendixB) != 18 {
		t.Fatalf("appendix A %d B %d", len(doc.AppendixA), len(doc.AppendixB))
	}
	for _, lang := range doc.Languages {
		for _, note := range lang.Notes {
			if strings.Contains(note.Text, "Next") {
				continue
			}
			t.Fatalf("note %q", note.Text)
		}
		for _, field := range lang.Fields {
			if strings.Contains(field.Name, " ") {
				t.Fatalf("prose stored as field %q", field.Name)
			}
		}
	}
}

func TestParseRejectsBadTables(t *testing.T) {
	good := minimalSpec()
	if _, err := parseSpec(good); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		html string
		want string
	}{
		{"no table", "<p>none</p>", "no field table"},
		{"unknown class", strings.Replace(good, `class="required">Required`, `class="weird">Required`, 1), "unknown cell class"},
		{"wrong column", strings.Replace(good, "<th>Fail</th>", "<th>Foo</th>", 1), "state-type columns"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseSpec(tc.html)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v", err)
			}
		})
	}
}

func names(doc *document) []string {
	var out []string
	for _, lang := range doc.Languages {
		out = append(out, lang.Name)
	}
	return out
}

func parseFile(path string) (*document, error) {
	return runParse(path)
}

func runParse(path string) (*document, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return parseSpec(string(data))
}

func minimalSpec() string {
	header := "<tr><td class=\"blank\"></td><th>Task</th><th>Parallel</th><th>Map</th><th>Pass</th><th>Wait</th><th>Choice</th><th>Succeed</th><th>Fail</th></tr>"
	row := "<tr><td class=\"field\">Type</td>" + strings.Repeat("<td class=\"required\">Required</td>", 8) + "</tr>"
	table := "<table>" + header + row + "</table>"
	return `<h3 id="state-type-table-jsonata">J</h3>` + table +
		`<h3 id="state-type-table-jsonpath">P</h3>` + table +
		`<h3 id="appendix-a">A</h3><table><tr><th>Code</th><th>Description</th></tr><tr><td>States.ALL</td><td>any</td></tr></table>` +
		`<h3 id="appendix-b">B</h3><h4>States.Format</h4><h3 id="later">L</h3>`
}
