package ci

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/catdevman/sfn-local/internal/asl/specgen"
	"github.com/catdevman/sfn-local/internal/manifest"
	"github.com/catdevman/sfn-local/internal/sdk"
)

func TestGenerateClean(t *testing.T) {
	root := moduleRoot(t)
	before := gitStatus(t, root)
	cmd := exec.Command("go", "generate", "./...")
	cmd.Dir = root
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("go generate: %v\n%s", err, out)
	}
	after := gitStatus(t, root)
	if before != after {
		t.Fatalf("go generate changed the tree\nbefore:\n%s\nafter:\n%s", before, after)
	}
}

func TestASLCatalog(t *testing.T) {
	root := moduleRoot(t)
	tables := mustParse(t, filepath.Join(root, "internal/asl/catalog.toml"))
	fields := map[string]string{}
	codes := map[string]string{}
	intrinsics := map[string]string{}
	for _, table := range tables {
		switch table.Name {
		case "field":
			key := table.Values["language"] + "\x00" + table.Values["name"]
			if _, ok := fields[key]; ok {
				t.Fatalf("duplicate field %s", key)
			}
			fields[key] = table.Values["status"]
		case "code":
			codes[table.Values["name"]] = table.Values["status"]
		case "intrinsic":
			intrinsics[table.Values["name"]] = table.Values["status"]
		default:
			t.Fatalf("unknown catalog table %s", table.Name)
		}
	}
	if len(specgen.Languages) != 2 {
		t.Fatalf("genasl languages %d", len(specgen.Languages))
	}
	seenLang := map[string]bool{}
	for _, lang := range specgen.Languages {
		seenLang[lang.Name] = true
		if len(lang.Fields) == 0 {
			t.Fatalf("genasl found no fields for %s", lang.Name)
		}
		for _, field := range lang.Fields {
			status, ok := fields[lang.Name+"\x00"+field.Name]
			if !ok || !classified(status) {
				t.Fatalf("field %s %s status %q", lang.Name, field.Name, status)
			}
			delete(fields, lang.Name+"\x00"+field.Name)
		}
	}
	if !seenLang["jsonata"] || !seenLang["jsonpath"] {
		t.Fatal("genasl did not find both field tables")
	}
	if len(fields) != 0 {
		t.Fatalf("extra fields %v", fields)
	}
	for _, code := range specgen.AppendixA {
		status, ok := codes[code]
		if !ok || !classified(status) {
			t.Fatalf("appendix A %s status %q", code, status)
		}
		delete(codes, code)
	}
	if len(codes) != 0 || len(specgen.AppendixA) == 0 {
		t.Fatalf("appendix A extra %v count %d", codes, len(specgen.AppendixA))
	}
	for _, name := range specgen.AppendixB {
		status, ok := intrinsics[name]
		if !ok || !classified(status) {
			t.Fatalf("appendix B %s status %q", name, status)
		}
		delete(intrinsics, name)
	}
	if len(intrinsics) != 0 || len(specgen.AppendixB) == 0 {
		t.Fatalf("appendix B extra %v count %d", intrinsics, len(specgen.AppendixB))
	}
}

func TestPageAcks(t *testing.T) {
	root := moduleRoot(t)
	raw, err := os.ReadFile(filepath.Join(root, "aws-spec/page-watch.json"))
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Pages []struct {
			ID     string `json:"id"`
			SHA256 string `json:"sha256"`
		} `json:"pages"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	tables := mustParse(t, filepath.Join(root, "internal/asl/page-acks.toml"))
	acks := map[string]string{}
	for _, table := range tables {
		if table.Name != "page" {
			t.Fatalf("table %s", table.Name)
		}
		acks[table.Values["id"]] = table.Values["sha256"]
	}
	if len(acks) != len(doc.Pages) {
		t.Fatalf("acks %d pages %d", len(acks), len(doc.Pages))
	}
	for _, page := range doc.Pages {
		if acks[page.ID] != page.SHA256 {
			t.Fatalf("page %s hash %s want %s", page.ID, acks[page.ID], page.SHA256)
		}
	}
}

func TestAllowList(t *testing.T) {
	root := moduleRoot(t)
	raw, err := os.ReadFile(filepath.Join(root, "aws-spec/sdk-integrations.json"))
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Added      []sdkRow `json:"added"`
		Deprecated []sdkRow `json:"deprecated"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{}
	for _, row := range append(doc.Added, doc.Deprecated...) {
		for _, name := range row.Services {
			want[name] = true
		}
	}
	if len(want) == 0 {
		t.Fatal("no services")
	}
	tables := mustParse(t, filepath.Join(root, "internal/invoke/allow.toml"))
	got := map[string]string{}
	for _, table := range tables {
		if table.Name != "service" {
			t.Fatalf("table %s", table.Name)
		}
		status := table.Values["status"]
		if status != "unsupported" && status != "endpoint" {
			t.Fatalf("%s status %s", table.Values["name"], status)
		}
		got[table.Values["name"]] = status
	}
	if len(got) != len(want) {
		t.Fatalf("allow %d services %d", len(got), len(want))
	}
	for name := range want {
		if _, ok := got[name]; !ok {
			t.Fatalf("missing service %s", name)
		}
	}
}

// TestOperations checks internal/invoke/operations.toml against internal/sdk.
// TestGenerateClean checks that the file is current. This test checks what a
// row may say.
func TestOperations(t *testing.T) {
	root := moduleRoot(t)
	tables := mustParse(t, filepath.Join(root, "internal/invoke/operations.toml"))
	rows := map[string]map[string]string{}
	for _, table := range tables {
		if table.Name != "operation" {
			t.Fatalf("table %s", table.Name)
		}
		id, action, status := table.Values["service"], table.Values["action"], table.Values["status"]
		switch status {
		case "implemented":
			if table.Values["opaque"] != "" {
				t.Fatalf("%s:%s runs through op with opaque members %s", id, action, table.Values["opaque"])
			}
		case "codec", "unsupported":
		default:
			t.Fatalf("%s:%s status %s", id, action, status)
		}
		if rows[id] == nil {
			rows[id] = map[string]string{}
		}
		rows[id][action] = status
	}
	for _, id := range sdk.Services() {
		if len(rows[id]) == 0 {
			t.Fatalf("no operations for %s", id)
		}
		for _, action := range sdk.Actions(id) {
			if status := rows[id][action]; status != "implemented" && status != "codec" {
				t.Fatalf("%s:%s runs but its row says %q", id, action, status)
			}
		}
	}
	if len(rows) != len(sdk.Services()) {
		t.Fatalf("operations.toml has %d services, internal/sdk has %d", len(rows), len(sdk.Services()))
	}
}

type sdkRow struct {
	Services []string `json:"services"`
	Summary  string   `json:"summary"`
}

func TestSyncWorkflow(t *testing.T) {
	root := moduleRoot(t)
	raw, err := os.ReadFile(filepath.Join(root, ".github/workflows/sync-aws-spec.yml"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	for _, phrase := range []string{
		"go generate ./...",
		"sync_commit_guard.py",
		"summarize_generated_diff.py",
		"actions/setup-go",
		"go-version-file: go.mod",
	} {
		if !strings.Contains(text, phrase) {
			t.Fatalf("workflow missing %s", phrase)
		}
	}
	guard := filepath.Join(root, "aws-spec/tools/sync_commit_guard.py")
	script, err := os.ReadFile(guard)
	if err != nil {
		t.Fatal(err)
	}
	allow := []string{
		"aws-spec/states-api/sfn-2016-11-23.json",
		"internal/api/gen/types.go",
		"internal/asl/specgen/matrix.go",
	}
	refuse := []string{
		"docs/spec.md",
		"internal/api/surface.toml",
		"internal/asl/catalog.toml",
		"internal/api/server.go",
		"cmd/sfnlocal/main.go",
		"internal/asl/page-acks.toml",
		"README.md",
	}
	if err := runGuard(t, guard, allow); err != nil {
		t.Fatal(err)
	}
	for _, path := range refuse {
		if err := runGuard(t, guard, []string{path}); err == nil {
			t.Fatalf("guard allowed %s", path)
		}
	}
	if !strings.Contains(string(script), "docs/spec.md") || !strings.Contains(string(script), "surface.toml") || !strings.Contains(string(script), "catalog.toml") {
		t.Fatal("guard does not name the files it must refuse")
	}
	summary := exec.Command("python3", filepath.Join(root, "aws-spec/tools/summarize_generated_diff.py"),
		"--old-surface", filepath.Join(root, "internal/api/gen/surface.gen.json"),
		"--new-surface", filepath.Join(root, "internal/api/gen/surface.gen.json"),
		"--old-matrix", filepath.Join(root, "internal/asl/specgen/matrix.json"),
		"--new-matrix", filepath.Join(root, "internal/asl/specgen/matrix.json"),
	)
	out, err := summary.CombinedOutput()
	if err != nil {
		t.Fatalf("summarize: %v\n%s", err, out)
	}
	for _, heading := range []string{"New operations", "New members", "New ASL fields", "New error names"} {
		if !strings.Contains(string(out), heading) {
			t.Fatalf("summary missing %s\n%s", heading, out)
		}
	}
	old := t.TempDir()
	writeJSON(t, filepath.Join(old, "surface.json"), map[string]any{"operations": []any{}, "errors": []any{}})
	writeJSON(t, filepath.Join(old, "matrix.json"), map[string]any{"languages": []any{}})
	diff := exec.Command("python3", filepath.Join(root, "aws-spec/tools/summarize_generated_diff.py"),
		"--old-surface", filepath.Join(old, "surface.json"),
		"--new-surface", filepath.Join(root, "internal/api/gen/surface.gen.json"),
		"--old-matrix", filepath.Join(old, "matrix.json"),
		"--new-matrix", filepath.Join(root, "internal/asl/specgen/matrix.json"),
	)
	out, err = diff.CombinedOutput()
	if err != nil {
		t.Fatalf("summarize diff: %v\n%s", err, out)
	}
	body := string(out)
	for _, phrase := range []string{"- ListStateMachines", "- CreateStateMachine input name", "- jsonata Type", "- ValidationException"} {
		if !strings.Contains(body, phrase) {
			t.Fatalf("summary missing %s\n%s", phrase, body)
		}
	}
}

func TestLandedSpecFixtures(t *testing.T) {
	root := moduleRoot(t)
	checked := checkedPhases(t, root)
	fixtures := fixturePhases(t, root)
	var pending []string
	for _, phase := range checked {
		if fixtures[phase] > 0 && !runnerRegistered(phase) {
			pending = append(pending, phase)
		}
	}
	if len(pending) != 0 {
		t.Fatalf("checked phases have acceptance fixtures and no runner: %s", strings.Join(pending, ", "))
	}
	if fixtures["G"] != 0 {
		t.Fatal("phase G does not execute states and has no acceptance fixture")
	}
}

func TestJarOracle(t *testing.T) {
	root := moduleRoot(t)
	reason, err := jarOracle(root)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(reason, jarImage) {
		t.Fatalf("missing image: %s", reason)
	}
	quiet := strings.Contains(reason, "no landed execution fixtures")
	skip := strings.Contains(reason, "SKIP")
	compared := strings.Contains(reason, "compared")
	if !quiet && !skip && !compared {
		t.Fatalf("quiet skip: %s", reason)
	}
	t.Log(reason)
}

func TestJSONataPin(t *testing.T) {
	root := moduleRoot(t)
	reason, err := jsonataPin(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, phrase := range []string{"gnata", "assign", "negative $nextX", "absent $eval"} {
		if !strings.Contains(strings.ToLower(reason), strings.ToLower(phrase)) {
			t.Fatalf("missing %s in %s", phrase, reason)
		}
	}
	t.Log(reason)
}

func classified(status string) bool {
	return status == "implemented" || status == "deferred"
}

const jarImage = "amazon/aws-stepfunctions-local:2.0.0"

func runnerRegistered(phase string) bool {
	switch phase {
	case "1", "2", "3", "4", "6":
		return true
	default:
		return false
	}
}

func jarOracle(root string) (string, error) {
	if len(landedFixtures(root)) == 0 {
		return "jar oracle: no landed execution fixtures; not starting " + jarImage, nil
	}
	if _, err := exec.LookPath("docker"); err != nil {
		return dockerUnavailable(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", "info")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return dockerUnavailable(fmt.Errorf("%v: %s", err, bytesTrim(out)))
	}
	return compareJar(jarImage)
}

func dockerUnavailable(err error) (string, error) {
	if os.Getenv("SFN_LOCAL_RELEASE") == "1" {
		return "", err
	}
	return fmt.Sprintf("jar oracle: SKIP docker is unusable (%v). Developer CI may skip with this message. The release pipeline may not skip quietly. Not starting %s", err, jarImage), nil
}

func bytesTrim(b []byte) string {
	return strings.TrimSpace(string(b))
}

type oracleError string

func (e oracleError) Error() string { return string(e) }

func jsonataPin(root string) (string, error) {
	phase6 := false
	for _, phase := range checkedPhases(nil, root) {
		if phase == "6" {
			phase6 = true
		}
	}
	if !phase6 {
		return "", oracleError("phase 6 heading is unchecked")
	}
	raw, err := os.ReadFile(filepath.Join(root, "aws-spec/jsonata/2.0.6/upstream.json"))
	if err != nil {
		return "", err
	}
	var doc struct {
		Commit string `json:"commit"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		return "", err
	}
	if doc.Commit != "6f1fc886aa1e3e6bdbfffcdd80e63ede1d7d6b6b" {
		return "", oracleError("upstream.json commit moved; re-record the Assign example")
	}
	mod, err := os.ReadFile(filepath.Join(root, "go.mod"))
	if err != nil {
		return "", err
	}
	if !strings.Contains(string(mod), "github.com/recolabs/gnata ") {
		return "", oracleError("go.mod is missing github.com/recolabs/gnata")
	}
	return "jsonata pin: gnata runtime, upstream 6f1fc886aa1e3e6bdbfffcdd80e63ede1d7d6b6b, assign example, negative $nextX, and absent $eval", nil
}

func landedFixtures(root string) []string {
	var out []string
	checked := map[string]bool{}
	for _, phase := range checkedPhases(nil, root) {
		checked[phase] = true
	}
	for phase, n := range fixturePhases(nil, root) {
		if checked[phase] && n > 0 && phase != "G" {
			out = append(out, phase)
		}
	}
	return out
}

func checkedPhases(t *testing.T, root string) []string {
	raw, err := os.ReadFile(filepath.Join(root, "ROADMAP.md"))
	if err != nil {
		if t != nil {
			t.Fatal(err)
		}
		return nil
	}
	var out []string
	for _, m := range regexp.MustCompile(`(?m)^## \[x\] Phase ([0-9A-Za-z]+)\b`).FindAllStringSubmatch(string(raw), -1) {
		out = append(out, m[1])
	}
	return out
}

func fixturePhases(t *testing.T, root string) map[string]int {
	raw, err := os.ReadFile(filepath.Join(root, "docs/spec.md"))
	if err != nil {
		if t != nil {
			t.Fatal(err)
		}
		return nil
	}
	counts := map[string]int{}
	for _, m := range regexp.MustCompile(`(?m)^\*\*Phase ([0-9A-Za-z]+)\b`).FindAllStringSubmatch(string(raw), -1) {
		counts[m[1]]++
	}
	return counts
}

func gitStatus(t *testing.T, root string) string {
	t.Helper()
	cmd := exec.Command("git", "status", "--porcelain")
	cmd.Dir = root
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git status: %v\n%s", err, out)
	}
	return string(out)
}

func runGuard(t *testing.T, script string, paths []string) error {
	t.Helper()
	args := []string{script}
	for _, path := range paths {
		args = append(args, "--path", path)
	}
	cmd := exec.Command("python3", args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return &guardError{err: err, out: string(out)}
	}
	return nil
}

type guardError struct {
	err error
	out string
}

func (e *guardError) Error() string { return e.err.Error() + "\n" + e.out }

func mustParse(t *testing.T, path string) []manifest.Table {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	tables, err := manifest.Parse(string(raw))
	if err != nil {
		t.Fatal(err)
	}
	return tables
}

func moduleRoot(t *testing.T) string {
	t.Helper()
	root, err := manifest.ModuleRoot()
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func writeJSON(t *testing.T, path string, v any) {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
}
