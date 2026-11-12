package api

import (
	"bytes"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/catdevman/sfn-local/internal/api/gen"
	"github.com/catdevman/sfn-local/internal/manifest"
)

func TestSurfaceClassification(t *testing.T) {
	root, err := manifest.ModuleRoot()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(root, "internal/api/gen/surface.gen.json"))
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Operations []struct {
			Name         string `json:"name"`
			InputMembers []struct {
				Name string `json:"name"`
			} `json:"inputMembers"`
			OutputMembers []struct {
				Name string `json:"name"`
			} `json:"outputMembers"`
		} `json:"operations"`
		Errors []struct {
			Name string `json:"name"`
		} `json:"errors"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	surface, err := LoadSurface()
	if err != nil {
		t.Fatal(err)
	}
	ops := surface.Operations()
	if len(ops) != len(doc.Operations) || len(doc.Operations) != len(gen.Operations) {
		t.Fatalf("operations surface %d generated %d list %d", len(ops), len(doc.Operations), len(gen.Operations))
	}
	wantMembers := 0
	for _, op := range doc.Operations {
		wantMembers += len(op.InputMembers) + len(op.OutputMembers)
	}
	gotOps, gotMembers, gotErrors := surface.Counts()
	if gotOps != len(doc.Operations) || gotMembers != wantMembers || gotErrors != len(doc.Errors) {
		t.Fatalf("counts ops %d members %d errors %d, want %d %d %d", gotOps, gotMembers, gotErrors, len(doc.Operations), wantMembers, len(doc.Errors))
	}
	methods := serviceMethods(t, filepath.Join(root, "internal/api"))
	for _, op := range doc.Operations {
		row, ok := ops[op.Name]
		if !ok {
			t.Fatalf("missing operation %s", op.Name)
		}
		if row.Status != "implemented" && row.Status != "deferred" {
			t.Fatalf("%s status %s", op.Name, row.Status)
		}
		if row.Status == "implemented" && !methods[op.Name] {
			t.Fatalf("%s is implemented while the method is still Unimplemented", op.Name)
		}
		delete(ops, op.Name)
		for _, m := range op.InputMembers {
			if _, ok := surface.MemberStatus(op.Name, "input", m.Name); !ok {
				t.Fatalf("missing input member %s.%s", op.Name, m.Name)
			}
		}
		for _, m := range op.OutputMembers {
			if _, ok := surface.MemberStatus(op.Name, "output", m.Name); !ok {
				t.Fatalf("missing output member %s.%s", op.Name, m.Name)
			}
		}
	}
	if len(ops) != 0 {
		t.Fatalf("extra operations %v", ops)
	}
	for _, e := range doc.Errors {
		status, ok := surface.ErrorStatus(e.Name)
		if !ok || (status != "implemented" && status != "deferred") {
			t.Fatalf("error %s status %q", e.Name, status)
		}
	}
	if status, ok := surface.ErrorStatus("ValidationException"); !ok || status != "implemented" {
		t.Fatalf("ValidationException %q", status)
	}
	if status, ok := surface.ErrorStatus("MissingRequiredParameter"); !ok || status != "implemented" {
		t.Fatalf("MissingRequiredParameter %q", status)
	}
}

func TestProtocolGolden(t *testing.T) {
	surface, err := LoadSurface()
	if err != nil {
		t.Fatal(err)
	}
	root, err := manifest.ModuleRoot()
	if err != nil {
		t.Fatal(err)
	}
	for name, row := range surface.Operations() {
		if row.Status != "implemented" {
			continue
		}
		path := filepath.Join(root, "internal/api/testdata/protocol", name+".json")
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		var golden struct {
			Request struct {
				Target string          `json:"target"`
				Body   json.RawMessage `json:"body"`
			} `json:"request"`
			Response struct {
				Status int             `json:"status"`
				Body   json.RawMessage `json:"body"`
			} `json:"response"`
			Error struct {
				Status int    `json:"status"`
				Type   string `json:"type"`
			} `json:"error"`
		}
		if err := json.Unmarshal(raw, &golden); err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		if !strings.Contains(golden.Request.Target, name) {
			t.Errorf("%s target %s", name, golden.Request.Target)
		}
		op, ok := gen.OperationByName(name)
		if !ok {
			t.Errorf("missing generated operation %s", name)
			continue
		}
		members := memberSet(t, root, op.Input)
		var body map[string]any
		if err := json.Unmarshal(golden.Request.Body, &body); err != nil {
			t.Errorf("%s request: %v", name, err)
			continue
		}
		for key := range body {
			if !members[key] {
				t.Errorf("%s request member %s is not in the model", name, key)
			}
		}
		if golden.Error.Type == "" || golden.Error.Status == 0 || !strings.Contains(golden.Error.Type, "#") {
			t.Errorf("%s golden needs error __type and status", name)
		}
		if golden.Response.Status == 0 || len(golden.Response.Body) == 0 {
			t.Errorf("%s golden needs a response status and body", name)
		}
		if !jsonHasNumber(golden.Response.Body) && !jsonHasNumber(golden.Request.Body) {
			t.Errorf("%s golden needs a numeric timestamp", name)
		}
	}
}

func memberSet(t *testing.T, root, input string) map[string]bool {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(root, "internal/api/gen/surface.gen.json"))
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Operations []struct {
			Input        string `json:"input"`
			InputMembers []struct {
				Name string `json:"name"`
			} `json:"inputMembers"`
		} `json:"operations"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	out := map[string]bool{}
	for _, op := range doc.Operations {
		if op.Input != input {
			continue
		}
		for _, m := range op.InputMembers {
			out[m.Name] = true
		}
		return out
	}
	t.Fatalf("input %s", input)
	return nil
}

func jsonHasNumber(raw json.RawMessage) bool {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return false
	}
	return walkNumber(v)
}

func walkNumber(v any) bool {
	switch x := v.(type) {
	case json.Number:
		return !strings.ContainsAny(string(x), "eE\"")
	case map[string]any:
		for _, child := range x {
			if walkNumber(child) {
				return true
			}
		}
	case []any:
		for _, child := range x {
			if walkNumber(child) {
				return true
			}
		}
	}
	return false
}

func serviceMethods(t *testing.T, dir string) map[string]bool {
	t.Helper()
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, dir, func(info os.FileInfo) bool {
		return strings.HasSuffix(info.Name(), ".go") && !strings.HasSuffix(info.Name(), "_test.go")
	}, 0)
	if err != nil {
		t.Fatal(err)
	}
	pkg := pkgs["api"]
	if pkg == nil {
		t.Fatal("package api")
	}
	found := map[string]bool{}
	for _, file := range pkg.Files {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv == nil || len(fn.Recv.List) != 1 {
				continue
			}
			if recvType(fn.Recv.List[0].Type) == "Service" {
				found[fn.Name.Name] = true
			}
		}
	}
	return found
}

func recvType(expr ast.Expr) string {
	switch x := expr.(type) {
	case *ast.StarExpr:
		return recvType(x.X)
	case *ast.Ident:
		return x.Name
	default:
		return ""
	}
}
