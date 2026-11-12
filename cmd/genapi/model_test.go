package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/catdevman/sfn-local/internal/manifest"
)

func TestRealModelShape(t *testing.T) {
	root, err := manifest.ModuleRoot()
	if err != nil {
		t.Fatal(err)
	}
	m, err := loadModel(filepath.Join(root, "aws-spec/states-api/sfn-2016-11-23.json"))
	if err != nil {
		t.Fatal(err)
	}
	if m.Service.Name != "AWSStepFunctions" || !m.Service.Traits.Protocol {
		t.Fatalf("service %+v", m.Service)
	}
	if len(m.Service.Operations) != 37 {
		t.Fatalf("operations %d", len(m.Service.Operations))
	}
	kinds := map[string]bool{}
	for _, sh := range m.Shapes {
		kinds[sh.Kind] = true
		if !allowedKinds[sh.Kind] {
			t.Fatalf("kind %s", sh.Kind)
		}
	}
	for kind := range allowedKinds {
		if kind != "service" && kind != "operation" && !kinds[kind] {
			t.Fatalf("model is missing kind %s", kind)
		}
	}
}

func TestUnknownShapeKindFails(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "model.json")
	const model = `{
	  "smithy": "2.0",
	  "shapes": {
	    "example#Svc": {
	      "type": "service",
	      "version": "2020-01-01",
	      "operations": [{"target": "example#Op"}],
	      "traits": {"aws.protocols#awsJson1_0": {}}
	    },
	    "example#Op": {
	      "type": "operation",
	      "input": {"target": "example#In"},
	      "output": {"target": "example#Out"}
	    },
	    "example#In": {"type": "structure", "members": {}},
	    "example#Out": {"type": "structure", "members": {}},
	    "example#Blob": {"type": "blob"}
	  }
	}`
	if err := os.WriteFile(path, []byte(model), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := loadModel(path)
	if err == nil || !strings.Contains(err.Error(), "unsupported shape kind") {
		t.Fatalf("err = %v", err)
	}
}
