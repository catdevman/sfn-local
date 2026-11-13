package examples_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/catdevman/sfn-local/internal/api"
	"github.com/catdevman/sfn-local/internal/api/gen"
	"github.com/catdevman/sfn-local/internal/exec"
	"github.com/catdevman/sfn-local/internal/store"
)

func TestDefinitions(t *testing.T) {
	for _, name := range []string{"small", "medium", "large", "parallel", "map", "jsonata", "catch"} {
		t.Run(name, func(t *testing.T) {
			def := readExample(t, name, "definition.json")
			input := readExample(t, name, "input.json")
			want := readExample(t, name, "output.json")
			got, status, cause := runExample(t, name, string(def), string(input))
			if status != store.ExecSucceeded {
				t.Fatalf("status %s cause %s output %s", status, cause, got)
			}
			if !jsonEqual(t, []byte(got), want) {
				t.Fatalf("output\n got %s\nwant %s", got, compact(t, want))
			}
		})
	}
}

func runExample(t *testing.T, name, def, input string) (string, string, string) {
	t.Helper()
	st := store.New()
	s := &api.Service{
		Store: st,
		Runner: &exec.Runner{
			Store:   st,
			Region:  "us-east-1",
			Account: "123456789012",
		},
		Account: "123456789012",
		Region:  "us-east-1",
		Profile: api.ProfileSmithy,
		Log:     io.Discard,
	}
	created, err := s.CreateStateMachine(context.Background(), &gen.CreateStateMachineInput{
		Name:       sp(name),
		Definition: sp(def),
		RoleArn:    sp("arn:aws:iam::123456789012:role/local"),
	})
	if err != nil {
		t.Fatal(err)
	}
	started, err := s.StartExecution(context.Background(), &gen.StartExecutionInput{
		StateMachineArn: created.StateMachineArn,
		Name:            sp("run"),
		Input:           sp(input),
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-s.Store.Finished(*started.ExecutionArn):
	case <-time.After(5 * time.Second):
		t.Fatal("timed out")
	}
	ex, ok := s.Store.Execution(*started.ExecutionArn)
	if !ok {
		t.Fatal("missing execution")
	}
	return ex.Output, ex.Status, ex.Error + " " + ex.Cause
}

func readExample(t *testing.T, name, file string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(name, file))
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func jsonEqual(t *testing.T, got, want []byte) bool {
	t.Helper()
	var left, right any
	if err := json.Unmarshal(got, &left); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(want, &right); err != nil {
		t.Fatal(err)
	}
	return compact(t, mustRaw(t, left)) == compact(t, mustRaw(t, right))
}

func compact(t *testing.T, raw []byte) string {
	t.Helper()
	var buf bytes.Buffer
	if err := json.Compact(&buf, raw); err != nil {
		t.Fatal(err)
	}
	return buf.String()
}

func mustRaw(t *testing.T, v any) []byte {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func sp(s string) *string { return &s }
