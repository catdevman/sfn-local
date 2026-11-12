package mock

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeFile(t *testing.T, body string) *Repo {
	t.Helper()
	path := filepath.Join(t.TempDir(), "MockConfigFile.json")
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return &Repo{Path: path}
}

func TestSelectorsOverlapAndReload(t *testing.T) {
	repo := writeFile(t, `{
		"StateMachines": {"retry-sm": {"TestCases": {"RetryOnce": {"Call": "Flaky"}}}},
		"MockedResponses": {"Flaky": {
			"0": {"Throw": {"Error": "CustomError", "Cause": "once"}},
			"1-3": {"Return": {"ok": true}}
		}}
	}`)
	if err := repo.Check("retry-sm", "RetryOnce"); err != nil {
		t.Fatal(err)
	}
	att, err := repo.Lookup("retry-sm", "RetryOnce", "Call", 0)
	if err != nil || !att.HasThrow || att.Error != "CustomError" {
		t.Fatalf("%+v %v", att, err)
	}
	att, err = repo.Lookup("retry-sm", "RetryOnce", "Call", 3)
	if err != nil || !att.HasReturn || string(att.Return) != `{"ok": true}` {
		t.Fatalf("%+v %v", att, err)
	}
	if _, err := repo.Lookup("retry-sm", "RetryOnce", "Call", 4); err == nil || !strings.Contains(err.Error(), "attempt 4") {
		t.Fatal(err)
	}
	if _, err := repo.Lookup("retry-sm", "RetryOnce", "Other", 0); err == nil || !os.IsNotExist(err) && err != StateNotMocked {
		if err != StateNotMocked {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(repo.Path, []byte(`{
		"StateMachines": {"retry-sm": {"TestCases": {"RetryOnce": {"Call": "Flaky"}}}},
		"MockedResponses": {"Flaky": {"0-5": {"Return": {"n": 1}}, "1": {"Return": {"n": 2}}}}
	}`), 0o644); err != nil {
		t.Fatal(err)
	}
	err = repo.Check("retry-sm", "RetryOnce")
	if err == nil || !strings.Contains(err.Error(), "Overlapping mock selectors in response Flaky") {
		t.Fatal(err)
	}
}

func TestMockFileErrors(t *testing.T) {
	var nilRepo *Repo
	if err := nilRepo.Check("a", "b"); err == nil || !strings.Contains(err.Error(), "Mock file not found") {
		t.Fatal(err)
	}
	missing := &Repo{Path: filepath.Join(t.TempDir(), "nope.json")}
	if err := missing.Check("a", "b"); err == nil || !strings.Contains(err.Error(), missing.Path) {
		t.Fatal(err)
	}
	repo := writeFile(t, `{"StateMachines":{"a":{"TestCases":{"T":{}}}},"MockedResponses":{}}`)
	if err := repo.Check("a", ""); err == nil || !strings.Contains(err.Error(), "Unknown mock test") {
		t.Fatal(err)
	}
	big := filepath.Join(t.TempDir(), "big.json")
	if err := os.WriteFile(big, []byte(strings.Repeat("x", 1_000_001)), 0o644); err != nil {
		t.Fatal(err)
	}
	err := (&Repo{Path: big}).Check("a", "b")
	if err == nil || !strings.Contains(err.Error(), "1000000") || !strings.Contains(err.Error(), big) {
		t.Fatal(err)
	}
	bad := writeFile(t, `{"MockedResponses":{"R":{"01":{"Return":1}}},"StateMachines":{"a":{"TestCases":{"T":{"S":"R"}}}}}`)
	if err := bad.Check("a", "T"); err == nil || !strings.Contains(err.Error(), "invalid attempt key 01") {
		t.Fatal(err)
	}
}
