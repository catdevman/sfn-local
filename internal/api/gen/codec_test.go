package gen

import (
	"encoding/json"
	"math"
	"os"
	"strings"
	"testing"
)

func TestProtocolConstants(t *testing.T) {
	if Protocol != "aws.protocols#awsJson1_0" || TargetPrefix != "AWSStepFunctions" || ServiceVersion != "2016-11-23" {
		t.Fatalf("protocol %s prefix %s version %s", Protocol, TargetPrefix, ServiceVersion)
	}
	if len(Operations) != 37 {
		t.Fatalf("operations %d", len(Operations))
	}
}

func TestPresenceOmitsDistinctFromZero(t *testing.T) {
	in, err := DecodeInput("CreateStateMachine", []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	got := in.(*CreateStateMachineInput)
	if got.Name != nil || got.Publish != nil || got.Definition != nil {
		t.Fatalf("omitted fields became present: name=%v publish=%v", got.Name, got.Publish)
	}
	in, err = DecodeInput("CreateStateMachine", []byte(`{"name":"","publish":false}`))
	if err != nil {
		t.Fatal(err)
	}
	got = in.(*CreateStateMachineInput)
	if got.Name == nil || *got.Name != "" {
		t.Fatalf("empty name = %v", got.Name)
	}
	if got.Publish == nil || *got.Publish {
		t.Fatalf("publish false = %v", got.Publish)
	}

	hist, err := DecodeInput("GetExecutionHistory", []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	h := hist.(*GetExecutionHistoryInput)
	if h.MaxResults != nil || h.IncludeExecutionData != nil {
		t.Fatalf("handler defaults were applied: maxResults=%v include=%v", h.MaxResults, h.IncludeExecutionData)
	}
	hist, err = DecodeInput("GetExecutionHistory", []byte(`{"maxResults":0,"includeExecutionData":false}`))
	if err != nil {
		t.Fatal(err)
	}
	h = hist.(*GetExecutionHistoryInput)
	if h.MaxResults == nil || *h.MaxResults != 0 {
		t.Fatalf("maxResults 0 = %v", h.MaxResults)
	}
	if h.IncludeExecutionData == nil || *h.IncludeExecutionData {
		t.Fatalf("includeExecutionData false = %v", h.IncludeExecutionData)
	}
}

func TestDefaultIsNotAppliedByDecoder(t *testing.T) {
	raw, err := osReadSurface(t)
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Operations []struct {
			Name         string `json:"name"`
			InputMembers []struct {
				Name       string          `json:"name"`
				HasDefault bool            `json:"hasDefault"`
				Default    json.RawMessage `json:"default"`
			} `json:"inputMembers"`
		} `json:"operations"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	var sawPublish, sawNull bool
	for _, op := range doc.Operations {
		for _, m := range op.InputMembers {
			if !m.HasDefault {
				continue
			}
			if op.Name == "CreateStateMachine" && m.Name == "publish" {
				sawPublish = true
				if string(m.Default) != "false" {
					t.Fatalf("publish default = %s", m.Default)
				}
			}
			if op.Name == "UpdateMapRun" && m.Name == "maxConcurrency" {
				sawNull = true
				if string(m.Default) != "null" {
					t.Fatalf("null default = %s", m.Default)
				}
			}
		}
	}
	if !sawPublish || !sawNull {
		t.Fatalf("surface defaults publish=%v null=%v", sawPublish, sawNull)
	}
	in, err := DecodeInput("CreateStateMachine", []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if in.(*CreateStateMachineInput).Publish != nil {
		t.Fatal("decoder applied @default false to publish")
	}
}

func TestTimestampPlainNumber(t *testing.T) {
	var ts Timestamp
	if err := ts.UnmarshalJSON([]byte("1715990400.123")); err != nil {
		t.Fatal(err)
	}
	if math.Abs(ts.Seconds()-1715990400.123) > 1e-6 {
		t.Fatalf("seconds %v", ts.Seconds())
	}
	raw, err := ts.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != "1715990400.123" {
		t.Fatalf("literal round trip %s", raw)
	}
	if strings.ContainsAny(string(raw), "eE") {
		t.Fatalf("scientific %s", raw)
	}
	out, err := NewTimestamp(1715990400.123).MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	if strings.ContainsAny(string(out), "eE") || strings.Contains(string(out), `"`) {
		t.Fatalf("marshal %s", out)
	}
	var echoed float64
	if err := json.Unmarshal(out, &echoed); err != nil {
		t.Fatal(err)
	}
	if math.Abs(echoed-1715990400.123) > 1e-3 {
		t.Fatalf("numeric %v from %s", echoed, out)
	}
	for _, bad := range []string{`1e2`, `1E2`, `"1715990400"`, `+1`, `01`, `1.`, ``} {
		var ts Timestamp
		if err := ts.UnmarshalJSON([]byte(bad)); err == nil {
			t.Fatalf("accepted %s", bad)
		}
	}
}

func TestNamePatternLookahead(t *testing.T) {
	bad := "123"
	in := &CreateStateMachineAliasInput{Name: &bad}
	err := in.Validate()
	if err == nil || !strings.Contains(err.Error(), "regular expression") {
		t.Fatalf("digits-only name: %v", err)
	}
	good := "rebuild-check"
	in.Name = &good
	err = in.Validate()
	if err != nil && strings.Contains(err.Error(), "regular expression") {
		t.Fatalf("valid name rejected by pattern: %v", err)
	}
}

func TestDecodeRejectsTrailingTokenAndUnknownMember(t *testing.T) {
	if _, err := DecodeInput("ListStateMachines", []byte(`{}{}`)); err == nil {
		t.Fatal("trailing token accepted")
	}
	in, err := DecodeInput("ListStateMachines", []byte(`{"notAMember":1}`))
	if err != nil {
		t.Fatal(err)
	}
	if in.(*ListStateMachinesInput).MaxResults != nil {
		t.Fatal("unknown member changed maxResults")
	}
	in, err = DecodeInput("ListStateMachines", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := in.(*ListStateMachinesInput).Validate(); err != nil {
		t.Fatal(err)
	}
}

func osReadSurface(t *testing.T) ([]byte, error) {
	t.Helper()
	return os.ReadFile("surface.gen.json")
}
