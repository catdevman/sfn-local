package exec

import (
	"context"
	"testing"
	"time"

	"github.com/catdevman/sfn-local/internal/asl"
)

func TestComparators(t *testing.T) {
	root := map[string]any{
		"n":  int64(1),
		"s":  "ada",
		"b":  true,
		"z":  nil,
		"ts": "2020-01-01T00:00:00Z",
	}
	cases := []struct {
		raw  string
		want bool
	}{
		{`{"Variable":"$.s","StringMatches":"a*","Next":"Y"}`, true},
		{`{"Variable":"$.s","StringMatches":"\\*","Next":"Y"}`, false},
		{`{"Variable":"$.s","StringMatches":"ada","Next":"Y"}`, true},
		{`{"Variable":"$.n","StringEquals":"1","Next":"Y"}`, false},
		{`{"Variable":"$.n","NumericEquals":1,"Next":"Y"}`, true},
		{`{"Variable":"$.s","NumericEquals":1,"Next":"Y"}`, false},
		{`{"Variable":"$.b","BooleanEquals":true,"Next":"Y"}`, true},
		{`{"Variable":"$.n","BooleanEquals":false,"Next":"Y"}`, false},
		{`{"Variable":"$.b","BooleanEquals":false,"Next":"Y"}`, false},
		{`{"Variable":"$.z","IsNull":true,"Next":"Y"}`, true},
		{`{"Variable":"$.z","IsNull":false,"Next":"Y"}`, false},
		{`{"Variable":"$.s","IsNull":false,"Next":"Y"}`, true},
		{`{"Variable":"$.missing","IsPresent":false,"Next":"Y"}`, true},
		{`{"Variable":"$.s","IsPresent":false,"Next":"Y"}`, false},
		{`{"Variable":"$.n","IsNumeric":false,"Next":"Y"}`, false},
		{`{"Variable":"$.s","IsNumeric":false,"Next":"Y"}`, true},
		{`{"Variable":"$.s","IsString":true,"Next":"Y"}`, true},
		{`{"Variable":"$.n","IsString":false,"Next":"Y"}`, true},
		{`{"Variable":"$.b","IsBoolean":true,"Next":"Y"}`, true},
		{`{"Variable":"$.s","IsBoolean":false,"Next":"Y"}`, true},
		{`{"Variable":"$.s","IsTimestamp":false,"Next":"Y"}`, true},
		{`{"Variable":"$.ts","IsTimestamp":true,"Next":"Y"}`, true},
		{`{"Variable":"$.ts","TimestampEquals":"2020-01-01T00:00:00+00:00","Next":"Y"}`, true},
		{`{"Variable":"$.s","TimestampEquals":"2020-01-01T00:00:00Z","Next":"Y"}`, false},
		{`{"Variable":"$.s","StringGreaterThan":"abc","Next":"Y"}`, true},
		{`{"Variable":"$.n","NumericEqualsPath":"$.n","Next":"Y"}`, true},
	}
	for _, tc := range cases {
		st := &asl.State{Choices: []byte("[" + tc.raw + "]"), HasDefault: true, Default: "N"}
		got, err := choose(st, root, map[string]any{})
		if err != nil {
			t.Fatalf("%s: %v", tc.raw, err)
		}
		want := "N"
		if tc.want {
			want = "Y"
		}
		if got != want {
			t.Fatalf("%s -> %s", tc.raw, got)
		}
	}
	if _, err := choose(&asl.State{Choices: []byte(`[{"Variable":"$.missing","IsNull":true,"Next":"Y"}]`)}, root, nil); err == nil {
		t.Fatal("missing IsNull")
	}
	if !stringMatches(`a\*b`, "a*b") || stringMatches(`a\*b`, "axb") {
		t.Fatal("literal star")
	}
	if cmpUTF16("a", "b") >= 0 || cmpUTF16("b", "a") <= 0 || cmpUTF16("a", "a") != 0 {
		t.Fatal("utf16 order")
	}
}

func TestWaitScale(t *testing.T) {
	if scaleWait(2000, nil) != 2000 {
		t.Fatal("unset")
	}
	zero := float32(0)
	if scaleWait(2000, &zero) != 0 {
		t.Fatal("zero")
	}
	half := float32(0.5)
	if scaleWait(1000, &half) != 500 {
		t.Fatal("half")
	}
	neg := float32(-2)
	if scaleWait(1000, &neg) != 2000 {
		t.Fatal("negative")
	}
	ms := int64(16777217)
	one := float32(1)
	got := scaleWait(ms, &one)
	want := int64(float32(ms) * one)
	if got != want || got == ms {
		t.Fatalf("float32 %d want %d raw %d", got, want, ms)
	}
	when, ok := parseOffsetTime("2020-01-01T00:00:00+00:00")
	if !ok {
		t.Fatal("offset")
	}
	if _, ok := parseOffsetTime("2020-01-01T00:00:00"); ok {
		t.Fatal("bare timestamp")
	}
	now := when.Add(-1500 * time.Millisecond)
	if delayUntil(when, now) != 1500 {
		t.Fatal(delayUntil(when, now))
	}
	if delayUntil(when, when.Add(time.Second)) != 0 {
		t.Fatal("past")
	}
}

func TestSleepInterrupts(t *testing.T) {
	r := &Runner{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	err := r.sleep(ctx, "missing", time.Second)
	if err == nil || time.Since(start) > 200*time.Millisecond {
		t.Fatal(err, time.Since(start))
	}
}
