package store

import (
	"testing"
	"time"
)

func TestOpenStateOutermostFrame(t *testing.T) {
	history := []HistoryEvent{
		{Type: "MapStateEntered", Name: "Work", Input: `{"a":1}`},
		{Type: "PassStateEntered", Name: "Work", Input: "1"},
		{Type: "PassStateExited", Name: "Work", Output: "1"},
		{Type: "ExecutionFailed"},
	}
	name, input, ok := OpenState(history)
	if !ok || name != "Work" || input != `{"a":1}` {
		t.Fatalf("open %q %q %v", name, input, ok)
	}
	if _, _, ok := OpenState([]HistoryEvent{{Type: "PassStateEntered", Name: "A"}, {Type: "PassStateExited", Name: "A"}}); ok {
		t.Fatal("exited state stayed open")
	}
}

func TestBeginRedriveHistoryLimit(t *testing.T) {
	st := New()
	arn := "arn:aws:states:us-east-1:123456789012:execution:sm:run"
	ex := Execution{ARN: arn, Status: ExecRunning, Type: TypeStandard, Input: "{}", Started: time.Now(), Deadline: time.Now().Add(time.Hour)}
	if _, handle, existed := st.PutExecution(ex); existed || handle == nil {
		t.Fatal("put")
	}
	for i := 0; i < MaxHistory-1; i++ {
		if err := st.Append(arn, HistoryEvent{Type: "PassStateEntered", Name: "P", Input: "{}", HasInput: true}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if _, err := st.Complete(arn, ExecFailed, time.Now(), HistoryEvent{Type: "ExecutionFailed", Error: "E", HasError: true}, "", false, "E", "", true, false); err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.BeginRedrive(arn, time.Now(), "tok", time.Now().Add(time.Hour)); err == nil {
		t.Fatal("history at the cap was redriven")
	} else if refused, ok := err.(RedriveRefusal); !ok || refused.Reason == "" {
		t.Fatalf("%v", err)
	}
}
