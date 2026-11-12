package exec

import (
	"testing"
	"time"

	"github.com/catdevman/sfn-local/internal/asl"
)

func TestRetryDelay(t *testing.T) {
	r := &Runner{}
	if d := r.retryDelay(asl.Retrier{IntervalSeconds: 3, BackoffRate: 1.5}, 2); d != 4*time.Second {
		t.Fatalf("floor %s", d)
	}
	if d := r.retryDelay(asl.Retrier{IntervalSeconds: 10, BackoffRate: 2, MaxDelaySeconds: 5}, 2); d != 5*time.Second {
		t.Fatalf("cap %s", d)
	}
	if d := r.retryDelay(asl.Retrier{IntervalSeconds: 2, BackoffRate: 2}, 1); d != 2*time.Second {
		t.Fatalf("first %s", d)
	}
	r.Rand = func(max int64) int64 { return 0 }
	if d := r.retryDelay(asl.Retrier{IntervalSeconds: 2, BackoffRate: 2, JitterStrategy: "FULL"}, 1); d != 0 {
		t.Fatalf("jitter zero %s", d)
	}
	r.Rand = func(max int64) int64 { return max }
	if d := r.retryDelay(asl.Retrier{IntervalSeconds: 2, BackoffRate: 2, JitterStrategy: "FULL"}, 1); d != 2*time.Second {
		t.Fatalf("jitter full %s", d)
	}
	if matchesError([]string{"States.TaskFailed"}, "States.Timeout") {
		t.Fatal("TaskFailed matched timeout")
	}
	if !matchesError([]string{"States.TaskFailed"}, "CustomError") {
		t.Fatal("TaskFailed missed CustomError")
	}
	if !matchesError([]string{"States.ALL"}, "States.Timeout") {
		t.Fatal("ALL missed timeout")
	}
	if !matchesError([]string{"CustomError"}, "CustomError") {
		t.Fatal("name")
	}
}
