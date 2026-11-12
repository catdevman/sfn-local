package exec

import (
	"context"
	"errors"
	"time"

	"github.com/catdevman/sfn-local/internal/asl"
	"github.com/catdevman/sfn-local/internal/jsonpath"
)

var errDeadline = errors.New("deadline")

// scaleWait converts a millisecond delay. Seconds has already been multiplied
// by 1000. A nil scale leaves the delay alone. A set scale multiplies as
// float32 and casts toward zero. Negative scales are absolute values.
// Retry sleeps must not call this.
func scaleWait(ms int64, scale *float32) int64 {
	if ms < 0 {
		ms = 0
	}
	if scale == nil {
		return ms
	}
	s := *scale
	if s < 0 {
		s = -s
	}
	scaled := float32(ms) * s
	if scaled <= 0 {
		return 0
	}
	return int64(scaled)
}

func waitMillis(st *asl.State, root, ctx any, now time.Time) (int64, error) {
	switch {
	case st.Seconds != nil:
		return *st.Seconds * 1000, nil
	case st.HasTimestamp:
		when, ok := parseOffsetTime(st.Timestamp)
		if !ok {
			return 0, jsonpath.Runtime("Timestamp must be an ISO-8601 offset date-time")
		}
		return delayUntil(when, now), nil
	case st.SecondsPath != "":
		val, found, err := jsonpath.Get(st.SecondsPath, root, ctx)
		if err != nil {
			return 0, err
		}
		if !found {
			return 0, jsonpath.Runtime("path " + st.SecondsPath + " was not found")
		}
		n, ok := asFloat(val)
		if !ok {
			return 0, jsonpath.Runtime("SecondsPath must yield a number")
		}
		sec := int64(n)
		if sec < 0 {
			sec = 0
		}
		return sec * 1000, nil
	case st.TimestampPath != "":
		val, found, err := jsonpath.Get(st.TimestampPath, root, ctx)
		if err != nil {
			return 0, err
		}
		if !found {
			return 0, jsonpath.Runtime("path " + st.TimestampPath + " was not found")
		}
		text, ok := val.(string)
		if !ok {
			return 0, jsonpath.Runtime("TimestampPath must yield a string")
		}
		when, ok := parseOffsetTime(text)
		if !ok {
			return 0, jsonpath.Runtime("TimestampPath must yield an ISO-8601 offset date-time")
		}
		return delayUntil(when, now), nil
	default:
		return 0, jsonpath.Runtime("Wait has no time field")
	}
}

func delayUntil(when, now time.Time) int64 {
	ms := when.Sub(now).Milliseconds()
	if ms < 0 {
		return 0
	}
	return ms
}

func (r *Runner) sleep(ctx context.Context, arn string, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	if r.Sleep != nil {
		return r.Sleep(ctx, d)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	var deadline <-chan time.Time
	if r.Store != nil {
		if ex, ok := r.Store.Execution(arn); ok {
			remain := ex.Deadline.Sub(r.now())
			if remain <= 0 {
				return errDeadline
			}
			dt := time.NewTimer(remain)
			defer dt.Stop()
			deadline = dt.C
		}
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-deadline:
		return errDeadline
	case <-timer.C:
		return nil
	}
}
