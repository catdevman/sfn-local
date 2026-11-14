package exec

import (
	"context"
	"crypto/rand"
	"fmt"
	"strings"
	"time"

	"github.com/catdevman/sfn-local/internal/asl"
	"github.com/catdevman/sfn-local/internal/store"
)

func newTaskID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

func (r *Runner) runParked(ctx context.Context, arn string, st *asl.State, input, effective string, effVal, ctxObj any, kind taskKind) (string, string, bool, bool) {
	base := effective
	baseVal := effVal
	retries := 0
	for {
		setRetryCount(ctxObj, retries)
		if r.halt(ctx, arn) {
			return "", "", false, true
		}
		if r.pastDeadline(arn) {
			r.finish(arn, store.ExecTimedOut, "ExecutionTimedOut", "", false, "States.Timeout", "", true, false)
			return "", "", false, true
		}
		seconds, hasTimeout, err := r.timeoutSeconds(ctx, st, baseVal, ctxObj)
		if err != nil {
			next, output, terminal, done, again, handled := r.catchQuery(ctx, arn, st, input, ctxObj, err, &retries)
			if handled {
				if again {
					continue
				}
				return next, output, terminal, done
			}
			r.failEval(ctx, arn, err)
			return "", "", false, true
		}
		hb, hasHB, err := r.heartbeatSeconds(ctx, st, baseVal, ctxObj)
		if err != nil {
			next, output, terminal, done, again, handled := r.catchQuery(ctx, arn, st, input, ctxObj, err, &retries)
			if handled {
				if again {
					continue
				}
				return next, output, terminal, done
			}
			r.failEval(ctx, arn, err)
			return "", "", false, true
		}
		if hasHB && hasTimeout && hb >= seconds {
			r.failCtx(ctx, arn, "States.Runtime", "HeartbeatSeconds must be less than TimeoutSeconds")
			return "", "", false, true
		}
		attemptInput := base
		token := newTaskID()
		if obj, ok := ctxObj.(map[string]any); ok {
			obj["Task"] = map[string]any{"Token": token}
		}
		if st.HasParameters {
			_, attemptInput, err = evalParameters(st.Parameters, baseVal, ctxObj, entryVars(ctx))
			if err != nil {
				r.failEval(ctx, arn, err)
				return "", "", false, true
			}
		}
		meta := taskMeta{
			kind: kind, state: st.Name, resource: st.Resource, shown: shownResource(st.Resource), resourceType: resourceTypeOf(st.Resource),
			region: r.Region, input: attemptInput,
		}
		if hasTimeout {
			meta.timeout = &seconds
		}
		if hasHB {
			meta.heartbeat = &hb
		}
		result, failure := r.onePark(ctx, arn, meta, token, seconds, hasTimeout, hb, hasHB)
		if failure == nil {
			return r.completeTask(ctx, arn, st, input, result, ctxObj)
		}
		if failure.Halt {
			if r.halt(ctx, arn) {
				return "", "", false, true
			}
			return "", "", false, true
		}
		if failure.ExecDeadline {
			r.finish(arn, store.ExecTimedOut, "ExecutionTimedOut", "", false, "States.Timeout", "", true, false)
			return "", "", false, true
		}
		if rt, ok := pickRetry(st.Retry, failure.Name); ok {
			n := retries + 1
			if int64(n) <= rt.MaxAttempts {
				if !r.retrySleep(ctx, arn, rt, n) {
					return "", "", false, true
				}
				retries = n
				continue
			}
		}
		if catcher, ok := pickCatch(st.Catch, failure.Name); ok {
			return r.catchTask(ctx, arn, st, input, catcher, failure, ctxObj)
		}
		r.failCtx(ctx, arn, failure.Name, failure.Cause)
		return "", "", false, true
	}
}

func (r *Runner) onePark(ctx context.Context, arn string, meta taskMeta, token string, seconds int64, hasTimeout bool, hb int64, hasHB bool) (string, *taskFailure) {
	now := r.now()
	fresh, ok := r.Store.Execution(arn)
	if !ok {
		return "", &taskFailure{Halt: true}
	}
	limit := fresh.Deadline
	taskWins := false
	if hasTimeout {
		taskDL := now.Add(time.Duration(seconds) * time.Second)
		if !taskDL.After(limit) {
			limit = taskDL
			taskWins = true
		}
	}
	if meta.kind == kindActivity {
		if !r.emitActivityScheduled(ctx, arn, meta) {
			return "", &taskFailure{Halt: true}
		}
	} else if !r.emitScheduled(ctx, arn, meta) || !r.emitStarted(ctx, arn, meta) || !r.emitSubmitted(ctx, arn, meta) {
		return "", &taskFailure{Halt: true}
	}
	tok := newTaskToken(token, arn, meta)
	tok.Resource = reportedResource(meta.resource, r.Region, r.Account)
	if meta.kind == kindActivity {
		tok.ActivityARN = meta.resource
	}
	if taskWins && !now.Before(limit) {
		r.Store.KeepExpired(tok)
		if !r.emitParkEnd(ctx, arn, meta, "", &taskFailure{Name: "States.Timeout", Timeout: true}) {
			return "", &taskFailure{Halt: true}
		}
		return "", &taskFailure{Name: "States.Timeout", Timeout: true}
	}
	if meta.kind == kindActivity {
		r.Store.EnqueueActivity(tok)
	} else {
		r.Store.RegisterCallback(tok)
	}
	res := r.awaitToken(ctx, arn, tok, limit, taskWins, hb, hasHB)
	switch res.status {
	case store.TokenSucceeded:
		if !r.emitParkEnd(ctx, arn, meta, res.output, nil) {
			return "", &taskFailure{Halt: true}
		}
		return res.output, nil
	case store.TokenFailed:
		tf := &taskFailure{Name: res.errName, Cause: res.cause}
		if !r.emitParkEnd(ctx, arn, meta, "", tf) {
			return "", &taskFailure{Halt: true}
		}
		return "", tf
	case store.TokenTimedOut:
		// A heartbeat expiry is a task failure even when TimeoutSeconds is unset.
		// The execution deadline is the only TokenTimedOut that is not.
		if !taskWins && !res.heartbeat {
			return "", &taskFailure{ExecDeadline: true}
		}
		tf := &taskFailure{Name: "States.Timeout", Timeout: true}
		if !r.emitParkEnd(ctx, arn, meta, "", tf) {
			return "", &taskFailure{Halt: true}
		}
		return "", tf
	case "deadline":
		r.Store.AbortToken(tok.ID)
		return "", &taskFailure{ExecDeadline: true}
	default:
		r.Store.AbortToken(tok.ID)
		return "", &taskFailure{Halt: true}
	}
}

type parkResult struct {
	status    string
	output    string
	errName   string
	cause     string
	heartbeat bool
}

func (r *Runner) awaitToken(ctx context.Context, arn string, tok *store.TaskToken, limit time.Time, taskWins bool, hb int64, hasHB bool) parkResult {
	// A closed started channel stays readable forever. Observe it once, then
	// drop it from the select so a later heartbeat or result is not starved.
	started := false
	select {
	case <-tok.Started():
		started = true
	default:
	}
	var startedC <-chan struct{}
	if !started {
		startedC = tok.Started()
	}
	announced := false
	var hbTimer *time.Timer
	var hbC <-chan time.Time
	armHB := func() {
		if !hasHB {
			return
		}
		if hbTimer != nil {
			hbTimer.Stop()
		}
		if hb <= 0 {
			hbC = closedTime()
			return
		}
		hbTimer = time.NewTimer(time.Duration(hb) * time.Second)
		hbC = hbTimer.C
	}
	defer func() {
		if hbTimer != nil {
			hbTimer.Stop()
		}
	}()
	remain := limit.Sub(r.now())
	if remain <= 0 {
		if taskWins {
			r.Store.ExpireToken(tok.ID)
			return parkResult{status: store.TokenTimedOut}
		}
		return parkResult{status: "deadline"}
	}
	limitTimer := time.NewTimer(remain)
	defer limitTimer.Stop()
	limitC := limitTimer.C
	for {
		if started && !announced {
			if tok.Kind == store.TokenActivity && !r.emitActivityStarted(ctx, arn, tok.Worker) {
				r.Store.AbortToken(tok.ID)
				return parkResult{status: store.TokenAborted}
			}
			announced = true
			// Callbacks arm here, at the start of the wait, after TaskSubmitted.
			armHB()
		}
		select {
		case <-ctx.Done():
			r.Store.AbortToken(tok.ID)
			return parkResult{status: store.TokenAborted}
		case <-startedC:
			started = true
			startedC = nil
		case res := <-tok.Done():
			// Success can arrive in the same select as the start signal. Announce
			// ActivityStarted before the terminal task event.
			if !announced {
				select {
				case <-tok.Started():
					started = true
				default:
				}
				if started {
					if tok.Kind == store.TokenActivity && !r.emitActivityStarted(ctx, arn, tok.Worker) {
						r.Store.AbortToken(tok.ID)
						return parkResult{status: store.TokenAborted}
					}
					announced = true
				}
			}
			return parkResult{status: res.Status, output: res.Output, errName: res.Error, cause: res.Cause}
		case <-tok.Heartbeats():
			if announced {
				armHB()
			}
		case <-hbC:
			r.Store.ExpireToken(tok.ID)
			return parkResult{status: store.TokenTimedOut, heartbeat: true}
		case <-limitC:
			if taskWins {
				r.Store.ExpireToken(tok.ID)
				return parkResult{status: store.TokenTimedOut}
			}
			r.Store.AbortToken(tok.ID)
			return parkResult{status: "deadline"}
		}
	}
}

func closedTime() <-chan time.Time {
	ch := make(chan time.Time)
	close(ch)
	return ch
}

// reportedResource is the string the 2.0.0 jar puts in Task Timed Out.
// An activity ARN is already complete. A callback resource with an empty
// region and account is filled from the process, so
// arn:aws:states:::sqs:sendMessage.waitForTaskToken becomes
// arn:aws:states:<region>:<account>:sqs:sendMessage.waitForTaskToken.
func reportedResource(resource, region, account string) string {
	parts := strings.Split(resource, ":")
	if len(parts) < 5 || parts[0] != "arn" || parts[1] != "aws" {
		return resource
	}
	if parts[3] == "" {
		parts[3] = region
	}
	if parts[4] == "" {
		parts[4] = account
	}
	return strings.Join(parts, ":")
}

func newTaskToken(id, arn string, meta taskMeta) *store.TaskToken {
	kind := store.TokenActivity
	if meta.kind == kindCallback {
		kind = store.TokenCallback
	}
	return store.NewTaskToken(id, kind, arn, meta.input)
}

func (r *Runner) emitActivityScheduled(ctx context.Context, arn string, meta taskMeta) bool {
	ev := store.HistoryEvent{
		Type: "ActivityScheduled", Resource: meta.resource, HasResource: meta.resource != "",
		Input: meta.input, HasInput: true,
	}
	if meta.timeout != nil {
		ev.TimeoutSeconds = *meta.timeout
		ev.HasTimeout = true
	}
	if meta.heartbeat != nil {
		ev.HeartbeatSeconds = *meta.heartbeat
		ev.HasHeartbeat = true
	}
	return r.emit(ctx, arn, ev)
}

func (r *Runner) emitActivityStarted(ctx context.Context, arn, worker string) bool {
	ev := store.HistoryEvent{Type: "ActivityStarted"}
	if worker != "" {
		ev.Worker = worker
		ev.HasWorker = true
	}
	return r.emit(ctx, arn, ev)
}

func (r *Runner) emitSubmitted(ctx context.Context, arn string, meta taskMeta) bool {
	ev := store.HistoryEvent{
		Type: "TaskSubmitted", Resource: meta.shown, HasResource: meta.shown != "",
		ResourceType: meta.resourceType, HasResourceType: meta.resourceType != "",
		Output: "", HasOutput: true,
	}
	return r.emit(ctx, arn, ev)
}

func (r *Runner) emitParkEnd(ctx context.Context, arn string, meta taskMeta, result string, tf *taskFailure) bool {
	if meta.kind == kindActivity {
		return r.emitActivityEnd(ctx, arn, result, tf)
	}
	return r.emitAttemptEnd(ctx, arn, meta, result, tf)
}

func (r *Runner) emitActivityEnd(ctx context.Context, arn, result string, tf *taskFailure) bool {
	ev := store.HistoryEvent{}
	if tf == nil {
		ev.Type = "ActivitySucceeded"
		ev.Output = result
		ev.HasOutput = true
		return r.emit(ctx, arn, ev)
	}
	if tf.Timeout {
		ev.Type = "ActivityTimedOut"
	} else {
		ev.Type = "ActivityFailed"
	}
	ev.Error = tf.Name
	ev.HasError = tf.Name != ""
	ev.Cause = tf.Cause
	ev.HasCause = tf.Cause != ""
	return r.emit(ctx, arn, ev)
}

func (r *Runner) deferTaskParameters(ex store.Execution, st *asl.State) bool {
	if st == nil || st.Type != "Task" {
		return false
	}
	if !isActivityARN(st.Resource) && !strings.HasSuffix(st.Resource, ".waitForTaskToken") {
		return false
	}
	mocked, err := r.resourceMocked(ex, st.Name)
	if err != nil || mocked {
		return false
	}
	return true
}

func (r *Runner) taskContextToken(ex store.Execution, st *asl.State) (string, bool) {
	if st == nil || st.Type != "Task" || isActivityARN(st.Resource) {
		return "", false
	}
	if _, ok := parseLambdaARN(st.Resource, r.Region, r.Account); ok {
		return "", false
	}
	mocked, err := r.resourceMocked(ex, st.Name)
	if err != nil || !mocked {
		return "", false
	}
	return newTaskID(), true
}

func (r *Runner) resourceMocked(ex store.Execution, state string) (bool, error) {
	if _, ok := r.stateMock(ex.ARN); ok {
		return true, nil
	}
	if ex.MockTest == "" {
		return false, nil
	}
	if r.Mocks == nil {
		return false, nil
	}
	return r.Mocks.Mocked(mockMachine(ex), ex.MockTest, state)
}
