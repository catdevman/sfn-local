package exec

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/catdevman/sfn-local/internal/asl"
	"github.com/catdevman/sfn-local/internal/jsonpath"
	"github.com/catdevman/sfn-local/internal/mock"
	"github.com/catdevman/sfn-local/internal/store"
)

const maxRetryDelay = int64(31622400)

// LambdaCall is one direct function invoke.
type LambdaCall struct {
	Function  string
	Qualifier string
	Payload   string
	Endpoint  string
}

// LambdaReply is the Invoke result. Err is a transport failure.
type LambdaReply struct {
	Status        int
	Payload       string
	FunctionError string
	Err           error
}

type taskKind int

const (
	kindLambda taskKind = iota
	kindGeneric
)

type taskFailure struct {
	Name         string
	Cause        string
	Timeout      bool
	ExecDeadline bool
	Halt         bool
}

type lambdaTarget struct {
	name, qualifier string
}

type taskMeta struct {
	kind         taskKind
	state        string
	resource     string
	resourceType string
	region       string
	input        string
	timeout      *int64
	heartbeat    *int64
}

type errDoc struct {
	Error string `json:"Error"`
	Cause string `json:"Cause"`
}

func (r *Runner) runTask(ctx context.Context, arn string, st *asl.State, input, effective string, effVal, ctxObj any) (next, output string, terminal, done bool) {
	ex, ok := r.Store.Execution(arn)
	if !ok {
		return "", "", false, true
	}
	machine := arnName(ex.StateMachineARN, ":stateMachine:")
	kind, target, mocked, err := r.classify(ex, st, machine)
	if err != nil {
		r.failCtx(ctx, arn, "States.Runtime", err.Error())
		return "", "", false, true
	}
	meta := taskMeta{
		kind: kind, state: st.Name, resource: st.Resource, resourceType: resourceTypeOf(st.Resource),
		region: r.Region, input: effective, heartbeat: st.HeartbeatSeconds,
	}
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
		seconds, hasTimeout, err := timeoutSeconds(st, effVal, ctxObj)
		if err != nil {
			r.failEval(ctx, arn, err)
			return "", "", false, true
		}
		if hasTimeout {
			meta.timeout = &seconds
		}
		result, failure := r.oneAttempt(ctx, arn, ex, meta, target, mocked, seconds, hasTimeout)
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
			if catcher.HasAssign {
				r.failCtx(ctx, arn, "States.Runtime", "Assign is not implemented by this local server.")
				return "", "", false, true
			}
			return r.catchTask(ctx, arn, st, input, catcher, failure, ctxObj)
		}
		r.failCtx(ctx, arn, failure.Name, failure.Cause)
		return "", "", false, true
	}
}

func (r *Runner) classify(ex store.Execution, st *asl.State, machine string) (taskKind, lambdaTarget, bool, error) {
	if _, ok := r.stateMock(ex.ARN); ok {
		if isActivityARN(st.Resource) {
			return 0, lambdaTarget{}, false, errors.New("activity state " + st.Name + " cannot be mocked")
		}
		if fn, ok := parseLambdaARN(st.Resource, r.Region, r.Account); ok {
			return kindLambda, fn, true, nil
		}
		return kindGeneric, lambdaTarget{}, true, nil
	}
	if isActivityARN(st.Resource) {
		if ex.MockTest != "" {
			on, err := r.Mocks.Mocked(machine, ex.MockTest, st.Name)
			if err != nil {
				return 0, lambdaTarget{}, false, err
			}
			if on {
				return 0, lambdaTarget{}, false, errors.New("activity state " + st.Name + " cannot be mocked")
			}
		}
		return 0, lambdaTarget{}, false, errors.New("unsupported resource: " + st.Resource)
	}
	if fn, ok := parseLambdaARN(st.Resource, r.Region, r.Account); ok {
		if ex.MockTest != "" {
			on, err := r.Mocks.Mocked(machine, ex.MockTest, st.Name)
			if err != nil {
				return 0, lambdaTarget{}, false, err
			}
			if on {
				return kindLambda, fn, true, nil
			}
		}
		if r.LambdaEndpoint == "" {
			return 0, lambdaTarget{}, false, errors.New("unsupported resource: " + st.Resource)
		}
		return kindLambda, fn, false, nil
	}
	if ex.MockTest != "" {
		on, err := r.Mocks.Mocked(machine, ex.MockTest, st.Name)
		if err != nil {
			return 0, lambdaTarget{}, false, err
		}
		if on {
			return kindGeneric, lambdaTarget{}, true, nil
		}
	}
	return 0, lambdaTarget{}, false, errors.New("unsupported resource: " + st.Resource)
}

func (r *Runner) oneAttempt(ctx context.Context, arn string, ex store.Execution, meta taskMeta, target lambdaTarget, mocked bool, seconds int64, hasTimeout bool) (string, *taskFailure) {
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
	if !now.Before(limit) {
		if taskWins {
			if !r.emitScheduled(arn, meta) {
				return "", &taskFailure{Halt: true}
			}
			tf := &taskFailure{Name: "States.Timeout", Timeout: true}
			if !r.emitAttemptEnd(arn, meta, "", tf) {
				return "", &taskFailure{Halt: true}
			}
			return "", tf
		}
		return "", &taskFailure{ExecDeadline: true}
	}
	if !r.emitScheduled(arn, meta) {
		return "", &taskFailure{Halt: true}
	}
	callCtx, cancel := context.WithDeadline(ctx, limit)
	defer cancel()
	if !r.emitStarted(arn, meta) {
		return "", &taskFailure{Halt: true}
	}
	var (
		result string
		tf     *taskFailure
	)
	if mocked {
		result, tf = r.readMock(arn, arnName(ex.StateMachineARN, ":stateMachine:"), ex.MockTest, meta.state, meta.kind)
	} else {
		result, tf = r.invokeLambda(callCtx, ctx, target, meta.input, taskWins)
	}
	if tf != nil && tf.Halt {
		return "", tf
	}
	if tf != nil && tf.ExecDeadline {
		return "", tf
	}
	if !r.emitAttemptEnd(arn, meta, result, tf) {
		return "", &taskFailure{Halt: true}
	}
	return result, tf
}

func (r *Runner) completeTask(ctx context.Context, arn string, st *asl.State, input, result string, ctxObj any) (string, string, bool, bool) {
	resultVal, err := parseJSON(result)
	if err != nil {
		r.failCtx(ctx, arn, "States.Runtime", "task result is not JSON")
		return "", "", false, true
	}
	if st.HasResultSelector {
		resultVal, result, err = evalSelector(st.ResultSelector, resultVal, ctxObj)
		if err != nil {
			r.failEval(ctx, arn, err)
			return "", "", false, true
		}
	}
	injected, injVal, err := inject(st.ResultPath, input, result, resultVal)
	if err != nil {
		r.failEval(ctx, arn, err)
		return "", "", false, true
	}
	output, _, err := project(st.OutputPath, injected, injVal, ctxObj)
	if err != nil {
		r.failEval(ctx, arn, err)
		return "", "", false, true
	}
	if !r.exit(ctx, arn, st, output) {
		return "", "", false, true
	}
	return transition(st, output)
}

func (r *Runner) catchTask(ctx context.Context, arn string, st *asl.State, input string, catcher asl.Catcher, failure *taskFailure, ctxObj any) (string, string, bool, bool) {
	doc := errDoc{Error: failure.Name, Cause: failure.Cause}
	text, err := marshalDoc(doc)
	if err != nil {
		r.failCtx(ctx, arn, "States.Runtime", err.Error())
		return "", "", false, true
	}
	injected, injVal, err := inject(catcher.ResultPath, input, text, doc)
	if err != nil {
		r.failEval(ctx, arn, err)
		return "", "", false, true
	}
	output, _, err := project(st.OutputPath, injected, injVal, ctxObj)
	if err != nil {
		r.failEval(ctx, arn, err)
		return "", "", false, true
	}
	if !r.exit(ctx, arn, st, output) {
		return "", "", false, true
	}
	if catcher.Next == "" {
		return "", output, true, false
	}
	return catcher.Next, output, false, false
}

func (r *Runner) retrySleep(ctx context.Context, arn string, rt asl.Retrier, n int) bool {
	if err := r.sleep(ctx, arn, r.retryDelay(rt, n)); err != nil {
		if ctx.Err() != nil && r.halt(ctx, arn) {
			return false
		}
		if errors.Is(err, errDeadline) || r.pastDeadline(arn) {
			r.finish(arn, store.ExecTimedOut, "ExecutionTimedOut", "", false, "States.Timeout", "", true, false)
			return false
		}
		if r.halt(ctx, arn) {
			return false
		}
		r.failCtx(ctx, arn, "States.Runtime", err.Error())
		return false
	}
	if r.halt(ctx, arn) {
		return false
	}
	if r.pastDeadline(arn) {
		r.finish(arn, store.ExecTimedOut, "ExecutionTimedOut", "", false, "States.Timeout", "", true, false)
		return false
	}
	return true
}

func (r *Runner) retryDelay(rt asl.Retrier, n int) time.Duration {
	rate := rt.BackoffRate
	if rate < 1 {
		rate = 2
	}
	raw := float64(rt.IntervalSeconds) * math.Pow(rate, float64(n-1))
	if math.IsNaN(raw) || raw < 0 {
		raw = 0
	}
	candidate := int64(math.Floor(raw))
	if candidate > maxRetryDelay {
		candidate = maxRetryDelay
	}
	if rt.MaxDelaySeconds > 0 && candidate > rt.MaxDelaySeconds {
		candidate = rt.MaxDelaySeconds
	}
	if rt.JitterStrategy == "FULL" {
		candidate = r.fullJitter(candidate)
	}
	return time.Duration(candidate) * time.Second
}

func (r *Runner) fullJitter(max int64) int64 {
	if max <= 0 {
		return 0
	}
	var v int64
	if r.Rand != nil {
		v = r.Rand(max)
	} else {
		v = rand.Int64N(max + 1)
	}
	if v < 0 {
		return 0
	}
	if v > max {
		return max
	}
	return v
}

func (r *Runner) takeAttempt(arn, state string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.attempts == nil {
		r.attempts = map[string]map[string]int{}
	}
	per := r.attempts[arn]
	if per == nil {
		per = map[string]int{}
		r.attempts[arn] = per
	}
	n := per[state]
	per[state] = n + 1
	return n
}

func (r *Runner) clearAttempts(arn string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.attempts, arn)
}

func (r *Runner) emitScheduled(arn string, meta taskMeta) bool {
	ev := store.HistoryEvent{Resource: meta.resource, HasResource: meta.resource != ""}
	if meta.kind == kindLambda {
		ev.Type = "LambdaFunctionScheduled"
		ev.Input = meta.input
		ev.HasInput = true
	} else {
		ev.Type = "TaskScheduled"
		ev.ResourceType = meta.resourceType
		ev.HasResourceType = meta.resourceType != ""
		ev.Region = meta.region
		ev.HasRegion = meta.region != ""
		ev.Parameters = meta.input
		ev.HasParameters = true
		if meta.heartbeat != nil {
			ev.HeartbeatSeconds = *meta.heartbeat
			ev.HasHeartbeat = true
		}
	}
	if meta.timeout != nil {
		ev.TimeoutSeconds = *meta.timeout
		ev.HasTimeout = true
	}
	return r.emit(arn, ev)
}

func (r *Runner) emitStarted(arn string, meta taskMeta) bool {
	ev := store.HistoryEvent{}
	if meta.kind == kindLambda {
		ev.Type = "LambdaFunctionStarted"
	} else {
		ev.Type = "TaskStarted"
		ev.Resource = meta.resource
		ev.HasResource = meta.resource != ""
		ev.ResourceType = meta.resourceType
		ev.HasResourceType = meta.resourceType != ""
	}
	return r.emit(arn, ev)
}

func (r *Runner) emitAttemptEnd(arn string, meta taskMeta, result string, tf *taskFailure) bool {
	ev := store.HistoryEvent{}
	if tf == nil {
		if meta.kind == kindLambda {
			ev.Type = "LambdaFunctionSucceeded"
		} else {
			ev.Type = "TaskSucceeded"
			ev.Resource = meta.resource
			ev.HasResource = meta.resource != ""
			ev.ResourceType = meta.resourceType
			ev.HasResourceType = meta.resourceType != ""
		}
		ev.Output = result
		ev.HasOutput = true
		return r.emit(arn, ev)
	}
	if meta.kind == kindLambda {
		if tf.Timeout {
			ev.Type = "LambdaFunctionTimedOut"
		} else {
			ev.Type = "LambdaFunctionFailed"
		}
	} else {
		ev.Resource = meta.resource
		ev.HasResource = meta.resource != ""
		ev.ResourceType = meta.resourceType
		ev.HasResourceType = meta.resourceType != ""
		if tf.Timeout {
			ev.Type = "TaskTimedOut"
		} else {
			ev.Type = "TaskFailed"
		}
	}
	ev.Error = tf.Name
	ev.HasError = tf.Name != ""
	ev.Cause = tf.Cause
	ev.HasCause = tf.Cause != ""
	return r.emit(arn, ev)
}

func (r *Runner) emit(arn string, ev store.HistoryEvent) bool {
	ev.Time = r.now()
	if err := r.append(arn, ev, store.ExecRunning, ""); err != nil {
		r.onAppendErr(arn, err)
		return false
	}
	return true
}

func (r *Runner) invokeLambda(callCtx, parent context.Context, target lambdaTarget, payload string, taskWins bool) (string, *taskFailure) {
	call := LambdaCall{Function: target.name, Qualifier: target.qualifier, Payload: payload, Endpoint: r.LambdaEndpoint}
	var reply LambdaReply
	if r.Invoke != nil {
		reply = r.Invoke(callCtx, call)
	} else {
		reply = httpInvoke(callCtx, call)
	}
	if reply.Err != nil {
		if parent.Err() != nil {
			return "", &taskFailure{Halt: true}
		}
		if errors.Is(reply.Err, context.DeadlineExceeded) || errors.Is(callCtx.Err(), context.DeadlineExceeded) {
			if taskWins {
				return "", &taskFailure{Name: "States.Timeout", Timeout: true}
			}
			return "", &taskFailure{ExecDeadline: true}
		}
		return "", &taskFailure{Name: "Lambda.ServiceException", Cause: reply.Err.Error()}
	}
	if reply.Status < 200 || reply.Status > 299 {
		return "", &taskFailure{Name: "Lambda.ServiceException", Cause: "Lambda invoke failed with status " + itoa(reply.Status)}
	}
	if reply.FunctionError != "" {
		return "", functionErrorFailure(reply.Payload)
	}
	if len(reply.Payload) > maxPayload {
		return "", &taskFailure{Name: "States.DataLimitExceeded", Cause: "The Lambda payload exceeded the maximum size of 262144 bytes."}
	}
	parsed, err := parseJSON(reply.Payload)
	if err != nil {
		return "", &taskFailure{Name: "States.Runtime", Cause: "Lambda payload is not JSON"}
	}
	text, err := compactValue(parsed)
	if err != nil {
		return "", &taskFailure{Name: "States.Runtime", Cause: err.Error()}
	}
	return text, nil
}

func httpInvoke(ctx context.Context, call LambdaCall) LambdaReply {
	endpoint := strings.TrimRight(call.Endpoint, "/")
	u := endpoint + "/2015-03-31/functions/" + url.PathEscape(call.Function) + "/invocations"
	if call.Qualifier != "" {
		u += "?Qualifier=" + url.QueryEscape(call.Qualifier)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, strings.NewReader(call.Payload))
	if err != nil {
		return LambdaReply{Err: err}
	}
	req.Header.Set("X-Amz-Invocation-Type", "RequestResponse")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return LambdaReply{Err: err}
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxPayload+1))
	if err != nil {
		return LambdaReply{Err: err}
	}
	return LambdaReply{Status: resp.StatusCode, Payload: string(body), FunctionError: resp.Header.Get("X-Amz-Function-Error")}
}

func (r *Runner) readMock(arn, machine, test, state string, kind taskKind) (string, *taskFailure) {
	if m, ok := r.stateMock(arn); ok {
		// Advance the attempt counter. The document is the same on every attempt.
		r.takeAttempt(arn, state)
		if m.HasThrow {
			if m.Error == "" {
				return "", &taskFailure{Name: "States.Runtime", Cause: "mock Throw is missing Error"}
			}
			return "", throwFailure(mock.Attempt{Error: m.Error, Cause: m.Cause, HasThrow: true})
		}
		if m.HasResult {
			text, err := compactRaw(m.Result)
			if err != nil {
				return "", asTaskFailure(err)
			}
			return text, nil
		}
		return "", &taskFailure{Name: "States.Runtime", Cause: "mock response is missing Return or Throw"}
	}
	n := r.takeAttempt(arn, state)
	att, err := r.Mocks.Lookup(machine, test, state, n)
	if err != nil {
		if errors.Is(err, mock.StateNotMocked) {
			return "", &taskFailure{Name: "States.Runtime", Cause: "unsupported resource: state " + state + " lost its mock"}
		}
		return "", asTaskFailure(err)
	}
	if att.HasThrow {
		return "", throwFailure(att)
	}
	if kind == kindLambda {
		return interpretLambdaReturn(att.Return)
	}
	text, err := compactRaw(att.Return)
	if err != nil {
		return "", asTaskFailure(err)
	}
	return text, nil
}

func throwFailure(att mock.Attempt) *taskFailure {
	name := att.Error
	timeout := strings.Contains(name, "timeout")
	if timeout {
		name = "States.Timeout"
	}
	return &taskFailure{Name: name, Cause: att.Cause, Timeout: timeout}
}

func interpretLambdaReturn(raw json.RawMessage) (string, *taskFailure) {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		text, err := compactRaw(raw)
		if err != nil {
			return "", asTaskFailure(err)
		}
		return text, nil
	}
	if functionErrorSet(obj["FunctionError"]) {
		return "", functionErrorPayload(obj["Payload"])
	}
	if payload, ok := obj["Payload"]; ok {
		text, err := compactRaw(payload)
		if err != nil {
			return "", asTaskFailure(err)
		}
		if len(text) > maxPayload {
			return "", &taskFailure{Name: "States.DataLimitExceeded", Cause: "The Lambda payload exceeded the maximum size of 262144 bytes."}
		}
		return text, nil
	}
	text, err := compactRaw(raw)
	if err != nil {
		return "", asTaskFailure(err)
	}
	return text, nil
}

func functionErrorSet(raw json.RawMessage) bool {
	if len(raw) == 0 {
		return false
	}
	s := strings.TrimSpace(string(raw))
	return s != "" && s != "null" && s != "false" && s != `""`
}

func functionErrorFailure(payload string) *taskFailure {
	return functionErrorPayload(json.RawMessage(payload))
}

func functionErrorPayload(payload json.RawMessage) *taskFailure {
	name := "Lambda.Unknown"
	cause := ""
	if len(payload) > 0 && string(payload) != "null" {
		var body struct {
			ErrorType    string `json:"errorType"`
			ErrorMessage string `json:"errorMessage"`
		}
		if err := json.Unmarshal(payload, &body); err == nil {
			if body.ErrorType != "" {
				name = body.ErrorType
			}
			cause = body.ErrorMessage
		} else {
			cause = string(payload)
		}
	}
	if strings.Contains(name, "timeout") {
		return &taskFailure{Name: "States.Timeout", Cause: cause, Timeout: true}
	}
	return &taskFailure{Name: name, Cause: cause}
}

func asTaskFailure(err error) *taskFailure {
	if err == nil {
		return nil
	}
	var ev *jsonpath.EvalError
	if errors.As(err, &ev) && ev != nil {
		return &taskFailure{Name: ev.Name, Cause: ev.Cause, Timeout: ev.Name == "States.Timeout"}
	}
	return &taskFailure{Name: "States.Runtime", Cause: err.Error()}
}

func timeoutSeconds(st *asl.State, root, ctx any) (int64, bool, error) {
	if st.TimeoutSeconds != nil {
		return *st.TimeoutSeconds, true, nil
	}
	if st.TimeoutSecondsPath == "" {
		return 0, false, nil
	}
	val, found, err := jsonpath.Get(st.TimeoutSecondsPath, root, ctx)
	if err != nil {
		return 0, false, err
	}
	if !found {
		return 0, false, jsonpath.Runtime("path " + st.TimeoutSecondsPath + " was not found")
	}
	n, ok := asFloat(val)
	if !ok {
		return 0, false, jsonpath.Runtime("TimeoutSecondsPath must yield a number")
	}
	sec := int64(n)
	if sec < 0 {
		sec = 0
	}
	return sec, true, nil
}

func pickRetry(list []asl.Retrier, name string) (asl.Retrier, bool) {
	for _, rt := range list {
		if matchesError(rt.ErrorEquals, name) {
			return rt, true
		}
	}
	return asl.Retrier{}, false
}

func pickCatch(list []asl.Catcher, name string) (asl.Catcher, bool) {
	for _, c := range list {
		if matchesError(c.ErrorEquals, name) {
			return c, true
		}
	}
	return asl.Catcher{}, false
}

func matchesError(equals []string, name string) bool {
	for _, item := range equals {
		switch item {
		case "States.ALL":
			return true
		case "States.TaskFailed":
			if name != "States.Timeout" {
				return true
			}
		default:
			if item == name {
				return true
			}
		}
	}
	return false
}

func isActivityARN(resource string) bool {
	parts := strings.Split(resource, ":")
	return len(parts) >= 7 && parts[0] == "arn" && parts[1] == "aws" && parts[2] == "states" && parts[5] == "activity" && parts[6] != ""
}

func parseLambdaARN(resource, region, account string) (lambdaTarget, bool) {
	parts := strings.Split(resource, ":")
	if len(parts) < 7 || parts[0] != "arn" || parts[1] != "aws" || parts[2] != "lambda" || parts[5] != "function" || parts[6] == "" {
		return lambdaTarget{}, false
	}
	if parts[3] == "" {
		parts[3] = region
	}
	if parts[4] == "" {
		parts[4] = account
	}
	target := lambdaTarget{name: parts[6]}
	if len(parts) >= 8 {
		target.qualifier = parts[7]
	}
	if parts[3] == "" || parts[4] == "" {
		return lambdaTarget{}, false
	}
	return target, true
}

func resourceTypeOf(resource string) string {
	parts := strings.Split(resource, ":")
	if len(parts) >= 3 && parts[0] == "arn" && parts[2] != "" {
		return parts[2]
	}
	return "states"
}

func evalSelector(raw []byte, root, ctx any) (any, string, error) {
	parsed, err := parseJSON(string(raw))
	if err != nil {
		return nil, "", err
	}
	out, err := jsonpath.Apply(parsed, root, ctx)
	if err != nil {
		return nil, "", err
	}
	text, err := compactValue(out)
	if err != nil {
		return nil, "", err
	}
	if len(text) > maxPayload {
		return nil, "", jsonpath.DataLimit("The ResultSelector payload exceeded the maximum size of 262144 bytes.")
	}
	return out, text, nil
}

func marshalDoc(doc errDoc) (string, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(doc); err != nil {
		return "", err
	}
	return string(bytes.TrimSpace(buf.Bytes())), nil
}

func itoa(n int) string {
	return strconv.Itoa(n)
}
