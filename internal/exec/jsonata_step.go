package exec

import (
	"bytes"
	"context"
	"errors"
	"math"
	"strings"
	"time"

	"github.com/catdevman/sfn-local/internal/asl"
	"github.com/catdevman/sfn-local/internal/jsonata"
	"github.com/catdevman/sfn-local/internal/jsonpath"
	"github.com/catdevman/sfn-local/internal/store"
)

var jsonataEval = jsonata.New()

var errNoChoice = errors.New("no choice")

func jsonataBody(s string) (string, bool) {
	if len(s) < 4 || !strings.HasPrefix(s, "{%") || !strings.HasSuffix(s, "%}") {
		return "", false
	}
	return s[2 : len(s)-2], true
}

func (r *Runner) stepJSONata(ctx context.Context, arn string, st *asl.State, input string) (string, string, bool, bool) {
	ex, ok := r.Store.Execution(arn)
	if !ok {
		return "", "", false, true
	}
	inputVal, err := parseJSON(input)
	if err != nil {
		r.failEval(ctx, arn, err)
		return "", "", false, true
	}
	now := r.now()
	ctxObj := r.jsonataContext(ctx, ex, st, now, 0)
	vars := entryVars(ctx)
	bind := r.bindings(inputVal, ctxObj, vars)

	effective := input
	effVal := inputVal
	if st.HasArguments {
		val, text, res, handled := r.evalArguments(ctx, arn, st, input, inputVal, bind, ctxObj)
		if handled {
			return res.next, res.output, res.terminal, res.done
		}
		effective = text
		effVal = val
	}

	switch st.Type {
	case "Fail":
		r.failJSONata(ctx, arn, st, inputVal, bind)
		return "", "", false, true
	case "Choice":
		return r.choiceJSONata(ctx, arn, st, input, inputVal, ctxObj, vars, bind)
	case "Wait":
		return r.waitJSONata(ctx, arn, st, input, inputVal, ctxObj, vars, bind, now)
	case "Pass", "Succeed":
		text, err := r.commitJSONata(ctx, inputVal, ctxObj, vars, nil, false, nil, st.Output, st.HasOutput, st.Assign, st)
		if err != nil {
			return r.failQuery(ctx, arn, st, input, inputVal, ctxObj, err)
		}
		if !r.exit(ctx, arn, st, text) {
			return "", "", false, true
		}
		return transition(st, text)
	case "Task":
		return r.runTask(ctx, arn, st, input, effective, effVal, ctxObj)
	case "Parallel":
		return r.runParallel(ctx, arn, st, input, effective, ctxObj)
	case "Map":
		return r.runMap(ctx, arn, st, input, effective, effVal, ctxObj)
	default:
		r.failCtx(ctx, arn, "States.Runtime", "State type "+st.Type+" is not implemented by this local server.")
		return "", "", false, true
	}
}

type carried struct {
	next, output   string
	terminal, done bool
}

func (r *Runner) evalArguments(ctx context.Context, arn string, st *asl.State, input string, inputVal any, bind map[string]any, ctxObj any) (any, string, carried, bool) {
	retries := 0
	for {
		val, err := r.evalRaw(ctx, st.Arguments, inputVal, bind)
		if err == nil {
			text, cerr := compactValue(val)
			if cerr != nil {
				err = cerr
			} else if len(text) > maxPayload {
				err = jsonpath.DataLimit("The Arguments payload exceeded the maximum size of 262144 bytes.")
			} else {
				return val, text, carried{}, false
			}
		}
		tf := taskFailureFrom(err)
		if rt, ok := pickRetry(st.Retry, tf.Name); ok && int64(retries+1) <= rt.MaxAttempts {
			if !r.retrySleep(ctx, arn, rt, retries+1) {
				return nil, "", carried{done: true}, true
			}
			retries++
			setRetryCount(ctxObj, retries)
			continue
		}
		if catcher, ok := pickCatch(st.Catch, tf.Name); ok {
			next, output, terminal, done := r.finishCatchJSONata(ctx, arn, st, input, catcher, tf, inputVal, ctxObj)
			return nil, "", carried{next: next, output: output, terminal: terminal, done: done}, true
		}
		r.failCtx(ctx, arn, tf.Name, tf.Cause)
		return nil, "", carried{done: true}, true
	}
}

func (r *Runner) choiceJSONata(ctx context.Context, arn string, st *asl.State, input string, inputVal, ctxObj any, vars, bind map[string]any) (string, string, bool, bool) {
	chosen, assignRaw, matched, err := r.matchJSONata(ctx, st, inputVal, bind)
	if errors.Is(err, errNoChoice) {
		r.failCtx(ctx, arn, "States.Runtime", "Failed to transition out of the state")
		return "", "", false, true
	}
	if err != nil {
		return r.failQuery(ctx, arn, st, input, inputVal, ctxObj, err)
	}
	raw := assignRaw
	if !matched && st.HasAssign {
		raw = st.Assign
	}
	if matched {
		// A matched rule applies only its own Assign, even when that Assign is absent.
		raw = assignRaw
	}
	text, err := r.commitJSONata(ctx, inputVal, ctxObj, vars, nil, false, nil, st.Output, st.HasOutput, raw, st)
	if err != nil {
		return r.failQuery(ctx, arn, st, input, inputVal, ctxObj, err)
	}
	if !r.exit(ctx, arn, st, text) {
		return "", "", false, true
	}
	return chosen, text, false, false
}

func (r *Runner) matchJSONata(ctx context.Context, st *asl.State, input any, bind map[string]any) (string, []byte, bool, error) {
	rules, err := decodeRuleList(st.Choices)
	if err != nil {
		return "", nil, false, err
	}
	for _, rule := range rules {
		body, ok := jsonataBody(rule.Condition)
		if !ok {
			body = strings.TrimSpace(rule.Condition)
		}
		val, err := r.evalExpr(ctx, body, input, bind)
		if err != nil {
			return "", nil, false, err
		}
		flag, ok := val.(bool)
		if !ok {
			return "", nil, false, &queryError{Cause: "Choice Condition must be a boolean"}
		}
		if flag {
			return rule.Next, []byte(rule.Assign), true, nil
		}
	}
	if st.HasDefault {
		return st.Default, nil, false, nil
	}
	return "", nil, false, errNoChoice
}

func (r *Runner) waitJSONata(ctx context.Context, arn string, st *asl.State, input string, inputVal, ctxObj any, vars, bind map[string]any, now time.Time) (string, string, bool, bool) {
	ms, err := r.waitDelay(ctx, st, inputVal, bind, now)
	if err != nil {
		return r.failQuery(ctx, arn, st, input, inputVal, ctxObj, err)
	}
	delay := scaleWait(ms, r.WaitScale)
	if err := r.sleep(ctx, arn, time.Duration(delay)*time.Millisecond); err != nil {
		if ctx.Err() != nil && r.halt(ctx, arn) {
			return "", "", false, true
		}
		if errors.Is(err, errDeadline) || r.pastDeadline(arn) {
			r.finish(arn, store.ExecTimedOut, "ExecutionTimedOut", "", false, "States.Timeout", "", true, false)
			return "", "", false, true
		}
		if r.halt(ctx, arn) {
			return "", "", false, true
		}
		return r.failQuery(ctx, arn, st, input, inputVal, ctxObj, err)
	}
	if r.halt(ctx, arn) || r.pastDeadline(arn) {
		if r.Store.Running(arn) && r.pastDeadline(arn) {
			r.finish(arn, store.ExecTimedOut, "ExecutionTimedOut", "", false, "States.Timeout", "", true, false)
		}
		return "", "", false, true
	}
	text, err := r.commitJSONata(ctx, inputVal, ctxObj, vars, nil, false, nil, st.Output, st.HasOutput, st.Assign, st)
	if err != nil {
		return r.failQuery(ctx, arn, st, input, inputVal, ctxObj, err)
	}
	if !r.exit(ctx, arn, st, text) {
		return "", "", false, true
	}
	return transition(st, text)
}

func (r *Runner) waitDelay(ctx context.Context, st *asl.State, input any, bind map[string]any, now time.Time) (int64, error) {
	switch {
	case st.SecondsExpr != "":
		val, err := r.evalExpr(ctx, st.SecondsExpr, input, bind)
		if err != nil {
			return 0, err
		}
		sec, err := floored(val, "Seconds")
		if err != nil {
			return 0, err
		}
		return sec * 1000, nil
	case st.Seconds != nil:
		return *st.Seconds * 1000, nil
	case st.TimestampExpr != "":
		val, err := r.evalExpr(ctx, st.TimestampExpr, input, bind)
		if err != nil {
			return 0, err
		}
		text, ok := val.(string)
		if !ok {
			return 0, &queryError{Cause: "Timestamp must be a string"}
		}
		when, ok := parseOffsetTime(text)
		if !ok {
			return 0, &queryError{Cause: "Timestamp must be an ISO-8601 offset date-time"}
		}
		return delayUntil(when, now), nil
	case st.HasTimestamp:
		when, ok := parseOffsetTime(st.Timestamp)
		if !ok {
			return 0, jsonpath.Runtime("Timestamp must be an ISO-8601 offset date-time")
		}
		return delayUntil(when, now), nil
	default:
		return 0, &queryError{Cause: "Wait has no time field"}
	}
}

func (r *Runner) failJSONata(ctx context.Context, arn string, st *asl.State, input any, bind map[string]any) {
	errName, hasErr, err := r.resolveJSONataText(ctx, st.ErrorExpr, st.HasError, st.Error, input, bind, "Error")
	if err != nil {
		r.failEval(ctx, arn, err)
		return
	}
	cause, hasCause, err := r.resolveJSONataText(ctx, st.CauseExpr, st.HasCause, st.Cause, input, bind, "Cause")
	if err != nil {
		r.failEval(ctx, arn, err)
		return
	}
	if hasErr {
		errName = clip(errName, 256)
	}
	if hasCause {
		cause = clip(cause, 32768)
	}
	if nestOf(ctx) != nil {
		if !hasErr {
			errName = "States.Runtime"
		}
		if !hasErr && !hasCause {
			cause = "Fail state"
		}
		r.failCtx(ctx, arn, errName, cause)
		return
	}
	r.finish(arn, store.ExecFailed, "ExecutionFailed", "", false, errName, cause, hasErr, hasCause)
}

func (r *Runner) resolveJSONataText(ctx context.Context, expr string, hasLit bool, lit string, input any, bind map[string]any, label string) (string, bool, error) {
	if expr != "" {
		val, err := r.evalExpr(ctx, expr, input, bind)
		if err != nil {
			return "", false, err
		}
		text, ok := val.(string)
		if !ok {
			return "", false, &queryError{Cause: label + " must yield a string"}
		}
		return text, true, nil
	}
	if hasLit {
		return lit, true, nil
	}
	return "", false, nil
}

func (r *Runner) finishCatchJSONata(ctx context.Context, arn string, st *asl.State, input string, catcher asl.Catcher, tf *taskFailure, inputVal, ctxObj any) (string, string, bool, bool) {
	if tf == nil {
		tf = &taskFailure{Name: "States.Runtime"}
	}
	errDoc := map[string]any{"Error": tf.Name, "Cause": tf.Cause}
	text, err := r.commitJSONata(ctx, inputVal, ctxObj, entryVars(ctx), nil, false, errDoc, catcher.Output, catcher.HasOutput, catcher.Assign, st)
	if err != nil {
		r.failEval(ctx, arn, err)
		return "", "", false, true
	}
	if !r.exit(ctx, arn, st, text) {
		return "", "", false, true
	}
	if catcher.Next == "" {
		return "", text, true, false
	}
	return catcher.Next, text, false, false
}

func (r *Runner) failQuery(ctx context.Context, arn string, st *asl.State, input string, inputVal, ctxObj any, err error) (string, string, bool, bool) {
	tf := taskFailureFrom(err)
	if st.Type == "Task" || st.Type == "Parallel" || st.Type == "Map" {
		if catcher, ok := pickCatch(st.Catch, tf.Name); ok {
			return r.finishCatchJSONata(ctx, arn, st, input, catcher, tf, inputVal, ctxObj)
		}
	}
	r.failCtx(ctx, arn, tf.Name, tf.Cause)
	return "", "", false, true
}

func (r *Runner) commitJSONata(ctx context.Context, inputVal, ctxObj any, vars map[string]any, result any, hasResult bool, errDoc any, outputRaw []byte, hasOutput bool, assignRaw []byte, st *asl.State) (string, error) {
	bind := r.bindings(inputVal, ctxObj, vars)
	states, _ := bind["states"].(map[string]any)
	if states == nil {
		states = map[string]any{"input": inputVal, "context": ctxObj}
		bind["states"] = states
	}
	if hasResult {
		states["result"] = result
	}
	if errDoc != nil {
		states["errorOutput"] = errDoc
	}
	var output any
	if hasOutput {
		val, err := r.evalRaw(ctx, outputRaw, inputVal, bind)
		if err != nil {
			return "", err
		}
		output = val
	} else if errDoc != nil {
		output = errDoc
	} else if st != nil && (st.Type == "Task" || st.Type == "Map" || st.Type == "Parallel") {
		output = result
	} else {
		output = inputVal
	}
	var assigns map[string]any
	if len(bytes.TrimSpace(assignRaw)) > 0 {
		val, err := r.evalRaw(ctx, assignRaw, inputVal, bind)
		if err != nil {
			return "", err
		}
		obj, ok := val.(map[string]any)
		if !ok {
			return "", &queryError{Cause: "Assign must be an object"}
		}
		assigns = obj
	}
	text, err := compactValue(output)
	if err != nil {
		return "", err
	}
	if len(text) > maxPayload {
		return "", jsonpath.DataLimit("The state output exceeded the maximum size of 262144 bytes.")
	}
	if err := scopeOf(ctx).commit(assigns); err != nil {
		return "", err
	}
	return text, nil
}

func (r *Runner) bindings(input, ctxObj any, vars map[string]any) map[string]any {
	out := make(map[string]any, len(vars)+1)
	for key, val := range vars {
		out[key] = val
	}
	out["states"] = map[string]any{"input": input, "context": ctxObj}
	return out
}

func (r *Runner) jsonataContext(ctx context.Context, ex store.Execution, st *asl.State, now time.Time, retries int) map[string]any {
	obj := r.contextObject(ctx, ex, st, now, retries)
	exec, _ := obj["Execution"].(map[string]any)
	if exec != nil {
		exec["RedriveCount"] = int(ex.RedriveCount)
	}
	return obj
}

func (r *Runner) evalRaw(ctx context.Context, raw []byte, input any, bindings map[string]any) (any, error) {
	parsed, err := parseJSON(string(raw))
	if err != nil {
		return nil, err
	}
	return r.walkJSONata(ctx, parsed, input, bindings)
}

func (r *Runner) walkJSONata(ctx context.Context, v, input any, bindings map[string]any) (any, error) {
	switch node := v.(type) {
	case string:
		if body, ok := jsonataBody(node); ok {
			return r.evalExpr(ctx, body, input, bindings)
		}
		return node, nil
	case map[string]any:
		out := make(map[string]any, len(node))
		for key, child := range node {
			val, err := r.walkJSONata(ctx, child, input, bindings)
			if err != nil {
				return nil, err
			}
			out[key] = val
		}
		return out, nil
	case []any:
		out := make([]any, len(node))
		for i, child := range node {
			val, err := r.walkJSONata(ctx, child, input, bindings)
			if err != nil {
				return nil, err
			}
			out[i] = val
		}
		return out, nil
	default:
		return v, nil
	}
}

func (r *Runner) evalExpr(ctx context.Context, expr string, input any, bindings map[string]any) (any, error) {
	return jsonataEval.Eval(ctx, expr, input, bindings, jsonata.Limits{})
}

func (r *Runner) evalItems(ctx context.Context, st *asl.State, input any, ctxObj map[string]any, vars map[string]any) ([]any, error) {
	if !st.HasItems {
		arr, ok := input.([]any)
		if !ok {
			return nil, &queryError{Cause: "Items must be an array"}
		}
		return arr, nil
	}
	val, err := r.evalRaw(ctx, st.Items, input, r.bindings(input, ctxObj, vars))
	if err != nil {
		return nil, err
	}
	arr, ok := val.([]any)
	if !ok {
		return nil, &queryError{Cause: "Items must be an array"}
	}
	return arr, nil
}

func (r *Runner) evalMaxConcurrency(ctx context.Context, st *asl.State, input, ctxObj any, vars map[string]any) (*int64, error) {
	if st.MaxConcurrencyExpr == "" {
		return st.MaxConcurrency, nil
	}
	val, err := r.evalExpr(ctx, st.MaxConcurrencyExpr, input, r.bindings(input, ctxObj, vars))
	if err != nil {
		return nil, err
	}
	f, ok := asFloat(val)
	if !ok || math.IsNaN(f) || math.IsInf(f, 0) || f < 0 {
		return nil, &queryError{Cause: "MaxConcurrency must be a number >= 0"}
	}
	n := int64(math.Floor(f))
	return &n, nil
}

func floored(v any, label string) (int64, error) {
	f, ok := asFloat(v)
	if !ok || math.IsNaN(f) || math.IsInf(f, 0) {
		return 0, &queryError{Cause: label + " must be a number"}
	}
	sec := int64(math.Floor(f))
	if sec < 0 {
		sec = 0
	}
	return sec, nil
}

// catchQuery retries or catches a JSONata evaluation error. handled is false
// when err is not a query error. again tells the caller to run the attempt once more.
func (r *Runner) catchQuery(ctx context.Context, arn string, st *asl.State, input string, ctxObj any, err error, retries *int) (next, output string, terminal, done, again, handled bool) {
	if _, ok := queryCause(err); !ok {
		return "", "", false, false, false, false
	}
	tf := taskFailureFrom(err)
	if retries != nil {
		if rt, ok := pickRetry(st.Retry, tf.Name); ok && int64(*retries+1) <= rt.MaxAttempts {
			if !r.retrySleep(ctx, arn, rt, *retries+1) {
				return "", "", false, true, false, true
			}
			*retries++
			return "", "", false, false, true, true
		}
	}
	if catcher, ok := pickCatch(st.Catch, tf.Name); ok && (st.Type == "Task" || st.Type == "Parallel" || st.Type == "Map") {
		inputVal, perr := parseJSON(input)
		if perr != nil {
			r.failEval(ctx, arn, perr)
			return "", "", false, true, false, true
		}
		next, output, terminal, done = r.finishCatchJSONata(ctx, arn, st, input, catcher, tf, inputVal, ctxObj)
		return next, output, terminal, done, false, true
	}
	r.failCtx(ctx, arn, tf.Name, tf.Cause)
	return "", "", false, true, false, true
}

func taskFailureFrom(err error) *taskFailure {
	if err == nil {
		return &taskFailure{Name: "States.Runtime"}
	}
	if cause, ok := queryCause(err); ok {
		return &taskFailure{Name: "States.QueryEvaluationError", Cause: cause}
	}
	var ev *jsonpath.EvalError
	if errors.As(err, &ev) && ev != nil {
		return &taskFailure{Name: ev.Name, Cause: ev.Cause, Timeout: ev.Name == "States.Timeout"}
	}
	return &taskFailure{Name: "States.Runtime", Cause: err.Error()}
}

func queryCause(err error) (string, bool) {
	var qe *jsonata.EvalError
	if errors.As(err, &qe) && qe != nil {
		return qe.Cause, true
	}
	var q *queryError
	if errors.As(err, &q) && q != nil {
		return q.Cause, true
	}
	return "", false
}

func (r *Runner) applyJSONPathAssign(ctx context.Context, raw []byte, root, ctxObj any, vars map[string]any) error {
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil
	}
	parsed, err := parseJSON(string(raw))
	if err != nil {
		return err
	}
	out, err := jsonpath.ApplyBound(parsed, root, ctxObj, vars)
	if err != nil {
		return err
	}
	obj, ok := out.(map[string]any)
	if !ok {
		return &queryError{Cause: "Assign must be an object"}
	}
	return scopeOf(ctx).commit(obj)
}
