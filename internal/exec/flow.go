package exec

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/catdevman/sfn-local/internal/asl"
	"github.com/catdevman/sfn-local/internal/jsonpath"
	"github.com/catdevman/sfn-local/internal/store"
)

func (r *Runner) step(ctx context.Context, arn string, st *asl.State, input string) (next, output string, terminal, done bool) {
	ctx = r.ensureScope(ctx, arn)
	if err := r.entered(ctx, arn, st, input); err != nil {
		r.onAppendErr(arn, err)
		return "", "", false, true
	}
	ctx = withEntryVars(ctx, scopeOf(ctx).snapshot())
	if st.JSONata {
		return r.stepJSONata(ctx, arn, st, input)
	}
	ex, ok := r.Store.Execution(arn)
	if !ok {
		return "", "", false, true
	}
	now := r.now()
	root, err := parseJSON(input)
	if err != nil {
		r.failEval(ctx, arn, err)
		return "", "", false, true
	}
	ctxObj := r.contextObject(ctx, ex, st, now, 0)
	vars := entryVars(ctx)
	effective, effVal, err := project(st.InputPath, input, root, ctxObj, vars)
	if err != nil {
		r.failEval(ctx, arn, err)
		return "", "", false, true
	}
	// An unmocked activity or callback re-evaluates Parameters on each attempt
	// after the task token exists. Every other task evaluates them once here.
	if !(st.Type == "Task" && r.deferTaskParameters(ex, st)) {
		if st.Type == "Task" {
			if tok, ok := r.taskContextToken(ex, st); ok {
				ctxObj["Task"] = map[string]any{"Token": tok}
			}
		}
		if st.HasParameters && st.Type != "Map" {
			effVal, effective, err = evalParameters(st.Parameters, effVal, ctxObj, vars)
			if err != nil {
				r.failEval(ctx, arn, err)
				return "", "", false, true
			}
		}
	}
	switch st.Type {
	case "Fail":
		r.failState(ctx, arn, st, effVal, ctxObj)
		return "", "", false, true
	case "Choice":
		chosen, assignRaw, matched, err := choose(st, effVal, ctxObj, vars)
		if err != nil {
			r.failEval(ctx, arn, err)
			return "", "", false, true
		}
		output, _, err = project(st.OutputPath, effective, effVal, ctxObj, vars)
		if err != nil {
			r.failEval(ctx, arn, err)
			return "", "", false, true
		}
		raw := st.Assign
		if matched {
			raw = assignRaw
		}
		if !r.commitJSONPath(ctx, arn, st, output, raw, effVal, ctxObj, vars) {
			return "", "", false, true
		}
		return chosen, output, false, false
	case "Wait":
		ms, err := waitMillis(st, effVal, ctxObj, vars, now)
		if err != nil {
			r.failEval(ctx, arn, err)
			return "", "", false, true
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
			r.failEval(ctx, arn, err)
			return "", "", false, true
		}
		if r.halt(ctx, arn) || r.pastDeadline(arn) {
			if r.Store.Running(arn) && r.pastDeadline(arn) {
				r.finish(arn, store.ExecTimedOut, "ExecutionTimedOut", "", false, "States.Timeout", "", true, false)
			}
			return "", "", false, true
		}
		output, _, err = project(st.OutputPath, effective, effVal, ctxObj, vars)
		if err != nil {
			r.failEval(ctx, arn, err)
			return "", "", false, true
		}
		if !r.commitJSONPath(ctx, arn, st, output, st.Assign, effVal, ctxObj, vars) {
			return "", "", false, true
		}
		return transition(st, output)
	case "Task":
		return r.runTask(ctx, arn, st, input, effective, effVal, ctxObj)
	case "Parallel":
		return r.runParallel(ctx, arn, st, input, effective, ctxObj)
	case "Map":
		return r.runMap(ctx, arn, st, input, effective, effVal, ctxObj)
	case "Pass":
		resultText := effective
		resultVal := effVal
		if st.HasResult {
			resultText, err = compactRaw(st.Result)
			if err != nil {
				r.failEval(ctx, arn, err)
				return "", "", false, true
			}
			resultVal, err = parseJSON(resultText)
			if err != nil {
				r.failEval(ctx, arn, err)
				return "", "", false, true
			}
		}
		injected, injVal, err := inject(st.ResultPath, input, resultText, resultVal)
		if err != nil {
			r.failEval(ctx, arn, err)
			return "", "", false, true
		}
		output, _, err = project(st.OutputPath, injected, injVal, ctxObj, vars)
		if err != nil {
			r.failEval(ctx, arn, err)
			return "", "", false, true
		}
		if !r.commitJSONPath(ctx, arn, st, output, st.Assign, resultVal, ctxObj, vars) {
			return "", "", false, true
		}
		return transition(st, output)
	case "Succeed":
		output, _, err = project(st.OutputPath, effective, effVal, ctxObj, vars)
		if err != nil {
			r.failEval(ctx, arn, err)
			return "", "", false, true
		}
	default:
		r.failCtx(ctx, arn, "States.Runtime", "State type "+st.Type+" is not implemented by this local server.")
		return "", "", false, true
	}
	if !r.exit(ctx, arn, st, output) {
		return "", "", false, true
	}
	return transition(st, output)
}

func transition(st *asl.State, output string) (string, string, bool, bool) {
	if st.End || st.Next == "" {
		return "", output, true, false
	}
	return st.Next, output, false, false
}

func (r *Runner) exit(ctx context.Context, arn string, st *asl.State, output string) bool {
	if len(output) > maxPayload {
		r.failCtx(ctx, arn, "States.DataLimitExceeded", "The state output exceeded the maximum size of 262144 bytes.")
		return false
	}
	if err := r.exited(ctx, arn, st, output); err != nil {
		r.onAppendErr(arn, err)
		return false
	}
	return true
}

func (r *Runner) failState(ctx context.Context, arn string, st *asl.State, effective, ctxObj any) {
	vars := entryVars(ctx)
	errName, hasErr, err := resolveText(st.HasErrorPath, st.ErrorPath, st.HasError, st.Error, effective, ctxObj, vars, "ErrorPath")
	if err != nil {
		r.failEval(ctx, arn, err)
		return
	}
	cause, hasCause, err := resolveText(st.HasCausePath, st.CausePath, st.HasCause, st.Cause, effective, ctxObj, vars, "CausePath")
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

func setRetryCount(ctxObj any, n int) {
	obj, ok := ctxObj.(map[string]any)
	if !ok {
		return
	}
	state, ok := obj["State"].(map[string]any)
	if !ok {
		return
	}
	if _, ok := state["RetryCount"]; ok {
		state["RetryCount"] = n
	}
}

func resolveText(hasPath bool, path string, hasLit bool, lit string, root, ctx any, vars map[string]any, label string) (string, bool, error) {
	if hasPath {
		val, found, err := jsonpath.GetBound(path, root, ctx, vars)
		if err != nil {
			return "", false, err
		}
		if !found {
			return "", false, missingPath(path)
		}
		text, ok := val.(string)
		if !ok {
			return "", false, jsonpath.Runtime(label + " must yield a string")
		}
		return text, true, nil
	}
	if hasLit {
		return lit, true, nil
	}
	return "", false, nil
}

func project(path asl.Path, text string, val, ctx any, vars map[string]any) (string, any, error) {
	switch path.Kind {
	case asl.PathDefault, asl.PathIdentity:
		return text, val, nil
	case asl.PathNull:
		return "{}", map[string]any{}, nil
	default:
		got, found, err := jsonpath.GetBound(path.Text, val, ctx, vars)
		if err != nil {
			return "", nil, err
		}
		if !found {
			return "", nil, missingPath(path.Text)
		}
		out, err := compactValue(got)
		if err != nil {
			return "", nil, err
		}
		return out, got, nil
	}
}

func inject(path asl.Path, originalText, resultText string, result any) (string, any, error) {
	switch path.Kind {
	case asl.PathDefault, asl.PathIdentity:
		return resultText, result, nil
	case asl.PathNull:
		orig, err := parseJSON(originalText)
		if err != nil {
			return "", nil, err
		}
		return originalText, orig, nil
	default:
		orig, err := parseJSON(originalText)
		if err != nil {
			return "", nil, err
		}
		updated, err := jsonpath.Set(orig, path.Text, result)
		if err != nil {
			return "", nil, err
		}
		text, err := compactValue(updated)
		if err != nil {
			return "", nil, err
		}
		return text, updated, nil
	}
}

func evalParameters(raw []byte, root, ctx any, vars map[string]any) (any, string, error) {
	parsed, err := parseJSON(string(raw))
	if err != nil {
		return nil, "", err
	}
	out, err := jsonpath.ApplyBound(parsed, root, ctx, vars)
	if err != nil {
		return nil, "", err
	}
	text, err := compactValue(out)
	if err != nil {
		return nil, "", err
	}
	if len(text) > maxPayload {
		return nil, "", jsonpath.DataLimit("The Parameters payload exceeded the maximum size of 262144 bytes.")
	}
	return out, text, nil
}

func (r *Runner) contextObject(ctx context.Context, ex store.Execution, st *asl.State, now time.Time, retries int) map[string]any {
	input, err := parseJSON(ex.Input)
	if err != nil {
		input = map[string]any{}
	}
	var role any
	if ex.RoleArn != "" {
		role = ex.RoleArn
	}
	state := map[string]any{
		"EnteredTime": isoInstant(now),
		"Name":        st.Name,
	}
	if st.Type == "Task" || st.Type == "Parallel" || st.Type == "Map" {
		state["RetryCount"] = retries
	}
	obj := map[string]any{
		"Execution": map[string]any{
			"Id":        ex.ARN,
			"Input":     input,
			"Name":      ex.Name,
			"RoleArn":   role,
			"StartTime": isoInstant(ex.Started),
		},
		"State": state,
		"StateMachine": map[string]any{
			"Id":   ex.StateMachineARN,
			"Name": arnName(ex.StateMachineARN, ":stateMachine:"),
		},
	}
	if ctx != nil {
		if item, ok := ctx.Value(itemKey{}).(mapItem); ok {
			obj["Map"] = map[string]any{
				"Item": map[string]any{"Index": item.Index, "Value": item.Value},
			}
		}
	}
	return obj
}

// mockMachine is the machine name the mock file keys on. A Map Run child
// uses its parent's machine.
func mockMachine(ex store.Execution) string {
	return arnName(ex.Machine(), ":stateMachine:")
}

func arnName(arn, mark string) string {
	if i := strings.LastIndex(arn, mark); i >= 0 {
		return arn[i+len(mark):]
	}
	return ""
}

func isoInstant(t time.Time) string {
	if t.IsZero() {
		t = time.Unix(0, 0)
	}
	return t.UTC().Format("2006-01-02T15:04:05.000Z")
}

func missingPath(path string) error {
	if jsonpath.ReferencesVariable(path) {
		body := strings.TrimPrefix(path, "$")
		if !strings.ContainsAny(body, ".[") {
			return jsonpath.Runtime("variable $" + body + " was not found")
		}
	}
	return jsonpath.Runtime("path " + path + " was not found")
}

func namedFailure(err error) (string, string, bool) {
	if cause, ok := queryCause(err); ok {
		return "States.QueryEvaluationError", cause, true
	}
	var ev *jsonpath.EvalError
	if errors.As(err, &ev) && ev != nil {
		return ev.Name, ev.Cause, true
	}
	return "", "", false
}

func (r *Runner) failEval(ctx context.Context, arn string, err error) {
	name := "States.Runtime"
	cause := ""
	if err != nil {
		cause = err.Error()
	}
	if n, c, ok := namedFailure(err); ok {
		name, cause = n, c
	}
	r.failCtx(ctx, arn, name, cause)
}

// commitJSONPath checks the output size, writes Assign against root, then exits.
// root is the pre-assignment snapshot for this state. Output was already projected.
func (r *Runner) commitJSONPath(ctx context.Context, arn string, st *asl.State, output string, assign []byte, root, ctxObj any, vars map[string]any) bool {
	if len(output) > maxPayload {
		r.failCtx(ctx, arn, "States.DataLimitExceeded", "The state output exceeded the maximum size of 262144 bytes.")
		return false
	}
	if err := r.applyJSONPathAssign(ctx, assign, root, ctxObj, vars); err != nil {
		r.failEval(ctx, arn, err)
		return false
	}
	return r.exit(ctx, arn, st, output)
}
