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
	if err := r.entered(arn, st, input); err != nil {
		r.onAppendErr(arn, err)
		return "", "", false, true
	}
	if st.HasAssign {
		r.fail(arn, "States.Runtime", "Assign is not implemented by this local server.")
		return "", "", false, true
	}
	ex, ok := r.Store.Execution(arn)
	if !ok {
		return "", "", false, true
	}
	now := r.now()
	root, err := parseJSON(input)
	if err != nil {
		r.failEval(arn, err)
		return "", "", false, true
	}
	ctxObj := r.contextObject(ex, st, now)
	effective, effVal, err := project(st.InputPath, input, root, ctxObj)
	if err != nil {
		r.failEval(arn, err)
		return "", "", false, true
	}
	if st.HasParameters {
		effVal, effective, err = evalParameters(st.Parameters, effVal, ctxObj)
		if err != nil {
			r.failEval(arn, err)
			return "", "", false, true
		}
	}
	switch st.Type {
	case "Fail":
		r.failState(arn, st, effVal, ctxObj)
		return "", "", false, true
	case "Choice":
		chosen, err := choose(st, effVal, ctxObj)
		if err != nil {
			r.failEval(arn, err)
			return "", "", false, true
		}
		output, _, err = project(st.OutputPath, effective, effVal, ctxObj)
		if err != nil {
			r.failEval(arn, err)
			return "", "", false, true
		}
		if !r.exit(arn, st, output) {
			return "", "", false, true
		}
		return chosen, output, false, false
	case "Wait":
		ms, err := waitMillis(st, effVal, ctxObj, now)
		if err != nil {
			r.failEval(arn, err)
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
			r.failEval(arn, err)
			return "", "", false, true
		}
		if r.halt(ctx, arn) || r.pastDeadline(arn) {
			if r.Store.Running(arn) && r.pastDeadline(arn) {
				r.finish(arn, store.ExecTimedOut, "ExecutionTimedOut", "", false, "States.Timeout", "", true, false)
			}
			return "", "", false, true
		}
		output, _, err = project(st.OutputPath, effective, effVal, ctxObj)
		if err != nil {
			r.failEval(arn, err)
			return "", "", false, true
		}
		if !r.exit(arn, st, output) {
			return "", "", false, true
		}
		return transition(st, output)
	case "Task":
		return r.runTask(ctx, arn, st, input, effective, effVal, ctxObj)
	case "Pass":
		resultText := effective
		resultVal := effVal
		if st.HasResult {
			resultText, err = compactRaw(st.Result)
			if err != nil {
				r.failEval(arn, err)
				return "", "", false, true
			}
			resultVal, err = parseJSON(resultText)
			if err != nil {
				r.failEval(arn, err)
				return "", "", false, true
			}
		}
		injected, injVal, err := inject(st.ResultPath, input, resultText, resultVal)
		if err != nil {
			r.failEval(arn, err)
			return "", "", false, true
		}
		output, _, err = project(st.OutputPath, injected, injVal, ctxObj)
		if err != nil {
			r.failEval(arn, err)
			return "", "", false, true
		}
	case "Succeed":
		output, _, err = project(st.OutputPath, effective, effVal, ctxObj)
		if err != nil {
			r.failEval(arn, err)
			return "", "", false, true
		}
	default:
		r.fail(arn, "States.Runtime", "State type "+st.Type+" is not implemented by this local server.")
		return "", "", false, true
	}
	if !r.exit(arn, st, output) {
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

func (r *Runner) exit(arn string, st *asl.State, output string) bool {
	if len(output) > maxPayload {
		r.fail(arn, "States.DataLimitExceeded", "The state output exceeded the maximum size of 262144 bytes.")
		return false
	}
	if err := r.exited(arn, st, output); err != nil {
		r.onAppendErr(arn, err)
		return false
	}
	return true
}

func (r *Runner) failState(arn string, st *asl.State, effective, ctx any) {
	errName, hasErr, err := resolveText(st.HasErrorPath, st.ErrorPath, st.HasError, st.Error, effective, ctx, "ErrorPath")
	if err != nil {
		r.failEval(arn, err)
		return
	}
	cause, hasCause, err := resolveText(st.HasCausePath, st.CausePath, st.HasCause, st.Cause, effective, ctx, "CausePath")
	if err != nil {
		r.failEval(arn, err)
		return
	}
	if hasErr {
		errName = clip(errName, 256)
	}
	if hasCause {
		cause = clip(cause, 32768)
	}
	r.finish(arn, store.ExecFailed, "ExecutionFailed", "", false, errName, cause, hasErr, hasCause)
}

func resolveText(hasPath bool, path string, hasLit bool, lit string, root, ctx any, label string) (string, bool, error) {
	if hasPath {
		val, found, err := jsonpath.Get(path, root, ctx)
		if err != nil {
			return "", false, err
		}
		if !found {
			return "", false, jsonpath.Runtime("path " + path + " was not found")
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

func project(path asl.Path, text string, val, ctx any) (string, any, error) {
	switch path.Kind {
	case asl.PathDefault, asl.PathIdentity:
		return text, val, nil
	case asl.PathNull:
		return "{}", map[string]any{}, nil
	default:
		got, found, err := jsonpath.Get(path.Text, val, ctx)
		if err != nil {
			return "", nil, err
		}
		if !found {
			return "", nil, jsonpath.Runtime("path " + path.Text + " was not found")
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

func evalParameters(raw []byte, root, ctx any) (any, string, error) {
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
		return nil, "", jsonpath.DataLimit("The Parameters payload exceeded the maximum size of 262144 bytes.")
	}
	return out, text, nil
}

func (r *Runner) contextObject(ex store.Execution, st *asl.State, now time.Time) map[string]any {
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
	return map[string]any{
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

func (r *Runner) failEval(arn string, err error) {
	name := "States.Runtime"
	cause := ""
	if err != nil {
		cause = err.Error()
	}
	var ev *jsonpath.EvalError
	if errors.As(err, &ev) && ev != nil {
		name = ev.Name
		cause = ev.Cause
	}
	r.fail(arn, name, cause)
}
