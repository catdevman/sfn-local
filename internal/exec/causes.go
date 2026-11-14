package exec

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/catdevman/sfn-local/internal/jsonata"
	"github.com/catdevman/sfn-local/internal/jsonpath"
	"github.com/catdevman/sfn-local/internal/store"
)

// runningState is the state a step is running, for the causes AWS writes:
// "An error occurred while executing the state 'S' (entered at the event id #N)."
type runningState struct {
	arn, name string
	entered   int64
}

type runningKey struct{}

func withRunning(ctx context.Context, rs *runningState) context.Context {
	return context.WithValue(ctx, runningKey{}, rs)
}

func runningOf(ctx context.Context) *runningState {
	if ctx == nil {
		return nil
	}
	rs, _ := ctx.Value(runningKey{}).(*runningState)
	return rs
}

// statePrefix starts most state failure causes. It is empty outside a step.
func statePrefix(ctx context.Context) string {
	rs := runningOf(ctx)
	if rs == nil {
		return ""
	}
	return fmt.Sprintf("An error occurred while executing the state '%s' (entered at the event id #%d). ", rs.name, rs.entered)
}

// pathCause words a JSONPath failure as AWS does. A missing path inside a
// template names the template key and the input. A missing path anywhere
// else is AWS's JSONPath library message.
func pathCause(ctx context.Context, ev *jsonpath.EvalError) string {
	cause := ev.Cause
	switch {
	case ev.Missing != "" && ev.Field != "":
		input, _ := compactValue(ev.Input)
		cause = fmt.Sprintf("The JSONPath '%s' specified for the field '%s' could not be found in the input '%s'", ev.Missing, ev.Field, input)
	case ev.Missing != "":
		cause = fmt.Sprintf("Invalid path '%s' : No results for path: %s", ev.Missing, jsonpath.Bracketed(ev.Missing))
	}
	if ev.Stated || ev.Missing != "" {
		cause = statePrefix(ctx) + cause
	}
	return cause
}

const noTransition = "Failed to transition out of the state. The state does not point to a next state."

// stated is a States.Runtime failure whose cause gets the state prefix.
func stated(cause string) error {
	return &jsonpath.EvalError{Name: "States.Runtime", Cause: cause, Stated: true}
}

// queryFailed words a JSONata failure in field as AWS does and records the
// EvaluationFailed event that precedes the failure or the Catch.
func (r *Runner) queryFailed(ctx context.Context, field, expr string, err error) error {
	var qe *jsonata.EvalError
	if !errors.As(err, &qe) {
		return err
	}
	detail := "The JSONata expression '" + strings.TrimSpace(expr) + "' specified for the field '" + field + "' "
	if qe.Undefined {
		detail += "returned nothing (undefined)."
	} else {
		detail += "threw an error during evaluation. " + qe.Cause
	}
	return r.evaluationFailed(ctx, "States.QueryEvaluationError", field, statePrefix(ctx)+detail)
}

// queryType is a JSONata result of the wrong JSON type.
func (r *Runner) queryType(ctx context.Context, field, expr, want string, got any) error {
	text, _ := compactValue(got)
	detail := fmt.Sprintf("The JSONata expression '%s' specified for the field '%s' returned an unexpected result type. Expected '%s', but was '%s' for value: %s",
		strings.TrimSpace(expr), field, want, jsonType(got), text)
	return r.evaluationFailed(ctx, "States.QueryEvaluationError", field, statePrefix(ctx)+detail)
}

// outputTooLarge is a JSONata state whose Output passes the payload limit.
func (r *Runner) outputTooLarge(ctx context.Context, state string) error {
	return r.evaluationFailed(ctx, "States.DataLimitExceeded", "Output", dataLimitCause(state))
}

func dataLimitCause(state string) string {
	return "The state/task '" + state + "' returned a result with a size exceeding the maximum number of bytes service limit."
}

func (r *Runner) evaluationFailed(ctx context.Context, name, field, cause string) error {
	if rs := runningOf(ctx); rs != nil {
		ev := store.HistoryEvent{Type: "EvaluationFailed", Time: r.now(), Name: rs.name, Location: field,
			Error: name, HasError: true, Cause: cause, HasCause: true}
		stampParent(ctx, &ev)
		if err := r.Store.Append(rs.arn, ev); err != nil {
			r.onAppendErr(rs.arn, err)
		}
	}
	return &queryError{Name: name, Cause: cause, recorded: true}
}

func jsonType(v any) string {
	switch v.(type) {
	case nil:
		return "null"
	case string:
		return "string"
	case bool:
		return "boolean"
	case []any:
		return "array"
	case map[string]any:
		return "object"
	default:
		return "number"
	}
}
