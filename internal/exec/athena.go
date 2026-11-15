package exec

import (
	"context"
	"encoding/json"
	"time"
)

const athenaStopTimeout = 5 * time.Second

// invokeAthenaSync runs athena:startQueryExecution.sync. The
// StartQueryExecution reply is TaskSubmitted. GetQueryExecution is then
// read until the query is SUCCEEDED, FAILED, or CANCELLED. Success returns
// that GetQueryExecution reply. A failed or cancelled query fails with
// States.TaskFailed and the reply as the cause. When the execution stops or
// the task times out, the query is stopped, best effort.
func (r *Runner) invokeAthenaSync(callCtx, parent context.Context, arn string, meta taskMeta, taskWins bool) (string, *taskFailure) {
	const fallback = "Athena.AmazonAthenaException"
	out, err := r.SDK.CallIntegration(callCtx, athenaStartQuery, []byte(meta.input))
	if err != nil {
		tf := sdkFailure(err, parent, callCtx, taskWins, fallback)
		tf.Submit = tf.Name != "" && !tf.Timeout && !tf.Halt && !tf.ExecDeadline
		return "", tf
	}
	var started struct{ QueryExecutionId string }
	if err := json.Unmarshal([]byte(out), &started); err != nil || started.QueryExecutionId == "" {
		return "", &taskFailure{Name: "States.Runtime", Cause: "StartQueryExecution did not return a QueryExecutionId", Submit: true}
	}
	req, _ := json.Marshal(map[string]string{"QueryExecutionId": started.QueryExecutionId})
	if !r.emit(parent, arn, storeHistorySubmitted(meta, out)) {
		r.stopAthenaQuery(req)
		return "", &taskFailure{Halt: true}
	}
	for {
		if callCtx.Err() != nil {
			r.stopAthenaQuery(req)
			return "", transportFailure(callCtx.Err(), parent, callCtx, taskWins, fallback)
		}
		desc, err := r.SDK.CallIntegrationAction(callCtx, athenaStartQuery, "getQueryExecution", req)
		if err != nil {
			if callCtx.Err() != nil {
				r.stopAthenaQuery(req)
			}
			return "", sdkFailure(err, parent, callCtx, taskWins, fallback)
		}
		var reply struct {
			QueryExecution struct {
				Status struct{ State string }
			}
		}
		if err := json.Unmarshal([]byte(desc), &reply); err != nil {
			return "", &taskFailure{Name: "States.Runtime", Cause: "GetQueryExecution reply is not JSON"}
		}
		switch reply.QueryExecution.Status.State {
		case "SUCCEEDED":
			return desc, nil
		case "FAILED", "CANCELLED":
			return "", &taskFailure{Name: "States.TaskFailed", Cause: clip(desc, 32768)}
		}
		if err := r.sleep(callCtx, arn, r.syncPoll()); err != nil {
			r.stopAthenaQuery(req)
			return "", transportFailure(err, parent, callCtx, taskWins, fallback)
		}
	}
}

// stopAthenaQuery stops a query a .sync state started. Errors are ignored.
func (r *Runner) stopAthenaQuery(req []byte) {
	ctx, cancel := context.WithTimeout(context.Background(), athenaStopTimeout)
	defer cancel()
	_, _ = r.SDK.CallIntegrationAction(ctx, athenaStartQuery, "stopQueryExecution", req)
}
