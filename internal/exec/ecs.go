package exec

import (
	"context"
	"encoding/json"
	"time"
)

// ecsStopTimeout bounds the StopTask call made after a .sync task is aborted.
const ecsStopTimeout = 5 * time.Second

// ecsRunFailures fails a .sync or .waitForTaskToken ecs:runTask whose
// RunTask reply has a non-empty Failures list, as AWS documents. The
// request-response form returns that reply and does not fail.
func ecsRunFailures(out string) *taskFailure {
	var reply struct {
		Failures []json.RawMessage
	}
	if err := json.Unmarshal([]byte(out), &reply); err != nil || len(reply.Failures) == 0 {
		return nil
	}
	return &taskFailure{Name: "AmazonECS.Unknown", Cause: clip(out, 32768)}
}

// ecsTask is the part of an ECS Task the .sync poll reads. The whole task
// stays in raw for the result.
type ecsTask struct {
	TaskArn    string
	ClusterArn string
	LastStatus string
	Containers []struct {
		ExitCode *int
	}
	raw json.RawMessage
}

func parseECSTasks(out string) ([]ecsTask, error) {
	var reply struct {
		Tasks []json.RawMessage
	}
	if err := json.Unmarshal([]byte(out), &reply); err != nil {
		return nil, err
	}
	tasks := make([]ecsTask, 0, len(reply.Tasks))
	for _, raw := range reply.Tasks {
		var t ecsTask
		if err := json.Unmarshal(raw, &t); err != nil {
			return nil, err
		}
		t.raw = raw
		tasks = append(tasks, t)
	}
	return tasks, nil
}

// succeeded reports whether every container of a stopped task exited 0. A
// container with no exit code, such as one that never started, fails it.
func (t ecsTask) succeeded() bool {
	for _, c := range t.Containers {
		if c.ExitCode == nil || *c.ExitCode != 0 {
			return false
		}
	}
	return true
}

// invokeECSSync runs ecs:runTask.sync. RunTask's reply is TaskSubmitted.
// The task is then described until every started task is STOPPED. The
// result is the first task as DescribeTasks returns it. A task whose
// containers did not all exit 0 fails with States.TaskFailed and that task as
// the cause. When the execution stops or the task times out, each task is
// stopped, best effort.
func (r *Runner) invokeECSSync(callCtx, parent context.Context, arn string, meta taskMeta, taskWins bool) (string, *taskFailure) {
	out, err := r.SDK.CallIntegration(callCtx, ecsRunTask, []byte(meta.input))
	if err != nil {
		tf := sdkFailure(err, parent, callCtx, taskWins, "ECS.AmazonECSException")
		tf.Submit = tf.Name != "" && !tf.Timeout && !tf.Halt && !tf.ExecDeadline
		return "", tf
	}
	if tf := ecsRunFailures(out); tf != nil {
		return "", tf
	}
	started, err := parseECSTasks(out)
	if err != nil || len(started) == 0 {
		return "", &taskFailure{Name: "States.Runtime", Cause: "RunTask did not return a task"}
	}
	if !r.emit(parent, arn, storeHistorySubmitted(meta, out)) {
		r.stopECSTasks(started)
		return "", &taskFailure{Halt: true}
	}
	arns := make([]string, len(started))
	for i, t := range started {
		arns[i] = t.TaskArn
	}
	req, err := json.Marshal(map[string]any{"Cluster": started[0].ClusterArn, "Tasks": arns})
	if err != nil {
		return "", &taskFailure{Name: "States.Runtime", Cause: err.Error()}
	}
	poll := r.syncPoll()
	for {
		if callCtx.Err() != nil {
			r.stopECSTasks(started)
			return "", transportFailure(callCtx.Err(), parent, callCtx, taskWins, "ECS.AmazonECSException")
		}
		desc, err := r.SDK.CallIntegrationAction(callCtx, ecsRunTask, "describeTasks", req)
		if err != nil {
			if callCtx.Err() != nil {
				r.stopECSTasks(started)
			}
			return "", sdkFailure(err, parent, callCtx, taskWins, "ECS.AmazonECSException")
		}
		tasks, err := parseECSTasks(desc)
		if err != nil || len(tasks) == 0 {
			return "", &taskFailure{Name: "States.Runtime", Cause: "DescribeTasks did not return the task"}
		}
		stopped := true
		for _, t := range tasks {
			if t.LastStatus != "STOPPED" {
				stopped = false
			}
		}
		if stopped {
			for _, t := range tasks {
				if !t.succeeded() {
					return "", &taskFailure{Name: "States.TaskFailed", Cause: clip(string(t.raw), 32768)}
				}
			}
			return string(tasks[0].raw), nil
		}
		if err := r.sleep(callCtx, arn, poll); err != nil {
			r.stopECSTasks(started)
			return "", transportFailure(err, parent, callCtx, taskWins, "ECS.AmazonECSException")
		}
	}
}

// stopECSTasks stops tasks a .sync state started. Errors are ignored.
func (r *Runner) stopECSTasks(tasks []ecsTask) {
	ctx, cancel := context.WithTimeout(context.Background(), ecsStopTimeout)
	defer cancel()
	for _, t := range tasks {
		req, err := json.Marshal(map[string]string{
			"Cluster": t.ClusterArn,
			"Task":    t.TaskArn,
			"Reason":  "Task stopped by AWS Step Functions",
		})
		if err != nil {
			continue
		}
		_, _ = r.SDK.CallIntegrationAction(ctx, ecsRunTask, "stopTask", req)
	}
}
