package awscheck

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
	"github.com/aws/aws-sdk-go-v2/service/sfn/types"
	"github.com/aws/smithy-go"

	"github.com/catdevman/sfn-local/internal/sdk"
)

// These scenarios cover runtime failures, whose Error and Cause workflows
// match on, and the operations that hand work to someone outside the
// execution: task tokens, activities, and synchronous Express runs. The last
// two settle event links that a concurrent Map and a redrive of a Pass,
// Choice, or Wait state produce.

// jsonPathErrors runs one failing state per case. Most of these errors fail
// the execution even under a Catch, so each case is its own execution.
func jsonPathErrors(ctx context.Context, e *Env) (map[string]any, error) {
	cases := map[string]string{
		"InputPath":    `{"Type":"Pass","InputPath":"$.missing","End":true}`,
		"Parameters":   `{"Type":"Pass","Parameters":{"x.$":"$.missing"},"End":true}`,
		"OutputPath":   `{"Type":"Pass","OutputPath":"$.missing","End":true}`,
		"ResultPath":   `{"Type":"Pass","Result":"r","ResultPath":"$.case.inner","End":true}`,
		"StringToJson": `{"Type":"Pass","Parameters":{"x.$":"States.StringToJson($.case)"},"End":true}`,
		"ArrayGetItem": `{"Type":"Pass","Parameters":{"x.$":"States.ArrayGetItem(States.Array(1,2), 5)"},"End":true}`,
		"NoMatch":      `{"Type":"Choice","Choices":[{"Variable":"$.case","StringEquals":"never","Next":"Done"}]}`,
		"ItemsPath":    `{"Type":"Map","ItemsPath":"$.case","ItemProcessor":{"StartAt":"P","States":{"P":{"Type":"Pass","End":true}}},"End":true}`,
		"SecondsPath":  `{"Type":"Wait","SecondsPath":"$.missing","End":true}`,
		"ErrorPath":    `{"Type":"Fail","ErrorPath":"$.missing","Cause":"c"}`,
		"Caught": `{"Type":"Task","Resource":"arn:aws:states:::aws-sdk:s3:headObject","Parameters":{"Bucket.$":"$.missing","Key":"k"},
			"Catch":[{"ErrorEquals":["States.ALL"],"Next":"Done"}],"End":true}`,
	}
	route := func(name string) string {
		return `{"Variable":"$.case","StringEquals":"` + name + `","Next":"` + name + `"}`
	}
	return errorCases(ctx, e, "errors-jsonpath", "", cases, route)
}

// jsonataErrors does the same for JSONata expressions.
func jsonataErrors(ctx context.Context, e *Env) (map[string]any, error) {
	cases := map[string]string{
		"Undefined":  `{"Type":"Pass","Output":"{% $states.input.missing %}","End":true}`,
		"NoFunction": `{"Type":"Pass","Output":"{% $nope() %}","End":true}`,
		"Throw":      `{"Type":"Pass","Output":"{% $error('custom message') %}","End":true}`,
		"TypeError":  `{"Type":"Pass","Output":"{% 'a' + 1 %}","End":true}`,
		"DivideZero": `{"Type":"Pass","Output":"{% 1 / 0 %}","End":true}`,
		"NotArray":   `{"Type":"Map","Items":"{% 'abc' %}","ItemProcessor":{"StartAt":"P","States":{"P":{"Type":"Pass","End":true}}},"End":true}`,
		"BadSeconds": `{"Type":"Wait","Seconds":"{% 'x' %}","End":true}`,
		"NotBoolean": `{"Type":"Choice","Choices":[{"Condition":"{% 'yes' %}","Next":"Done"}],"Default":"Done"}`,
		"BadAssign":  `{"Type":"Pass","Assign":{"v":"{% $nope() %}"},"End":true}`,
		"DataLimit":  `{"Type":"Pass","Output":"{% $join([1..20000].('abcdefghijklmnop'), '') %}","End":true}`,
		"OutputCatch": `{"Type":"Task","Resource":"arn:aws:states:::aws-sdk:s3:headObject","Arguments":{"Bucket":"{% $nope() %}","Key":"k"},
			"Catch":[{"ErrorEquals":["States.ALL"],"Next":"Done"}],"End":true}`,
	}
	route := func(name string) string {
		return `{"Condition":"{% $states.input.case = '` + name + `' %}","Next":"` + name + `"}`
	}
	return errorCases(ctx, e, "errors-jsonata", `"QueryLanguage":"JSONata",`, cases, route)
}

// errorCases builds one machine whose Route state picks a case by $.case,
// runs every case, and records each execution's result and history.
func errorCases(ctx context.Context, e *Env, name, head string, cases map[string]string, route func(string) string) (map[string]any, error) {
	names := make([]string, 0, len(cases))
	for n := range cases {
		names = append(names, n)
	}
	sort.Strings(names)
	var choices, states []string
	for _, n := range names {
		choices = append(choices, route(n))
		states = append(states, `"`+n+`":`+cases[n])
	}
	def := `{` + head + `"StartAt":"Route","States":{"Route":{"Type":"Choice","Choices":[` + strings.Join(choices, ",") + `],"Default":"Done"},` +
		strings.Join(states, ",") + `,"Done":{"Type":"Pass","End":true}}}`
	arn, err := e.machine(ctx, name, def)
	if err != nil {
		return nil, err
	}
	doc := map[string]any{}
	for _, n := range names {
		ex, err := e.run(ctx, arn, n, `{"case":"`+n+`"}`)
		if err != nil {
			return nil, err
		}
		d, err := e.describe(ctx, ex)
		if err != nil {
			return nil, err
		}
		h, err := e.history(ctx, ex)
		if err != nil {
			return nil, err
		}
		doc[n] = map[string]any{"status": d["Status"], "error": d["Error"], "cause": d["Cause"], "output": d["Output"], "history": h}
	}
	return doc, nil
}

// callbackDef passes the task token to the echo function and waits for it.
func (e *Env) callbackDef(timing string) string {
	return `{"StartAt":"Wait","States":{"Wait":{"Type":"Task","Resource":"arn:aws:states:::lambda:invoke.waitForTaskToken",
		"Parameters":{"FunctionName":"` + e.Function + `","Payload":{"token.$":"$$.Task.Token"}},` + timing + `,"End":true}}}`
}

// taskTokens completes, fails, and times out waitForTaskToken tasks, and
// sends results for a closed token and for a token that never existed.
func taskTokens(ctx context.Context, e *Env) (map[string]any, error) {
	if e.Function == "" {
		return nil, errors.New("task-tokens needs a function")
	}
	paths, err := e.machine(ctx, "callback", e.callbackDef(`"TimeoutSecondsPath":"$.timeout","HeartbeatSecondsPath":"$.heartbeat"`))
	if err != nil {
		return nil, err
	}
	short, err := e.machine(ctx, "callback-timeout", e.callbackDef(`"TimeoutSeconds":1`))
	if err != nil {
		return nil, err
	}
	doc := map[string]any{}
	var tokens []string
	record := func(key, ex string, extra map[string]any) error {
		if _, err := e.wait(ctx, ex); err != nil {
			return err
		}
		d, err := e.describe(ctx, ex)
		if err != nil {
			return err
		}
		h, err := e.history(ctx, ex)
		if err != nil {
			return err
		}
		part := map[string]any{"execution": d, "history": h}
		for k, v := range extra {
			part[k] = v
		}
		doc[key] = part
		return nil
	}

	ex, token, err := e.startCallback(ctx, paths, "success", `{"timeout":60,"heartbeat":30}`)
	if err != nil {
		return nil, err
	}
	tokens = append(tokens, token)
	_, hbErr := e.SFN.SendTaskHeartbeat(ctx, &sfn.SendTaskHeartbeatInput{TaskToken: &token})
	_, okErr := e.SFN.SendTaskSuccess(ctx, &sfn.SendTaskSuccessInput{TaskToken: &token, Output: aws.String(`{"done":true}`)})
	if err := record("success", ex, map[string]any{"heartbeat": apiError(hbErr), "send": apiError(okErr)}); err != nil {
		return nil, err
	}
	_, again := e.SFN.SendTaskSuccess(ctx, &sfn.SendTaskSuccessInput{TaskToken: &token, Output: aws.String(`{}`)})
	_, lateHB := e.SFN.SendTaskHeartbeat(ctx, &sfn.SendTaskHeartbeatInput{TaskToken: &token})
	_, bogus := e.SFN.SendTaskSuccess(ctx, &sfn.SendTaskSuccessInput{TaskToken: aws.String("not-a-token"), Output: aws.String(`{}`)})
	_, badOutput := e.SFN.SendTaskFailure(ctx, &sfn.SendTaskFailureInput{TaskToken: &token, Error: aws.String("E")})
	doc["closedToken"] = map[string]any{"success": apiError(again), "heartbeat": apiError(lateHB), "failure": apiError(badOutput)}
	doc["unknownToken"] = apiError(bogus)

	ex, token, err = e.startCallback(ctx, paths, "failure", `{"timeout":60,"heartbeat":30}`)
	if err != nil {
		return nil, err
	}
	tokens = append(tokens, token)
	_, failErr := e.SFN.SendTaskFailure(ctx, &sfn.SendTaskFailureInput{TaskToken: &token, Error: aws.String("Worker.Failed"), Cause: aws.String("worker failed")})
	if err := record("failure", ex, map[string]any{"send": apiError(failErr)}); err != nil {
		return nil, err
	}

	ex, token, err = e.startCallback(ctx, paths, "heartbeat", `{"timeout":60,"heartbeat":1}`)
	if err != nil {
		return nil, err
	}
	tokens = append(tokens, token)
	if err := record("heartbeatTimeout", ex, nil); err != nil {
		return nil, err
	}

	ex, token, err = e.startCallback(ctx, short, "timeout", `{}`)
	if err != nil {
		return nil, err
	}
	tokens = append(tokens, token)
	if err := record("timeout", ex, nil); err != nil {
		return nil, err
	}

	// A call that fails never hands out the token.
	missing, err := e.machine(ctx, "callback-missing", `{"StartAt":"Wait","States":{"Wait":{"Type":"Task","Resource":"arn:aws:states:::lambda:invoke.waitForTaskToken",
		"Parameters":{"FunctionName":"`+e.Prefix+`missing","Payload":{"token.$":"$$.Task.Token"}},"End":true}}}`)
	if err != nil {
		return nil, err
	}
	ex, err = e.run(ctx, missing, "missing", `{}`)
	if err != nil {
		return nil, err
	}
	if err := record("submitFails", ex, nil); err != nil {
		return nil, err
	}
	failed, _ := doc["submitFails"].(map[string]any)
	h, _ := failed["history"].([]any)
	tokens = append(tokens, scheduledToken(h))
	return scrub(doc, tokens, "<token>").(map[string]any), nil
}

// startCallback starts an execution and returns its task token once the
// task has been submitted.
func (e *Env) startCallback(ctx context.Context, machine, name, input string) (string, string, error) {
	out, err := e.SFN.StartExecution(ctx, &sfn.StartExecutionInput{StateMachineArn: &machine, Name: &name, Input: &input})
	if err != nil {
		return "", "", fmt.Errorf("start %s: %w", name, err)
	}
	ex := *out.ExecutionArn
	var token string
	err = e.until(ctx, func() (bool, error) {
		h, err := e.history(ctx, ex)
		if err != nil {
			return false, err
		}
		token = scheduledToken(h)
		for _, ev := range h {
			m, _ := ev.(map[string]any)
			switch fmt.Sprint(m["Type"]) {
			case "TaskSubmitted", "TaskFailed", "TaskTimedOut":
				return token != "", nil
			}
		}
		return false, nil
	})
	return ex, token, err
}

// scheduledToken is the task token a callback task passed in its payload.
func scheduledToken(history []any) string {
	for _, ev := range history {
		m, _ := ev.(map[string]any)
		if fmt.Sprint(m["Type"]) != "TaskScheduled" {
			continue
		}
		d, _ := m["TaskScheduledEventDetails"].(map[string]any)
		text, _ := d["Parameters"].(string)
		var params struct{ Payload struct{ Token string } }
		_ = json.Unmarshal([]byte(text), &params)
		return params.Payload.Token
	}
	return ""
}

// activities runs an activity task through a worker, fails one, and lets
// one time out with no worker.
func activities(ctx context.Context, e *Env) (map[string]any, error) {
	name := e.Prefix + "worker"
	created, err := e.SFN.CreateActivity(ctx, &sfn.CreateActivityInput{Name: &name})
	if err != nil {
		return nil, fmt.Errorf("create activity: %w", err)
	}
	activity := *created.ActivityArn
	e.activities = append(e.activities, activity)
	doc := map[string]any{}
	if doc["create"], err = sdk.Plain(created); err != nil {
		return nil, err
	}
	desc, err := e.SFN.DescribeActivity(ctx, &sfn.DescribeActivityInput{ActivityArn: &activity})
	if err != nil {
		return nil, fmt.Errorf("describe activity: %w", err)
	}
	if doc["describe"], err = sdk.Plain(desc); err != nil {
		return nil, err
	}
	worked, err := e.machine(ctx, "activity", `{"StartAt":"Work","States":{"Work":{"Type":"Task","Resource":"`+activity+`",
		"Parameters":{"n.$":"$.n"},"TimeoutSeconds":60,"HeartbeatSeconds":30,"End":true}}}`)
	if err != nil {
		return nil, err
	}
	idle, err := e.machine(ctx, "activity-timeout", `{"StartAt":"Work","States":{"Work":{"Type":"Task","Resource":"`+activity+`","TimeoutSeconds":1,"End":true}}}`)
	if err != nil {
		return nil, err
	}
	var tokens []string
	for _, c := range []struct {
		key  string
		send func(token string) error
	}{
		{"success", func(token string) error {
			_, err := e.SFN.SendTaskSuccess(ctx, &sfn.SendTaskSuccessInput{TaskToken: &token, Output: aws.String(`{"worked":true}`)})
			return err
		}},
		{"failure", func(token string) error {
			_, err := e.SFN.SendTaskFailure(ctx, &sfn.SendTaskFailureInput{TaskToken: &token, Error: aws.String("Worker.Failed"), Cause: aws.String("worker failed")})
			return err
		}},
	} {
		out, err := e.SFN.StartExecution(ctx, &sfn.StartExecutionInput{StateMachineArn: &worked, Name: aws.String(c.key), Input: aws.String(`{"n":1}`)})
		if err != nil {
			return nil, err
		}
		task, err := e.SFN.GetActivityTask(ctx, &sfn.GetActivityTaskInput{ActivityArn: &activity, WorkerName: aws.String("checker")})
		if err != nil {
			return nil, fmt.Errorf("get activity task: %w", err)
		}
		if task.TaskToken == nil {
			return nil, errors.New("get activity task returned no task")
		}
		tokens = append(tokens, *task.TaskToken)
		part := map[string]any{"send": apiError(c.send(*task.TaskToken))}
		if part["task"], err = sdk.Plain(task); err != nil {
			return nil, err
		}
		if _, err := e.wait(ctx, *out.ExecutionArn); err != nil {
			return nil, err
		}
		if part["execution"], err = e.describe(ctx, *out.ExecutionArn); err != nil {
			return nil, err
		}
		if part["history"], err = e.history(ctx, *out.ExecutionArn); err != nil {
			return nil, err
		}
		doc[c.key] = part
	}
	// An activity's TimeoutSeconds counts from when a worker takes the task,
	// not from when it is scheduled. The worker here waits past the timeout
	// before polling, then never answers.
	out, err := e.SFN.StartExecution(ctx, &sfn.StartExecutionInput{StateMachineArn: &idle, Name: aws.String("timeout"), Input: aws.String(`{}`)})
	if err != nil {
		return nil, err
	}
	ex := *out.ExecutionArn
	time.Sleep(3 * time.Second)
	part := map[string]any{}
	before, err := e.describe(ctx, ex)
	if err != nil {
		return nil, err
	}
	part["statusBeforePoll"] = before["Status"]
	if part["historyBeforePoll"], err = e.history(ctx, ex); err != nil {
		return nil, err
	}
	task, err := e.SFN.GetActivityTask(ctx, &sfn.GetActivityTaskInput{ActivityArn: &activity, WorkerName: aws.String("checker")})
	if err != nil {
		return nil, fmt.Errorf("get activity task: %w", err)
	}
	if task.TaskToken != nil {
		tokens = append(tokens, *task.TaskToken)
	}
	if _, err := e.wait(ctx, ex); err != nil {
		return nil, err
	}
	if part["execution"], err = e.describe(ctx, ex); err != nil {
		return nil, err
	}
	if part["history"], err = e.history(ctx, ex); err != nil {
		return nil, err
	}
	page, err := e.SFN.GetExecutionHistory(ctx, &sfn.GetExecutionHistoryInput{ExecutionArn: &ex})
	if err != nil {
		return nil, err
	}
	var started, ended time.Time
	for _, ev := range page.Events {
		switch ev.Type {
		case types.HistoryEventTypeActivityStarted:
			started = aws.ToTime(ev.Timestamp)
		case types.HistoryEventTypeActivityTimedOut:
			ended = aws.ToTime(ev.Timestamp)
		}
	}
	part["startedToTimedOutSeconds"] = ended.Sub(started).Round(time.Second).Seconds()
	doc["timeout"] = part
	return scrub(doc, tokens, "<token>").(map[string]any), nil
}

// expressSync runs an Express machine with StartSyncExecution, and records
// the errors for a Standard machine and for DescribeExecution on an Express
// execution.
func expressSync(ctx context.Context, e *Env) (map[string]any, error) {
	express, err := e.createMachine(ctx, "express", `{"StartAt":"Check","States":{
		"Check":{"Type":"Choice","Choices":[{"Variable":"$.fail","BooleanEquals":true,"Next":"Fail"}],"Default":"Ok"},
		"Fail":{"Type":"Fail","Error":"Check.Failed","Cause":"failed on purpose"},
		"Ok":{"Type":"Pass","Parameters":{"input.$":"$","name.$":"$$.Execution.Name"},"End":true}}}`, types.StateMachineTypeExpress)
	if err != nil {
		return nil, err
	}
	standard, err := e.machine(ctx, "standard", `{"StartAt":"P","States":{"P":{"Type":"Pass","End":true}}}`)
	if err != nil {
		return nil, err
	}
	doc := map[string]any{}
	for _, c := range []struct{ name, input string }{{"ok", `{"fail":false}`}, {"bad", `{"fail":true}`}} {
		out, err := e.SFN.StartSyncExecution(ctx, &sfn.StartSyncExecutionInput{StateMachineArn: &express, Name: aws.String(c.name), Input: aws.String(c.input)})
		if err != nil {
			return nil, fmt.Errorf("start sync %s: %w", c.name, err)
		}
		if doc[c.name], err = sdk.Plain(out); err != nil {
			return nil, err
		}
		if c.name == "ok" {
			_, err := e.SFN.DescribeExecution(ctx, &sfn.DescribeExecutionInput{ExecutionArn: out.ExecutionArn})
			doc["describeExpress"] = apiError(err)
		}
	}
	_, err = e.SFN.StartSyncExecution(ctx, &sfn.StartSyncExecutionInput{StateMachineArn: &standard, Input: aws.String(`{}`)})
	doc["syncStandard"] = apiError(err)
	return doc, nil
}

// mapConcurrent runs an inline Map with no concurrency limit, and records
// which event each event follows, without the order AWS wrote them in.
func mapConcurrent(ctx context.Context, e *Env) (map[string]any, error) {
	arn, err := e.machine(ctx, "map-concurrent", `{"StartAt":"M","States":{"M":{"Type":"Map",
		"ItemProcessor":{"StartAt":"P","States":{"P":{"Type":"Pass","End":true}}},"End":true}}}`)
	if err != nil {
		return nil, err
	}
	ex, err := e.run(ctx, arn, "run", `[1,2,3]`)
	if err != nil {
		return nil, err
	}
	h, err := e.history(ctx, ex)
	if err != nil {
		return nil, err
	}
	doc := map[string]any{"links": links(h), "events": unordered(h)}
	doc["execution"], err = e.describe(ctx, ex)
	return doc, err
}

// links lists "event <- previous event" for each event, sorted. A Map
// iteration event names its index. The event pointed at does not, since
// which iteration finishes last depends on timing.
func links(events []any) []any {
	byID := map[float64]string{}
	label := func(m map[string]any) string {
		t := fmt.Sprint(m["Type"])
		for k, v := range m {
			if strings.HasPrefix(k, "MapIteration") {
				if d, ok := v.(map[string]any); ok {
					if i, ok := d["Index"]; ok {
						return fmt.Sprintf("%s[%v]", t, i)
					}
				}
			}
		}
		return t
	}
	var out []string
	for _, ev := range events {
		m, _ := ev.(map[string]any)
		id, _ := toFloat(m["Id"])
		byID[id] = label(m)
		prev, _ := toFloat(m["PreviousEventId"])
		from := "0"
		if p, ok := byID[prev]; ok {
			from, _, _ = strings.Cut(p, "[")
		}
		out = append(out, label(m)+" <- "+from)
	}
	sort.Strings(out)
	list := make([]any, len(out))
	for i, s := range out {
		list[i] = s
	}
	return list
}

func toFloat(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case int64:
		return float64(x), true
	case int:
		return float64(x), true
	case json.Number:
		f, err := x.Float64()
		return f, err == nil
	}
	return 0, false
}

// redriveStates fails a Pass, then a Choice, then a Wait, each on its own
// redrive, and records how AWS resumes each.
func redriveStates(ctx context.Context, e *Env) (map[string]any, error) {
	arn, err := e.machine(ctx, "redrive-states", `{"QueryLanguage":"JSONata","StartAt":"First","States":{
		"First":{"Type":"Pass","Output":"{% $states.context.Execution.RedriveCount = 0 ? $error('first') : $states.input %}","Next":"Pick"},
		"Pick":{"Type":"Choice","Choices":[{"Condition":"{% $states.context.Execution.RedriveCount = 1 ? $error('pick') : true %}","Next":"Hold"}]},
		"Hold":{"Type":"Wait","Seconds":"{% $states.context.Execution.RedriveCount = 2 ? $error('hold') : 1 %}","Next":"Done"},
		"Done":{"Type":"Pass","End":true}}}`)
	if err != nil {
		return nil, err
	}
	ex, err := e.run(ctx, arn, "run", `{"n":1}`)
	if err != nil {
		return nil, err
	}
	for i := 0; i < 3; i++ {
		if _, err := e.SFN.RedriveExecution(ctx, &sfn.RedriveExecutionInput{ExecutionArn: &ex}); err != nil {
			return nil, fmt.Errorf("redrive %d: %w", i+1, err)
		}
		// A redrive returns before the execution reports RUNNING again.
		if err := e.until(ctx, func() (bool, error) {
			d, err := e.SFN.DescribeExecution(ctx, &sfn.DescribeExecutionInput{ExecutionArn: &ex})
			return err == nil && aws.ToInt32(d.RedriveCount) == int32(i+1) && d.Status != types.ExecutionStatusRunning, err
		}); err != nil {
			return nil, err
		}
	}
	doc := map[string]any{}
	if doc["execution"], err = e.describe(ctx, ex); err != nil {
		return nil, err
	}
	doc["history"], err = e.history(ctx, ex)
	return doc, err
}

// apiError is the code and message of an API error, or "ok" for none.
func apiError(err error) any {
	if err == nil {
		return "ok"
	}
	var ae smithy.APIError
	if errors.As(err, &ae) {
		return map[string]any{"code": ae.ErrorCode(), "message": ae.ErrorMessage()}
	}
	return map[string]any{"error": err.Error()}
}

// scrub replaces each of olds with repl in every string and key.
func scrub(v any, olds []string, repl string) any {
	str := func(s string) string {
		for _, old := range olds {
			if old != "" {
				s = strings.ReplaceAll(s, old, repl)
			}
		}
		return s
	}
	switch x := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, val := range x {
			out[str(k)] = scrub(val, olds, repl)
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, val := range x {
			out[i] = scrub(val, olds, repl)
		}
		return out
	case string:
		return str(x)
	default:
		return v
	}
}
