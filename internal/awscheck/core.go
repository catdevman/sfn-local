package awscheck

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
	"github.com/aws/aws-sdk-go-v2/service/sfn/types"

	"github.com/catdevman/sfn-local/internal/sdk"
)

// These scenarios cover the states and operations the jar oracle compares
// only by event type: Parallel, inline Map, Retry and Catch, redrive inside
// them, StopExecution, and versions and aliases. The last ones close what
// the Distributed Map scenarios left open: pending and JSONL exports, a
// stopped Map Run, and a real Lambda invoke.

func parallelAndMap(ctx context.Context, e *Env) (map[string]any, error) {
	arn, err := e.machine(ctx, "flow", `{"StartAt":"One","States":{
		"One":{"Type":"Parallel","Branches":[{"StartAt":"A","States":{"A":{"Type":"Pass","Result":"a","End":true}}}],"ResultPath":"$.one","Next":"Each"},
		"Each":{"Type":"Map","ItemsPath":"$.items","MaxConcurrency":1,"ItemSelector":{"v.$":"$$.Map.Item.Value","i.$":"$$.Map.Item.Index"},
			"ItemProcessor":{"StartAt":"P","States":{"P":{"Type":"Pass","End":true}}},"ResultPath":"$.each","Next":"Two"},
		"Two":{"Type":"Parallel","Branches":[
			{"StartAt":"B","States":{"B":{"Type":"Pass","Result":"b","End":true}}},
			{"StartAt":"C","States":{"C":{"Type":"Pass","Result":"c","End":true}}}],"ResultPath":"$.two","End":true}}}`)
	if err != nil {
		return nil, err
	}
	ex, err := e.run(ctx, arn, "run", `{"items":["x","y"]}`)
	if err != nil {
		return nil, err
	}
	doc := map[string]any{}
	if doc["execution"], err = e.describe(ctx, ex); err != nil {
		return nil, err
	}
	h, err := e.history(ctx, ex)
	if err != nil {
		return nil, err
	}
	// The two-branch Parallel runs its branches at once, so its events are
	// compared as a set. Everything before it runs in order.
	split := len(h)
	for i, ev := range h {
		if m, _ := ev.(map[string]any); fmt.Sprint(m["Type"]) == "ParallelStateEntered" && stateName(m) == "Two" {
			split = i
		}
	}
	doc["history"] = h[:split]
	doc["concurrentEvents"] = unordered(h[split:])
	return doc, nil
}

func retryAndCatch(ctx context.Context, e *Env) (map[string]any, error) {
	arn, err := e.machine(ctx, "retry-catch", `{"StartAt":"Read","States":{
		"Read":{"Type":"Task","Resource":"arn:aws:states:::aws-sdk:s3:getObject","Parameters":{"Bucket":"`+e.Bucket+`","Key":"`+e.Prefix+`missing"},
			"Retry":[{"ErrorEquals":["S3.NoSuchKeyException"],"IntervalSeconds":1,"MaxAttempts":1,"BackoffRate":1}],
			"Catch":[{"ErrorEquals":["States.ALL"],"ResultPath":"$.readError","Next":"Branches"}],"End":true},
		"Branches":{"Type":"Parallel","Branches":[{"StartAt":"Boom","States":{"Boom":{"Type":"Fail","Error":"Branch.Failed","Cause":"branch failed"}}}],
			"Catch":[{"ErrorEquals":["Branch.Failed"],"ResultPath":"$.branchError","Next":"Done"}],"End":true},
		"Done":{"Type":"Pass","End":true}}}`)
	if err != nil {
		return nil, err
	}
	ex, err := e.run(ctx, arn, "run", `{}`)
	if err != nil {
		return nil, err
	}
	doc := map[string]any{}
	if doc["execution"], err = e.describe(ctx, ex); err != nil {
		return nil, err
	}
	doc["history"], err = e.history(ctx, ex)
	return doc, err
}

// redriveInside fails a Task inside a Parallel and inside an inline Map
// until the execution is redriven, then records how AWS resumes each.
func redriveInside(ctx context.Context, e *Env) (map[string]any, error) {
	present, err := e.putObject(ctx, "present.txt", "ok")
	if err != nil {
		return nil, err
	}
	read := `{"Type":"Task","Resource":"arn:aws:states:::aws-sdk:s3:getObject",
		"Arguments":{"Bucket":"` + e.Bucket + `","Key":"{% $states.input.bad and $states.context.Execution.RedriveCount = 0 ? '` + e.Prefix + `missing' : '` + present + `' %}"},
		"Output":"{% $states.input %}","End":true}`
	doc := map[string]any{}
	for _, c := range []struct{ name, def, input string }{
		{"parallel", `{"QueryLanguage":"JSONata","StartAt":"P","States":{"P":{"Type":"Parallel","Branches":[
			{"StartAt":"Ok","States":{"Ok":{"Type":"Pass","End":true}}},
			{"StartAt":"Read","States":{"Read":` + read + `}}],"End":true}}}`, `{"bad":true}`},
		{"map", `{"QueryLanguage":"JSONata","StartAt":"M","States":{"M":{"Type":"Map","MaxConcurrency":1,"Items":"{% $states.input %}",
			"ItemProcessor":{"StartAt":"Read","States":{"Read":` + read + `}},"End":true}}}`, `[{"bad":false},{"bad":true}]`},
	} {
		arn, err := e.machine(ctx, "redrive-"+c.name, c.def)
		if err != nil {
			return nil, err
		}
		ex, err := e.run(ctx, arn, "run", c.input)
		if err != nil {
			return nil, err
		}
		if _, err := e.SFN.RedriveExecution(ctx, &sfn.RedriveExecutionInput{ExecutionArn: &ex}); err != nil {
			return nil, fmt.Errorf("redrive %s: %w", c.name, err)
		}
		if _, err := e.wait(ctx, ex); err != nil {
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
		if c.name == "parallel" {
			// Both branches start at once, so the run is compared as a set.
			doc[c.name] = map[string]any{"execution": d, "events": unordered(h)}
		} else {
			doc[c.name] = map[string]any{"execution": d, "history": h}
		}
	}
	return doc, nil
}

func stopExecution(ctx context.Context, e *Env) (map[string]any, error) {
	arn, err := e.machine(ctx, "stop", `{"StartAt":"Hold","States":{"Hold":{"Type":"Wait","Seconds":300,"End":true}}}`)
	if err != nil {
		return nil, err
	}
	out, err := e.SFN.StartExecution(ctx, &sfn.StartExecutionInput{StateMachineArn: &arn, Name: aws.String("run"), Input: aws.String(`{}`)})
	if err != nil {
		return nil, err
	}
	ex := *out.ExecutionArn
	if err := e.until(ctx, func() (bool, error) {
		h, err := e.history(ctx, ex)
		return len(h) >= 2, err
	}); err != nil {
		return nil, err
	}
	stop, err := e.SFN.StopExecution(ctx, &sfn.StopExecutionInput{ExecutionArn: &ex, Error: aws.String("Check.Stopped"), Cause: aws.String("stopped on purpose")})
	if err != nil {
		return nil, fmt.Errorf("stop: %w", err)
	}
	if _, err := e.wait(ctx, ex); err != nil {
		return nil, err
	}
	doc := map[string]any{}
	if doc["stop"], err = sdk.Plain(stop); err != nil {
		return nil, err
	}
	if doc["execution"], err = e.describe(ctx, ex); err != nil {
		return nil, err
	}
	doc["history"], err = e.history(ctx, ex)
	return doc, err
}

// dmapStop stops a parent while its children wait, and records how the
// Map Run and the children end.
func dmapStop(ctx context.Context, e *Env) (map[string]any, error) {
	arn, err := e.machine(ctx, "dmap-stop", `{"StartAt":"M","States":{"M":{"Type":"Map",
		"ItemProcessor":{"ProcessorConfig":{"Mode":"DISTRIBUTED","ExecutionType":"STANDARD"},"StartAt":"Hold",
			"States":{"Hold":{"Type":"Wait","Seconds":300,"End":true}}},"End":true}}}`)
	if err != nil {
		return nil, err
	}
	out, err := e.SFN.StartExecution(ctx, &sfn.StartExecutionInput{StateMachineArn: &arn, Name: aws.String("run"), Input: aws.String(`[1,2]`)})
	if err != nil {
		return nil, err
	}
	ex := *out.ExecutionArn
	var runARN string
	if err := e.until(ctx, func() (bool, error) {
		runs, err := e.SFN.ListMapRuns(ctx, &sfn.ListMapRunsInput{ExecutionArn: &ex})
		if err != nil || len(runs.MapRuns) == 0 {
			return false, err
		}
		runARN = *runs.MapRuns[0].MapRunArn
		kids, err := e.SFN.ListExecutions(ctx, &sfn.ListExecutionsInput{MapRunArn: &runARN, StatusFilter: types.ExecutionStatusRunning})
		if err != nil || len(kids.Executions) < 2 {
			return false, err
		}
		for _, k := range kids.Executions {
			if h, err := e.history(ctx, *k.ExecutionArn); err != nil || len(h) < 2 {
				return false, err
			}
		}
		return true, nil
	}); err != nil {
		return nil, err
	}
	if _, err := e.SFN.StopExecution(ctx, &sfn.StopExecutionInput{ExecutionArn: &ex}); err != nil {
		return nil, fmt.Errorf("stop: %w", err)
	}
	if _, err := e.wait(ctx, ex); err != nil {
		return nil, err
	}
	if err := e.until(ctx, func() (bool, error) {
		run, err := e.SFN.DescribeMapRun(ctx, &sfn.DescribeMapRunInput{MapRunArn: &runARN})
		if err != nil {
			return false, err
		}
		return run.Status != types.MapRunStatusRunning && run.ExecutionCounts.Running == 0, nil
	}); err != nil {
		return nil, err
	}
	doc, err := e.parent(ctx, ex)
	if err != nil {
		return nil, err
	}
	return doc, e.children(ctx, doc, "ABORTED")
}

func versionsAndAliases(ctx context.Context, e *Env) (map[string]any, error) {
	def := `{"StartAt":"P","States":{"P":{"Type":"Pass","End":true}}}`
	arn, err := e.machine(ctx, "versions", def)
	if err != nil {
		return nil, err
	}
	doc := map[string]any{}
	add := func(key string, out any, err error) error {
		if err != nil {
			return fmt.Errorf("%s: %w", key, err)
		}
		doc[key], err = sdk.Plain(out)
		return err
	}
	desc, err := e.SFN.DescribeStateMachine(ctx, &sfn.DescribeStateMachineInput{StateMachineArn: &arn})
	if err := add("describe", desc, err); err != nil {
		return nil, err
	}
	pub, err := e.SFN.PublishStateMachineVersion(ctx, &sfn.PublishStateMachineVersionInput{StateMachineArn: &arn, Description: aws.String("first")})
	if err := add("publish", pub, err); err != nil {
		return nil, err
	}
	version := *pub.StateMachineVersionArn
	dv, err := e.SFN.DescribeStateMachine(ctx, &sfn.DescribeStateMachineInput{StateMachineArn: &version})
	if err := add("describeVersion", dv, err); err != nil {
		return nil, err
	}
	alias, err := e.SFN.CreateStateMachineAlias(ctx, &sfn.CreateStateMachineAliasInput{Name: aws.String("live"),
		RoutingConfiguration: []types.RoutingConfigurationListItem{{StateMachineVersionArn: &version, Weight: 100}}})
	if err := add("createAlias", alias, err); err != nil {
		return nil, err
	}
	aliasARN := *alias.StateMachineAliasArn
	da, err := e.SFN.DescribeStateMachineAlias(ctx, &sfn.DescribeStateMachineAliasInput{StateMachineAliasArn: &aliasARN})
	if err := add("describeAlias", da, err); err != nil {
		return nil, err
	}
	lv, err := e.SFN.ListStateMachineVersions(ctx, &sfn.ListStateMachineVersionsInput{StateMachineArn: &arn})
	if err := add("listVersions", lv, err); err != nil {
		return nil, err
	}
	la, err := e.SFN.ListStateMachineAliases(ctx, &sfn.ListStateMachineAliasesInput{StateMachineArn: &arn})
	if err := add("listAliases", la, err); err != nil {
		return nil, err
	}
	ex, err := e.run(ctx, aliasARN, "run", `{}`)
	if err != nil {
		return nil, err
	}
	if doc["execution"], err = e.describe(ctx, ex); err != nil {
		return nil, err
	}
	if doc["history"], err = e.history(ctx, ex); err != nil {
		return nil, err
	}
	fe, err := e.SFN.DescribeStateMachineForExecution(ctx, &sfn.DescribeStateMachineForExecutionInput{ExecutionArn: &ex})
	if err := add("machineForExecution", fe, err); err != nil {
		return nil, err
	}
	ok, err := e.SFN.ValidateStateMachineDefinition(ctx, &sfn.ValidateStateMachineDefinitionInput{Definition: &def})
	if err := add("validateOk", ok, err); err != nil {
		return nil, err
	}
	bad := `{"StartAt":"P","States":{"P":{"Type":"Pass","Next":"Nowhere"}}}`
	vb, err := e.SFN.ValidateStateMachineDefinition(ctx, &sfn.ValidateStateMachineDefinitionInput{Definition: &bad})
	if err := add("validateBad", vb, err); err != nil {
		return nil, err
	}
	// Aliases go before the machine. DeleteStateMachine removes versions.
	_, _ = e.SFN.DeleteStateMachineAlias(ctx, &sfn.DeleteStateMachineAliasInput{StateMachineAliasArn: &aliasARN})
	return doc, nil
}

// writerPending fails every item with no tolerance, so the export has a
// FAILED file and, for the items that never started, a PENDING file, in
// JSONL, under COMPACT and under NONE. How many items start before AWS
// stops the run varies, so the items are all alike and only the first
// record of each file and the file names are compared.
func writerPending(ctx context.Context, e *Env) (map[string]any, error) {
	if e.S3 == nil {
		return nil, errors.New("writer-pending needs S3")
	}
	doc := map[string]any{}
	for _, transform := range []string{"COMPACT", "NONE"} {
		name := "writer-pending-" + strings.ToLower(transform)
		arn, err := e.machine(ctx, name, `{"StartAt":"M","States":{"M":{"Type":"Map","MaxConcurrency":1,
			"ItemProcessor":{"ProcessorConfig":{"Mode":"DISTRIBUTED","ExecutionType":"STANDARD"},"StartAt":"Boom",
				"States":{"Boom":{"Type":"Fail","Error":"Item.Bad","Cause":"bad item"}}},
			"ResultWriter":{"Resource":"arn:aws:states:::s3:putObject","WriterConfig":{"OutputType":"JSONL","Transformation":"`+transform+`"},
				"Parameters":{"Bucket":"`+e.Bucket+`","Prefix":"`+e.Prefix+name+`"}},"End":true}}}`)
		if err != nil {
			return nil, err
		}
		ex, err := e.run(ctx, arn, "run", `[{"n":7},{"n":7},{"n":7},{"n":7},{"n":7},{"n":7},{"n":7},{"n":7},{"n":7},{"n":7}]`)
		if err != nil {
			return nil, err
		}
		d, err := e.describe(ctx, ex)
		if err != nil {
			return nil, err
		}
		part := map[string]any{"status": d["Status"], "error": d["Error"], "cause": d["Cause"]}
		prefix := e.Prefix + name + "/"
		list, err := e.S3.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: &e.Bucket, Prefix: &prefix})
		if err != nil {
			return nil, err
		}
		var names []string
		for _, obj := range list.Contents {
			base := path.Base(*obj.Key)
			names = append(names, base)
			body, err := e.getObject(ctx, *obj.Key)
			if err != nil {
				return nil, err
			}
			if base == "manifest.json" {
				var m any
				if err := json.Unmarshal(body, &m); err != nil {
					return nil, err
				}
				part["manifest"] = firstOnly(m)
				continue
			}
			line, _, _ := strings.Cut(string(body), "\n")
			var first any
			if json.Unmarshal([]byte(line), &first) != nil {
				first = line
			}
			part["first:"+base] = first
		}
		sort.Strings(names)
		part["files"] = names
		doc[strings.ToLower(transform)] = part
	}
	return doc, nil
}

// lambdaInvoke calls a real function through the optimized integration and
// through aws-sdk. The function returns {"echo": <event>}.
func lambdaInvoke(ctx context.Context, e *Env) (map[string]any, error) {
	if e.Function == "" {
		return nil, errors.New("lambda-invoke needs a function")
	}
	arn, err := e.machine(ctx, "lambda", `{"StartAt":"Optimized","States":{
		"Optimized":{"Type":"Task","Resource":"arn:aws:states:::lambda:invoke","Parameters":{"FunctionName":"`+e.Function+`","Payload":{"n":1}},"ResultPath":"$.optimized","Next":"Sdk"},
		"Sdk":{"Type":"Task","Resource":"arn:aws:states:::aws-sdk:lambda:invoke","Parameters":{"FunctionName":"`+e.Function+`","Payload":{"n":2}},"ResultPath":"$.sdk","End":true}}}`)
	if err != nil {
		return nil, err
	}
	ex, err := e.run(ctx, arn, "run", `{}`)
	if err != nil {
		return nil, err
	}
	doc := map[string]any{}
	if doc["execution"], err = e.describe(ctx, ex); err != nil {
		return nil, err
	}
	doc["history"], err = e.history(ctx, ex)
	return doc, err
}

func (e *Env) putObject(ctx context.Context, name, body string) (string, error) {
	key := e.Prefix + name
	if _, err := e.S3.PutObject(ctx, &s3.PutObjectInput{Bucket: &e.Bucket, Key: &key, Body: strings.NewReader(body)}); err != nil {
		return "", fmt.Errorf("put %s: %w", key, err)
	}
	return key, nil
}

func (e *Env) getObject(ctx context.Context, key string) ([]byte, error) {
	out, err := e.S3.GetObject(ctx, &s3.GetObjectInput{Bucket: &e.Bucket, Key: &key})
	if err != nil {
		return nil, fmt.Errorf("get %s: %w", key, err)
	}
	defer out.Body.Close()
	return io.ReadAll(out.Body)
}

// until polls ok every Poll until it is true or Timeout passes.
func (e *Env) until(ctx context.Context, ok func() (bool, error)) error {
	deadline := time.Now().Add(e.Timeout)
	for {
		done, err := ok()
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("condition not met after %s", e.Timeout)
		}
		time.Sleep(e.Poll)
	}
}

func stateName(ev map[string]any) string {
	d, _ := ev["StateEnteredEventDetails"].(map[string]any)
	name, _ := d["Name"].(string)
	return name
}

// unordered drops event ids and sorts events, for events that AWS may write
// in any order.
func unordered(events []any) []any {
	type keyed struct {
		key string
		ev  any
	}
	var list []keyed
	for _, ev := range events {
		m, _ := ev.(map[string]any)
		c := make(map[string]any, len(m))
		for k, v := range m {
			if k != "Id" && k != "PreviousEventId" && k != "Timestamp" {
				c[k] = v
			}
		}
		text, _ := json.Marshal(c)
		list = append(list, keyed{string(text), c})
	}
	sort.Slice(list, func(i, j int) bool { return list[i].key < list[j].key })
	out := make([]any, len(list))
	for i, k := range list {
		out[i] = k.ev
	}
	return out
}

// firstOnly keeps the first entry of every array, for documents whose array
// lengths depend on timing.
func firstOnly(v any) any {
	switch x := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, val := range x {
			out[k] = firstOnly(val)
		}
		return out
	case []any:
		if len(x) == 0 {
			return x
		}
		return []any{firstOnly(x[0])}
	default:
		return v
	}
}
