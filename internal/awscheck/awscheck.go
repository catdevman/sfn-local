// Package awscheck runs the same scenarios against AWS and against this
// server. cmd/awsrecord records AWS's answers in testdata. The api package's
// TestAWSRecordings runs each recorded scenario here and compares.
//
// A scenario covers behavior that AWS does not document and this server
// chose: Distributed Map events, Map Run counts, redrive, the ResultWriter
// files, and aws-sdk error names.
package awscheck

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
	"github.com/aws/aws-sdk-go-v2/service/sfn/types"

	"github.com/catdevman/sfn-local/internal/sdk"
)

// Env is where a scenario runs.
type Env struct {
	SFN     *sfn.Client
	S3      *s3.Client
	Account string
	Region  string
	RoleARN string
	// Bucket holds ResultWriter output and ItemReader input.
	Bucket string
	// Prefix starts every machine name, object key, and missing resource
	// name, so runs in one account do not collide. It is removed when the
	// result is normalized.
	Prefix  string
	Poll    time.Duration
	Timeout time.Duration
	// RoleWait retries CreateStateMachine while a new IAM role propagates.
	RoleWait time.Duration

	machines []string
}

// Scenario returns a document of API answers. The caller normalizes it.
type Scenario struct {
	Name string
	Run  func(ctx context.Context, e *Env) (map[string]any, error)
}

// Scenarios is every scenario, in recording order.
var Scenarios = []Scenario{
	{"execution-status", executionStatus},
	{"dmap-success", dmapSuccess},
	{"dmap-threshold", dmapThreshold},
	{"dmap-redrive", dmapRedrive},
	{"dmap-writer", dmapWriter},
	{"sdk-errors", sdkErrors},
}

func executionStatus(ctx context.Context, e *Env) (map[string]any, error) {
	arn, err := e.machine(ctx, "status", `{"StartAt":"Check","States":{
		"Check":{"Type":"Choice","Choices":[{"Variable":"$.fail","BooleanEquals":true,"Next":"Fail"}],"Default":"Ok"},
		"Fail":{"Type":"Fail","Error":"Check.Failed","Cause":"failed on purpose"},
		"Ok":{"Type":"Pass","End":true}}}`)
	if err != nil {
		return nil, err
	}
	ok, err := e.run(ctx, arn, "ok", `{"fail":false}`)
	if err != nil {
		return nil, err
	}
	bad, err := e.run(ctx, arn, "bad", `{"fail":true}`)
	if err != nil {
		return nil, err
	}
	doc := map[string]any{}
	doc["succeeded"], err = e.describe(ctx, ok)
	if err != nil {
		return nil, err
	}
	doc["failed"], err = e.describe(ctx, bad)
	if err != nil {
		return nil, err
	}
	doc["failedHistory"], err = e.history(ctx, bad)
	return doc, err
}

func dmapSuccess(ctx context.Context, e *Env) (map[string]any, error) {
	arn, err := e.machine(ctx, "dmap-success", `{"StartAt":"M","States":{"M":{"Type":"Map","ItemsPath":"$.items","Label":"items",
		"ItemSelector":{"v.$":"$$.Map.Item.Value","i.$":"$$.Map.Item.Index"},
		"ItemProcessor":{"ProcessorConfig":{"Mode":"DISTRIBUTED","ExecutionType":"STANDARD"},"StartAt":"P",
			"States":{"P":{"Type":"Pass","End":true}}},"End":true}}}`)
	if err != nil {
		return nil, err
	}
	ex, err := e.run(ctx, arn, "run", `{"items":["a","b","c"]}`)
	if err != nil {
		return nil, err
	}
	doc, err := e.parent(ctx, ex)
	if err != nil {
		return nil, err
	}
	return doc, e.children(ctx, doc, "SUCCEEDED")
}

// thresholdChild fails the items whose bad member is true.
const thresholdChild = `"ItemProcessor":{"ProcessorConfig":{"Mode":"DISTRIBUTED","ExecutionType":"STANDARD"},"StartAt":"Check",
	"States":{"Check":{"Type":"Choice","Choices":[{"Variable":"$.bad","BooleanEquals":true,"Next":"Fail"}],"Default":"Ok"},
		"Fail":{"Type":"Fail","Error":"Item.Bad","Cause":"bad item"},"Ok":{"Type":"Pass","End":true}}}`

func dmapThreshold(ctx context.Context, e *Env) (map[string]any, error) {
	arn, err := e.machine(ctx, "dmap-threshold", `{"StartAt":"M","States":{"M":{"Type":"Map","MaxConcurrency":1,`+thresholdChild+`,"End":true}}}`)
	if err != nil {
		return nil, err
	}
	ex, err := e.run(ctx, arn, "run", `[{"bad":false},{"bad":true},{"bad":false},{"bad":false}]`)
	if err != nil {
		return nil, err
	}
	doc, err := e.parent(ctx, ex)
	if err != nil {
		return nil, err
	}
	return doc, e.children(ctx, doc, "FAILED", "SUCCEEDED")
}

// dmapRedrive fails one child until the Map Run is redriven. The child reads
// a missing S3 key while its own RedriveCount is 0, so no outside state is
// needed. A redrive resumes at the failed Task, which then reads a key that
// exists. A Fail state would fail again, because a redrive resumes at it.
func dmapRedrive(ctx context.Context, e *Env) (map[string]any, error) {
	if e.S3 == nil {
		return nil, errors.New("dmap-redrive needs S3")
	}
	present := e.Prefix + "present.txt"
	if _, err := e.S3.PutObject(ctx, &s3.PutObjectInput{Bucket: &e.Bucket, Key: &present, Body: strings.NewReader("ok")}); err != nil {
		return nil, fmt.Errorf("put object: %w", err)
	}
	arn, err := e.machine(ctx, "dmap-redrive", `{"QueryLanguage":"JSONata","StartAt":"M","States":{"M":{"Type":"Map","MaxConcurrency":1,"Items":"{% $states.input %}",
		"ItemProcessor":{"ProcessorConfig":{"Mode":"DISTRIBUTED","ExecutionType":"STANDARD"},"StartAt":"Read",
			"States":{"Read":{"Type":"Task","Resource":"arn:aws:states:::aws-sdk:s3:getObject",
				"Arguments":{"Bucket":"`+e.Bucket+`","Key":"{% $states.input.bad and $states.context.Execution.RedriveCount = 0 ? '`+e.Prefix+`missing' : '`+present+`' %}"},
				"Output":"{% $states.input %}","End":true}}},"End":true}}}`)
	if err != nil {
		return nil, err
	}
	ex, err := e.run(ctx, arn, "run", `[{"bad":false},{"bad":true},{"bad":false}]`)
	if err != nil {
		return nil, err
	}
	before, err := e.parent(ctx, ex)
	if err != nil {
		return nil, err
	}
	redrive, err := e.SFN.RedriveExecution(ctx, &sfn.RedriveExecutionInput{ExecutionArn: &ex})
	if err != nil {
		return nil, fmt.Errorf("redrive: %w", err)
	}
	if _, err := e.wait(ctx, ex); err != nil {
		return nil, err
	}
	after, err := e.parent(ctx, ex)
	if err != nil {
		return nil, err
	}
	if err := e.children(ctx, after, "SUCCEEDED"); err != nil {
		return nil, err
	}
	doc := map[string]any{"before": before, "after": after}
	doc["redrive"], err = sdk.Plain(redrive)
	if err != nil {
		return nil, err
	}
	runs, _ := after["mapRuns"].([]any)
	if len(runs) == 1 {
		run, _ := runs[0].(map[string]any)
		runARN, _ := run["MapRunArn"].(string)
		redriven, err := e.SFN.ListExecutions(ctx, &sfn.ListExecutionsInput{MapRunArn: &runARN, RedriveFilter: types.ExecutionRedriveFilterRedriven})
		if err != nil {
			return nil, err
		}
		doc["redrivenChildren"] = len(redriven.Executions)
	}
	return doc, nil
}

func dmapWriter(ctx context.Context, e *Env) (map[string]any, error) {
	if e.S3 == nil {
		return nil, errors.New("dmap-writer needs S3")
	}
	key := e.Prefix + "input.csv"
	if _, err := e.S3.PutObject(ctx, &s3.PutObjectInput{Bucket: &e.Bucket, Key: &key, Body: strings.NewReader("id\n1\n2\n3\n")}); err != nil {
		return nil, fmt.Errorf("put input: %w", err)
	}
	arn, err := e.machine(ctx, "dmap-writer", `{"StartAt":"M","States":{"M":{"Type":"Map","ToleratedFailureCount":1,
		"ItemReader":{"Resource":"arn:aws:states:::s3:getObject","ReaderConfig":{"InputType":"CSV","CSVHeaderLocation":"FIRST_ROW"},
			"Parameters":{"Bucket":"`+e.Bucket+`","Key":"`+key+`"}},
		"ItemSelector":{"id.$":"$$.Map.Item.Value.id"},
		"ItemProcessor":{"ProcessorConfig":{"Mode":"DISTRIBUTED","ExecutionType":"STANDARD"},"StartAt":"Check",
			"States":{"Check":{"Type":"Choice","Choices":[{"Variable":"$.id","StringEquals":"2","Next":"Fail"}],"Default":"Ok"},
				"Fail":{"Type":"Fail","Error":"Item.Bad","Cause":"bad item"},"Ok":{"Type":"Pass","End":true}}},
		"ResultWriter":{"Resource":"arn:aws:states:::s3:putObject","Parameters":{"Bucket":"`+e.Bucket+`","Prefix":"`+e.Prefix+`results"}},
		"End":true}}}`)
	if err != nil {
		return nil, err
	}
	ex, err := e.run(ctx, arn, "run", `{}`)
	if err != nil {
		return nil, err
	}
	doc, err := e.parent(ctx, ex)
	if err != nil {
		return nil, err
	}
	files := map[string]any{}
	prefix := e.Prefix + "results/"
	list, err := e.S3.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: &e.Bucket, Prefix: &prefix})
	if err != nil {
		return nil, fmt.Errorf("list results: %w", err)
	}
	for _, obj := range list.Contents {
		out, err := e.S3.GetObject(ctx, &s3.GetObjectInput{Bucket: &e.Bucket, Key: obj.Key})
		if err != nil {
			return nil, err
		}
		body, err := io.ReadAll(out.Body)
		out.Body.Close()
		if err != nil {
			return nil, err
		}
		var parsed any
		if json.Unmarshal(body, &parsed) != nil {
			parsed = string(body)
		}
		// The run id directory is a UUID, which normalizes away.
		files[*obj.Key] = parsed
	}
	doc["resultFiles"] = files
	return doc, nil
}

// sdkErrors catches one missing-resource error per service, to check the
// error names and the cause format.
func sdkErrors(ctx context.Context, e *Env) (map[string]any, error) {
	missing := e.Prefix + "missing"
	step := func(name, resource, params, next string) string {
		return `"` + name + `":{"Type":"Task","Resource":"arn:aws:states:::aws-sdk:` + resource + `","Parameters":` + params + `,
			"Catch":[{"ErrorEquals":["States.ALL"],"ResultPath":"$.` + name + `","Next":"` + next + `"}],"ResultPath":"$.` + name + `","Next":"` + next + `"}`
	}
	def := `{"StartAt":"S3","States":{` +
		step("S3", "s3:getObject", `{"Bucket":"`+e.Bucket+`","Key":"`+missing+`"}`, "Sfn") + `,` +
		step("Sfn", "sfn:describeExecution", `{"ExecutionArn":"arn:aws:states:`+e.Region+`:`+e.Account+`:execution:`+missing+`:x"}`, "Ssm") + `,` +
		step("Ssm", "ssm:getParameter", `{"Name":"/`+missing+`"}`, "SecretsManager") + `,` +
		step("SecretsManager", "secretsmanager:getSecretValue", `{"SecretId":"`+missing+`"}`, "Lambda") + `,` +
		step("Lambda", "lambda:invoke", `{"FunctionName":"`+missing+`","Payload":{"n":1}}`, "Done") + `,
		"Done":{"Type":"Pass","End":true}}}`
	arn, err := e.machine(ctx, "sdk-errors", def)
	if err != nil {
		return nil, err
	}
	ex, err := e.run(ctx, arn, "run", `{}`)
	if err != nil {
		return nil, err
	}
	d, err := e.describe(ctx, ex)
	if err != nil {
		return nil, err
	}
	var output any
	if text, ok := d["Output"].(string); ok {
		_ = json.Unmarshal([]byte(text), &output)
	}
	return map[string]any{"status": d["Status"], "output": output}, nil
}

// parent is a parent execution, its history, and its Map Runs.
func (e *Env) parent(ctx context.Context, ex string) (map[string]any, error) {
	doc := map[string]any{}
	var err error
	if doc["execution"], err = e.describe(ctx, ex); err != nil {
		return nil, err
	}
	if doc["history"], err = e.history(ctx, ex); err != nil {
		return nil, err
	}
	list, err := e.SFN.ListMapRuns(ctx, &sfn.ListMapRunsInput{ExecutionArn: &ex})
	if err != nil {
		return nil, fmt.Errorf("list map runs: %w", err)
	}
	var runs []any
	for _, item := range list.MapRuns {
		run, err := e.SFN.DescribeMapRun(ctx, &sfn.DescribeMapRunInput{MapRunArn: item.MapRunArn})
		if err != nil {
			return nil, fmt.Errorf("describe map run: %w", err)
		}
		p, err := sdk.Plain(run)
		if err != nil {
			return nil, err
		}
		runs = append(runs, p)
	}
	doc["mapRuns"] = runs
	return doc, nil
}

// children adds the child count by status, and for each listed status one
// child's description, history, and state machine.
func (e *Env) children(ctx context.Context, doc map[string]any, statuses ...string) error {
	runs, _ := doc["mapRuns"].([]any)
	if len(runs) == 0 {
		return nil
	}
	run, _ := runs[len(runs)-1].(map[string]any)
	runARN, _ := run["MapRunArn"].(string)
	var items []types.ExecutionListItem
	var token *string
	for {
		page, err := e.SFN.ListExecutions(ctx, &sfn.ListExecutionsInput{MapRunArn: &runARN, NextToken: token})
		if err != nil {
			return fmt.Errorf("list children: %w", err)
		}
		items = append(items, page.Executions...)
		if token = page.NextToken; token == nil {
			break
		}
	}
	counts := map[string]any{}
	for _, item := range items {
		n, _ := counts[string(item.Status)].(int)
		counts[string(item.Status)] = n + 1
	}
	doc["childCounts"] = counts
	sample := map[string]any{}
	for _, status := range statuses {
		for _, item := range items {
			if string(item.Status) != status {
				continue
			}
			child := map[string]any{}
			var err error
			if child["listItem"], err = sdk.Plain(item); err != nil {
				return err
			}
			if child["execution"], err = e.describe(ctx, *item.ExecutionArn); err != nil {
				return err
			}
			if child["history"], err = e.history(ctx, *item.ExecutionArn); err != nil {
				return err
			}
			sm, err := e.SFN.DescribeStateMachineForExecution(ctx, &sfn.DescribeStateMachineForExecutionInput{ExecutionArn: item.ExecutionArn})
			if err != nil {
				return fmt.Errorf("describe child machine: %w", err)
			}
			if child["stateMachine"], err = sdk.Plain(sm); err != nil {
				return err
			}
			sample[status] = child
			break
		}
	}
	doc["children"] = sample
	return nil
}

// machine creates a STANDARD state machine named Prefix+name.
func (e *Env) machine(ctx context.Context, name, definition string) (string, error) {
	full := e.Prefix + name
	deadline := time.Now().Add(e.RoleWait)
	for {
		out, err := e.SFN.CreateStateMachine(ctx, &sfn.CreateStateMachineInput{Name: &full, Definition: &definition, RoleArn: &e.RoleARN})
		if err == nil {
			e.machines = append(e.machines, *out.StateMachineArn)
			return *out.StateMachineArn, nil
		}
		var denied *types.InvalidArn
		if (errors.As(err, &denied) || strings.Contains(err.Error(), "role")) && time.Now().Before(deadline) {
			time.Sleep(5 * time.Second)
			continue
		}
		return "", fmt.Errorf("create %s: %w", full, err)
	}
}

// run starts an execution and waits for it to stop.
func (e *Env) run(ctx context.Context, machine, name, input string) (string, error) {
	out, err := e.SFN.StartExecution(ctx, &sfn.StartExecutionInput{StateMachineArn: &machine, Name: &name, Input: &input})
	if err != nil {
		return "", fmt.Errorf("start %s: %w", name, err)
	}
	_, err = e.wait(ctx, *out.ExecutionArn)
	return *out.ExecutionArn, err
}

func (e *Env) wait(ctx context.Context, arn string) (*sfn.DescribeExecutionOutput, error) {
	deadline := time.Now().Add(e.Timeout)
	for {
		out, err := e.SFN.DescribeExecution(ctx, &sfn.DescribeExecutionInput{ExecutionArn: &arn})
		if err != nil {
			return nil, fmt.Errorf("describe %s: %w", arn, err)
		}
		if out.Status != types.ExecutionStatusRunning {
			return out, nil
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("%s still RUNNING after %s", arn, e.Timeout)
		}
		time.Sleep(e.Poll)
	}
}

func (e *Env) describe(ctx context.Context, arn string) (map[string]any, error) {
	out, err := e.SFN.DescribeExecution(ctx, &sfn.DescribeExecutionInput{ExecutionArn: &arn})
	if err != nil {
		return nil, fmt.Errorf("describe %s: %w", arn, err)
	}
	p, err := sdk.Plain(out)
	if err != nil {
		return nil, err
	}
	m, _ := p.(map[string]any)
	return m, nil
}

func (e *Env) history(ctx context.Context, arn string) ([]any, error) {
	var events []any
	var token *string
	for {
		page, err := e.SFN.GetExecutionHistory(ctx, &sfn.GetExecutionHistoryInput{ExecutionArn: &arn, NextToken: token, MaxResults: 1000})
		if err != nil {
			return nil, fmt.Errorf("history %s: %w", arn, err)
		}
		for _, ev := range page.Events {
			p, err := sdk.Plain(ev)
			if err != nil {
				return nil, err
			}
			events = append(events, p)
		}
		if token = page.NextToken; token == nil {
			return events, nil
		}
	}
}

// Cleanup deletes the state machines this Env created.
func (e *Env) Cleanup(ctx context.Context) {
	for _, arn := range e.machines {
		_, _ = e.SFN.DeleteStateMachine(ctx, &sfn.DeleteStateMachineInput{StateMachineArn: aws.String(arn)})
	}
	e.machines = nil
}

var (
	uuidPattern    = regexp.MustCompile(`[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`)
	timePattern    = regexp.MustCompile(`\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})`)
	requestPattern = regexp.MustCompile(`Request ID: [^,)]*`)
	causePattern   = regexp.MustCompile(`^(?s)(.*) \((Service: [^,]+, Status Code: \d+)(, Request ID: <id>)?\)$`)
)

// opaqueKeys hold values that differ on every run.
var opaqueKeys = map[string]string{
	"Size": "<size>", "ContentLength": "<size>", "ETag": "<etag>", "Etag": "<etag>",
	"RoleArn": "<role>", "roleArn": "<role>", "VersionId": "<version>",
}

// Normalize replaces the parts of a document that differ between runs and
// accounts: the prefix, bucket, account, region, UUIDs, timestamps, request
// IDs, sizes, and ETags. It looks inside JSON strings too.
func (e *Env) Normalize(v any) any {
	switch x := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, val := range x {
			if repl, ok := opaqueKeys[k]; ok && val != nil {
				out[e.normString(k)] = repl
				continue
			}
			out[e.normString(k)] = e.Normalize(val)
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, val := range x {
			out[i] = e.Normalize(val)
		}
		return out
	case string:
		s := e.normString(x)
		var inner any
		if (strings.HasPrefix(s, "{") || strings.HasPrefix(s, "[")) && json.Unmarshal([]byte(x), &inner) == nil {
			text, _ := json.Marshal(e.Normalize(inner))
			return string(text)
		}
		return s
	default:
		return v
	}
}

func (e *Env) normString(s string) string {
	// The bucket name starts with the prefix, so it goes first.
	if e.Bucket != "" {
		s = strings.ReplaceAll(s, e.Bucket, "<bucket>")
	}
	if e.Prefix != "" {
		s = strings.ReplaceAll(s, e.Prefix, "")
	}
	s = strings.ReplaceAll(s, e.Account, "<account>")
	s = strings.ReplaceAll(s, ":"+e.Region+":", ":<region>:")
	s = uuidPattern.ReplaceAllString(s, "<uuid>")
	s = timePattern.ReplaceAllString(s, "<time>")
	return requestPattern.ReplaceAllString(s, "Request ID: <id>")
}

// Loosen drops what a fake service decides rather than this server: the
// message and request ID of an aws-sdk error cause.
func Loosen(v any) any {
	switch x := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, val := range x {
			out[k] = Loosen(val)
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, val := range x {
			out[i] = Loosen(val)
		}
		return out
	case string:
		return causePattern.ReplaceAllString(x, "<message> ($2)")
	default:
		return v
	}
}

// Diff lists every path where want and got differ, sorted.
func Diff(want, got any) []string {
	var out []string
	diff("", want, got, &out)
	sort.Strings(out)
	return out
}

func diff(path string, want, got any, out *[]string) {
	switch w := want.(type) {
	case map[string]any:
		g, ok := got.(map[string]any)
		if !ok {
			*out = append(*out, fmt.Sprintf("%s: AWS %s, local %s", orRoot(path), short(want), short(got)))
			return
		}
		for k, wv := range w {
			gv, ok := g[k]
			if !ok {
				*out = append(*out, fmt.Sprintf("%s.%s: missing locally (AWS %s)", path, k, short(wv)))
				continue
			}
			diff(path+"."+k, wv, gv, out)
		}
		for k, gv := range g {
			if _, ok := w[k]; !ok {
				*out = append(*out, fmt.Sprintf("%s.%s: only local (%s)", path, k, short(gv)))
			}
		}
	case []any:
		g, ok := got.([]any)
		if !ok {
			*out = append(*out, fmt.Sprintf("%s: AWS %s, local %s", orRoot(path), short(want), short(got)))
			return
		}
		if len(g) != len(w) {
			*out = append(*out, fmt.Sprintf("%s: AWS has %d entries, local %d", orRoot(path), len(w), len(g)))
		}
		for i := 0; i < len(w) && i < len(g); i++ {
			diff(path+"["+strconv.Itoa(i)+"]", w[i], g[i], out)
		}
	default:
		wt, _ := json.Marshal(want)
		gt, _ := json.Marshal(got)
		if string(wt) != string(gt) {
			*out = append(*out, fmt.Sprintf("%s: AWS %s, local %s", orRoot(path), wt, gt))
		}
	}
}

func orRoot(path string) string {
	if path == "" {
		return "."
	}
	return path
}

func short(v any) string {
	text, _ := json.Marshal(v)
	if len(text) > 300 {
		return string(text[:300]) + "..."
	}
	return string(text)
}

// Canonical round-trips a document through JSON, so numbers and nesting
// match a document read back from a file.
func Canonical(v any) (any, error) {
	text, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	var out any
	err = json.Unmarshal(text, &out)
	return out, err
}
