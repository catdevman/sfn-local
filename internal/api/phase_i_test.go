package api

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/catdevman/sfn-local/internal/api/gen"
	"github.com/catdevman/sfn-local/internal/sdk"
	"github.com/catdevman/sfn-local/internal/store"
)

func TestPhaseILambdaInvoke(t *testing.T) {
	s := newService()
	opt := `{"StartAt":"Call","States":{"Call":{"Type":"Task","Resource":"arn:aws:states:::lambda:invoke","Parameters":{"FunctionName":"fn","Payload":{"n":1}},"End":true}}}`
	empty := runMachine(t, s, "lambda-empty", opt, "e", `{}`)
	if empty.Status != store.ExecFailed || empty.Error != "States.Runtime" || !strings.Contains(empty.Cause, "unsupported resource") || !strings.Contains(empty.Cause, "lambda:invoke") {
		t.Fatalf("empty endpoint %s %s", empty.Error, empty.Cause)
	}

	var sawDirect, sawOpt bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Amz-Invocation-Type") != "RequestResponse" {
			t.Errorf("invocation type %s", r.Header.Get("X-Amz-Invocation-Type"))
		}
		if !strings.Contains(r.URL.Path, "/2015-03-31/functions/") || !strings.HasSuffix(r.URL.Path, "/invocations") {
			t.Errorf("path %s", r.URL.Path)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		switch strings.TrimSpace(string(body)) {
		case `{"n":1}`:
			if strings.Contains(r.URL.Path, "/functions/direct/") {
				sawDirect = true
			}
			if strings.Contains(r.URL.Path, "/functions/fn/") {
				sawOpt = true
			}
			w.Write([]byte(`{"ok":true}`))
		case `{"bad":true}`:
			w.Header().Set("X-Amz-Function-Error", "Unhandled")
			w.Write([]byte(`{"errorType":"Boom","errorMessage":"no"}`))
		default:
			t.Errorf("payload %s path %s", body, r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer srv.Close()
	s.Runner.LambdaEndpoint = srv.URL

	direct := `{"StartAt":"Call","States":{"Call":{"Type":"Task","Resource":"arn:aws:lambda:us-east-1:123456789012:function:direct","End":true}}}`
	raw := runMachine(t, s, "lambda-direct", direct, "d", `{"n":1}`)
	if raw.Status != store.ExecSucceeded || raw.Output != `{"ok":true}` || countEvents(raw, "LambdaFunctionSucceeded") != 1 {
		t.Fatalf("direct %s %s %s %v", raw.Status, raw.Output, raw.Cause, typesOf(raw))
	}

	got := runMachine(t, s, "lambda-opt", opt, "o", `{}`)
	if got.Status != store.ExecSucceeded {
		t.Fatalf("optimized %s %s %v", got.Error, got.Cause, typesOf(got))
	}
	jsonEqual(t, got.Output, `{"StatusCode":200,"Payload":{"ok":true}}`)
	if countEvents(got, "LambdaFunctionScheduled") != 0 || countEvents(got, "TaskScheduled") != 1 || countEvents(got, "TaskSucceeded") != 1 {
		t.Fatalf("optimized history %v", typesOf(got))
	}
	if !sawDirect || !sawOpt {
		t.Fatalf("direct %v optimized %v", sawDirect, sawOpt)
	}

	bad := `{"StartAt":"Call","States":{"Call":{"Type":"Task","Resource":"arn:aws:states:::lambda:invoke","Parameters":{"FunctionName":"fn","Payload":{"bad":true}},"End":true}}}`
	fx := runMachine(t, s, "lambda-fe", bad, "f", `{}`)
	if fx.Status != store.ExecFailed || fx.Error != "Boom" || fx.Cause != "no" || countEvents(fx, "TaskFailed") != 1 {
		t.Fatalf("function error %s %s %v", fx.Error, fx.Cause, typesOf(fx))
	}

	missing := `{"StartAt":"Call","States":{"Call":{"Type":"Task","Resource":"arn:aws:states:::lambda:invoke","Parameters":{"Payload":{}},"End":true}}}`
	mx := runMachine(t, s, "lambda-miss", missing, "m", `{}`)
	if mx.Status != store.ExecFailed || mx.Error != "States.Runtime" || !strings.Contains(mx.Cause, "FunctionName is required") || countEvents(mx, "TaskScheduled") != 1 || countEvents(mx, "TaskSucceeded") != 0 {
		t.Fatalf("missing name %s %s %v", mx.Error, mx.Cause, typesOf(mx))
	}
}

func TestPhaseIQueryIntegrations(t *testing.T) {
	s := newService()
	var sqsTarget, sqsBody string
	sqs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Content-Type") != "application/x-amz-json-1.0" {
			t.Errorf("content type %s", r.Header.Get("Content-Type"))
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		sqsTarget, sqsBody = r.Header.Get("X-Amz-Target"), string(body)
		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		if strings.Contains(sqsBody, "missing") {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(`{"__type":"com.amazonaws.sqs#QueueDoesNotExist","message":"The specified queue does not exist."}`))
			return
		}
		var req struct{ MessageBody string }
		json.Unmarshal(body, &req)
		sum := md5.Sum([]byte(req.MessageBody))
		w.Write([]byte(`{"MD5OfMessageBody":"` + hex.EncodeToString(sum[:]) + `","MessageId":"m1"}`))
	}))
	defer sqs.Close()

	var snsBody string
	sns := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		snsBody = string(body)
		w.Header().Set("Content-Type", "text/xml")
		w.Write([]byte(`<PublishResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/"><PublishResult><MessageId>mid</MessageId></PublishResult><ResponseMetadata><RequestId>r1</RequestId></ResponseMetadata></PublishResponse>`))
	}))
	defer sns.Close()
	s.Runner.SDK = sdk.New(sdk.Config{Region: "us-east-1", Endpoints: map[string]string{"sqs": sqs.URL, "sns": sns.URL}})

	sqsDef := `{"StartAt":"Call","States":{"Call":{"Type":"Task","Resource":"arn:aws:states:::sqs:sendMessage","Parameters":{"QueueUrl":"https://sqs.local/q","MessageBody":{"order":"o1"},"DelaySeconds":5},"End":true}}}`
	sx := runMachine(t, s, "sqs", sqsDef, "s", `{}`)
	if sx.Status != store.ExecSucceeded {
		t.Fatalf("sqs %s %s", sx.Error, sx.Cause)
	}
	jsonEqual(t, sx.Output, `{"MD5OfMessageBody":"e7203d3d5a2f2356e399e2fde28cb2e8","MessageId":"m1"}`)
	if sqsTarget != "AmazonSQS.SendMessage" {
		t.Fatal(sqsTarget)
	}
	jsonEqual(t, sqsBody, `{"QueueUrl":"https://sqs.local/q","MessageBody":"{\"order\":\"o1\"}","DelaySeconds":5}`)

	missing := `{"StartAt":"Call","States":{"Call":{"Type":"Task","Resource":"arn:aws:states:::sqs:sendMessage","Parameters":{"QueueUrl":"https://sqs.local/missing","MessageBody":"hi"},"End":true}}}`
	mx := runMachine(t, s, "sqs-missing", missing, "m", `{}`)
	if mx.Status != store.ExecFailed || mx.Error != "SQS.QueueDoesNotExistException" || !strings.HasPrefix(mx.Cause, "The specified queue does not exist. (Service: SQS, Status Code: 400") {
		t.Fatalf("sqs missing %s %s", mx.Error, mx.Cause)
	}

	snsDef := `{"StartAt":"Call","States":{"Call":{"Type":"Task","Resource":"arn:aws:states:::sns:publish","Parameters":{"TopicArn":"arn:aws:sns:us-east-1:123456789012:t","Message":"hi"},"End":true}}}`
	nx := runMachine(t, s, "sns", snsDef, "n", `{}`)
	if nx.Status != store.ExecSucceeded || nx.Output != `{"MessageId":"mid"}` {
		t.Fatalf("sns %s %s %s", nx.Status, nx.Output, nx.Cause)
	}
	if !strings.Contains(snsBody, "Action=Publish") || !strings.Contains(snsBody, "Version=2010-03-31") || !strings.Contains(snsBody, "Message=hi") {
		t.Fatalf("sns form %s", snsBody)
	}
}

func TestPhaseIDynamoDBAndEventBridge(t *testing.T) {
	s := newService()
	var mu sync.Mutex
	var targets []string
	var bodies []string
	ddb := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Content-Type") != "application/x-amz-json-1.0" {
			t.Errorf("content type %s", r.Header.Get("Content-Type"))
		}
		if auth := r.Header.Get("Authorization"); !strings.HasPrefix(auth, "AWS4-HMAC-SHA256 Credential=local/") || !strings.Contains(auth, "/us-east-1/dynamodb/aws4_request") {
			t.Errorf("authorization %q", auth)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		target := r.Header.Get("X-Amz-Target")
		mu.Lock()
		targets = append(targets, target)
		bodies = append(bodies, string(body))
		mu.Unlock()
		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		switch {
		case strings.Contains(string(body), "attribute_not_exists"):
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(`{"__type":"com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException","message":"The conditional request failed"}`))
		case target == "DynamoDB_20120810.GetItem":
			w.Write([]byte(`{"Item":{"pk":{"S":"a"},"n":{"N":"1"}}}`))
		case target == "DynamoDB_20120810.DeleteItem":
			w.Write([]byte(`{"Attributes":{"pk":{"S":"a"}}}`))
		default:
			w.Write([]byte(`{}`))
		}
	}))
	defer ddb.Close()

	var ebTarget, ebBody string
	eb := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Content-Type") != "application/x-amz-json-1.1" {
			t.Errorf("content type %s", r.Header.Get("Content-Type"))
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		ebTarget = r.Header.Get("X-Amz-Target")
		ebBody = string(body)
		w.Write([]byte(`{"FailedEntryCount":0,"Entries":[{"EventId":"e1"}]}`))
	}))
	defer eb.Close()
	s.Runner.SDK = sdk.New(sdk.Config{Region: "us-east-1", AccessKeyID: "local", SecretAccessKey: "local", Endpoints: map[string]string{"dynamodb": ddb.URL, "eventbridge": eb.URL}})

	def := `{"StartAt":"Get","States":{` +
		`"Get":{"Type":"Task","Resource":"arn:aws:states:::dynamodb:getItem","Parameters":{"TableName":"T","Key":{"pk":{"S":"a"}}},"ResultPath":"$.got","Next":"Put"},` +
		`"Put":{"Type":"Task","Resource":"arn:aws:states:::dynamodb:putItem","Parameters":{"TableName":"T","Item":{"pk":{"S":"a"}}},"ResultPath":null,"Next":"Update"},` +
		`"Update":{"Type":"Task","Resource":"arn:aws:states:::dynamodb:updateItem","Parameters":{"TableName":"T","Key":{"pk":{"S":"a"}},"UpdateExpression":"SET n = :n","ExpressionAttributeValues":{":n":{"N":"2"}}},"ResultPath":null,"Next":"Delete"},` +
		`"Delete":{"Type":"Task","Resource":"arn:aws:states:::dynamodb:deleteItem","Parameters":{"TableName":"T","Key":{"pk":{"S":"a"}},"ReturnValues":"ALL_OLD"},"ResultPath":"$.deleted","End":true}}}`
	ex := runMachine(t, s, "ddb", def, "d", `{}`)
	if ex.Status != store.ExecSucceeded {
		t.Fatalf("dynamodb %s %s %s %v", ex.Status, ex.Output, ex.Cause, typesOf(ex))
	}
	jsonEqual(t, ex.Output, `{"got":{"Item":{"pk":{"S":"a"},"n":{"N":"1"}}},"deleted":{"Attributes":{"pk":{"S":"a"}}}}`)
	wantTargets := []string{
		"DynamoDB_20120810.GetItem",
		"DynamoDB_20120810.PutItem",
		"DynamoDB_20120810.UpdateItem",
		"DynamoDB_20120810.DeleteItem",
	}
	if strings.Join(targets, ",") != strings.Join(wantTargets, ",") {
		t.Fatalf("targets %v", targets)
	}
	jsonEqual(t, bodies[0], `{"TableName":"T","Key":{"pk":{"S":"a"}}}`)

	cond := `{"StartAt":"Put","States":{"Put":{"Type":"Task","Resource":"arn:aws:states:::dynamodb:putItem","Parameters":{"TableName":"T","Item":{"pk":{"S":"a"}},"ConditionExpression":"attribute_not_exists(pk)"},"End":true}}}`
	cx := runMachine(t, s, "ddb-cond", cond, "c", `{}`)
	if cx.Status != store.ExecFailed || cx.Error != "DynamoDB.ConditionalCheckFailedException" || !strings.HasPrefix(cx.Cause, "The conditional request failed (Service: DynamoDB, Status Code: 400") {
		t.Fatalf("condition %s %s", cx.Error, cx.Cause)
	}

	ebDef := `{"StartAt":"Call","States":{"Call":{"Type":"Task","Resource":"arn:aws:states:::events:putEvents","Parameters":{"Entries":[{"Source":"app","DetailType":"x","Detail":{"k":1}}]},"End":true}}}`
	bx := runMachine(t, s, "events", ebDef, "b", `{}`)
	if bx.Status != store.ExecSucceeded {
		t.Fatalf("events %s %s", bx.Error, bx.Cause)
	}
	jsonEqual(t, bx.Output, `{"Entries":[{"EventId":"e1"}],"FailedEntryCount":0}`)
	if ebTarget != "AWSEvents.PutEvents" {
		t.Fatal(ebTarget)
	}
	jsonEqual(t, ebBody, `{"Entries":[{"Detail":"{\"k\":1}","DetailType":"x","Source":"app"}]}`)
}

func TestPhaseIStartExecutionSync(t *testing.T) {
	s := newService()
	const (
		plainARN = "arn:aws:states:us-east-1:123456789012:execution:child:plain"
		syncARN  = "arn:aws:states:us-east-1:123456789012:execution:child:sync"
		sync2ARN = "arn:aws:states:us-east-1:123456789012:execution:child:sync2"
		boomARN  = "arn:aws:states:us-east-1:123456789012:execution:child:boom"
	)
	var mu sync.Mutex
	describes := map[string]int{}
	var sqsSyncHits int
	sfn := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Content-Type") != "application/x-amz-json-1.0" {
			t.Errorf("content type %s", r.Header.Get("Content-Type"))
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		text := string(body)
		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		switch r.Header.Get("X-Amz-Target") {
		case "AWSStepFunctions.StartExecution":
			switch {
			case strings.Contains(text, `"name":"bad"`):
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte("bad start"))
			case strings.Contains(text, `"name":"plain"`):
				w.Write([]byte(`{"executionArn":"` + plainARN + `","startDate":1715990400}`))
			case strings.Contains(text, `"name":"sync2"`):
				w.Write([]byte(`{"executionArn":"` + sync2ARN + `","startDate":1715990400}`))
			case strings.Contains(text, `"name":"boom"`):
				w.Write([]byte(`{"executionArn":"` + boomARN + `","startDate":1715990400}`))
			case strings.Contains(text, `"name":"sync"`):
				w.Write([]byte(`{"executionArn":"` + syncARN + `","startDate":1715990400}`))
			default:
				t.Errorf("start body %s", text)
				w.WriteHeader(http.StatusBadRequest)
			}
		case "AWSStepFunctions.DescribeExecution":
			mu.Lock()
			describes[text]++
			n := describes[text]
			mu.Unlock()
			switch {
			case strings.Contains(text, boomARN):
				w.Write([]byte(`{"status":"FAILED","error":"Boom","cause":"nope"}`))
			case strings.Contains(text, sync2ARN):
				w.Write([]byte(`{"executionArn":"` + sync2ARN + `","status":"SUCCEEDED","output":"{\"ok\":true}"}`))
			case strings.Contains(text, syncARN):
				if n == 1 {
					w.Write([]byte(`{"status":"RUNNING"}`))
					return
				}
				w.Write([]byte(`{"executionArn":"` + syncARN + `","status":"SUCCEEDED","output":"{\"ok\":true}"}`))
			default:
				t.Errorf("describe %s", text)
				w.WriteHeader(http.StatusBadRequest)
			}
		default:
			t.Errorf("target %s", r.Header.Get("X-Amz-Target"))
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer sfn.Close()
	s.Runner.StepFunctionsEndpoint = sfn.URL

	start := func(name, resource string) string {
		return `{"StartAt":"Call","States":{"Call":{"Type":"Task","Resource":"` + resource + `","Parameters":{"stateMachineArn":"arn:aws:states:us-east-1:123456789012:stateMachine:child","name":"` + name + `","input":"{}"},"End":true}}}`
	}
	plain := runMachine(t, s, "start-plain", start("plain", "arn:aws:states:::states:startExecution"), "p", `{}`)
	if plain.Status != store.ExecSucceeded {
		t.Fatalf("plain %s %s %v", plain.Error, plain.Cause, typesOf(plain))
	}
	jsonEqual(t, plain.Output, `{"executionArn":"`+plainARN+`","startDate":1715990400}`)
	if countEvents(plain, "TaskSubmitted") != 0 || countEvents(plain, "TaskSucceeded") != 1 {
		t.Fatalf("plain history %v", typesOf(plain))
	}

	syncEx := runMachine(t, s, "start-sync", start("sync", "arn:aws:states:::states:startExecution.sync"), "y", `{}`)
	if syncEx.Status != store.ExecSucceeded {
		t.Fatalf("sync %s %s %v", syncEx.Error, syncEx.Cause, typesOf(syncEx))
	}
	jsonEqual(t, syncEx.Output, `{"executionArn":"`+syncARN+`","output":"{\"ok\":true}","status":"SUCCEEDED"}`)
	if countEvents(syncEx, "TaskSubmitted") != 1 || countEvents(syncEx, "TaskSucceeded") != 1 {
		t.Fatalf("sync history %v", typesOf(syncEx))
	}
	var submitted string
	for _, ev := range syncEx.History {
		if ev.Type == "TaskSubmitted" {
			submitted = ev.Output
		}
	}
	jsonEqual(t, submitted, `{"executionArn":"`+syncARN+`","startDate":1715990400}`)
	if describes[`{"executionArn":"`+syncARN+`"}`] < 2 {
		t.Fatalf("describe calls %+v", describes)
	}

	sync2 := runMachine(t, s, "start-sync2", start("sync2", "arn:aws:states:::states:startExecution.sync:2"), "z", `{}`)
	if sync2.Status != store.ExecSucceeded {
		t.Fatalf("sync2 %s %s %v", sync2.Error, sync2.Cause, typesOf(sync2))
	}
	jsonEqual(t, sync2.Output, `{"ok":true}`)
	if countEvents(sync2, "TaskSubmitted") != 1 || countEvents(sync2, "TaskSucceeded") != 1 {
		t.Fatalf("sync2 history %v", typesOf(sync2))
	}

	boom := runMachine(t, s, "start-boom", start("boom", "arn:aws:states:::states:startExecution.sync"), "b", `{}`)
	if boom.Status != store.ExecFailed || boom.Error != "Boom" || boom.Cause != "nope" || countEvents(boom, "TaskSubmitted") != 1 || countEvents(boom, "TaskFailed") != 1 || countEvents(boom, "TaskSubmitFailed") != 0 {
		t.Fatalf("boom %s %s %v", boom.Error, boom.Cause, typesOf(boom))
	}

	bad := runMachine(t, s, "start-bad", start("bad", "arn:aws:states:::states:startExecution.sync"), "a", `{}`)
	if bad.Status != store.ExecFailed || bad.Error != "StepFunctions.ServiceException" || bad.Cause != "bad start" || countEvents(bad, "TaskSubmitFailed") != 1 || countEvents(bad, "TaskSubmitted") != 0 || countEvents(bad, "TaskSucceeded") != 0 {
		t.Fatalf("submit %s %s %v", bad.Error, bad.Cause, typesOf(bad))
	}
	hist, err := s.GetExecutionHistory(context.Background(), &gen.GetExecutionHistoryInput{ExecutionArn: sp(bad.ARN)})
	if err != nil || hist.Events == nil {
		t.Fatal(err)
	}
	sawSubmit := false
	for _, ev := range *hist.Events {
		if ev.Type == nil || *ev.Type != gen.HistoryEventTypeTaskSubmitFailed {
			continue
		}
		sawSubmit = true
		d := ev.TaskSubmitFailedEventDetails
		if d == nil || str(d.Error) != "StepFunctions.ServiceException" || str(d.Cause) != "bad start" || str(d.Resource) != "startExecution.sync" || str(d.ResourceType) != "states" {
			t.Fatalf("submit details %+v", d)
		}
	}
	if !sawSubmit {
		t.Fatal("history omitted TaskSubmitFailed")
	}

	sqs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sqsSyncHits++
		w.WriteHeader(http.StatusOK)
	}))
	defer sqs.Close()
	s.Runner.SDK = sdk.New(sdk.Config{Endpoints: map[string]string{"sqs": sqs.URL}})
	sqsSync := `{"StartAt":"Call","States":{"Call":{"Type":"Task","Resource":"arn:aws:states:::sqs:sendMessage.sync","Parameters":{"QueueUrl":"https://sqs.local/q","MessageBody":"hello"},"End":true}}}`
	qx := runMachine(t, s, "sqs-sync", sqsSync, "q", `{}`)
	if qx.Status != store.ExecFailed || qx.Error != "States.Runtime" || !strings.Contains(qx.Cause, "unsupported resource") || !strings.Contains(qx.Cause, "sqs:sendMessage.sync") || sqsSyncHits != 0 || countEvents(qx, "TaskScheduled") != 0 {
		t.Fatalf("sqs sync %s %s hits %d %v", qx.Error, qx.Cause, sqsSyncHits, typesOf(qx))
	}
}
