package api

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/catdevman/sfn-local/internal/sdk"
	"github.com/catdevman/sfn-local/internal/store"
)

func TestAWSSDKLambdaInvoke(t *testing.T) {
	s := newService()
	var mu sync.Mutex
	var got string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/functions/echo/invocations") {
			w.Header().Set("X-Amzn-ErrorType", "ResourceNotFoundException")
			w.WriteHeader(http.StatusNotFound)
			io.WriteString(w, `{"Type":"User","Message":"Function not found: nope"}`)
			return
		}
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		got = string(body)
		mu.Unlock()
		w.Header().Set("X-Amz-Executed-Version", "$LATEST")
		io.WriteString(w, `{"echo":`+string(body)+`}`)
	}))
	defer srv.Close()
	s.Runner.SDK = sdk.New(sdk.Config{Region: "us-east-1", Endpoints: map[string]string{"lambda": srv.URL}})
	def := `{"StartAt":"Call","States":{
		"Call":{"Type":"Task","Resource":"arn:aws:states:::aws-sdk:lambda:invoke","Parameters":{"FunctionName":"echo","Payload.$":"$"},"ResultPath":"$.call","Next":"Missing"},
		"Missing":{"Type":"Task","Resource":"arn:aws:states:::aws-sdk:lambda:invoke","Parameters":{"FunctionName":"nope"},
			"Catch":[{"ErrorEquals":["Lambda.ResourceNotFoundException"],"ResultPath":"$.missing","Next":"Done"}],"End":true},
		"Done":{"Type":"Pass","End":true}}}`
	ex := runMachine(t, s, "lambda-sdk", def, "run", `{"n":1}`)
	if ex.Status != store.ExecSucceeded {
		t.Fatalf("%s %s %s", ex.Status, ex.Error, ex.Cause)
	}
	if got != `{"n":1}` {
		t.Fatalf("sent payload %q", got)
	}
	var out struct {
		Call    map[string]any
		Missing map[string]any
	}
	if err := json.Unmarshal([]byte(ex.Output), &out); err != nil {
		t.Fatal(err)
	}
	if out.Call["Payload"] != `{"echo":{"n":1}}` || out.Call["StatusCode"] != float64(200) || out.Call["ExecutedVersion"] != "$LATEST" {
		t.Fatalf("call %v", out.Call)
	}
	if out.Missing["Error"] != "Lambda.ResourceNotFoundException" || !strings.HasPrefix(out.Missing["Cause"].(string), "Function not found: nope (Service: Lambda, Status Code: 404") {
		t.Fatalf("missing %v", out.Missing)
	}
}

// TestAWSSDKSecretsAndParameters runs Secrets Manager and SSM against one
// fake AWS JSON 1.1 endpoint.
func TestAWSSDKSecretsAndParameters(t *testing.T) {
	s := newService()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var in map[string]any
		json.NewDecoder(r.Body).Decode(&in)
		w.Header().Set("Content-Type", "application/x-amz-json-1.1")
		switch r.Header.Get("X-Amz-Target") {
		case "secretsmanager.GetSecretValue":
			if in["SecretId"] != "db" {
				w.WriteHeader(http.StatusBadRequest)
				io.WriteString(w, `{"__type":"ResourceNotFoundException","message":"Secrets Manager can't find the specified secret."}`)
				return
			}
			io.WriteString(w, `{"ARN":"arn:aws:secretsmanager:us-east-1:123456789012:secret:db-AbCdEf","Name":"db","SecretString":"{\"password\":\"hunter2\"}","VersionId":"v1","VersionStages":["AWSCURRENT"],"CreatedDate":1.7e9}`)
		case "AmazonSSM.GetParameter":
			if in["Name"] != "/app/url" {
				w.WriteHeader(http.StatusBadRequest)
				io.WriteString(w, `{"__type":"ParameterNotFound","message":""}`)
				return
			}
			io.WriteString(w, `{"Parameter":{"Name":"/app/url","Type":"String","Value":"https://example.test","Version":3,"LastModifiedDate":1.7e9}}`)
		default:
			w.WriteHeader(http.StatusBadRequest)
			io.WriteString(w, `{"__type":"UnknownOperationException"}`)
		}
	}))
	defer srv.Close()
	s.Runner.SDK = sdk.New(sdk.Config{Region: "us-east-1", Endpoints: map[string]string{"secretsmanager": srv.URL, "ssm": srv.URL}})
	def := `{"StartAt":"Secret","States":{
		"Secret":{"Type":"Task","Resource":"arn:aws:states:::aws-sdk:secretsmanager:getSecretValue","Parameters":{"SecretId":"db"},
			"ResultSelector":{"value.$":"States.StringToJson($.SecretString)","created.$":"$.CreatedDate"},"ResultPath":"$.secret","Next":"Param"},
		"Param":{"Type":"Task","Resource":"arn:aws:states:::aws-sdk:ssm:getParameter","Parameters":{"Name":"/app/url"},"ResultSelector":{"url.$":"$.Parameter.Value","v.$":"$.Parameter.Version"},"ResultPath":"$.param","Next":"NoSecret"},
		"NoSecret":{"Type":"Task","Resource":"arn:aws:states:::aws-sdk:secretsmanager:getSecretValue","Parameters":{"SecretId":"nope"},
			"Catch":[{"ErrorEquals":["SecretsManager.ResourceNotFoundException"],"ResultPath":"$.noSecret","Next":"NoParam"}],"Next":"NoParam"},
		"NoParam":{"Type":"Task","Resource":"arn:aws:states:::aws-sdk:ssm:getParameter","Parameters":{"Name":"/nope"},
			"Catch":[{"ErrorEquals":["Ssm.ParameterNotFoundException"],"ResultPath":"$.noParam","Next":"Done"}],"Next":"Done"},
		"Done":{"Type":"Pass","Parameters":{"secret.$":"$.secret","param.$":"$.param","errors.$":"States.Array($.noSecret.Error, $.noParam.Error)"},"End":true}}}`
	ex := runMachine(t, s, "secrets", def, "run", `{}`)
	if ex.Status != store.ExecSucceeded {
		t.Fatalf("%s %s %s", ex.Status, ex.Error, ex.Cause)
	}
	jsonEqual(t, ex.Output, `{"secret":{"value":{"password":"hunter2"},"created":"2023-11-14T22:13:20Z"},"param":{"url":"https://example.test","v":3},"errors":["SecretsManager.ResourceNotFoundException","Ssm.ParameterNotFoundException"]}`)
}

// TestAWSSDKSfnCallsThisServer points aws-sdk:sfn at this server's own API.
func TestAWSSDKSfnCallsThisServer(t *testing.T) {
	s := newService()
	surface, err := LoadSurface()
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(&Server{Handlers: s, Surface: surface, Profile: ProfileSmithy, Log: io.Discard})
	defer srv.Close()
	s.Runner.SDK = sdk.New(sdk.Config{Region: "us-east-1", Endpoints: map[string]string{"sfn": srv.URL}})
	child := createMachine(t, s, "child", `{"StartAt":"P","States":{"P":{"Type":"Pass","Result":"hi","End":true}}}`, roleARN)
	def := `{"StartAt":"Start","States":{
		"Start":{"Type":"Task","Resource":"arn:aws:states:::aws-sdk:sfn:startExecution","Parameters":{"StateMachineArn":"` + *child.StateMachineArn + `","Name":"from-sdk","Input":"{}"},"ResultSelector":{"arn.$":"$.ExecutionArn"},"ResultPath":"$.started","Next":"Missing"},
		"Missing":{"Type":"Task","Resource":"arn:aws:states:::aws-sdk:sfn:describeExecution","Parameters":{"ExecutionArn":"arn:aws:states:us-east-1:123456789012:execution:child:nope"},
			"Catch":[{"ErrorEquals":["Sfn.ExecutionDoesNotExistException"],"ResultPath":"$.missing","Next":"Done"}],"Next":"Done"},
		"Done":{"Type":"Pass","Parameters":{"arn.$":"$.started.arn","error.$":"$.missing.Error"},"End":true}}}`
	ex := runMachine(t, s, "caller", def, "run", `{}`)
	if ex.Status != store.ExecSucceeded {
		t.Fatalf("%s %s %s", ex.Status, ex.Error, ex.Cause)
	}
	jsonEqual(t, ex.Output, `{"arn":"arn:aws:states:us-east-1:123456789012:execution:child:from-sdk","error":"Sfn.ExecutionDoesNotExistException"}`)
	if started := waitExec(t, s, "arn:aws:states:us-east-1:123456789012:execution:child:from-sdk"); started.Output != `"hi"` {
		t.Fatalf("child %s %s", started.Status, started.Output)
	}
}

func TestUnsupportedResourceHints(t *testing.T) {
	s := newService()
	s.Runner.SDK = sdk.New(sdk.Config{Region: "us-east-1", Endpoints: map[string]string{"s3": "http://127.0.0.1:1"}})
	n := 0
	for resource, hint := range map[string]string{
		"arn:aws:states:::lambda:invoke":                         "(set LAMBDA_ENDPOINT (--lambda-endpoint))",
		"arn:aws:lambda:us-east-1:123456789012:function:hello":   "(set LAMBDA_ENDPOINT (--lambda-endpoint))",
		"arn:aws:states:::states:startExecution.sync":            "(set STEP_FUNCTIONS_ENDPOINT (--step-functions-endpoint))",
		"arn:aws:states:::sqs:sendMessage.sync":                  "(only arn:aws:states:::states:startExecution runs .sync)",
		"arn:aws:states:::dynamodb:putItem":                      "(set AWS_ENDPOINT_URL_DYNAMODB, DYNAMODB_ENDPOINT (--dynamodb-endpoint), or AWS_ENDPOINT_URL)",
		"arn:aws:states:::aws-sdk:secretsmanager:getSecretValue": "(set AWS_ENDPOINT_URL_SECRETS_MANAGER, or AWS_ENDPOINT_URL)",
		"arn:aws:states:::aws-sdk:s3:selectObjectContent":        "(this server does not run s3:selectObjectContent; internal/invoke/operations.toml lists the operations that run)",
		"arn:aws:states:::aws-sdk:nope:doThing":                  "(this server has no aws-sdk:nope service)",
	} {
		def := `{"StartAt":"T","States":{"T":{"Type":"Task","Resource":"` + resource + `","Parameters":{"FunctionName":"hello","StateMachineArn":"arn:aws:states:us-east-1:123456789012:stateMachine:x"},"End":true}}}`
		n++
		ex := runMachine(t, s, "hint-"+strconv.Itoa(n), def, "run", `{}`)
		if ex.Status != store.ExecFailed || ex.Error != "States.Runtime" || ex.Cause != "unsupported resource: "+resource+" "+hint {
			t.Errorf("%s: %s %s", resource, ex.Error, ex.Cause)
		}
	}
}
