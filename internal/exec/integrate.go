package exec

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/catdevman/sfn-local/internal/sdk"
	"github.com/catdevman/sfn-local/internal/store"
)

const syncPoll = 20 * time.Millisecond

// integrationResource reports an optimized integration this process can call.
// mode is "", "sync", or "sync2". .waitForTaskToken is not an integration.
func integrationResource(resource string) (base, mode string, ok bool) {
	switch {
	case strings.HasSuffix(resource, ".sync:2"):
		base = strings.TrimSuffix(resource, ".sync:2")
		mode = "sync2"
	case strings.HasSuffix(resource, ".sync"):
		base = strings.TrimSuffix(resource, ".sync")
		mode = "sync"
	default:
		if strings.HasSuffix(resource, ".waitForTaskToken") {
			return "", "", false
		}
		base = resource
	}
	switch base {
	case "arn:aws:states:::lambda:invoke",
		"arn:aws:states:::sqs:sendMessage",
		"arn:aws:states:::sns:publish",
		"arn:aws:states:::dynamodb:getItem",
		"arn:aws:states:::dynamodb:putItem",
		"arn:aws:states:::dynamodb:updateItem",
		"arn:aws:states:::dynamodb:deleteItem",
		"arn:aws:states:::states:startExecution",
		"arn:aws:states:::events:putEvents":
		return base, mode, true
	default:
		return "", "", false
	}
}

// integrationReady reports whether an optimized integration can be called.
// DynamoDB, SQS, SNS, and EventBridge run through internal/sdk.
func (r *Runner) integrationReady(base string) bool {
	if r == nil {
		return false
	}
	switch base {
	case "arn:aws:states:::lambda:invoke":
		return r.LambdaEndpoint != ""
	case "arn:aws:states:::states:startExecution":
		return r.StepFunctionsEndpoint != ""
	default:
		return r.SDK.SupportsIntegration(base)
	}
}

func (r *Runner) invokeService(callCtx, parent context.Context, arn string, meta taskMeta, taskWins bool) (string, *taskFailure) {
	if id, action, ok := sdk.ParseResource(meta.resource); ok {
		return r.invokeSDK(callCtx, parent, id, action, meta.input, taskWins)
	}
	base, mode, ok := integrationResource(meta.resource)
	if !ok || (mode != "" && base != "arn:aws:states:::states:startExecution") {
		return "", &taskFailure{Name: "States.Runtime", Cause: "unsupported resource: " + meta.resource}
	}
	if !r.integrationReady(base) {
		return "", &taskFailure{Name: "States.Runtime", Cause: "unsupported resource: " + meta.resource}
	}
	switch base {
	case "arn:aws:states:::lambda:invoke":
		return r.invokeLambdaOptimized(callCtx, parent, meta.input, taskWins)
	case "arn:aws:states:::states:startExecution":
		return r.invokeStartExecution(callCtx, parent, arn, meta, r.StepFunctionsEndpoint, mode, taskWins)
	default:
		out, err := r.SDK.CallIntegration(callCtx, base, []byte(meta.input))
		return out, sdkFailure(err, parent, callCtx, taskWins, sdk.IntegrationFallback(base))
	}
}

func (r *Runner) invokeLambdaOptimized(callCtx, parent context.Context, raw string, taskWins bool) (string, *taskFailure) {
	target, payload, err := lambdaInvokeRequest(raw, r.Region, r.Account)
	if err != nil {
		return "", &taskFailure{Name: "States.Runtime", Cause: err.Error()}
	}
	call := LambdaCall{Function: target.name, Qualifier: target.qualifier, Payload: payload, Endpoint: r.LambdaEndpoint}
	var reply LambdaReply
	if r.Invoke != nil {
		reply = r.Invoke(callCtx, call)
	} else {
		reply = httpInvoke(callCtx, call)
	}
	if reply.Err != nil {
		return "", transportFailure(reply.Err, parent, callCtx, taskWins, "Lambda.ServiceException")
	}
	if reply.Status < 200 || reply.Status > 299 {
		return "", &taskFailure{Name: "Lambda.ServiceException", Cause: "Lambda invoke failed with status " + itoa(reply.Status)}
	}
	if reply.FunctionError != "" {
		return "", functionErrorFailure(reply.Payload)
	}
	if len(reply.Payload) > maxPayload {
		return "", &taskFailure{Name: "States.DataLimitExceeded", Cause: "The Lambda payload exceeded the maximum size of 262144 bytes."}
	}
	payloadValue, err := parseJSON(reply.Payload)
	if err != nil {
		return "", &taskFailure{Name: "States.Runtime", Cause: "Lambda payload is not JSON"}
	}
	text, err := compactValue(map[string]any{"StatusCode": reply.Status, "Payload": payloadValue})
	if err != nil {
		return "", &taskFailure{Name: "States.Runtime", Cause: err.Error()}
	}
	return text, nil
}

func lambdaInvokeRequest(raw, region, account string) (lambdaTarget, string, error) {
	doc, err := parseJSON(raw)
	if err != nil {
		return lambdaTarget{}, "", err
	}
	obj, _ := doc.(map[string]any)
	if obj == nil {
		return lambdaTarget{}, "", errors.New("FunctionName is required")
	}
	name, _ := obj["FunctionName"].(string)
	if name == "" {
		return lambdaTarget{}, "", errors.New("FunctionName is required")
	}
	var target lambdaTarget
	if strings.HasPrefix(name, "arn:") {
		parsed, ok := parseLambdaARN(name, region, account)
		if !ok {
			return lambdaTarget{}, "", errors.New("FunctionName is not a function ARN")
		}
		target = parsed
	} else {
		target.name = name
	}
	if q, ok := obj["Qualifier"].(string); ok && q != "" {
		target.qualifier = q
	}
	payload := "{}"
	if p, ok := obj["Payload"]; ok && p != nil {
		text, err := compactValue(p)
		if err != nil {
			return lambdaTarget{}, "", err
		}
		payload = text
	}
	return target, payload, nil
}

// invokeSDK runs an aws-sdk: resource. A service error keeps its mapped name.
func (r *Runner) invokeSDK(callCtx, parent context.Context, id, action, raw string, taskWins bool) (string, *taskFailure) {
	out, err := r.SDK.Call(callCtx, id, action, []byte(raw))
	return out, sdkFailure(err, parent, callCtx, taskWins, sdk.FallbackName(id))
}

// sdkFailure keeps a service error's mapped name. Any other error is a
// transport failure named fallback.
func sdkFailure(err error, parent, callCtx context.Context, taskWins bool, fallback string) *taskFailure {
	if err == nil {
		return nil
	}
	var se *sdk.ServiceError
	if errors.As(err, &se) {
		return &taskFailure{Name: se.Name, Cause: clip(se.Cause, 300)}
	}
	return transportFailure(err, parent, callCtx, taskWins, fallback)
}

func (r *Runner) invokeStartExecution(callCtx, parent context.Context, arn string, meta taskMeta, endpoint, mode string, taskWins bool) (string, *taskFailure) {
	body, status, err := postJSON10(callCtx, endpoint, "AWSStepFunctions.StartExecution", meta.input)
	if err != nil {
		tf := transportFailure(err, parent, callCtx, taskWins, "StepFunctions.ServiceException")
		tf.Submit = tf.Name != "" && !tf.Timeout && !tf.Halt && !tf.ExecDeadline
		return "", tf
	}
	if status < 200 || status > 299 {
		return "", &taskFailure{Name: "StepFunctions.ServiceException", Cause: clip(body, 300), Submit: true}
	}
	parsed, err := parseJSON(body)
	if err != nil {
		return "", &taskFailure{Name: "States.Runtime", Cause: "service response is not JSON", Submit: true}
	}
	started, err := compactValue(parsed)
	if err != nil {
		return "", &taskFailure{Name: "States.Runtime", Cause: err.Error(), Submit: true}
	}
	if mode == "" {
		return started, nil
	}
	obj, _ := parsed.(map[string]any)
	execARN, _ := obj["executionArn"].(string)
	if execARN == "" {
		return "", &taskFailure{Name: "States.Runtime", Cause: "StartExecution did not return executionArn", Submit: true}
	}
	if !r.emit(parent, arn, storeHistorySubmitted(meta, started)) {
		return "", &taskFailure{Halt: true}
	}
	for {
		if callCtx.Err() != nil {
			return "", transportFailure(callCtx.Err(), parent, callCtx, taskWins, "StepFunctions.ServiceException")
		}
		reqBody, err := compactValue(map[string]any{"executionArn": execARN})
		if err != nil {
			return "", &taskFailure{Name: "States.Runtime", Cause: err.Error()}
		}
		descBody, descStatus, err := postJSON10(callCtx, endpoint, "AWSStepFunctions.DescribeExecution", reqBody)
		if err != nil {
			return "", transportFailure(err, parent, callCtx, taskWins, "StepFunctions.ServiceException")
		}
		if descStatus < 200 || descStatus > 299 {
			return "", &taskFailure{Name: "StepFunctions.ServiceException", Cause: clip(descBody, 300)}
		}
		desc, err := parseJSON(descBody)
		if err != nil {
			return "", &taskFailure{Name: "States.Runtime", Cause: "service response is not JSON"}
		}
		doc, _ := desc.(map[string]any)
		state, _ := doc["status"].(string)
		switch state {
		case "SUCCEEDED":
			if mode == "sync2" {
				out, _ := doc["output"].(string)
				if out == "" {
					out = "null"
				}
				if _, err := parseJSON(out); err != nil {
					return "", &taskFailure{Name: "States.Runtime", Cause: "nested output is not JSON"}
				}
				return out, nil
			}
			text, err := compactValue(desc)
			if err != nil {
				return "", &taskFailure{Name: "States.Runtime", Cause: err.Error()}
			}
			return text, nil
		case "FAILED", "TIMED_OUT", "ABORTED":
			name, _ := doc["error"].(string)
			if name == "" {
				name = "States.TaskFailed"
			}
			cause, _ := doc["cause"].(string)
			return "", &taskFailure{Name: name, Cause: cause}
		default:
			if err := r.sleep(callCtx, arn, syncPoll); err != nil {
				return "", transportFailure(err, parent, callCtx, taskWins, "StepFunctions.ServiceException")
			}
		}
	}
}

func storeHistorySubmitted(meta taskMeta, output string) store.HistoryEvent {
	return store.HistoryEvent{
		Type: "TaskSubmitted", Resource: meta.resource, HasResource: meta.resource != "",
		ResourceType: meta.resourceType, HasResourceType: meta.resourceType != "",
		Output: output, HasOutput: true,
	}
}

func transportFailure(err error, parent, callCtx context.Context, taskWins bool, name string) *taskFailure {
	if parent != nil && parent.Err() != nil {
		return &taskFailure{Halt: true}
	}
	if errors.Is(err, context.DeadlineExceeded) || (callCtx != nil && errors.Is(callCtx.Err(), context.DeadlineExceeded)) {
		if taskWins {
			return &taskFailure{Name: "States.Timeout", Timeout: true}
		}
		return &taskFailure{ExecDeadline: true}
	}
	if err == nil {
		err = errors.New(name)
	}
	return &taskFailure{Name: name, Cause: err.Error()}
}

func postJSON10(ctx context.Context, endpoint, target, body string) (string, int, error) {
	return postBody(ctx, strings.TrimRight(endpoint, "/"), "application/x-amz-json-1.0", body, target)
}

func postBody(ctx context.Context, endpoint, contentType, body string, target ...string) (string, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(body))
	if err != nil {
		return "", 0, err
	}
	req.Header.Set("Content-Type", contentType)
	if len(target) > 0 && target[0] != "" {
		req.Header.Set("X-Amz-Target", target[0])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", 0, err
	}
	defer resp.Body.Close()
	buf, err := io.ReadAll(io.LimitReader(resp.Body, maxPayload+1))
	if err != nil {
		return "", resp.StatusCode, err
	}
	if len(buf) > maxPayload {
		return "", resp.StatusCode, errors.New("service response exceeded the maximum size of 262144 bytes")
	}
	return string(buf), resp.StatusCode, nil
}
