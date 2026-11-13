package exec

import (
	"context"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

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

func (r *Runner) integrationEndpoint(base string) string {
	if r == nil {
		return ""
	}
	switch base {
	case "arn:aws:states:::lambda:invoke":
		return r.LambdaEndpoint
	case "arn:aws:states:::sqs:sendMessage":
		return r.SQSEndpoint
	case "arn:aws:states:::sns:publish":
		return r.SNSEndpoint
	case "arn:aws:states:::dynamodb:getItem", "arn:aws:states:::dynamodb:putItem", "arn:aws:states:::dynamodb:updateItem", "arn:aws:states:::dynamodb:deleteItem":
		return r.DynamoDBEndpoint
	case "arn:aws:states:::states:startExecution":
		return r.StepFunctionsEndpoint
	case "arn:aws:states:::events:putEvents":
		return r.EventBridgeEndpoint
	default:
		return ""
	}
}

func (r *Runner) invokeService(callCtx, parent context.Context, arn string, meta taskMeta, taskWins bool) (string, *taskFailure) {
	base, mode, ok := integrationResource(meta.resource)
	if !ok || (mode != "" && base != "arn:aws:states:::states:startExecution") {
		return "", &taskFailure{Name: "States.Runtime", Cause: "unsupported resource: " + meta.resource}
	}
	endpoint := r.integrationEndpoint(base)
	if endpoint == "" {
		return "", &taskFailure{Name: "States.Runtime", Cause: "unsupported resource: " + meta.resource}
	}
	switch base {
	case "arn:aws:states:::lambda:invoke":
		return r.invokeLambdaOptimized(callCtx, parent, meta.input, taskWins)
	case "arn:aws:states:::sqs:sendMessage":
		return r.invokeQuery(callCtx, parent, endpoint, "SendMessage", "2012-11-05", meta.input, "SQS", taskWins)
	case "arn:aws:states:::sns:publish":
		return r.invokeQuery(callCtx, parent, endpoint, "Publish", "2010-03-31", meta.input, "SNS", taskWins)
	case "arn:aws:states:::dynamodb:getItem":
		return r.invokeJSON10(callCtx, parent, endpoint, "DynamoDB_20120810.GetItem", meta.input, "DynamoDB", taskWins)
	case "arn:aws:states:::dynamodb:putItem":
		return r.invokeJSON10(callCtx, parent, endpoint, "DynamoDB_20120810.PutItem", meta.input, "DynamoDB", taskWins)
	case "arn:aws:states:::dynamodb:updateItem":
		return r.invokeJSON10(callCtx, parent, endpoint, "DynamoDB_20120810.UpdateItem", meta.input, "DynamoDB", taskWins)
	case "arn:aws:states:::dynamodb:deleteItem":
		return r.invokeJSON10(callCtx, parent, endpoint, "DynamoDB_20120810.DeleteItem", meta.input, "DynamoDB", taskWins)
	case "arn:aws:states:::events:putEvents":
		return r.invokeJSON10(callCtx, parent, endpoint, "AWSEvents.PutEvents", meta.input, "EventBridge", taskWins)
	case "arn:aws:states:::states:startExecution":
		return r.invokeStartExecution(callCtx, parent, arn, meta, endpoint, mode, taskWins)
	default:
		return "", &taskFailure{Name: "States.Runtime", Cause: "unsupported resource: " + meta.resource}
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

func (r *Runner) invokeQuery(callCtx, parent context.Context, endpoint, action, version, raw, service string, taskWins bool) (string, *taskFailure) {
	params, err := queryParams(raw)
	if err != nil {
		return "", &taskFailure{Name: "States.Runtime", Cause: err.Error()}
	}
	form := url.Values{}
	form.Set("Action", action)
	form.Set("Version", version)
	for key, val := range params {
		form.Set(key, val)
	}
	body, status, err := postBody(callCtx, strings.TrimRight(endpoint, "/"), "application/x-www-form-urlencoded", form.Encode(), nil)
	if err != nil {
		return "", transportFailure(err, parent, callCtx, taskWins, service+".ServiceException")
	}
	if status < 200 || status > 299 {
		return "", &taskFailure{Name: service + ".ServiceException", Cause: clip(body, 300)}
	}
	text, err := queryResult(body)
	if err != nil {
		return "", &taskFailure{Name: "States.Runtime", Cause: err.Error()}
	}
	return text, nil
}

func queryParams(raw string) (map[string]string, error) {
	doc, err := parseJSON(raw)
	if err != nil {
		return nil, err
	}
	obj, ok := doc.(map[string]any)
	if !ok {
		return nil, errors.New("service parameters must be an object")
	}
	out := make(map[string]string, len(obj))
	for key, val := range obj {
		switch x := val.(type) {
		case string:
			out[key] = x
		case bool:
			if x {
				out[key] = "true"
			} else {
				out[key] = "false"
			}
		case int64:
			out[key] = itoa(int(x))
		default:
			text, err := compactValue(x)
			if err != nil {
				return nil, err
			}
			out[key] = text
		}
	}
	return out, nil
}

func queryResult(body string) (string, error) {
	trim := strings.TrimSpace(body)
	if strings.HasPrefix(trim, "{") || strings.HasPrefix(trim, "[") {
		parsed, err := parseJSON(trim)
		if err != nil {
			return "", err
		}
		return compactValue(parsed)
	}
	leaves := xmlLeaves(trim)
	if len(leaves) == 0 {
		return "", errors.New("service response is not JSON")
	}
	obj := make(map[string]any, len(leaves))
	for key, val := range leaves {
		obj[key] = val
	}
	return compactValue(obj)
}

func xmlLeaves(body string) map[string]string {
	dec := xml.NewDecoder(strings.NewReader(body))
	var stack []string
	out := map[string]string{}
	for {
		tok, err := dec.Token()
		if err != nil {
			return out
		}
		switch t := tok.(type) {
		case xml.StartElement:
			stack = append(stack, t.Name.Local)
		case xml.EndElement:
			if len(stack) > 0 {
				stack = stack[:len(stack)-1]
			}
		case xml.CharData:
			text := strings.TrimSpace(string(t))
			if text != "" && len(stack) > 0 {
				out[stack[len(stack)-1]] = text
			}
		}
	}
}

func (r *Runner) invokeJSON10(callCtx, parent context.Context, endpoint, target, raw, service string, taskWins bool) (string, *taskFailure) {
	var sign *sigv4
	if service == "DynamoDB" {
		sign = r.signer("dynamodb")
	}
	body, status, err := postJSON10(callCtx, endpoint, target, raw, sign)
	if err != nil {
		return "", transportFailure(err, parent, callCtx, taskWins, service+".ServiceException")
	}
	if status < 200 || status > 299 {
		return "", &taskFailure{Name: service + ".ServiceException", Cause: clip(body, 300)}
	}
	parsed, err := parseJSON(body)
	if err != nil {
		return "", &taskFailure{Name: "States.Runtime", Cause: "service response is not JSON"}
	}
	text, err := compactValue(parsed)
	if err != nil {
		return "", &taskFailure{Name: "States.Runtime", Cause: err.Error()}
	}
	return text, nil
}

func (r *Runner) invokeStartExecution(callCtx, parent context.Context, arn string, meta taskMeta, endpoint, mode string, taskWins bool) (string, *taskFailure) {
	body, status, err := postJSON10(callCtx, endpoint, "AWSStepFunctions.StartExecution", meta.input, nil)
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
		descBody, descStatus, err := postJSON10(callCtx, endpoint, "AWSStepFunctions.DescribeExecution", reqBody, nil)
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

func postJSON10(ctx context.Context, endpoint, target, body string, sign *sigv4) (string, int, error) {
	return postBody(ctx, strings.TrimRight(endpoint, "/"), "application/x-amz-json-1.0", body, sign, target)
}

// postBody signs the request when sign is not nil.
func postBody(ctx context.Context, endpoint, contentType, body string, sign *sigv4, target ...string) (string, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(body))
	if err != nil {
		return "", 0, err
	}
	req.Header.Set("Content-Type", contentType)
	if len(target) > 0 && target[0] != "" {
		req.Header.Set("X-Amz-Target", target[0])
	}
	sign.sign(req, body, time.Now())
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
