package sdk

import (
	"context"
	"encoding/json"
	"fmt"
)

// integration is an optimized arn:aws:states::: resource that runs through
// an aws-sdk action. Its errors use the Step Functions service name, such as
// DynamoDB.ConditionalCheckFailedException, not the aws-sdk prefix.
type integration struct {
	id, action, prefix string
	// asText rewrites members that may be any JSON value into JSON text,
	// such as an SQS MessageBody given as an object.
	asText func(params map[string]any) error
}

var integrations = map[string]integration{
	"arn:aws:states:::dynamodb:getItem":    {id: "dynamodb", action: "getItem", prefix: "DynamoDB"},
	"arn:aws:states:::dynamodb:putItem":    {id: "dynamodb", action: "putItem", prefix: "DynamoDB"},
	"arn:aws:states:::dynamodb:updateItem": {id: "dynamodb", action: "updateItem", prefix: "DynamoDB"},
	"arn:aws:states:::dynamodb:deleteItem": {id: "dynamodb", action: "deleteItem", prefix: "DynamoDB"},
	"arn:aws:states:::sqs:sendMessage":     {id: "sqs", action: "sendMessage", prefix: "SQS", asText: memberText("MessageBody")},
	"arn:aws:states:::sns:publish":         {id: "sns", action: "publish", prefix: "SNS", asText: memberText("Message")},
	"arn:aws:states:::events:putEvents":    {id: "eventbridge", action: "putEvents", prefix: "EventBridge", asText: entriesDetail},
}

// IsIntegration reports whether resource is an optimized integration this
// package runs. A .sync or .waitForTaskToken suffix is not.
func IsIntegration(resource string) bool {
	_, ok := integrations[resource]
	return ok
}

// SupportsIntegration reports whether the integration's service has an endpoint.
func (c *Caller) SupportsIntegration(resource string) bool {
	in, ok := integrations[resource]
	return ok && c.Supports(in.id, in.action)
}

// CallIntegration runs an optimized integration. Errors are named as Call
// names them, with the integration's prefix.
func (c *Caller) CallIntegration(ctx context.Context, resource string, params []byte) (string, error) {
	in, ok := integrations[resource]
	if !ok {
		return "", fmt.Errorf("unsupported resource: %s", resource)
	}
	if in.asText != nil {
		raw, err := parse(params)
		obj, ok := raw.(map[string]any)
		if err != nil || !ok {
			return "", paramErr("service parameters must be a JSON object")
		}
		if err := in.asText(obj); err != nil {
			return "", err
		}
		text, err := json.Marshal(obj)
		if err != nil {
			return "", err
		}
		params = text
	}
	return c.call(ctx, in.id, in.action, in.prefix, IntegrationFallback(resource), params)
}

// IntegrationFallback names a failure the service did not classify, such as
// SQS.ServiceException.
func IntegrationFallback(resource string) string {
	return integrations[resource].prefix + ".ServiceException"
}

func memberText(name string) func(map[string]any) error {
	return func(obj map[string]any) error {
		return toText(obj, name)
	}
}

// entriesDetail writes each Entries[].Detail that is not a string as JSON text.
func entriesDetail(obj map[string]any) error {
	entries, _ := obj["Entries"].([]any)
	for _, e := range entries {
		if entry, ok := e.(map[string]any); ok {
			if err := toText(entry, "Detail"); err != nil {
				return err
			}
		}
	}
	return nil
}

func toText(obj map[string]any, name string) error {
	v, ok := obj[name]
	if !ok || v == nil {
		return nil
	}
	if _, isString := v.(string); isString {
		return nil
	}
	text, err := json.Marshal(v)
	if err != nil {
		return err
	}
	obj[name] = string(text)
	return nil
}
