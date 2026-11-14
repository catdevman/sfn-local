package sdk

import (
	"context"
	"encoding/json"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
)

var lambdaService = service{
	errorPrefix: "Lambda",
	env:         "LAMBDA",
	client:      (*lambda.Client)(nil),
	actions: func(cfg aws.Config, endpoint string) map[string]handler {
		c := lambda.NewFromConfig(cfg, func(o *lambda.Options) {
			o.BaseEndpoint = aws.String(endpoint)
		})
		return map[string]handler{
			"addPermission":               op(c.AddPermission),
			"createAlias":                 op(c.CreateAlias),
			"createEventSourceMapping":    op(c.CreateEventSourceMapping),
			"createFunction":              op(c.CreateFunction),
			"deleteAlias":                 op(c.DeleteAlias),
			"deleteEventSourceMapping":    op(c.DeleteEventSourceMapping),
			"deleteFunction":              op(c.DeleteFunction),
			"deleteFunctionConcurrency":   op(c.DeleteFunctionConcurrency),
			"getAccountSettings":          op(c.GetAccountSettings),
			"getAlias":                    op(c.GetAlias),
			"getEventSourceMapping":       op(c.GetEventSourceMapping),
			"getFunction":                 op(c.GetFunction),
			"getFunctionConcurrency":      op(c.GetFunctionConcurrency),
			"getFunctionConfiguration":    op(c.GetFunctionConfiguration),
			"getPolicy":                   op(c.GetPolicy),
			"invoke":                      lambdaInvoke(c),
			"listAliases":                 op(c.ListAliases),
			"listEventSourceMappings":     op(c.ListEventSourceMappings),
			"listFunctions":               op(c.ListFunctions),
			"listTags":                    op(c.ListTags),
			"listVersionsByFunction":      op(c.ListVersionsByFunction),
			"publishVersion":              op(c.PublishVersion),
			"putFunctionConcurrency":      op(c.PutFunctionConcurrency),
			"removePermission":            op(c.RemovePermission),
			"tagResource":                 op(c.TagResource),
			"untagResource":               op(c.UntagResource),
			"updateAlias":                 op(c.UpdateAlias),
			"updateEventSourceMapping":    op(c.UpdateEventSourceMapping),
			"updateFunctionCode":          op(c.UpdateFunctionCode),
			"updateFunctionConfiguration": op(c.UpdateFunctionConfiguration),
		}
	},
}

// lambdaInvoke takes Payload as a string, or any other JSON value as its
// JSON text, and returns Payload as a string. The codec would use base64.
func lambdaInvoke(c *lambda.Client) handler {
	return func(ctx context.Context, params []byte) (string, error) {
		rest, rawPayload, hasPayload, err := splitMember(params, "Payload")
		if err != nil {
			return "", err
		}
		in := new(lambda.InvokeInput)
		if err := decode(rest, in); err != nil {
			return "", err
		}
		if hasPayload {
			in.Payload = []byte(textOf(rawPayload))
		}
		out, err := c.Invoke(ctx, in)
		if err != nil {
			return "", err
		}
		payload := string(out.Payload)
		out.Payload = nil
		return encode(out, map[string]any{"Payload": payload})
	}
}

// splitMember removes one top-level member from aws-sdk parameters, so a
// handler can carry it itself.
func splitMember(params []byte, name string) (rest, member []byte, found bool, err error) {
	var top map[string]json.RawMessage
	if err := json.Unmarshal(params, &top); err != nil {
		return nil, nil, false, &ServiceError{Name: "States.Runtime", Cause: "aws-sdk parameters must be a JSON object"}
	}
	member, found = top[name]
	delete(top, name)
	rest, err = json.Marshal(top)
	return rest, member, found, err
}

// textOf is a JSON string's value, or any other JSON value's text.
func textOf(raw []byte) string {
	var text string
	if err := json.Unmarshal(raw, &text); err != nil {
		return string(raw)
	}
	return text
}
