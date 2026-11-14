package sdk

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
)

var eventbridgeService = service{
	errorPrefix: "EventBridge",
	env:         "EVENTBRIDGE",
	own:         "EVENTBRIDGE_ENDPOINT (--eventbridge-endpoint)",
	client:      (*eventbridge.Client)(nil),
	actions: func(cfg aws.Config, endpoint string) map[string]handler {
		c := eventbridge.NewFromConfig(cfg, func(o *eventbridge.Options) {
			o.BaseEndpoint = aws.String(endpoint)
		})
		return map[string]handler{
			"createEventBus":    op(c.CreateEventBus),
			"deleteEventBus":    op(c.DeleteEventBus),
			"deleteRule":        op(c.DeleteRule),
			"describeEventBus":  op(c.DescribeEventBus),
			"describeRule":      op(c.DescribeRule),
			"disableRule":       op(c.DisableRule),
			"enableRule":        op(c.EnableRule),
			"listEventBuses":    op(c.ListEventBuses),
			"listRules":         op(c.ListRules),
			"listTargetsByRule": op(c.ListTargetsByRule),
			"putEvents":         op(c.PutEvents),
			"putRule":           op(c.PutRule),
			"putTargets":        op(c.PutTargets),
			"removeTargets":     op(c.RemoveTargets),
		}
	},
}
