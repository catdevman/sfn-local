package sdk

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
)

var snsService = service{
	errorPrefix: "Sns",
	env:         "SNS",
	own:         "SNS_ENDPOINT (--sns-endpoint)",
	client:      (*sns.Client)(nil),
	actions: func(cfg aws.Config, endpoint string) map[string]handler {
		c := sns.NewFromConfig(cfg, func(o *sns.Options) {
			o.BaseEndpoint = aws.String(endpoint)
		})
		return map[string]handler{
			"createTopic":              op(c.CreateTopic),
			"deleteTopic":              op(c.DeleteTopic),
			"getTopicAttributes":       op(c.GetTopicAttributes),
			"listSubscriptions":        op(c.ListSubscriptions),
			"listSubscriptionsByTopic": op(c.ListSubscriptionsByTopic),
			"listTagsForResource":      op(c.ListTagsForResource),
			"listTopics":               op(c.ListTopics),
			"publish":                  op(c.Publish),
			"publishBatch":             op(c.PublishBatch),
			"setTopicAttributes":       op(c.SetTopicAttributes),
			"subscribe":                op(c.Subscribe),
			"tagResource":              op(c.TagResource),
			"unsubscribe":              op(c.Unsubscribe),
			"untagResource":            op(c.UntagResource),
		}
	},
}
