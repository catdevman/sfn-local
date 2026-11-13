package sdk

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

var sqsService = service{
	errorPrefix: "Sqs",
	env:         "SQS",
	client:      (*sqs.Client)(nil),
	actions: func(cfg aws.Config, endpoint string) map[string]handler {
		c := sqs.NewFromConfig(cfg, func(o *sqs.Options) {
			o.BaseEndpoint = aws.String(endpoint)
		})
		return map[string]handler{
			"changeMessageVisibility": op(c.ChangeMessageVisibility),
			"createQueue":             op(c.CreateQueue),
			"deleteMessage":           op(c.DeleteMessage),
			"deleteMessageBatch":      op(c.DeleteMessageBatch),
			"deleteQueue":             op(c.DeleteQueue),
			"getQueueAttributes":      op(c.GetQueueAttributes),
			"getQueueUrl":             op(c.GetQueueUrl),
			"listQueueTags":           op(c.ListQueueTags),
			"listQueues":              op(c.ListQueues),
			"purgeQueue":              op(c.PurgeQueue),
			"receiveMessage":          op(c.ReceiveMessage),
			"sendMessage":             op(c.SendMessage),
			"sendMessageBatch":        op(c.SendMessageBatch),
			"setQueueAttributes":      op(c.SetQueueAttributes),
			"tagQueue":                op(c.TagQueue),
			"untagQueue":              op(c.UntagQueue),
		}
	},
}
