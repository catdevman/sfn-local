package sdk

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
)

// ssmService is Parameter Store, Run Command, and tags. The rest of Systems
// Manager is not in the table.
var ssmService = service{
	errorPrefix: "Ssm",
	env:         "SSM",
	client:      (*ssm.Client)(nil),
	actions: func(cfg aws.Config, endpoint string) map[string]handler {
		c := ssm.NewFromConfig(cfg, func(o *ssm.Options) {
			o.BaseEndpoint = aws.String(endpoint)
		})
		return map[string]handler{
			"addTagsToResource":       op(c.AddTagsToResource),
			"cancelCommand":           op(c.CancelCommand),
			"deleteParameter":         op(c.DeleteParameter),
			"deleteParameters":        op(c.DeleteParameters),
			"describeParameters":      op(c.DescribeParameters),
			"getCommandInvocation":    op(c.GetCommandInvocation),
			"getParameter":            op(c.GetParameter),
			"getParameterHistory":     op(c.GetParameterHistory),
			"getParameters":           op(c.GetParameters),
			"getParametersByPath":     op(c.GetParametersByPath),
			"labelParameterVersion":   op(c.LabelParameterVersion),
			"listCommandInvocations":  op(c.ListCommandInvocations),
			"listCommands":            op(c.ListCommands),
			"listTagsForResource":     op(c.ListTagsForResource),
			"putParameter":            op(c.PutParameter),
			"removeTagsFromResource":  op(c.RemoveTagsFromResource),
			"sendCommand":             op(c.SendCommand),
			"unlabelParameterVersion": op(c.UnlabelParameterVersion),
		}
	},
}
