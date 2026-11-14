package sdk

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
)

// sfnService calls a Step Functions endpoint, this server included.
var sfnService = service{
	errorPrefix: "Sfn",
	env:         "SFN",
	client:      (*sfn.Client)(nil),
	actions: func(cfg aws.Config, endpoint string) map[string]handler {
		c := sfn.NewFromConfig(cfg, func(o *sfn.Options) {
			o.BaseEndpoint = aws.String(endpoint)
		})
		return map[string]handler{
			"createActivity":                   op(c.CreateActivity),
			"createStateMachine":               op(c.CreateStateMachine),
			"createStateMachineAlias":          op(c.CreateStateMachineAlias),
			"deleteActivity":                   op(c.DeleteActivity),
			"deleteStateMachine":               op(c.DeleteStateMachine),
			"deleteStateMachineAlias":          op(c.DeleteStateMachineAlias),
			"deleteStateMachineVersion":        op(c.DeleteStateMachineVersion),
			"describeActivity":                 op(c.DescribeActivity),
			"describeExecution":                op(c.DescribeExecution),
			"describeMapRun":                   op(c.DescribeMapRun),
			"describeStateMachine":             op(c.DescribeStateMachine),
			"describeStateMachineAlias":        op(c.DescribeStateMachineAlias),
			"describeStateMachineForExecution": op(c.DescribeStateMachineForExecution),
			"getActivityTask":                  op(c.GetActivityTask),
			"getExecutionHistory":              op(c.GetExecutionHistory),
			"listActivities":                   op(c.ListActivities),
			"listExecutions":                   op(c.ListExecutions),
			"listMapRuns":                      op(c.ListMapRuns),
			"listStateMachineAliases":          op(c.ListStateMachineAliases),
			"listStateMachineVersions":         op(c.ListStateMachineVersions),
			"listStateMachines":                op(c.ListStateMachines),
			"listTagsForResource":              op(c.ListTagsForResource),
			"publishStateMachineVersion":       op(c.PublishStateMachineVersion),
			"redriveExecution":                 op(c.RedriveExecution),
			"sendTaskFailure":                  op(c.SendTaskFailure),
			"sendTaskHeartbeat":                op(c.SendTaskHeartbeat),
			"sendTaskSuccess":                  op(c.SendTaskSuccess),
			"startExecution":                   op(c.StartExecution),
			"startSyncExecution":               op(c.StartSyncExecution),
			"stopExecution":                    op(c.StopExecution),
			"tagResource":                      op(c.TagResource),
			"testState":                        op(c.TestState),
			"untagResource":                    op(c.UntagResource),
			"updateMapRun":                     op(c.UpdateMapRun),
			"updateStateMachine":               op(c.UpdateStateMachine),
			"updateStateMachineAlias":          op(c.UpdateStateMachineAlias),
			"validateStateMachineDefinition":   op(c.ValidateStateMachineDefinition),
		}
	},
}
