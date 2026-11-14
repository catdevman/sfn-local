package sdk

import (
	"reflect"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	"github.com/aws/aws-sdk-go-v2/service/ecs/types"
)

// ecsService serves aws-sdk:ecs and the optimized ecs:runTask integration,
// whose .sync form polls describeTasks and stops the task on abort. Service
// operations whose shapes hold a document member are not in the table.
var ecsService = service{
	errorPrefix: "Ecs",
	env:         "ECS",
	own:         "ECS_ENDPOINT (--ecs-endpoint)",
	client:      (*ecs.Client)(nil),
	actions: func(cfg aws.Config, endpoint string) map[string]handler {
		c := ecs.NewFromConfig(cfg, func(o *ecs.Options) {
			o.BaseEndpoint = aws.String(endpoint)
		})
		return map[string]handler{
			"createCluster":            op(c.CreateCluster),
			"deleteCluster":            op(c.DeleteCluster),
			"deregisterTaskDefinition": op(c.DeregisterTaskDefinition),
			"describeClusters":         op(c.DescribeClusters),
			"describeTaskDefinition":   op(c.DescribeTaskDefinition),
			"describeTasks":            op(c.DescribeTasks),
			"listClusters":             op(c.ListClusters),
			"listServices":             op(c.ListServices),
			"listTagsForResource":      op(c.ListTagsForResource),
			"listTaskDefinitions":      op(c.ListTaskDefinitions),
			"listTasks":                op(c.ListTasks),
			"registerTaskDefinition":   op(c.RegisterTaskDefinition),
			"runTask":                  op(c.RunTask),
			"startTask":                op(c.StartTask),
			"stopTask":                 op(c.StopTask),
			"tagResource":              op(c.TagResource),
			"untagResource":            op(c.UntagResource),
		}
	},
}

// SharingStrategy is a union with one member, {"NvidiaMps":{...}}.
func init() {
	unions[reflect.TypeFor[types.SharingStrategy]()] = union{
		decode: func(raw any, path string) (reflect.Value, error) {
			obj, ok := raw.(map[string]any)
			inner, has := obj["NvidiaMps"]
			if !ok || len(obj) != 1 || !has {
				return reflect.Value{}, typeErr(path, `{"NvidiaMps":{...}}`)
			}
			m := &types.SharingStrategyMemberNvidiaMps{}
			if err := fill(reflect.ValueOf(&m.Value).Elem(), inner, join(path, "NvidiaMps")); err != nil {
				return reflect.Value{}, err
			}
			var s types.SharingStrategy = m
			return reflect.ValueOf(&s).Elem(), nil
		},
		encode: func(member reflect.Value) (any, error) {
			switch m := member.Interface().(type) {
			case *types.SharingStrategyMemberNvidiaMps:
				v, err := plain(reflect.ValueOf(m.Value))
				return map[string]any{"NvidiaMps": v}, err
			default:
				return nil, nil
			}
		},
	}
}
