package sdk

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
)

var secretsmanagerService = service{
	errorPrefix: "SecretsManager",
	env:         "SECRETS_MANAGER",
	client:      (*secretsmanager.Client)(nil),
	actions: func(cfg aws.Config, endpoint string) map[string]handler {
		c := secretsmanager.NewFromConfig(cfg, func(o *secretsmanager.Options) {
			o.BaseEndpoint = aws.String(endpoint)
		})
		return map[string]handler{
			"batchGetSecretValue":          op(c.BatchGetSecretValue),
			"cancelRotateSecret":           op(c.CancelRotateSecret),
			"createSecret":                 op(c.CreateSecret),
			"deleteResourcePolicy":         op(c.DeleteResourcePolicy),
			"deleteSecret":                 op(c.DeleteSecret),
			"describeSecret":               op(c.DescribeSecret),
			"getRandomPassword":            op(c.GetRandomPassword),
			"getResourcePolicy":            op(c.GetResourcePolicy),
			"getSecretValue":               op(c.GetSecretValue),
			"listSecretVersionIds":         op(c.ListSecretVersionIds),
			"listSecrets":                  op(c.ListSecrets),
			"putResourcePolicy":            op(c.PutResourcePolicy),
			"putSecretValue":               op(c.PutSecretValue),
			"removeRegionsFromReplication": op(c.RemoveRegionsFromReplication),
			"replicateSecretToRegions":     op(c.ReplicateSecretToRegions),
			"restoreSecret":                op(c.RestoreSecret),
			"rotateSecret":                 op(c.RotateSecret),
			"stopReplicationToReplica":     op(c.StopReplicationToReplica),
			"tagResource":                  op(c.TagResource),
			"untagResource":                op(c.UntagResource),
			"updateSecret":                 op(c.UpdateSecret),
			"updateSecretVersionStage":     op(c.UpdateSecretVersionStage),
			"validateResourcePolicy":       op(c.ValidateResourcePolicy),
		}
	},
}
