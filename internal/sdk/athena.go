package sdk

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/athena"
)

// athenaService serves aws-sdk:athena and the optimized athena: resources,
// whose startQueryExecution.sync polls getQueryExecution and stops the query
// on abort.
var athenaService = service{
	errorPrefix: "Athena",
	env:         "ATHENA",
	own:         "STEP_FUNCTIONS_ATHENA (--step-functions-athena)",
	client:      (*athena.Client)(nil),
	actions: func(cfg aws.Config, endpoint string) map[string]handler {
		c := athena.NewFromConfig(cfg, func(o *athena.Options) {
			o.BaseEndpoint = aws.String(endpoint)
		})
		return map[string]handler{
			"batchGetNamedQuery":        op(c.BatchGetNamedQuery),
			"batchGetQueryExecution":    op(c.BatchGetQueryExecution),
			"createNamedQuery":          op(c.CreateNamedQuery),
			"createWorkGroup":           op(c.CreateWorkGroup),
			"deleteNamedQuery":          op(c.DeleteNamedQuery),
			"deleteWorkGroup":           op(c.DeleteWorkGroup),
			"getDataCatalog":            op(c.GetDataCatalog),
			"getDatabase":               op(c.GetDatabase),
			"getNamedQuery":             op(c.GetNamedQuery),
			"getQueryExecution":         op(c.GetQueryExecution),
			"getQueryResults":           op(c.GetQueryResults),
			"getQueryRuntimeStatistics": op(c.GetQueryRuntimeStatistics),
			"getTableMetadata":          op(c.GetTableMetadata),
			"getWorkGroup":              op(c.GetWorkGroup),
			"listDataCatalogs":          op(c.ListDataCatalogs),
			"listDatabases":             op(c.ListDatabases),
			"listNamedQueries":          op(c.ListNamedQueries),
			"listQueryExecutions":       op(c.ListQueryExecutions),
			"listTableMetadata":         op(c.ListTableMetadata),
			"listTagsForResource":       op(c.ListTagsForResource),
			"listWorkGroups":            op(c.ListWorkGroups),
			"startQueryExecution":       op(c.StartQueryExecution),
			"stopQueryExecution":        op(c.StopQueryExecution),
			"tagResource":               op(c.TagResource),
			"untagResource":             op(c.UntagResource),
			"updateWorkGroup":           op(c.UpdateWorkGroup),
		}
	},
}
