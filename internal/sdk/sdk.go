// Package sdk runs arn:aws:states:::aws-sdk:<service>:<action> tasks with the
// AWS SDK for Go v2. Each service is a hand-written table from action name to
// client method. Dispatch never reflects over the SDK. A service runs only
// when its endpoint is set, so an empty endpoint never calls the network.
//
// internal/invoke/operations.toml lists every operation of every service here
// and whether it runs. internal/ci checks that file against these tables.
package sdk

//go:generate go run github.com/catdevman/sfn-local/cmd/genops -out ../invoke/operations.toml

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

const resourcePrefix = "arn:aws:states:::aws-sdk:"

// Config is what every client shares. Endpoints maps a service id, the
// resource segment after aws-sdk:, to its endpoint URL.
type Config struct {
	Region          string
	AccessKeyID     string
	SecretAccessKey string
	Endpoints       map[string]string
}

// ServiceError is a failure the task reports as Error and Cause. Name is
// <ErrorPrefix>.<Code>, with Exception appended when the code lacks it.
type ServiceError struct {
	Name  string
	Cause string
}

func (e *ServiceError) Error() string { return e.Name + ": " + e.Cause }

// handler runs one action. params is the task's Parameters or Arguments JSON.
// The result is compact JSON.
type handler func(ctx context.Context, params []byte) (string, error)

// service is one aws-sdk:<id> integration.
type service struct {
	// errorPrefix starts each error name, such as S3 in S3.NoSuchKeyException.
	errorPrefix string
	// env is the suffix of AWS_ENDPOINT_URL_<env>.
	env string
	// client is a nil client pointer. cmd/genops lists its methods.
	client any
	// actions builds the action table around a client for cfg.
	actions func(cfg aws.Config, endpoint string) map[string]handler
}

// services is every integration this package runs, keyed by service id.
var services = map[string]service{
	"dynamodb":       dynamodbService,
	"eventbridge":    eventbridgeService,
	"lambda":         lambdaService,
	"s3":             s3Service,
	"secretsmanager": secretsmanagerService,
	"sfn":            sfnService,
	"sns":            snsService,
	"sqs":            sqsService,
	"ssm":            ssmService,
}

// Services returns the service ids in order.
func Services() []string {
	ids := make([]string, 0, len(services))
	for id := range services {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// EndpointEnv is the AWS_ENDPOINT_URL_<SERVICE> variable for a service id.
func EndpointEnv(id string) string {
	return "AWS_ENDPOINT_URL_" + services[id].env
}

// Client returns a nil client pointer for the service, for cmd/genops.
func Client(id string) any {
	return services[id].client
}

// Actions returns the action names a service runs, in order. It builds the
// table without a network call.
func Actions(id string) []string {
	svc, ok := services[id]
	if !ok {
		return nil
	}
	table := svc.actions(aws.Config{Region: "us-east-1", Credentials: aws.AnonymousCredentials{}}, "http://127.0.0.1")
	names := make([]string, 0, len(table))
	for name := range table {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// ParseResource splits arn:aws:states:::aws-sdk:<service>:<action>. A suffix
// such as .waitForTaskToken is not an aws-sdk call this package makes.
func ParseResource(resource string) (id, action string, ok bool) {
	rest, found := strings.CutPrefix(resource, resourcePrefix)
	if !found {
		return "", "", false
	}
	id, action, found = strings.Cut(rest, ":")
	if !found || id == "" || action == "" || strings.ContainsAny(action, ":.") {
		return "", "", false
	}
	return id, action, true
}

// Caller builds each service's client on first use and keeps it.
type Caller struct {
	cfg    Config
	mu     sync.Mutex
	tables map[string]map[string]handler
	// s3c serves the Distributed Map reader and writer.
	s3c *s3.Client
}

// New returns a Caller. A nil Caller supports nothing.
func New(cfg Config) *Caller {
	return &Caller{cfg: cfg, tables: map[string]map[string]handler{}}
}

// Supports reports whether the service has an endpoint and the action runs.
func (c *Caller) Supports(id, action string) bool {
	_, ok := c.lookup(id, action)
	return ok
}

// Call runs one action. A service failure is a *ServiceError. Any other error
// is a transport failure, and FallbackName names it.
func (c *Caller) Call(ctx context.Context, id, action string, params []byte) (string, error) {
	return c.call(ctx, id, action, services[id].errorPrefix, FallbackName(id), params)
}

// call runs one action and names its errors with prefix. An error the
// service did not name is fallback.
func (c *Caller) call(ctx context.Context, id, action, prefix, fallback string, params []byte) (string, error) {
	h, ok := c.lookup(id, action)
	if !ok {
		return "", fmt.Errorf("unsupported resource: %s%s:%s", resourcePrefix, id, action)
	}
	out, err := h(ctx, params)
	if err != nil {
		return "", mapError(prefix, fallback, err)
	}
	return out, nil
}

// FallbackName is the error name for a failure the service did not classify,
// such as S3.S3Exception.
func FallbackName(id string) string {
	p := services[id].errorPrefix
	return p + "." + p + "Exception"
}

func (c *Caller) lookup(id, action string) (handler, bool) {
	if c == nil {
		return nil, false
	}
	svc, ok := services[id]
	if !ok {
		return nil, false
	}
	endpoint := c.cfg.Endpoints[id]
	if endpoint == "" {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	table, ok := c.tables[id]
	if !ok {
		table = svc.actions(c.awsConfig(), endpoint)
		c.tables[id] = table
	}
	h, ok := table[action]
	return h, ok
}

// awsConfig never reads shared config files or instance metadata. Retries
// are off because the state's Retry field owns them.
func (c *Caller) awsConfig() aws.Config {
	cfg := aws.Config{
		Region:      c.cfg.Region,
		Credentials: aws.AnonymousCredentials{},
		Retryer:     func() aws.Retryer { return aws.NopRetryer{} },
	}
	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}
	if c.cfg.AccessKeyID != "" {
		cfg.Credentials = credentials.NewStaticCredentialsProvider(c.cfg.AccessKeyID, c.cfg.SecretAccessKey, "")
	}
	return cfg
}

// errorCode drops a namespace and a suffix from an AWS JSON error type, so
// com.amazonaws.sqs#QueueDoesNotExist:http://... is QueueDoesNotExist.
func errorCode(code string) string {
	if i := strings.LastIndexByte(code, '#'); i >= 0 {
		code = code[i+1:]
	}
	if i := strings.IndexByte(code, ':'); i >= 0 {
		code = code[:i]
	}
	return code
}

// mapError turns an SDK API error into a ServiceError and passes any other
// error through. The cause follows the AWS SDK for Java v2 message shape. A
// response with no error code, which the SDK calls UnknownError, is fallback.
func mapError(prefix, fallback string, err error) error {
	var se *ServiceError
	if errors.As(err, &se) {
		return se
	}
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		return err
	}
	code := errorCode(apiErr.ErrorCode())
	name := prefix + "." + code
	if code == "" || code == "UnknownError" {
		name = fallback
	} else if !strings.HasSuffix(code, "Exception") {
		name += "Exception"
	}
	msg := apiErr.ErrorMessage()
	if msg == "" {
		msg = "Service returned error code " + code
	}
	detail := "Service: " + prefix
	var respErr *awshttp.ResponseError
	if errors.As(err, &respErr) {
		detail += fmt.Sprintf(", Status Code: %d", respErr.HTTPStatusCode())
		if id := respErr.ServiceRequestID(); id != "" {
			detail += ", Request ID: " + id
		}
	}
	var host interface{ ServiceHostID() string }
	if errors.As(err, &host) && host.ServiceHostID() != "" {
		detail += ", Extended Request ID: " + host.ServiceHostID()
	}
	// SDK retries are off, so every call is one attempt.
	return &ServiceError{Name: name, Cause: msg + " (" + detail + ") (SDK Attempt Count: 1)"}
}
