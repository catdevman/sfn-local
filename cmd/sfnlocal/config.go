package main

import (
	"errors"
	"flag"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/catdevman/sfn-local/internal/api"
)

var (
	errHelp    = errors.New("help")
	errVersion = errors.New("version")
)

// Config is the process configuration. Flags override environment variables.
// The server reads MockConfig when a StartExecution ARN contains one # test suffix.
type Config struct {
	Account       string
	Region        string
	GlobalRegion  string
	Port          int
	BindAddress   string
	Profile       api.Profile
	MockConfig    string
	WaitTimeScale *float64
	WaitInvalid   bool
	WaitRaw       string

	LambdaEndpoint        string
	BatchEndpoint         string
	DynamoDBEndpoint      string
	ECSEndpoint           string
	GlueEndpoint          string
	SageMakerEndpoint     string
	SQSEndpoint           string
	SNSEndpoint           string
	StepFunctionsEndpoint string
	AthenaEndpoint        string
	EKSEndpoint           string
	DataBrewEndpoint      string
	APIGatewayEndpoint    string
	EventBridgeEndpoint   string
	EMROnEKSEndpoint      string

	AccessKeyID     string
	SecretAccessKey string
}

type envFunc func(string) (string, bool)

func parseArgs(args []string, getenv envFunc) (Config, error) {
	if getenv == nil {
		getenv = func(string) (string, bool) { return "", false }
	}
	cfg := Config{}
	fs := flag.NewFlagSet(Name, flag.ContinueOnError)
	fs.SetOutput(ioDiscard{})
	var help, version bool
	fs.BoolVar(&help, "h", false, "Print help and exit.")
	fs.BoolVar(&help, "help", false, "Print help and exit.")
	fs.BoolVar(&version, "v", false, "Print the version and exit.")
	fs.BoolVar(&version, "version", false, "Print the version and exit.")

	stringFlag(fs, &cfg.Account, "account", "aws-account", "AWS_ACCOUNT_ID", "123456789012", "Account id used inside ARNs.", getenv)
	stringFlag(fs, &cfg.Region, "region", "aws-region", "AWS_DEFAULT_REGION", "us-east-1", "AWS region.", getenv)
	stringFlag(fs, &cfg.GlobalRegion, "gr", "aws-global-region", "AWS_DEFAULT_GLOBAL_REGION", "aws-global", "Global region. Stored for a later integration.", getenv)
	stringFlag(fs, &cfg.LambdaEndpoint, "lambdaEndpoint", "lambda-endpoint", "LAMBDA_ENDPOINT", "", "Lambda endpoint.", getenv)
	stringFlag(fs, &cfg.BatchEndpoint, "batchEndpoint", "batch-endpoint", "BATCH_ENDPOINT", "", "Batch endpoint.", getenv)
	stringFlag(fs, &cfg.DynamoDBEndpoint, "dynamoDBEndpoint", "dynamodb-endpoint", "DYNAMODB_ENDPOINT", "", "DynamoDB endpoint.", getenv)
	stringFlag(fs, &cfg.ECSEndpoint, "ecsEndpoint", "ecs-endpoint", "ECS_ENDPOINT", "", "ECS endpoint.", getenv)
	stringFlag(fs, &cfg.GlueEndpoint, "glueEndpoint", "glue-endpoint", "GLUE_ENDPOINT", "", "Glue endpoint.", getenv)
	stringFlag(fs, &cfg.SageMakerEndpoint, "sageMakerEndpoint", "sagemaker-endpoint", "SAGEMAKER_ENDPOINT", "", "SageMaker endpoint.", getenv)
	stringFlag(fs, &cfg.SQSEndpoint, "sqsEndpoint", "sqs-endpoint", "SQS_ENDPOINT", "", "SQS endpoint.", getenv)
	stringFlag(fs, &cfg.SNSEndpoint, "snsEndpoint", "sns-endpoint", "SNS_ENDPOINT", "", "SNS endpoint.", getenv)
	stringFlag(fs, &cfg.StepFunctionsEndpoint, "stepFunctionsEndpoint", "step-functions-endpoint", "STEP_FUNCTIONS_ENDPOINT", "", "Nested Step Functions endpoint.", getenv)
	stringFlag(fs, &cfg.AthenaEndpoint, "stepFunctionsAthena", "step-functions-athena", "STEP_FUNCTIONS_ATHENA", "", "Athena endpoint.", getenv)
	stringFlag(fs, &cfg.EKSEndpoint, "stepFunctionsEKS", "step-functions-eks", "STEP_FUNCTIONS_EKS", "", "EKS endpoint.", getenv)
	stringFlag(fs, &cfg.DataBrewEndpoint, "stepFunctionsDataBrew", "step-functions-databrew", "STEP_FUNCTIONS_DATABREW", "", "DataBrew endpoint.", getenv)
	stringFlag(fs, &cfg.APIGatewayEndpoint, "apiGatewayEndpoint", "api-gateway-endpoint", "API_GATEWAY_ENDPOINT", "", "API Gateway endpoint.", getenv)
	stringFlag(fs, &cfg.EventBridgeEndpoint, "eventBridgeEndpoint", "eventbridge-endpoint", "EVENTBRIDGE_ENDPOINT", "", "EventBridge endpoint.", getenv)
	stringFlag(fs, &cfg.EMROnEKSEndpoint, "emronEKSEndpoint", "emr-on-eks-endpoint", "EMR_ON_EKS_ENDPOINT", "", "EMR on EKS endpoint.", getenv)

	portDefault, err := envInt(getenv, "PORT", 8083)
	if err != nil {
		return Config{}, err
	}
	fs.IntVar(&cfg.Port, "port", portDefault, "Listen port.")
	stringFlag(fs, &cfg.BindAddress, "bindAddress", "bind-address", "BIND_ADDRESS", "0.0.0.0", "Bind address.", getenv)

	compatDefault := "smithy"
	if v, ok := getenv("SFN_COMPAT"); ok {
		compatDefault = v
	}
	var compat string
	fs.StringVar(&compat, "compat", compatDefault, "Error profile: smithy or sfn-local.")

	waitRaw := ""
	waitFromEnv := false
	if v, ok := getenv("WAIT_TIME_SCALE"); ok {
		waitRaw = v
		waitFromEnv = true
	}
	fs.StringVar(&waitRaw, "waitTimeScale", waitRaw, "Wait duration scale. A non-numeric value is ignored.")
	fs.StringVar(&waitRaw, "wait-time-scale", waitRaw, "Wait duration scale. A non-numeric value is ignored.")

	cfg.MockConfig = "./MockConfigFile.json"
	if v, ok := getenv("SFN_MOCK_CONFIG"); ok {
		cfg.MockConfig = v
	}
	if v, ok := getenv("AWS_ACCESS_KEY_ID"); ok && v != "" {
		cfg.AccessKeyID = v
		cfg.SecretAccessKey, _ = getenv("AWS_SECRET_ACCESS_KEY")
	}

	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}
	if fs.NArg() > 0 {
		return Config{}, fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}
	if help {
		return cfg, errHelp
	}
	if version {
		return cfg, errVersion
	}
	if cfg.Port < 0 || cfg.Port > 65535 {
		return Config{}, fmt.Errorf("port %d is outside 0..65535", cfg.Port)
	}
	profile, ok := api.ParseProfile(compat)
	if !ok {
		return Config{}, fmt.Errorf("invalid --compat value %q (smithy or sfn-local)", compat)
	}
	cfg.Profile = profile
	visitedWait := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "waitTimeScale" || f.Name == "wait-time-scale" {
			visitedWait = true
		}
	})
	cfg.WaitRaw = waitRaw
	if visitedWait || waitFromEnv {
		f, err := strconv.ParseFloat(strings.TrimSpace(waitRaw), 64)
		if err != nil {
			cfg.WaitInvalid = true
		} else {
			f = math.Abs(f)
			cfg.WaitTimeScale = &f
		}
	}
	return cfg, nil
}

func stringFlag(fs *flag.FlagSet, dest *string, short, long, envKey, fallback, usage string, getenv envFunc) {
	value := fallback
	if v, ok := getenv(envKey); ok {
		value = v
	}
	*dest = value
	if short == long {
		fs.StringVar(dest, long, value, usage)
		return
	}
	fs.StringVar(dest, short, value, usage)
	fs.StringVar(dest, long, value, usage)
}

func envInt(getenv envFunc, key string, fallback int) (int, error) {
	v, ok := getenv(key)
	if !ok {
		return fallback, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s %q is not an integer", key, v)
	}
	return n, nil
}

func helpText() string {
	return Name + ` ` + Version + `

Local Step Functions server. The store is in memory. Process exit drops definitions, inputs, outputs, and tokens.
Bind 127.0.0.1 unless the port is published on purpose. Do not expose this port beyond the host or the compose network. Core dumps and swap can retain inputs, outputs, and tokens.
GET /dashboard serves a console of state machines, executions, and history. API calls stay POST.

SigV4 is not checked. The default error profile is the public Smithy shape id com.amazonaws.sfn#<ShapeName>. --compat=sfn-local selects the Step Functions Local error profile.

Step Functions Local 2.0.0 leaves API Gateway and EventBridge endpoint options off the list that parseArgs walks, so those flags and variables do nothing in that jar. This server accepts -apiGatewayEndpoint, --api-gateway-endpoint, API_GATEWAY_ENDPOINT, and the EventBridge equivalents.

The mock file defaults to ./MockConfigFile.json (SFN_MOCK_CONFIG). An execution name may end in #TestCase, the same suffix Step Functions Local and LocalStack use. This process reads the file when a StartExecution state machine ARN contains one # test suffix.

Flags override environment variables.

` + flagDefaults()
}

func flagDefaults() string {
	fs := flag.NewFlagSet(Name, flag.ContinueOnError)
	fs.SetOutput(ioDiscard{})
	var ignored Config
	var help, version bool
	fs.BoolVar(&help, "h", false, "Print help and exit.")
	fs.BoolVar(&help, "help", false, "Print help and exit.")
	fs.BoolVar(&version, "v", false, "Print the version and exit.")
	fs.BoolVar(&version, "version", false, "Print the version and exit.")
	none := func(string) (string, bool) { return "", false }
	stringFlag(fs, &ignored.Account, "account", "aws-account", "AWS_ACCOUNT_ID", "123456789012", "Account id used inside ARNs.", none)
	stringFlag(fs, &ignored.Region, "region", "aws-region", "AWS_DEFAULT_REGION", "us-east-1", "AWS region.", none)
	stringFlag(fs, &ignored.GlobalRegion, "gr", "aws-global-region", "AWS_DEFAULT_GLOBAL_REGION", "aws-global", "Global region. Stored for a later integration.", none)
	stringFlag(fs, &ignored.LambdaEndpoint, "lambdaEndpoint", "lambda-endpoint", "LAMBDA_ENDPOINT", "", "Lambda endpoint.", none)
	stringFlag(fs, &ignored.BatchEndpoint, "batchEndpoint", "batch-endpoint", "BATCH_ENDPOINT", "", "Batch endpoint.", none)
	stringFlag(fs, &ignored.DynamoDBEndpoint, "dynamoDBEndpoint", "dynamodb-endpoint", "DYNAMODB_ENDPOINT", "", "DynamoDB endpoint.", none)
	stringFlag(fs, &ignored.ECSEndpoint, "ecsEndpoint", "ecs-endpoint", "ECS_ENDPOINT", "", "ECS endpoint.", none)
	stringFlag(fs, &ignored.GlueEndpoint, "glueEndpoint", "glue-endpoint", "GLUE_ENDPOINT", "", "Glue endpoint.", none)
	stringFlag(fs, &ignored.SageMakerEndpoint, "sageMakerEndpoint", "sagemaker-endpoint", "SAGEMAKER_ENDPOINT", "", "SageMaker endpoint.", none)
	stringFlag(fs, &ignored.SQSEndpoint, "sqsEndpoint", "sqs-endpoint", "SQS_ENDPOINT", "", "SQS endpoint.", none)
	stringFlag(fs, &ignored.SNSEndpoint, "snsEndpoint", "sns-endpoint", "SNS_ENDPOINT", "", "SNS endpoint.", none)
	stringFlag(fs, &ignored.StepFunctionsEndpoint, "stepFunctionsEndpoint", "step-functions-endpoint", "STEP_FUNCTIONS_ENDPOINT", "", "Nested Step Functions endpoint.", none)
	stringFlag(fs, &ignored.AthenaEndpoint, "stepFunctionsAthena", "step-functions-athena", "STEP_FUNCTIONS_ATHENA", "", "Athena endpoint.", none)
	stringFlag(fs, &ignored.EKSEndpoint, "stepFunctionsEKS", "step-functions-eks", "STEP_FUNCTIONS_EKS", "", "EKS endpoint.", none)
	stringFlag(fs, &ignored.DataBrewEndpoint, "stepFunctionsDataBrew", "step-functions-databrew", "STEP_FUNCTIONS_DATABREW", "", "DataBrew endpoint.", none)
	stringFlag(fs, &ignored.APIGatewayEndpoint, "apiGatewayEndpoint", "api-gateway-endpoint", "API_GATEWAY_ENDPOINT", "", "API Gateway endpoint.", none)
	stringFlag(fs, &ignored.EventBridgeEndpoint, "eventBridgeEndpoint", "eventbridge-endpoint", "EVENTBRIDGE_ENDPOINT", "", "EventBridge endpoint.", none)
	stringFlag(fs, &ignored.EMROnEKSEndpoint, "emronEKSEndpoint", "emr-on-eks-endpoint", "EMR_ON_EKS_ENDPOINT", "", "EMR on EKS endpoint.", none)
	fs.IntVar(&ignored.Port, "port", 8083, "Listen port.")
	stringFlag(fs, &ignored.BindAddress, "bindAddress", "bind-address", "BIND_ADDRESS", "0.0.0.0", "Bind address.", none)
	var compat string
	fs.StringVar(&compat, "compat", "smithy", "Error profile: smithy or sfn-local.")
	var wait string
	fs.StringVar(&wait, "waitTimeScale", "", "Wait duration scale. A non-numeric value is ignored.")
	fs.StringVar(&wait, "wait-time-scale", "", "Wait duration scale. A non-numeric value is ignored.")
	var b strings.Builder
	fs.SetOutput(&b)
	fs.PrintDefaults()
	return b.String()
}

// flagNames returns the registered flag names, for tests.
func flagNames() []string {
	fs := flag.NewFlagSet(Name, flag.ContinueOnError)
	fs.SetOutput(ioDiscard{})
	var cfg Config
	var help, version bool
	fs.BoolVar(&help, "h", false, "")
	fs.BoolVar(&help, "help", false, "")
	fs.BoolVar(&version, "v", false, "")
	fs.BoolVar(&version, "version", false, "")
	none := func(string) (string, bool) { return "", false }
	stringFlag(fs, &cfg.Account, "account", "aws-account", "AWS_ACCOUNT_ID", "123456789012", "", none)
	stringFlag(fs, &cfg.Region, "region", "aws-region", "AWS_DEFAULT_REGION", "us-east-1", "", none)
	stringFlag(fs, &cfg.GlobalRegion, "gr", "aws-global-region", "AWS_DEFAULT_GLOBAL_REGION", "aws-global", "", none)
	stringFlag(fs, &cfg.LambdaEndpoint, "lambdaEndpoint", "lambda-endpoint", "LAMBDA_ENDPOINT", "", "", none)
	stringFlag(fs, &cfg.BatchEndpoint, "batchEndpoint", "batch-endpoint", "BATCH_ENDPOINT", "", "", none)
	stringFlag(fs, &cfg.DynamoDBEndpoint, "dynamoDBEndpoint", "dynamodb-endpoint", "DYNAMODB_ENDPOINT", "", "", none)
	stringFlag(fs, &cfg.ECSEndpoint, "ecsEndpoint", "ecs-endpoint", "ECS_ENDPOINT", "", "", none)
	stringFlag(fs, &cfg.GlueEndpoint, "glueEndpoint", "glue-endpoint", "GLUE_ENDPOINT", "", "", none)
	stringFlag(fs, &cfg.SageMakerEndpoint, "sageMakerEndpoint", "sagemaker-endpoint", "SAGEMAKER_ENDPOINT", "", "", none)
	stringFlag(fs, &cfg.SQSEndpoint, "sqsEndpoint", "sqs-endpoint", "SQS_ENDPOINT", "", "", none)
	stringFlag(fs, &cfg.SNSEndpoint, "snsEndpoint", "sns-endpoint", "SNS_ENDPOINT", "", "", none)
	stringFlag(fs, &cfg.StepFunctionsEndpoint, "stepFunctionsEndpoint", "step-functions-endpoint", "STEP_FUNCTIONS_ENDPOINT", "", "", none)
	stringFlag(fs, &cfg.AthenaEndpoint, "stepFunctionsAthena", "step-functions-athena", "STEP_FUNCTIONS_ATHENA", "", "", none)
	stringFlag(fs, &cfg.EKSEndpoint, "stepFunctionsEKS", "step-functions-eks", "STEP_FUNCTIONS_EKS", "", "", none)
	stringFlag(fs, &cfg.DataBrewEndpoint, "stepFunctionsDataBrew", "step-functions-databrew", "STEP_FUNCTIONS_DATABREW", "", "", none)
	stringFlag(fs, &cfg.APIGatewayEndpoint, "apiGatewayEndpoint", "api-gateway-endpoint", "API_GATEWAY_ENDPOINT", "", "", none)
	stringFlag(fs, &cfg.EventBridgeEndpoint, "eventBridgeEndpoint", "eventbridge-endpoint", "EVENTBRIDGE_ENDPOINT", "", "", none)
	stringFlag(fs, &cfg.EMROnEKSEndpoint, "emronEKSEndpoint", "emr-on-eks-endpoint", "EMR_ON_EKS_ENDPOINT", "", "", none)
	fs.IntVar(&cfg.Port, "port", 8083, "")
	stringFlag(fs, &cfg.BindAddress, "bindAddress", "bind-address", "BIND_ADDRESS", "0.0.0.0", "", none)
	var compat, wait string
	fs.StringVar(&compat, "compat", "smithy", "")
	fs.StringVar(&wait, "waitTimeScale", "", "")
	fs.StringVar(&wait, "wait-time-scale", "", "")
	var names []string
	fs.VisitAll(func(f *flag.Flag) { names = append(names, f.Name) })
	return names
}

type ioDiscard struct{}

func (ioDiscard) Write(p []byte) (int, error) { return len(p), nil }
