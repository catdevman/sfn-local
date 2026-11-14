// Command awsrecord runs the awscheck scenarios against a real AWS account
// and writes the normalized answers to internal/awscheck/testdata. The api
// package's TestAWSRecordings then compares this server with them.
//
// It uses the AWS CLI's credentials and region (AWS_PROFILE, AWS_REGION, and
// the shared config files). It creates two IAM roles, one S3 bucket, one
// Lambda function, and a few state machines, all named sfnlocal-check-<id>-...,
// and deletes them when it finishes unless -keep is set. A run costs a few
// cents at most.
package main

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sfn"

	"github.com/catdevman/sfn-local/internal/awscheck"
)

const trustPolicy = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"states.amazonaws.com"},"Action":"sts:AssumeRole"}]}`

// rolePolicy lets the scenarios start child executions and reach every
// service sdk-errors calls, so a missing resource is reported as missing
// rather than as AccessDenied.
func rolePolicy(bucket string) string {
	return `{"Version":"2012-10-17","Statement":[
		{"Effect":"Allow","Action":["states:StartExecution","states:DescribeExecution","states:StopExecution","states:RedriveExecution"],"Resource":"*"},
		{"Effect":"Allow","Action":["s3:GetObject","s3:PutObject","s3:ListBucket","s3:ListMultipartUploadParts","s3:AbortMultipartUpload"],"Resource":["arn:aws:s3:::` + bucket + `","arn:aws:s3:::` + bucket + `/*"]},
		{"Effect":"Allow","Action":["ssm:GetParameter","secretsmanager:GetSecretValue","lambda:InvokeFunction"],"Resource":"*"}]}`
}

func main() {
	out := flag.String("out", filepath.Join("internal", "awscheck", "testdata"), "Directory for the recordings.")
	only := flag.String("only", "", "Comma-separated scenario names. Empty runs every scenario.")
	keep := flag.Bool("keep", false, "Leave the role, bucket, and state machines in the account.")
	timeout := flag.Duration("timeout", 5*time.Minute, "How long one execution may run.")
	flag.Parse()
	if err := run(*out, *only, *keep, *timeout); err != nil {
		fmt.Fprintln(os.Stderr, "awsrecord:", err)
		os.Exit(1)
	}
}

func run(outDir, only string, keep bool, timeout time.Duration) error {
	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return err
	}
	if cfg.Region == "" {
		return errors.New("no region: set AWS_REGION or a region in the AWS CLI config")
	}
	id := randomID()
	prefix := "sfnlocal-check-" + id + "-"
	iamc := iam.NewFromConfig(cfg)
	s3c := s3.NewFromConfig(cfg)
	sfnc := sfn.NewFromConfig(cfg)

	roleName := prefix + "role"
	role, err := iamc.CreateRole(ctx, &iam.CreateRoleInput{RoleName: &roleName, AssumeRolePolicyDocument: aws.String(trustPolicy)})
	if err != nil {
		return fmt.Errorf("create role: %w", err)
	}
	account := strings.Split(*role.Role.Arn, ":")[4]
	bucket := prefix + account
	if len(bucket) > 63 {
		bucket = bucket[:63]
	}
	env := &awscheck.Env{
		SFN: sfnc, S3: s3c, Account: account, Region: cfg.Region, RoleARN: *role.Role.Arn,
		Bucket: bucket, Prefix: prefix, Poll: time.Second, Timeout: timeout, RoleWait: 90 * time.Second,
	}
	fmt.Printf("account %s, region %s, prefix %s\n", account, cfg.Region, prefix)
	if !keep {
		defer cleanup(ctx, env, iamc, s3c, roleName)
	}
	create := &s3.CreateBucketInput{Bucket: &bucket}
	if cfg.Region != "us-east-1" {
		create.CreateBucketConfiguration = &s3types.CreateBucketConfiguration{LocationConstraint: s3types.BucketLocationConstraint(cfg.Region)}
	}
	if _, err := s3c.CreateBucket(ctx, create); err != nil {
		return fmt.Errorf("create bucket: %w", err)
	}
	if _, err := iamc.PutRolePolicy(ctx, &iam.PutRolePolicyInput{RoleName: &roleName, PolicyName: aws.String("scenarios"), PolicyDocument: aws.String(rolePolicy(bucket))}); err != nil {
		return fmt.Errorf("put role policy: %w", err)
	}
	fmt.Println("waiting for the role to work ...")
	if err := env.WaitForRole(ctx, 4*time.Minute); err != nil {
		return err
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}
	want := map[string]bool{}
	for _, name := range strings.Split(only, ",") {
		if name = strings.TrimSpace(name); name != "" {
			want[name] = true
		}
	}
	if len(want) == 0 || want["lambda-invoke"] {
		fmt.Println("creating the echo function ...")
		fn, drop, err := echoFunction(ctx, cfg, iamc, prefix)
		if drop != nil && !keep {
			defer drop()
		}
		if err != nil {
			return err
		}
		env.Function = fn
	}
	failed := 0
	for _, sc := range awscheck.Scenarios {
		if len(want) > 0 && !want[sc.Name] {
			continue
		}
		fmt.Printf("%s ... ", sc.Name)
		doc, err := sc.Run(ctx, env)
		if err != nil {
			failed++
			fmt.Println("error:", err)
			continue
		}
		canon, err := awscheck.Canonical(env.Normalize(doc))
		if err != nil {
			return err
		}
		text, err := json.MarshalIndent(canon, "", "  ")
		if err != nil {
			return err
		}
		path := filepath.Join(outDir, sc.Name+".json")
		if err := os.WriteFile(path, append(text, '\n'), 0o644); err != nil {
			return err
		}
		fmt.Println("wrote", path)
	}
	if failed > 0 {
		return fmt.Errorf("%d scenarios failed", failed)
	}
	return nil
}

// cleanup deletes everything run created. Errors are printed, not returned,
// so one leftover does not hide the others.
func cleanup(ctx context.Context, env *awscheck.Env, iamc *iam.Client, s3c *s3.Client, roleName string) {
	env.Cleanup(ctx)
	var token *string
	for {
		list, err := s3c.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: &env.Bucket, ContinuationToken: token})
		if err != nil {
			break
		}
		for _, obj := range list.Contents {
			if _, err := s3c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &env.Bucket, Key: obj.Key}); err != nil {
				fmt.Fprintln(os.Stderr, "cleanup: delete object:", err)
			}
		}
		if token = list.NextContinuationToken; token == nil {
			break
		}
	}
	if _, err := s3c.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: &env.Bucket}); err != nil {
		fmt.Fprintln(os.Stderr, "cleanup: delete bucket:", err)
	}
	if _, err := iamc.DeleteRolePolicy(ctx, &iam.DeleteRolePolicyInput{RoleName: &roleName, PolicyName: aws.String("scenarios")}); err != nil {
		fmt.Fprintln(os.Stderr, "cleanup: delete role policy:", err)
	}
	if _, err := iamc.DeleteRole(ctx, &iam.DeleteRoleInput{RoleName: &roleName}); err != nil {
		fmt.Fprintln(os.Stderr, "cleanup: delete role:", err)
	}
}

// echoCode is the function lambda-invoke calls.
const echoCode = `export const handler = async (event) => ({ echo: event });
`

// echoFunction creates a Node.js function that returns {"echo": <event>},
// with a role it can assume. drop deletes both, and is set as soon as there
// is something to delete.
func echoFunction(ctx context.Context, cfg aws.Config, iamc *iam.Client, prefix string) (string, func(), error) {
	lc := lambda.NewFromConfig(cfg)
	roleName := prefix + "lambda"
	name := prefix + "fn"
	role, err := iamc.CreateRole(ctx, &iam.CreateRoleInput{RoleName: &roleName,
		AssumeRolePolicyDocument: aws.String(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}`)})
	if err != nil {
		return "", nil, fmt.Errorf("create lambda role: %w", err)
	}
	drop := func() {
		if _, err := lc.DeleteFunction(ctx, &lambda.DeleteFunctionInput{FunctionName: &name}); err != nil {
			var missing *lambdatypes.ResourceNotFoundException
			if !errors.As(err, &missing) {
				fmt.Fprintln(os.Stderr, "cleanup: delete function:", err)
			}
		}
		if _, err := iamc.DeleteRole(ctx, &iam.DeleteRoleInput{RoleName: &roleName}); err != nil {
			fmt.Fprintln(os.Stderr, "cleanup: delete lambda role:", err)
		}
	}
	var zipped bytes.Buffer
	zw := zip.NewWriter(&zipped)
	w, err := zw.Create("index.mjs")
	if err == nil {
		_, err = w.Write([]byte(echoCode))
	}
	if err == nil {
		err = zw.Close()
	}
	if err != nil {
		return "", drop, err
	}
	// A new role takes a few seconds before Lambda can assume it.
	deadline := time.Now().Add(90 * time.Second)
	for {
		_, err = lc.CreateFunction(ctx, &lambda.CreateFunctionInput{FunctionName: &name, Role: role.Role.Arn,
			Runtime: lambdatypes.RuntimeNodejs20x, Handler: aws.String("index.handler"), Code: &lambdatypes.FunctionCode{ZipFile: zipped.Bytes()}})
		var invalid *lambdatypes.InvalidParameterValueException
		if err == nil || !errors.As(err, &invalid) || time.Now().After(deadline) {
			break
		}
		time.Sleep(5 * time.Second)
	}
	if err != nil {
		return "", drop, fmt.Errorf("create function: %w", err)
	}
	if err := lambda.NewFunctionActiveV2Waiter(lc).Wait(ctx, &lambda.GetFunctionInput{FunctionName: &name}, 2*time.Minute); err != nil {
		return "", drop, fmt.Errorf("wait for function: %w", err)
	}
	return name, drop, nil
}

func randomID() string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
