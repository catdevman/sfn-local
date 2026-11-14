package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sfn"

	"github.com/catdevman/sfn-local/internal/awscheck"
	"github.com/catdevman/sfn-local/internal/sdk"
)

// TestAWSRecordings runs each scenario that cmd/awsrecord recorded against
// this server and lists every difference. A scenario without a recording is
// skipped. Record with: go run ./cmd/awsrecord
//
// AWSCHECK_LOCAL_DIR=<dir> runs every scenario and also writes this server's
// normalized answers there, for reading next to the recordings.
func TestAWSRecordings(t *testing.T) {
	dir := filepath.Join("..", "awscheck", "testdata")
	dump := os.Getenv("AWSCHECK_LOCAL_DIR")
	for _, sc := range awscheck.Scenarios {
		t.Run(sc.Name, func(t *testing.T) {
			var want any
			text, err := os.ReadFile(filepath.Join(dir, sc.Name+".json"))
			switch {
			case os.IsNotExist(err) && dump == "":
				t.Skip("no recording; run go run ./cmd/awsrecord")
			case os.IsNotExist(err):
			case err != nil:
				t.Fatal(err)
			default:
				if err := json.Unmarshal(text, &want); err != nil {
					t.Fatal(err)
				}
			}
			env := localEnv(t)
			doc, err := sc.Run(context.Background(), env)
			if err != nil {
				t.Fatal(err)
			}
			got, err := awscheck.Canonical(env.Normalize(doc))
			if err != nil {
				t.Fatal(err)
			}
			if dump != "" {
				out, _ := json.MarshalIndent(got, "", "  ")
				if err := os.WriteFile(filepath.Join(dump, sc.Name+".json"), append(out, '\n'), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			if want == nil {
				return
			}
			if diffs := awscheck.Diff(awscheck.Loosen(want), awscheck.Loosen(got)); len(diffs) > 0 {
				t.Errorf("%d differences from AWS:\n%s", len(diffs), strings.Join(diffs, "\n"))
			}
		})
	}
}

// localEnv serves this server over HTTP, with an in-memory S3 and a fake
// that answers Lambda, SSM, and Secrets Manager with not-found errors.
func localEnv(t *testing.T) *awscheck.Env {
	s := newService()
	surface, err := LoadSurface()
	if err != nil {
		t.Fatal(err)
	}
	api := httptest.NewServer(&Server{Handlers: s, Surface: surface, Profile: ProfileSmithy, Log: io.Discard})
	t.Cleanup(api.Close)
	store := httptest.NewServer(&memS3{objects: map[string][]byte{}, pageMax: 1000})
	t.Cleanup(store.Close)
	missing := httptest.NewServer(http.HandlerFunc(notFound))
	t.Cleanup(missing.Close)
	s.Runner.SDK = sdk.New(sdk.Config{Region: "us-east-1", AccessKeyID: "local", SecretAccessKey: "local", Endpoints: map[string]string{
		"s3": store.URL, "sfn": api.URL, "lambda": missing.URL, "ssm": missing.URL, "secretsmanager": missing.URL,
	}})
	creds := credentials.NewStaticCredentialsProvider("local", "local", "")
	return &awscheck.Env{
		SFN: sfn.New(sfn.Options{Region: "us-east-1", Credentials: creds, BaseEndpoint: aws.String(api.URL)}),
		S3: s3.New(s3.Options{Region: "us-east-1", Credentials: creds, BaseEndpoint: aws.String(store.URL), UsePathStyle: true,
			RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired}),
		Account: "123456789012", Region: "us-east-1", RoleARN: roleARN,
		Bucket: "sfnlocal-check-local-123456789012", Prefix: "sfnlocal-check-local-",
		Poll: 5 * time.Millisecond, Timeout: 10 * time.Second,
	}
}

// notFound answers the sdk-errors calls the way each service reports a
// missing resource. Only the error code and status matter: Loosen drops
// the message.
func notFound(w http.ResponseWriter, r *http.Request) {
	switch target := r.Header.Get("X-Amz-Target"); {
	case strings.HasPrefix(target, "AmazonSSM."):
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, `{"__type":"ParameterNotFound"}`)
	case strings.HasPrefix(target, "secretsmanager."):
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, `{"__type":"ResourceNotFoundException","message":"Secrets Manager can't find the specified secret."}`)
	default:
		w.Header().Set("X-Amzn-ErrorType", "ResourceNotFoundException")
		w.WriteHeader(http.StatusNotFound)
		io.WriteString(w, `{"Type":"User","Message":"Function not found"}`)
	}
}
