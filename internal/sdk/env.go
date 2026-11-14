package sdk

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
)

// Outbound is how this server's own calls reach a service: the HTTP client,
// the credentials, and whether to skip the SDK's host prefixes. It comes from
// the standard AWS SDK environment variables, so a setup written for an AWS
// SDK, such as the AWS Emulator's, works unchanged. None of it is reachable
// from a state machine definition.
type Outbound struct {
	// HTTPClient trusts AWS_CA_BUNDLE as well as the system roots. It is
	// http.DefaultClient when AWS_CA_BUNDLE is unset.
	HTTPClient *http.Client
	// Credentials is nil for anonymous requests.
	Credentials aws.CredentialsProvider
	// DisableHostPrefix is AWS_DISABLE_HOST_PREFIX_INJECTION. It stops the
	// SDK sending a call such as StartSyncExecution to sync-<host>.
	DisableHostPrefix bool
	// Sources names the variables that set each part, for the start banner.
	Sources []string
}

// LoadOutbound reads, in order:
//
//   - AWS_CA_BUNDLE: a PEM file of extra roots.
//   - AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and AWS_SESSION_TOKEN.
//   - Otherwise AWS_SHARED_CREDENTIALS_FILE and AWS_CONFIG_FILE, profile
//     AWS_PROFILE or default. Only files named by these variables are read.
//     ~/.aws is never read, so a developer's own keys never reach a local
//     endpoint by accident.
//   - AWS_DISABLE_HOST_PREFIX_INJECTION: true.
//
// AWS_EC2_METADATA_DISABLED needs nothing here: instance metadata is never read.
func LoadOutbound(ctx context.Context, getenv func(string) (string, bool)) (Outbound, error) {
	env := func(k string) string {
		v, _ := getenv(k)
		return strings.TrimSpace(v)
	}
	out := Outbound{HTTPClient: http.DefaultClient}
	if path := env("AWS_CA_BUNDLE"); path != "" {
		client, err := caClient(path)
		if err != nil {
			return Outbound{}, err
		}
		out.HTTPClient = client
		out.Sources = append(out.Sources, "AWS_CA_BUNDLE")
	}
	if id := env("AWS_ACCESS_KEY_ID"); id != "" {
		out.Credentials = credentials.NewStaticCredentialsProvider(id, env("AWS_SECRET_ACCESS_KEY"), env("AWS_SESSION_TOKEN"))
		out.Sources = append(out.Sources, "AWS_ACCESS_KEY_ID")
	} else if credFile, confFile := env("AWS_SHARED_CREDENTIALS_FILE"), env("AWS_CONFIG_FILE"); credFile != "" || confFile != "" {
		profile := env("AWS_PROFILE")
		if profile == "" {
			profile = "default"
		}
		sc, err := config.LoadSharedConfigProfile(ctx, profile, func(o *config.LoadSharedConfigOptions) {
			o.CredentialsFiles = nonEmpty(credFile)
			o.ConfigFiles = nonEmpty(confFile)
		})
		if err != nil {
			return Outbound{}, fmt.Errorf("read AWS credentials for profile %s: %w", profile, err)
		}
		if sc.Credentials.HasKeys() {
			out.Credentials = credentials.StaticCredentialsProvider{Value: sc.Credentials}
			out.Sources = append(out.Sources, "AWS_SHARED_CREDENTIALS_FILE")
		} else if credFile != "" {
			return Outbound{}, fmt.Errorf("profile %s in %s has no access key", profile, credFile)
		}
	}
	if strings.EqualFold(env("AWS_DISABLE_HOST_PREFIX_INJECTION"), "true") {
		out.DisableHostPrefix = true
		out.Sources = append(out.Sources, "AWS_DISABLE_HOST_PREFIX_INJECTION")
	}
	return out, nil
}

// nonEmpty is a file list with path, or an empty, non-nil list so the SDK
// does not fall back to its default ~/.aws files.
func nonEmpty(path string) []string {
	if path == "" {
		return []string{}
	}
	return []string{path}
}

func caClient(path string) (*http.Client, error) {
	pem, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("AWS_CA_BUNDLE: %w", err)
	}
	pool, err := x509.SystemCertPool()
	if err != nil || pool == nil {
		pool = x509.NewCertPool()
	}
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("AWS_CA_BUNDLE: %s holds no PEM certificate", path)
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	return &http.Client{Transport: transport}, nil
}
