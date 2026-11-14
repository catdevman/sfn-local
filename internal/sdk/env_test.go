package sdk

import (
	"context"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func envOf(m map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) {
		v, ok := m[k]
		return v, ok
	}
}

func TestLoadOutboundCABundle(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "ok")
	}))
	defer srv.Close()
	ca := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}

	plain, err := LoadOutbound(context.Background(), envOf(nil))
	if err != nil || plain.HTTPClient != http.DefaultClient || plain.Credentials != nil || plain.DisableHostPrefix {
		t.Fatalf("empty env %+v %v", plain, err)
	}
	if _, err := plain.HTTPClient.Get(srv.URL); err == nil {
		t.Fatal("default client trusted the test CA")
	}

	out, err := LoadOutbound(context.Background(), envOf(map[string]string{"AWS_CA_BUNDLE": ca}))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := out.HTTPClient.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if strings.Join(out.Sources, ",") != "AWS_CA_BUNDLE" {
		t.Fatalf("sources %v", out.Sources)
	}

	empty := filepath.Join(t.TempDir(), "empty.pem")
	os.WriteFile(empty, []byte("nothing"), 0o600)
	for _, path := range []string{empty, filepath.Join(t.TempDir(), "missing.pem")} {
		if _, err := LoadOutbound(context.Background(), envOf(map[string]string{"AWS_CA_BUNDLE": path})); err == nil || !strings.Contains(err.Error(), "AWS_CA_BUNDLE") {
			t.Fatalf("%s: %v", path, err)
		}
	}
}

func TestLoadOutboundCredentials(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	creds := filepath.Join(dir, "credentials")
	os.WriteFile(creds, []byte("[default]\naws_access_key_id = EMULATORAAAA\naws_secret_access_key = SECRET\n[other]\naws_access_key_id = OTHER\naws_secret_access_key = S2\n[nokey]\nregion = us-east-1\n"), 0o600)
	conf := filepath.Join(dir, "config")
	os.WriteFile(conf, []byte("[default]\nregion = us-east-1\n"), 0o600)

	for name, tc := range map[string]struct {
		env  map[string]string
		want string
	}{
		"file":         {map[string]string{"AWS_SHARED_CREDENTIALS_FILE": creds, "AWS_CONFIG_FILE": conf}, "EMULATORAAAA"},
		"profile":      {map[string]string{"AWS_SHARED_CREDENTIALS_FILE": creds, "AWS_PROFILE": "other"}, "OTHER"},
		"env wins":     {map[string]string{"AWS_SHARED_CREDENTIALS_FILE": creds, "AWS_ACCESS_KEY_ID": "ENV", "AWS_SECRET_ACCESS_KEY": "x"}, "ENV"},
		"config alone": {map[string]string{"AWS_CONFIG_FILE": conf}, ""},
	} {
		out, err := LoadOutbound(ctx, envOf(tc.env))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		got := ""
		if out.Credentials != nil {
			v, err := out.Credentials.Retrieve(ctx)
			if err != nil {
				t.Fatal(err)
			}
			got = v.AccessKeyID
		}
		if got != tc.want {
			t.Errorf("%s: key %q, want %q", name, got, tc.want)
		}
	}

	if _, err := LoadOutbound(ctx, envOf(map[string]string{"AWS_SHARED_CREDENTIALS_FILE": creds, "AWS_PROFILE": "nokey"})); err == nil {
		t.Fatal("profile with no key loaded")
	}
}

// TestDisableHostPrefix sends aws-sdk:sfn startSyncExecution, which the SDK
// would send to sync-127.0.0.1, to the endpoint's own host.
func TestDisableHostPrefix(t *testing.T) {
	var target string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		target = r.Header.Get("X-Amz-Target")
		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		io.WriteString(w, `{"executionArn":"arn:aws:states:us-east-1:123456789012:express:m:e:1","startDate":1,"stopDate":2,"status":"SUCCEEDED"}`)
	}))
	defer srv.Close()
	out, err := LoadOutbound(context.Background(), envOf(map[string]string{"AWS_DISABLE_HOST_PREFIX_INJECTION": "true"}))
	if err != nil || !out.DisableHostPrefix {
		t.Fatalf("%+v %v", out, err)
	}
	params := []byte(`{"StateMachineArn":"arn:aws:states:us-east-1:123456789012:stateMachine:m"}`)
	c := New(Config{Region: "us-east-1", Endpoints: map[string]string{"sfn": srv.URL}, DisableHostPrefix: true})
	if _, err := c.Call(context.Background(), "sfn", "startSyncExecution", params); err != nil || target != "AWSStepFunctions.StartSyncExecution" {
		t.Fatalf("%q %v", target, err)
	}
	prefixed := New(Config{Region: "us-east-1", Endpoints: map[string]string{"sfn": srv.URL}})
	if _, err := prefixed.Call(context.Background(), "sfn", "startSyncExecution", params); err == nil {
		t.Fatal("sync-127.0.0.1 resolved")
	}
}
