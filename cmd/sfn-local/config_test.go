package main

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestFlagsOverrideEnvironment(t *testing.T) {
	env := map[string]string{
		"PORT":                    "8080",
		"AWS_ACCOUNT_ID":          "2",
		"AWS_DEFAULT_REGION":      "eu-west-1",
		"SFN_COMPAT":              "sfn-local",
		"WAIT_TIME_SCALE":         "-2",
		"LAMBDA_ENDPOINT":         "http://lambda",
		"API_GATEWAY_ENDPOINT":    "http://api",
		"EVENTBRIDGE_ENDPOINT":    "http://events",
		"STEP_FUNCTIONS_ENDPOINT": "http://sfn",
		"SFN_ENDPOINT":            "http://ignored",
		"SFN_MOCK_CONFIG":         "MockConfigFile.json",
		"BIND_ADDRESS":            "127.0.0.1",
	}
	cfg, err := parseArgs(nil, lookup(env))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Port != 8080 || cfg.Account != "2" || cfg.Region != "eu-west-1" || cfg.Profile != "sfn-local" {
		t.Fatalf("%+v", cfg)
	}
	if cfg.WaitTimeScale == nil || *cfg.WaitTimeScale != 2 {
		t.Fatalf("scale %v", cfg.WaitTimeScale)
	}
	if cfg.LambdaEndpoint != "http://lambda" || cfg.APIGatewayEndpoint != "http://api" || cfg.EventBridgeEndpoint != "http://events" {
		t.Fatalf("endpoints %+v", cfg)
	}
	if cfg.StepFunctionsEndpoint != "http://sfn" {
		t.Fatalf("step functions endpoint %s", cfg.StepFunctionsEndpoint)
	}
	if cfg.MockConfig != "MockConfigFile.json" || cfg.BindAddress != "127.0.0.1" || cfg.GlobalRegion != "aws-global" {
		t.Fatalf("%+v", cfg)
	}
	cfg, err = parseArgs([]string{"--port", "9", "--aws-account", "1", "--compat", "smithy", "--wait-time-scale", "0", "--bind-address", "0.0.0.0"}, lookup(env))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Port != 9 || cfg.Account != "1" || cfg.Profile != "smithy" || cfg.BindAddress != "0.0.0.0" {
		t.Fatalf("flag did not override: %+v", cfg)
	}
	if cfg.WaitTimeScale == nil || *cfg.WaitTimeScale != 0 {
		t.Fatalf("zero scale %v", cfg.WaitTimeScale)
	}
}

func TestWaitScaleInvalidAndHelp(t *testing.T) {
	cfg, err := parseArgs(nil, lookup(map[string]string{"WAIT_TIME_SCALE": "nope"}))
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.WaitInvalid || cfg.WaitTimeScale != nil {
		t.Fatalf("%+v", cfg)
	}
	var out, errb bytes.Buffer
	if code := run([]string{"-h"}, lookup(nil), &out, &errb, context.Background()); code != 0 {
		t.Fatalf("help %d %s", code, errb.String())
	}
	help := out.String()
	for _, phrase := range []string{"in memory", "127.0.0.1", "compose network", "Core dumps", "apiGatewayEndpoint", "eventBridgeEndpoint", "MockConfigFile.json", "#TestCase", "sfn-local dev"} {
		if !strings.Contains(help, phrase) {
			t.Fatalf("help missing %q\n%s", phrase, help)
		}
	}
	out.Reset()
	if code := run([]string{"-v"}, lookup(nil), &out, &errb, context.Background()); code != 0 {
		t.Fatal(code)
	}
	if strings.Count(out.String(), "sfn-local dev") != 2 || strings.Contains(out.String(), "Starting server") {
		t.Fatalf("%q", out.String())
	}
	out.Reset()
	errb.Reset()
	if code := run([]string{"--no-such"}, lookup(nil), &out, &errb, context.Background()); code != 1 {
		t.Fatalf("invalid %d", code)
	}
	if !strings.Contains(errb.String(), "in memory") {
		t.Fatalf("stderr %s", errb.String())
	}
	names := map[string]bool{}
	for _, name := range flagNames() {
		names[name] = true
	}
	for _, name := range []string{"account", "aws-account", "dynamoDBEndpoint", "emronEKSEndpoint", "stepFunctionsEndpoint", "compat", "port", "bindAddress"} {
		if !names[name] {
			t.Fatalf("missing flag %s in %v", name, names)
		}
	}
	if names["failure-count"] || names["SFN_ENDPOINT"] {
		t.Fatalf("unexpected flag %v", names)
	}
	cfg, err = parseArgs(nil, lookup(nil))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Port != 8083 || cfg.BindAddress != "0.0.0.0" || cfg.Account != "123456789012" || cfg.Profile != "smithy" || cfg.MockConfig != "./MockConfigFile.json" {
		t.Fatalf("defaults %+v", cfg)
	}
	if _, err := parseArgs([]string{"--compat", "other"}, lookup(nil)); err == nil {
		t.Fatal("bad compat accepted")
	}
}

func TestStartupBanner(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var out safeBuf
	done := make(chan int, 1)
	go func() {
		done <- run([]string{"-bindAddress", "127.0.0.1", "-port", "0"}, lookup(nil), &out, &out, ctx)
	}()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && !strings.Contains(out.String(), "Starting server on port") {
		time.Sleep(5 * time.Millisecond)
	}
	text := out.String()
	cancel()
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("exit %d\n%s", code, text)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown timeout")
	}
	if !strings.HasPrefix(text, "sfn-local dev\n") {
		t.Fatalf("version line %q", text)
	}
	if !strings.Contains(text, "with account 123456789012, region us-east-1, bind address 127.0.0.1") {
		t.Fatalf("banner %q", text)
	}
	if !strings.Contains(text, "Failed to load credentials, default to dummy credentials...") {
		t.Fatalf("credentials %q", text)
	}
	if strings.Contains(text, "abcd") {
		t.Fatal("logged the dummy access key")
	}
}

func lookup(env map[string]string) func(string) (string, bool) {
	return func(key string) (string, bool) {
		v, ok := env[key]
		return v, ok
	}
}

type safeBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *safeBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *safeBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}
