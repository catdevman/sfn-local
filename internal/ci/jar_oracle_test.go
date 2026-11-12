package ci

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"strings"
	"time"
)

type jarFixture struct {
	name       string
	definition string
	input      string
	status     string
	output     string
	events     []string
	stop       bool
	errorName  string
}

func compareJar(image string) (string, error) {
	name := fmt.Sprintf("sfn-oracle-%d", time.Now().UnixNano())
	run := exec.Command("docker", "run", "-d", "--name", name, "-p", "127.0.0.1::8083", image)
	out, err := run.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("docker run %s: %v: %s", image, err, bytesTrim(out))
	}
	defer exec.Command("docker", "rm", "-f", name).Run()
	portRaw, err := exec.Command("docker", "port", name, "8083/tcp").CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("docker port: %v: %s", err, bytesTrim(portRaw))
	}
	portText := bytesTrim(portRaw)
	if i := strings.LastIndex(portText, ":"); i < 0 {
		return "", fmt.Errorf("docker port %q", portText)
	}
	endpoint := "http://127.0.0.1" + portText[strings.LastIndex(portText, ":"):]
	if err := waitJar(endpoint); err != nil {
		return "", err
	}
	fixtures := []jarFixture{
		{
			name:       "pass-result",
			definition: `{"StartAt":"P","States":{"P":{"Type":"Pass","Result":{"ok":true},"End":true}}}`,
			input:      `{"n":1}`,
			status:     "SUCCEEDED",
			output:     `{"ok":true}`,
			events:     []string{"ExecutionStarted", "PassStateEntered", "PassStateExited", "ExecutionSucceeded"},
		},
		{
			name:       "fail-boom",
			definition: `{"StartAt":"F","States":{"F":{"Type":"Fail","Error":"Boom","Cause":"no"}}}`,
			input:      `{}`,
			status:     "FAILED",
			events:     []string{"ExecutionStarted", "FailStateEntered", "ExecutionFailed"},
			errorName:  "Boom",
		},
		{
			name:       "choice",
			definition: `{"StartAt":"C","States":{"C":{"Type":"Choice","Choices":[{"Variable":"$.n","NumericGreaterThan":1,"Next":"Yes"}],"Default":"No"},"Yes":{"Type":"Succeed"},"No":{"Type":"Fail","Error":"TooSmall","Cause":"n<=1"}}}`,
			input:      `{"n":2}`,
			status:     "SUCCEEDED",
			output:     `{"n":2}`,
			events:     []string{"ExecutionStarted", "ChoiceStateEntered", "ChoiceStateExited", "SucceedStateEntered", "SucceedStateExited", "ExecutionSucceeded"},
		},
		{
			name:       "parameters",
			definition: `{"StartAt":"P","States":{"P":{"Type":"Pass","Parameters":{"name.$":"$.user.name","static":1},"End":true}}}`,
			input:      `{"user":{"name":"ada"}}`,
			status:     "SUCCEEDED",
			output:     `{"name":"ada","static":1}`,
			events:     []string{"ExecutionStarted", "PassStateEntered", "PassStateExited", "ExecutionSucceeded"},
		},
		{
			name:       "stop-wait",
			definition: `{"StartAt":"W","States":{"W":{"Type":"Wait","Seconds":30,"End":true}}}`,
			input:      `{}`,
			status:     "ABORTED",
			stop:       true,
			events:     []string{"ExecutionAborted"},
		},
	}
	for _, fx := range fixtures {
		if err := runJarFixture(endpoint, fx); err != nil {
			return "", fmt.Errorf("%s: %w", fx.name, err)
		}
	}
	return "jar oracle: compared status, output JSON, and event types against " + image, nil
}

func waitJar(endpoint string) error {
	deadline := time.Now().Add(30 * time.Second)
	var last error
	for time.Now().Before(deadline) {
		_, err := jarCall(endpoint, "ListStateMachines", map[string]any{})
		if err == nil {
			return nil
		}
		last = err
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("jar did not listen: %v", last)
}

func runJarFixture(endpoint string, fx jarFixture) error {
	role := "arn:aws:iam::123456789012:role/local"
	created, err := jarCall(endpoint, "CreateStateMachine", map[string]any{
		"name": fx.name, "definition": fx.definition, "roleArn": role,
	})
	if err != nil {
		return err
	}
	sm, _ := created["stateMachineArn"].(string)
	started, err := jarCall(endpoint, "StartExecution", map[string]any{
		"stateMachineArn": sm, "name": "run1", "input": fx.input,
	})
	if err != nil {
		return err
	}
	execARN, _ := started["executionArn"].(string)
	if fx.stop {
		if _, err := jarCall(endpoint, "StopExecution", map[string]any{"executionArn": execARN}); err != nil {
			return err
		}
	}
	deadline := time.Now().Add(20 * time.Second)
	var described map[string]any
	for {
		described, err = jarCall(endpoint, "DescribeExecution", map[string]any{"executionArn": execARN})
		if err != nil {
			return err
		}
		status, _ := described["status"].(string)
		if status != "" && status != "RUNNING" {
			break
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("still %v", described["status"])
		}
		time.Sleep(100 * time.Millisecond)
	}
	if described["status"] != fx.status {
		return fmt.Errorf("status %v", described["status"])
	}
	if fx.output != "" {
		if err := jsonEqual(described["output"], fx.output); err != nil {
			return err
		}
	}
	history, err := jarCall(endpoint, "GetExecutionHistory", map[string]any{"executionArn": execARN})
	if err != nil {
		return err
	}
	events, _ := history["events"].([]any)
	var types []string
	for _, item := range events {
		ev, _ := item.(map[string]any)
		typ, _ := ev["type"].(string)
		types = append(types, typ)
		if typ == "ExecutionFailed" && fx.errorName != "" {
			details, _ := ev["executionFailedEventDetails"].(map[string]any)
			if details["error"] != fx.errorName {
				return fmt.Errorf("error %v", details["error"])
			}
		}
	}
	if fx.stop {
		if count(types, "ExecutionAborted") != 1 || !containsAll(types, fx.events) {
			return fmt.Errorf("events %v", types)
		}
		return nil
	}
	if strings.Join(types, ",") != strings.Join(fx.events, ",") {
		return fmt.Errorf("events %v", types)
	}
	if contains(types, "FailStateExited") {
		return fmt.Errorf("FailStateExited in %v", types)
	}
	return nil
}

func jarCall(endpoint, op string, body map[string]any) (map[string]any, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest(http.MethodPost, endpoint+"/", bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "AWSStepFunctions."+op)
	client := &http.Client{Timeout: 5 * time.Second}
	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	if res.StatusCode != 200 {
		return nil, fmt.Errorf("%s %d %s", op, res.StatusCode, payload)
	}
	var out map[string]any
	if err := json.Unmarshal(payload, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func jsonEqual(got any, want string) error {
	text, _ := got.(string)
	var a, b any
	if err := json.Unmarshal([]byte(text), &a); err != nil {
		return fmt.Errorf("output %q: %w", text, err)
	}
	if err := json.Unmarshal([]byte(want), &b); err != nil {
		return err
	}
	ab, _ := json.Marshal(a)
	bb, _ := json.Marshal(b)
	if string(ab) != string(bb) {
		return fmt.Errorf("output %s want %s", ab, bb)
	}
	return nil
}

func count(items []string, want string) int {
	n := 0
	for _, item := range items {
		if item == want {
			n++
		}
	}
	return n
}

func contains(items []string, want string) bool {
	return count(items, want) > 0
}

func containsAll(items, want []string) bool {
	for _, item := range want {
		if !contains(items, item) {
			return false
		}
	}
	return true
}
