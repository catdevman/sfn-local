package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestActionsBuild(t *testing.T) {
	for _, id := range Services() {
		if len(Actions(id)) == 0 {
			t.Fatalf("%s has no actions", id)
		}
	}
}

func TestParseResource(t *testing.T) {
	for resource, want := range map[string][2]string{
		"arn:aws:states:::aws-sdk:s3:getObject":                  {"s3", "getObject"},
		"arn:aws:states:::aws-sdk:s3:listObjectsV2":              {"s3", "listObjectsV2"},
		"arn:aws:states:::aws-sdk:s3:getObject.waitForTaskToken": {},
		"arn:aws:states:::aws-sdk:s3":                            {},
		"arn:aws:states:::dynamodb:putItem":                      {},
	} {
		id, action, ok := ParseResource(resource)
		if ok != (want[0] != "") || id != want[0] || action != want[1] {
			t.Errorf("%s: %q %q %v", resource, id, action, ok)
		}
	}
}

func TestNoEndpointIsUnsupported(t *testing.T) {
	c := New(Config{})
	if c.Supports("s3", "getObject") {
		t.Fatal("s3 without an endpoint")
	}
	var nilCaller *Caller
	if nilCaller.Supports("s3", "getObject") {
		t.Fatal("nil caller")
	}
	c = New(Config{Endpoints: map[string]string{"s3": "http://127.0.0.1:1"}})
	if !c.Supports("s3", "getObject") || c.Supports("s3", "selectObjectContent") || c.Supports("nope", "x") {
		t.Fatal("supports")
	}
}

// fakeS3 keeps objects by path. It speaks just enough of the REST-XML API.
type fakeS3 struct {
	mu      sync.Mutex
	objects map[string]string
	auth    []string
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.auth = append(f.auth, r.Header.Get("Authorization"))
	switch {
	case r.Method == http.MethodPut:
		body, _ := io.ReadAll(r.Body)
		f.objects[r.URL.Path] = string(body)
		w.Header().Set("ETag", `"etag-1"`)
	case r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2":
		w.Header().Set("Content-Type", "application/xml")
		io.WriteString(w, `<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><Name>b</Name><KeyCount>1</KeyCount><IsTruncated>false</IsTruncated><Contents><Key>k.txt</Key><Size>5</Size></Contents></ListBucketResult>`)
	case r.Method == http.MethodGet:
		body, ok := f.objects[r.URL.Path]
		if !ok {
			w.Header().Set("Content-Type", "application/xml")
			w.Header().Set("X-Amz-Request-Id", "req-1")
			w.WriteHeader(http.StatusNotFound)
			io.WriteString(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>NoSuchKey</Code><Message>The specified key does not exist.</Message></Error>`)
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		io.WriteString(w, body)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func newFake(t *testing.T) (*fakeS3, *Caller) {
	t.Helper()
	f := &fakeS3{objects: map[string]string{}}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	return f, New(Config{Region: "us-east-1", AccessKeyID: "local", SecretAccessKey: "local", Endpoints: map[string]string{"s3": srv.URL}})
}

func TestS3PutGetList(t *testing.T) {
	f, c := newFake(t)
	ctx := context.Background()
	out, err := c.Call(ctx, "s3", "putObject", []byte(`{"Bucket":"b","Key":"k.txt","Body":"hello"}`))
	if err != nil {
		t.Fatal(err)
	}
	if out != `{"ETag":"\"etag-1\""}` {
		t.Fatalf("put %s", out)
	}
	if f.objects["/b/k.txt"] != "hello" {
		t.Fatalf("objects %v", f.objects)
	}
	if !strings.HasPrefix(f.auth[0], "AWS4-HMAC-SHA256 Credential=local/") {
		t.Fatalf("auth %q", f.auth[0])
	}

	out, err = c.Call(ctx, "s3", "putObject", []byte(`{"Bucket":"b","Key":"doc.json","Body":{"a":1}}`))
	if err != nil {
		t.Fatal(err)
	}
	if f.objects["/b/doc.json"] != `{"a":1}` {
		t.Fatalf("json body %q", f.objects["/b/doc.json"])
	}

	out, err = c.Call(ctx, "s3", "getObject", []byte(`{"Bucket":"b","Key":"k.txt"}`))
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatal(err)
	}
	if got["Body"] != "hello" || got["ContentType"] != "text/plain" {
		t.Fatalf("get %s", out)
	}
	if _, ok := got["ResultMetadata"]; ok {
		t.Fatalf("metadata kept %s", out)
	}

	out, err = c.Call(ctx, "s3", "listObjectsV2", []byte(`{"Bucket":"b"}`))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, `"Contents":[{"Key":"k.txt","Size":5}]`) || strings.Contains(out, "null") {
		t.Fatalf("list %s", out)
	}
}

func TestS3Errors(t *testing.T) {
	_, c := newFake(t)
	ctx := context.Background()
	_, err := c.Call(ctx, "s3", "getObject", []byte(`{"Bucket":"b","Key":"missing"}`))
	var se *ServiceError
	if !errors.As(err, &se) || se.Name != "S3.NoSuchKeyException" {
		t.Fatalf("missing %v", err)
	}
	if se.Cause != "The specified key does not exist. (Service: S3, Status Code: 404, Request ID: req-1)" {
		t.Fatalf("cause %q", se.Cause)
	}
	for params, want := range map[string]string{
		`{"Bucket":"b","Key":"k","Nope":1}`: `unknown aws-sdk parameter "Nope"`,
		`{"bucket":"b","Key":"k"}`:          `unknown aws-sdk parameter "bucket"`,
		`["b"]`:                             "aws-sdk parameters must be a JSON object",
	} {
		_, err := c.Call(ctx, "s3", "headObject", []byte(params))
		if !errors.As(err, &se) || se.Name != "States.Runtime" || se.Cause != want {
			t.Errorf("%s: %v", params, err)
		}
	}
	if FallbackName("s3") != "S3.S3Exception" {
		t.Fatal(FallbackName("s3"))
	}
}

func TestErrorCode(t *testing.T) {
	for in, want := range map[string]string{
		"NoSuchKey":                           "NoSuchKey",
		"com.amazonaws.sqs#QueueDoesNotExist": "QueueDoesNotExist",
		"QueueDoesNotExist:http://internal.amazon.com/coral/com.amazon.coral.validate/": "QueueDoesNotExist",
	} {
		if got := errorCode(in); got != want {
			t.Errorf("%s: %s", in, got)
		}
	}
}
