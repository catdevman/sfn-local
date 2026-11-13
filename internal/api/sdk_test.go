package api

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/catdevman/sfn-local/internal/sdk"
	"github.com/catdevman/sfn-local/internal/store"
)

func TestAWSSDKS3(t *testing.T) {
	s := newService()
	def := `{"StartAt":"Put","States":{` +
		`"Put":{"Type":"Task","Resource":"arn:aws:states:::aws-sdk:s3:putObject","Parameters":{"Bucket":"b","Key.$":"$.key","Body.$":"$.doc"},"ResultPath":null,"Next":"Get"},` +
		`"Get":{"Type":"Task","Resource":"arn:aws:states:::aws-sdk:s3:getObject","Parameters":{"Bucket":"b","Key.$":"$.key"},"ResultSelector":{"doc.$":"States.StringToJson($.Body)"},"ResultPath":"$.read","Next":"Missing"},` +
		`"Missing":{"Type":"Task","Resource":"arn:aws:states:::aws-sdk:s3:getObject","Parameters":{"Bucket":"b","Key":"nope"},` +
		`"Catch":[{"ErrorEquals":["S3.NoSuchKeyException"],"ResultPath":"$.missing","Next":"Done"}],"Next":"Done"},` +
		`"Done":{"Type":"Pass","Parameters":{"n.$":"$.read.doc.n","error.$":"$.missing.Error"},"End":true}}}`

	empty := runMachine(t, s, "s3-empty", def, "e", `{"key":"k","doc":{"n":1}}`)
	if empty.Status != store.ExecFailed || empty.Error != "States.Runtime" || !strings.Contains(empty.Cause, "unsupported resource") || !strings.Contains(empty.Cause, "aws-sdk:s3:putObject") {
		t.Fatalf("no endpoint %s %s %s", empty.Status, empty.Error, empty.Cause)
	}

	var mu sync.Mutex
	objects := map[string]string{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch r.Method {
		case http.MethodPut:
			body, _ := io.ReadAll(r.Body)
			objects[r.URL.Path] = string(body)
		case http.MethodGet:
			body, ok := objects[r.URL.Path]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				io.WriteString(w, `<Error><Code>NoSuchKey</Code><Message>The specified key does not exist.</Message></Error>`)
				return
			}
			io.WriteString(w, body)
		}
	}))
	defer srv.Close()
	s.Runner.SDK = sdk.New(sdk.Config{Region: "us-east-1", Endpoints: map[string]string{"s3": srv.URL}})

	ex := runMachine(t, s, "s3", def, "run", `{"key":"k","doc":{"n":1}}`)
	if ex.Status != store.ExecSucceeded {
		t.Fatalf("s3 %s %s %s %v", ex.Status, ex.Error, ex.Cause, typesOf(ex))
	}
	jsonEqual(t, ex.Output, `{"n":1,"error":"S3.NoSuchKeyException"}`)
	if objects["/b/k"] != `{"n":1}` {
		t.Fatalf("objects %v", objects)
	}
}
