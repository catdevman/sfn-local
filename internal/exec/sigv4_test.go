package exec

import (
	"net/http"
	"testing"
	"time"
)

// The vectors are get-vanilla and post-vanilla from the AWS Signature Version 4 test suite.
func TestSigV4Vectors(t *testing.T) {
	s := &sigv4{key: "AKIDEXAMPLE", secret: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY", region: "us-east-1", service: "service"}
	at := time.Date(2015, 8, 30, 12, 36, 0, 0, time.UTC)
	for method, want := range map[string]string{
		http.MethodGet:  "5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31",
		http.MethodPost: "5da7c1a2acd57cee7505fc6676e4e544621c30862966e37dddb68e92efbe5d6b",
	} {
		req, err := http.NewRequest(method, "http://example.amazonaws.com/", nil)
		if err != nil {
			t.Fatal(err)
		}
		s.sign(req, "", at)
		auth := "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-date, Signature=" + want
		if got := req.Header.Get("Authorization"); got != auth {
			t.Errorf("%s\n got %s\nwant %s", method, got, auth)
		}
		if got := req.Header.Get("X-Amz-Date"); got != "20150830T123600Z" {
			t.Errorf("%s date %s", method, got)
		}
	}
}

func TestSignerNeedsKey(t *testing.T) {
	if (&Runner{}).signer("dynamodb") != nil {
		t.Fatal("signer without an access key")
	}
	s := (&Runner{AccessKeyID: "local", SecretAccessKey: "local"}).signer("dynamodb")
	if s == nil || s.region != "us-east-1" {
		t.Fatalf("signer %+v", s)
	}
}
