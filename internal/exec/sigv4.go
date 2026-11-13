package exec

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"sort"
	"strings"
	"time"
)

// sigv4 signs a request with AWS Signature Version 4. DynamoDB Local refuses
// a request without an Authorization header and files tables under the access
// key and region it names, so the key must match the one used to create them.
type sigv4 struct {
	key, secret, region, service string
}

// signer returns nil when no access key is configured, and the request goes
// out unsigned.
func (r *Runner) signer(service string) *sigv4 {
	if r == nil || r.AccessKeyID == "" {
		return nil
	}
	region := r.Region
	if region == "" {
		region = "us-east-1"
	}
	return &sigv4{key: r.AccessKeyID, secret: r.SecretAccessKey, region: region, service: service}
}

// sign sets X-Amz-Date and Authorization. Every header already on req is signed.
func (s *sigv4) sign(req *http.Request, body string, now time.Time) {
	if s == nil {
		return
	}
	stamp := now.UTC().Format("20060102T150405Z")
	day := stamp[:8]
	req.Header.Set("X-Amz-Date", stamp)

	host := req.Host
	if host == "" {
		host = req.URL.Host
	}
	headers := map[string]string{"host": host}
	for name, vals := range req.Header {
		headers[strings.ToLower(name)] = strings.TrimSpace(strings.Join(vals, ","))
	}
	names := make([]string, 0, len(headers))
	for name := range headers {
		names = append(names, name)
	}
	sort.Strings(names)
	var canonHeaders strings.Builder
	for _, name := range names {
		canonHeaders.WriteString(name + ":" + headers[name] + "\n")
	}
	signed := strings.Join(names, ";")

	path := req.URL.EscapedPath()
	if path == "" {
		path = "/"
	}
	query := strings.ReplaceAll(req.URL.Query().Encode(), "+", "%20")
	canonical := strings.Join([]string{req.Method, path, query, canonHeaders.String(), signed, hashHex(body)}, "\n")

	scope := day + "/" + s.region + "/" + s.service + "/aws4_request"
	toSign := "AWS4-HMAC-SHA256\n" + stamp + "\n" + scope + "\n" + hashHex(canonical)
	k := hmacSHA256([]byte("AWS4"+s.secret), day)
	k = hmacSHA256(k, s.region)
	k = hmacSHA256(k, s.service)
	k = hmacSHA256(k, "aws4_request")
	sig := hex.EncodeToString(hmacSHA256(k, toSign))
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+s.key+"/"+scope+", SignedHeaders="+signed+", Signature="+sig)
}

func hashHex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func hmacSHA256(key []byte, data string) []byte {
	m := hmac.New(sha256.New, key)
	m.Write([]byte(data))
	return m.Sum(nil)
}
