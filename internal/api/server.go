package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"regexp"
	"strings"
	"sync"

	"github.com/catdevman/sfn-local/internal/api/gen"
)

const (
	contentTypeJSON10 = "application/x-amz-json-1.0"
	contentTypeJSON   = "application/json"
	maxBody           = 8 << 20
	targetHeader      = "X-Amz-Target"
)

// Server is the AWS JSON 1.0 HTTP adapter.
type Server struct {
	Handlers gen.Handlers
	Surface  *Surface
	Profile  Profile
	Log      io.Writer

	mu sync.Mutex
}

type access struct {
	op       string
	payload  string
	status   int
	response []byte
}

// ServeHTTP serves the console for GET /dashboard, its assets, and
// POST /dashboard/stop. Every other request is AWS JSON 1.0. The operation
// is the X-Amz-Target value after the first dot. POST is accepted on any
// path, including /dashboard. The console response is not access-logged.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if dashboardRequest(r) {
		s.serveDashboard(w, r)
		return
	}
	acc := access{op: "-", payload: "{}", status: http.StatusInternalServerError}
	defer s.writeLog(&acc)
	if r.Method != http.MethodPost {
		s.fail(&acc, w, http.StatusBadRequest, s.Profile.TypeFor(validation("HTTP method must be POST")), "HTTP method must be POST")
		return
	}
	if !contentTypeOK(r.Header.Get("Content-Type")) {
		msg := "Content-Type must be application/x-amz-json-1.0 or application/json"
		s.fail(&acc, w, http.StatusBadRequest, s.Profile.TypeFor(validation(msg)), msg)
		return
	}
	body, err := readBody(r.Body)
	if err != nil {
		msg := "request body is too large"
		if !errors.Is(err, errTooLarge) {
			msg = "the request body could not be read"
		}
		s.fail(&acc, w, http.StatusBadRequest, s.Profile.TypeFor(validation(msg)), msg)
		return
	}
	acc.payload = payloadText(body)
	target := r.Header.Get(targetHeader)
	if target == "" || !strings.Contains(target, ".") {
		msg := "Missing Required Header: '" + targetHeader + "'"
		s.fail(&acc, w, http.StatusBadRequest, s.Profile.MissingHeaderType(), msg)
		return
	}
	name := target[strings.IndexByte(target, '.')+1:]
	acc.op = name
	if _, ok := gen.OperationByName(name); !ok {
		msg := "Unknown operation: '" + target + "'"
		s.fail(&acc, w, http.StatusBadRequest, UnknownOperationType, msg)
		return
	}
	if s.Profile == ProfileSFNLocal && JarAbsent(name) {
		msg := "Unsupported Operation: '" + name + "'"
		s.fail(&acc, w, http.StatusInternalServerError, jarUnsupported, msg)
		return
	}
	in, err := gen.DecodeInput(name, body)
	if err != nil {
		s.failError(&acc, w, err)
		return
	}
	restore := relaxCompatRole(s.Profile, name, in)
	if level, bad := unknownLogLevel(in); bad {
		restore()
		apiErr, ok := gen.NewModeledError("InvalidLoggingConfiguration", "Invalid Logging Configuration: '"+level+"'")
		if !ok {
			s.failError(&acc, w, fmt.Errorf("InvalidLoggingConfiguration"))
			return
		}
		s.failError(&acc, w, apiErr)
		return
	}
	if err := gen.ValidateInput(name, in); err != nil {
		restore()
		if s.Profile == ProfileSFNLocal {
			err = rewriteCompatTagLength(err)
		}
		s.failError(&acc, w, err)
		return
	}
	restore()
	row, ok := s.Surface.Operation(name)
	if !ok {
		s.failError(&acc, w, fmt.Errorf("operation %s is not classified", name))
		return
	}
	if row.Status == "deferred" {
		apiErr, ok := gen.NewModeledError(row.Error, row.Message)
		if !ok {
			s.failError(&acc, w, fmt.Errorf("deferred error %s is not a modeled error", row.Error))
			return
		}
		s.failError(&acc, w, apiErr)
		return
	}
	if row.Status != "implemented" {
		s.failError(&acc, w, fmt.Errorf("operation %s has status %s", name, row.Status))
		return
	}
	if s.Handlers == nil {
		s.failError(&acc, w, errors.New("no handler"))
		return
	}
	out, err := gen.Call(r.Context(), name, s.Handlers, in)
	if err != nil {
		s.failError(&acc, w, err)
		return
	}
	raw, err := marshalJSON(out)
	if err != nil {
		s.failError(&acc, w, err)
		return
	}
	if len(raw) == 0 {
		raw = []byte("{}")
	}
	s.ok(&acc, w, raw)
}

// relaxCompatRole lets --compat=sfn-local omit roleArn. Smithy marks the
// member required, so a placeholder satisfies ValidateInput and is cleared
// before the handler. The public profile does not relax.
func relaxCompatRole(profile Profile, name string, in any) func() {
	if profile != ProfileSFNLocal || name != "CreateStateMachine" {
		return func() {}
	}
	input, ok := in.(*gen.CreateStateMachineInput)
	if !ok || input == nil || input.RoleArn != nil {
		return func() {}
	}
	placeholder := "arn:aws:iam::123456789012:role/omitted"
	input.RoleArn = &placeholder
	return func() { input.RoleArn = nil }
}

func (s *Server) failError(acc *access, w http.ResponseWriter, err error) {
	var apiErr gen.APIError
	if errors.As(err, &apiErr) {
		s.fail(acc, w, apiErr.StatusCode(), s.Profile.TypeFor(apiErr), messageOf(apiErr))
		return
	}
	s.fail(acc, w, http.StatusInternalServerError, s.Profile.InternalFailureType(), plainMessage(err))
}

func (s *Server) fail(acc *access, w http.ResponseWriter, status int, typeName, message string) {
	body := mustJSON(struct {
		Type    string `json:"__type"`
		Message string `json:"message"`
	}{typeName, message})
	acc.status = status
	acc.response = body
	w.Header().Set("Content-Type", contentTypeJSON10)
	w.Header().Set("X-Amzn-Errortype", typeName)
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

func (s *Server) ok(acc *access, w http.ResponseWriter, body []byte) {
	acc.status = http.StatusOK
	acc.response = body
	w.Header().Set("Content-Type", contentTypeJSON10)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

func (s *Server) writeLog(acc *access) {
	var b strings.Builder
	fmt.Fprintf(&b, "%s => %s\n", acc.op, acc.payload)
	fmt.Fprintf(&b, "[%d] %s <= %s\n", acc.status, acc.op, string(acc.response))
	s.mu.Lock()
	defer s.mu.Unlock()
	_, _ = io.WriteString(s.log(), b.String())
}

func (s *Server) log() io.Writer {
	if s.Log != nil {
		return s.Log
	}
	return io.Discard
}

func validation(message string) gen.APIError {
	err, ok := gen.NewModeledError("ValidationException", message)
	if !ok {
		panic("ValidationException missing from generated errors")
	}
	return err
}

// tagLengthPath is tags[N].key or tags[N].value from generated Validate.
var tagLengthPath = regexp.MustCompile(`^tags\[\d+\]\.(?:key|value)$`)

// unknownLogLevel reports a logging level the generated enum would reject.
// The HTTP adapter answers that as InvalidLoggingConfiguration before Validate.
func unknownLogLevel(in any) (string, bool) {
	var cfg *gen.LoggingConfiguration
	switch v := in.(type) {
	case *gen.CreateStateMachineInput:
		if v != nil {
			cfg = v.LoggingConfiguration
		}
	case *gen.UpdateStateMachineInput:
		if v != nil {
			cfg = v.LoggingConfiguration
		}
	}
	if cfg == nil || cfg.Level == nil || cfg.Level.Valid() {
		return "", false
	}
	return string(*cfg.Level), true
}

// rewriteCompatTagLength keeps the Smithy __type and replaces the tag
// key/value length sentence with the jar's hardcoded 80-character text.
func rewriteCompatTagLength(err error) error {
	var apiErr gen.APIError
	if !errors.As(err, &apiErr) || apiErr.ErrorName() != "ValidationException" {
		return err
	}
	msg := messageOf(apiErr)
	const marker = " failed to satisfy constraint: "
	i := strings.LastIndex(msg, marker)
	if i < 0 {
		return err
	}
	head, constraint := msg[:i], msg[i+len(marker):]
	if !strings.HasPrefix(constraint, "Member must have length ") {
		return err
	}
	const at = " at '"
	j := strings.LastIndex(head, at)
	if j < 0 || !strings.HasSuffix(head, "'") {
		return err
	}
	path := head[j+len(at) : len(head)-1]
	if !tagLengthPath.MatchString(path) {
		return err
	}
	rewritten, ok := gen.NewModeledError("ValidationException", head+marker+"Member must have length less than or equal to 80")
	if !ok {
		return err
	}
	return rewritten
}

func messageOf(err gen.APIError) string {
	text := err.Error()
	prefix := err.ErrorName() + ": "
	if strings.HasPrefix(text, prefix) {
		return strings.TrimPrefix(text, prefix)
	}
	return text
}

func plainMessage(err error) string {
	msg := err.Error()
	if i := strings.IndexByte(msg, '\n'); i >= 0 {
		msg = msg[:i]
	}
	return msg
}

var errTooLarge = errors.New("request body is too large")

func readBody(body io.ReadCloser) ([]byte, error) {
	if body == nil {
		return nil, nil
	}
	defer body.Close()
	raw, err := io.ReadAll(io.LimitReader(body, maxBody+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > maxBody {
		return nil, errTooLarge
	}
	return raw, nil
}

func payloadText(body []byte) string {
	if len(bytes.TrimSpace(body)) == 0 {
		return "{}"
	}
	return string(body)
}

func contentTypeOK(v string) bool {
	v = strings.TrimSpace(v)
	if v == "" {
		return true
	}
	mt, _, err := mime.ParseMediaType(v)
	if err != nil {
		return false
	}
	return mt == contentTypeJSON10 || mt == contentTypeJSON
}

func marshalJSON(v any) ([]byte, error) {
	if v == nil {
		return []byte("{}"), nil
	}
	return mustJSONErr(v)
}

func mustJSON(v any) []byte {
	raw, err := mustJSONErr(v)
	if err != nil {
		return []byte(`{"__type":"com.amazonaws.sfn#InternalFailure","message":"internal failure"}`)
	}
	return raw
}

func mustJSONErr(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimSpace(buf.Bytes()), nil
}
