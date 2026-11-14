package exec

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
)

var apiGatewayMethods = map[string]bool{
	"GET": true, "POST": true, "PUT": true, "DELETE": true, "PATCH": true, "HEAD": true, "OPTIONS": true,
}

var apiGatewayAuthTypes = map[string]bool{"NO_AUTH": true, "IAM_ROLE": true, "RESOURCE_POLICY": true}

// apiGatewayForbidden are the request headers AWS refuses, lower case. Any
// header starting with x-forwarded, x-amz, or x-amzn is refused too.
var apiGatewayForbidden = map[string]bool{
	"authorization": true, "connection": true, "content-md5": true, "expect": true, "host": true,
	"max-forwards": true, "proxy-authenticate": true, "server": true, "te": true,
	"transfer-encoding": true, "trailer": true, "upgrade": true, "via": true, "www-authenticate": true,
}

// apiGatewayRequest is apigateway:invoke's parameters, checked.
type apiGatewayRequest struct {
	host    string
	apiID   string
	method  string
	stage   string
	path    string
	headers http.Header
	query   url.Values
	body    *string
}

func apiGatewayParamErr(format string, args ...any) *taskFailure {
	return &taskFailure{Name: "States.Runtime", Cause: "invalid apigateway:invoke parameters: " + fmt.Sprintf(format, args...)}
}

func parseAPIGatewayRequest(raw string) (apiGatewayRequest, *taskFailure) {
	var req apiGatewayRequest
	doc, err := parseJSON(raw)
	obj, ok := doc.(map[string]any)
	if err != nil || !ok {
		return req, apiGatewayParamErr("expected a JSON object")
	}
	known := map[string]bool{
		"ApiEndpoint": true, "Method": true, "Headers": true, "Stage": true, "Path": true,
		"QueryParameters": true, "RequestBody": true, "AllowNullValues": true, "AuthType": true,
	}
	for k := range obj {
		if !known[k] {
			return req, apiGatewayParamErr("unknown field %q", k)
		}
	}
	endpoint, _ := obj["ApiEndpoint"].(string)
	if endpoint == "" {
		return req, apiGatewayParamErr("ApiEndpoint is required")
	}
	req.host = endpoint
	req.apiID, _, _ = strings.Cut(endpoint, ".")
	req.method, _ = obj["Method"].(string)
	if !apiGatewayMethods[req.method] {
		return req, apiGatewayParamErr("Method must be one of GET, POST, PUT, DELETE, PATCH, HEAD, OPTIONS")
	}
	for _, name := range []string{"Stage", "Path"} {
		if v, ok := obj[name]; ok && v != nil {
			if _, isString := v.(string); !isString {
				return req, apiGatewayParamErr("%s must be a string", name)
			}
		}
	}
	req.stage, _ = obj["Stage"].(string)
	req.path, _ = obj["Path"].(string)
	allowNull := false
	if v, ok := obj["AllowNullValues"]; ok && v != nil {
		b, isBool := v.(bool)
		if !isBool {
			return req, apiGatewayParamErr("AllowNullValues must be a boolean")
		}
		allowNull = b
	}
	if v, ok := obj["AuthType"]; ok && v != nil {
		s, _ := v.(string)
		if !apiGatewayAuthTypes[s] {
			return req, apiGatewayParamErr("AuthType must be one of NO_AUTH, IAM_ROLE, RESOURCE_POLICY")
		}
	}
	headers, tf := stringLists(obj, "Headers")
	if tf != nil {
		return req, tf
	}
	req.headers = http.Header{}
	for name, values := range headers {
		lower := strings.ToLower(name)
		if apiGatewayForbidden[lower] || strings.HasPrefix(lower, "x-forwarded") || strings.HasPrefix(lower, "x-amz") {
			return req, apiGatewayParamErr("header %q is not allowed", name)
		}
		for _, v := range values {
			req.headers.Add(name, v)
		}
	}
	query, tf := stringLists(obj, "QueryParameters")
	if tf != nil {
		return req, tf
	}
	req.query = url.Values(query)
	switch req.method {
	case "POST", "PUT", "PATCH":
		if v, ok := obj["RequestBody"]; ok && v != nil {
			text, isString := v.(string)
			if !isString {
				if !allowNull {
					v = dropNulls(v)
				}
				text, err = compactValue(v)
				if err != nil {
					return req, apiGatewayParamErr("RequestBody: %v", err)
				}
				if req.headers.Get("Content-Type") == "" {
					req.headers.Set("Content-Type", "application/json")
				}
			}
			req.body = &text
		}
	}
	return req, nil
}

// stringLists reads a member that maps each name to a list of strings. A
// null in a list is left out.
func stringLists(obj map[string]any, member string) (map[string][]string, *taskFailure) {
	v, ok := obj[member]
	if !ok || v == nil {
		return nil, nil
	}
	m, ok := v.(map[string]any)
	if !ok {
		return nil, apiGatewayParamErr("%s must be an object whose values are lists of strings", member)
	}
	out := map[string][]string{}
	for name, raw := range m {
		list, ok := raw.([]any)
		if !ok {
			return nil, apiGatewayParamErr("%s.%s must be a list of strings", member, name)
		}
		for _, item := range list {
			if item == nil {
				continue
			}
			s, ok := item.(string)
			if !ok {
				return nil, apiGatewayParamErr("%s.%s must be a list of strings", member, name)
			}
			out[name] = append(out[name], s)
		}
	}
	return out, nil
}

// dropNulls removes object members whose value is null, at every depth.
func dropNulls(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, item := range t {
			if item != nil {
				out[k] = dropNulls(item)
			}
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, item := range t {
			out[i] = dropNulls(item)
		}
		return out
	default:
		return v
	}
}

// apiGatewayURL is API_GATEWAY_ENDPOINT, then /<Stage> when Stage is set,
// then /<Path>, then the query. {apiId} in the endpoint is replaced with the
// first label of ApiEndpoint, for servers that route by URL host.
func apiGatewayURL(endpoint string, req apiGatewayRequest) string {
	u := strings.TrimRight(strings.ReplaceAll(endpoint, "{apiId}", req.apiID), "/")
	if req.stage != "" {
		u += "/" + strings.Trim(req.stage, "/")
	}
	if p := strings.TrimLeft(req.path, "/"); p != "" {
		u += "/" + p
	}
	if len(req.query) > 0 {
		u += "?" + req.query.Encode()
	}
	return u
}

// invokeAPIGateway runs apigateway:invoke. A 2xx reply is {ResponseBody,
// Headers, StatusCode, StatusText}. Any other status fails as
// ApiGateway.<status> with the response body as the cause.
func (r *Runner) invokeAPIGateway(callCtx, parent context.Context, raw string, taskWins bool) (string, *taskFailure) {
	req, tf := parseAPIGatewayRequest(raw)
	if tf != nil {
		return "", tf
	}
	var body io.Reader
	if req.body != nil {
		body = strings.NewReader(*req.body)
	}
	httpReq, err := http.NewRequestWithContext(callCtx, req.method, apiGatewayURL(r.APIGatewayEndpoint, req), body)
	if err != nil {
		return "", &taskFailure{Name: "States.Runtime", Cause: err.Error()}
	}
	httpReq.Header = req.headers
	// AWS dials ApiEndpoint, so its Host header is ApiEndpoint. A server
	// that serves several APIs on one address, such as the AWS Emulator,
	// routes by it. An {apiId} endpoint routes by its own host instead.
	if !strings.Contains(r.APIGatewayEndpoint, "{apiId}") {
		httpReq.Host = req.host
	}
	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return "", transportFailure(err, parent, callCtx, taskWins, "ApiGateway.ServiceException")
	}
	defer resp.Body.Close()
	buf, err := io.ReadAll(io.LimitReader(resp.Body, maxPayload+1))
	if err != nil {
		return "", transportFailure(err, parent, callCtx, taskWins, "ApiGateway.ServiceException")
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return "", &taskFailure{Name: "ApiGateway." + strconv.Itoa(resp.StatusCode), Cause: clip(string(buf), 32768)}
	}
	if len(buf) > maxPayload {
		return "", &taskFailure{Name: "States.DataLimitExceeded", Cause: "The API Gateway response exceeded the maximum size of 262144 bytes."}
	}
	headers := map[string]any{}
	names := make([]string, 0, len(resp.Header))
	for name := range resp.Header {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		list := make([]any, len(resp.Header[name]))
		for i, v := range resp.Header[name] {
			list[i] = v
		}
		headers[name] = list
	}
	text, err := compactValue(map[string]any{
		"ResponseBody": responseBody(resp.Header.Get("Content-Type"), buf),
		"Headers":      headers,
		"StatusCode":   resp.StatusCode,
		"StatusText":   statusText(resp),
	})
	if err != nil {
		return "", &taskFailure{Name: "States.Runtime", Cause: err.Error()}
	}
	return text, nil
}

// responseBody is the parsed body when the reply says it is JSON and it
// parses. Otherwise it is the body as a string.
func responseBody(contentType string, buf []byte) any {
	media, _, _ := mime.ParseMediaType(contentType)
	if media == "application/json" || strings.HasSuffix(media, "+json") {
		var v any
		if err := json.Unmarshal(buf, &v); err == nil {
			return v
		}
	}
	return string(buf)
}

func statusText(resp *http.Response) string {
	code := strconv.Itoa(resp.StatusCode)
	if text := strings.TrimSpace(strings.TrimPrefix(resp.Status, code)); text != "" {
		return text
	}
	return http.StatusText(resp.StatusCode)
}
