package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

// item has every attribute value type, in the wire shape.
const item = `{"pk":{"S":"ord-1"},"total":{"N":"4900"},"raw":{"B":"aGk="},"paid":{"BOOL":true},"none":{"NULL":true},` +
	`"tags":{"SS":["a","b"]},"sizes":{"NS":["1","2"]},"blobs":{"BS":["aGk="]},` +
	`"lines":{"L":[{"M":{"sku":{"S":"pen"},"qty":{"N":"2"}}}]}}`

func TestDynamoDBAttributeValues(t *testing.T) {
	var target, body string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		target, body = r.Header.Get("X-Amz-Target"), string(raw)
		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		switch target {
		case "DynamoDB_20120810.GetItem":
			io.WriteString(w, `{"Item":`+item+`}`)
		case "DynamoDB_20120810.PutItem":
			w.Header().Set("X-Amzn-Requestid", "req-9")
			w.WriteHeader(http.StatusBadRequest)
			io.WriteString(w, `{"__type":"com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException","message":"The conditional request failed"}`)
		default:
			io.WriteString(w, `{}`)
		}
	}))
	defer srv.Close()
	c := New(Config{Region: "us-east-1", AccessKeyID: "local", SecretAccessKey: "local", Endpoints: map[string]string{"dynamodb": srv.URL}})
	ctx := context.Background()

	out, err := c.Call(ctx, "dynamodb", "getItem", []byte(`{"TableName":"orders","Key":{"pk":{"S":"ord-1"}},"ConsistentRead":true}`))
	if err != nil {
		t.Fatal(err)
	}
	sameJSON(t, body, `{"TableName":"orders","Key":{"pk":{"S":"ord-1"}},"ConsistentRead":true}`)
	sameJSON(t, out, `{"Item":`+item+`}`)

	_, err = c.Call(ctx, "dynamodb", "putItem", []byte(`{"TableName":"orders","Item":`+item+`,"ConditionExpression":"attribute_not_exists(pk)"}`))
	sameJSON(t, body, `{"TableName":"orders","Item":`+item+`,"ConditionExpression":"attribute_not_exists(pk)"}`)
	var se *ServiceError
	if !errors.As(err, &se) || se.Name != "DynamoDb.ConditionalCheckFailedException" || se.Cause != "The conditional request failed (Service: DynamoDb, Status Code: 400, Request ID: req-9) (SDK Attempt Count: 1)" {
		t.Fatalf("put %v", err)
	}
}

func TestDynamoDBParameterErrors(t *testing.T) {
	c := New(Config{Endpoints: map[string]string{"dynamodb": "http://127.0.0.1:1"}})
	for params, want := range map[string]string{
		`{"TableName":"t","Key":{"pk":{"S":"a","N":"1"}}}`:      `invalid aws-sdk parameter "Key.pk": expected an attribute value with one type key`,
		`{"TableName":"t","Key":{"pk":{"X":"a"}}}`:              `unknown attribute value type "X" at "Key.pk"`,
		`{"TableName":"t","Key":{"pk":{"N":1}}}`:                `invalid aws-sdk parameter "Key.pk.N": expected a number as a string`,
		`{"TableName":"t","Key":{"pk":{"M":{"x":{"s":"a"}}}}}`:  `unknown attribute value type "s" at "Key.pk.M.x"`,
		`{"TableName":"t","ProjectionExpression":1}`:            `invalid aws-sdk parameter "ProjectionExpression": expected a string`,
		`{"TableName":"t","ExpressionAttributeNames":{"#a":1}}`: `invalid aws-sdk parameter "ExpressionAttributeNames.#a": expected a string`,
	} {
		_, err := c.Call(context.Background(), "dynamodb", "getItem", []byte(params))
		var se *ServiceError
		if !errors.As(err, &se) || se.Name != "States.Runtime" || se.Cause != want {
			t.Errorf("%s:\n got %v\nwant %s", params, err, want)
		}
	}
	_, err := c.Call(context.Background(), "dynamodb", "query", []byte(`{"TableName":"t","KeyConditions":{"pk":{"comparisonOperator":"EQ"}}}`))
	var se *ServiceError
	if !errors.As(err, &se) || se.Cause != `unknown aws-sdk parameter "KeyConditions.pk.comparisonOperator"` {
		t.Fatalf("nested case %v", err)
	}
}

func sameJSON(t *testing.T, got, want string) {
	t.Helper()
	var a, b any
	if err := json.Unmarshal([]byte(got), &a); err != nil {
		t.Fatalf("%v: %s", err, got)
	}
	if err := json.Unmarshal([]byte(want), &b); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
}
