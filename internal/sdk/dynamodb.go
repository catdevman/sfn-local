package sdk

import (
	"encoding/base64"
	"fmt"
	"reflect"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

var dynamodbService = service{
	errorPrefix: "DynamoDb",
	env:         "DYNAMODB",
	client:      (*dynamodb.Client)(nil),
	actions: func(cfg aws.Config, endpoint string) map[string]handler {
		c := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
			o.BaseEndpoint = aws.String(endpoint)
		})
		return map[string]handler{
			"batchExecuteStatement": op(c.BatchExecuteStatement),
			"batchGetItem":          op(c.BatchGetItem),
			"batchWriteItem":        op(c.BatchWriteItem),
			"createTable":           op(c.CreateTable),
			"deleteItem":            op(c.DeleteItem),
			"deleteTable":           op(c.DeleteTable),
			"describeTable":         op(c.DescribeTable),
			"describeTimeToLive":    op(c.DescribeTimeToLive),
			"executeStatement":      op(c.ExecuteStatement),
			"executeTransaction":    op(c.ExecuteTransaction),
			"getItem":               op(c.GetItem),
			"listTables":            op(c.ListTables),
			"listTagsOfResource":    op(c.ListTagsOfResource),
			"putItem":               op(c.PutItem),
			"query":                 op(c.Query),
			"scan":                  op(c.Scan),
			"tagResource":           op(c.TagResource),
			"transactGetItems":      op(c.TransactGetItems),
			"transactWriteItems":    op(c.TransactWriteItems),
			"untagResource":         op(c.UntagResource),
			"updateItem":            op(c.UpdateItem),
			"updateTable":           op(c.UpdateTable),
			"updateTimeToLive":      op(c.UpdateTimeToLive),
		}
	},
}

func init() {
	unions[reflect.TypeFor[types.AttributeValue]()] = union{
		decode: func(raw any, path string) (reflect.Value, error) {
			av, err := decodeAttr(raw, path)
			if err != nil {
				return reflect.Value{}, err
			}
			return reflect.ValueOf(&av).Elem(), nil
		},
		encode: func(member reflect.Value) (any, error) {
			return encodeAttr(member.Interface())
		},
	}
}

// decodeAttr reads the wire shape, an object with one type key such as
// {"S":"x"} or {"M":{"n":{"N":"1"}}}.
func decodeAttr(raw any, path string) (types.AttributeValue, error) {
	obj, ok := raw.(map[string]any)
	if !ok || len(obj) != 1 {
		return nil, typeErr(path, "an attribute value with one type key")
	}
	for key, val := range obj {
		switch key {
		case "S":
			s, ok := val.(string)
			if !ok {
				return nil, typeErr(path+".S", "a string")
			}
			return &types.AttributeValueMemberS{Value: s}, nil
		case "N":
			s, ok := val.(string)
			if !ok {
				return nil, typeErr(path+".N", "a number as a string")
			}
			return &types.AttributeValueMemberN{Value: s}, nil
		case "B":
			b, err := decodeB64(val, path+".B")
			if err != nil {
				return nil, err
			}
			return &types.AttributeValueMemberB{Value: b}, nil
		case "BOOL":
			b, ok := val.(bool)
			if !ok {
				return nil, typeErr(path+".BOOL", "a boolean")
			}
			return &types.AttributeValueMemberBOOL{Value: b}, nil
		case "NULL":
			b, ok := val.(bool)
			if !ok {
				return nil, typeErr(path+".NULL", "a boolean")
			}
			return &types.AttributeValueMemberNULL{Value: b}, nil
		case "SS", "NS":
			items, err := stringList(val, path+"."+key)
			if err != nil {
				return nil, err
			}
			if key == "SS" {
				return &types.AttributeValueMemberSS{Value: items}, nil
			}
			return &types.AttributeValueMemberNS{Value: items}, nil
		case "BS":
			arr, ok := val.([]any)
			if !ok {
				return nil, typeErr(path+".BS", "an array")
			}
			out := make([][]byte, len(arr))
			for i, item := range arr {
				b, err := decodeB64(item, fmt.Sprintf("%s.BS[%d]", path, i))
				if err != nil {
					return nil, err
				}
				out[i] = b
			}
			return &types.AttributeValueMemberBS{Value: out}, nil
		case "M":
			m, ok := val.(map[string]any)
			if !ok {
				return nil, typeErr(path+".M", "an object")
			}
			out := make(map[string]types.AttributeValue, len(m))
			for k, item := range m {
				av, err := decodeAttr(item, path+".M."+k)
				if err != nil {
					return nil, err
				}
				out[k] = av
			}
			return &types.AttributeValueMemberM{Value: out}, nil
		case "L":
			arr, ok := val.([]any)
			if !ok {
				return nil, typeErr(path+".L", "an array")
			}
			out := make([]types.AttributeValue, len(arr))
			for i, item := range arr {
				av, err := decodeAttr(item, fmt.Sprintf("%s.L[%d]", path, i))
				if err != nil {
					return nil, err
				}
				out[i] = av
			}
			return &types.AttributeValueMemberL{Value: out}, nil
		default:
			return nil, paramErr(fmt.Sprintf("unknown attribute value type %q at %q", key, path))
		}
	}
	return nil, nil
}

// encodeAttr writes the wire shape. B and BS are base64, as encoding/json
// writes []byte.
func encodeAttr(av any) (any, error) {
	switch t := av.(type) {
	case *types.AttributeValueMemberS:
		return map[string]any{"S": t.Value}, nil
	case *types.AttributeValueMemberN:
		return map[string]any{"N": t.Value}, nil
	case *types.AttributeValueMemberB:
		return map[string]any{"B": t.Value}, nil
	case *types.AttributeValueMemberBOOL:
		return map[string]any{"BOOL": t.Value}, nil
	case *types.AttributeValueMemberNULL:
		return map[string]any{"NULL": t.Value}, nil
	case *types.AttributeValueMemberSS:
		return map[string]any{"SS": t.Value}, nil
	case *types.AttributeValueMemberNS:
		return map[string]any{"NS": t.Value}, nil
	case *types.AttributeValueMemberBS:
		return map[string]any{"BS": t.Value}, nil
	case *types.AttributeValueMemberM:
		out := make(map[string]any, len(t.Value))
		for k, item := range t.Value {
			v, err := encodeAttr(item)
			if err != nil {
				return nil, err
			}
			out[k] = v
		}
		return map[string]any{"M": out}, nil
	case *types.AttributeValueMemberL:
		out := make([]any, len(t.Value))
		for i, item := range t.Value {
			v, err := encodeAttr(item)
			if err != nil {
				return nil, err
			}
			out[i] = v
		}
		return map[string]any{"L": out}, nil
	default:
		return nil, fmt.Errorf("DynamoDB returned an attribute value type this server does not know: %T", av)
	}
}

func decodeB64(val any, path string) ([]byte, error) {
	s, ok := val.(string)
	if !ok {
		return nil, typeErr(path, "a base64 string")
	}
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, typeErr(path, "a base64 string")
	}
	return b, nil
}

func stringList(val any, path string) ([]string, error) {
	arr, ok := val.([]any)
	if !ok {
		return nil, typeErr(path, "an array of strings")
	}
	out := make([]string, len(arr))
	for i, item := range arr {
		s, ok := item.(string)
		if !ok {
			return nil, typeErr(fmt.Sprintf("%s[%d]", path, i), "a string")
		}
		out[i] = s
	}
	return out, nil
}
