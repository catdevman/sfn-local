package sdk

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"time"
)

// op adapts one SDK client method. The parameters decode into the method's
// input struct and the output struct encodes back to JSON. Most actions are
// one op line. An action with a member neither codec can carry, such as a
// streaming body, gets its own handler.
//
// op panics when I or O holds such a member. Every table is built by the
// inventory test, so that panic fails CI, not a task.
func op[I, O, Opt any](f func(context.Context, *I, ...func(*Opt)) (*O, error)) handler {
	for _, t := range []reflect.Type{reflect.TypeFor[I](), reflect.TypeFor[O]()} {
		if members := Opaque(t); len(members) > 0 {
			panic(fmt.Sprintf("sdk: %s has %v, which the codec cannot carry; write a handler", t, members))
		}
	}
	return func(ctx context.Context, params []byte) (string, error) {
		in := new(I)
		if err := decode(params, in); err != nil {
			return "", err
		}
		out, err := f(ctx, in)
		if err != nil {
			return "", err
		}
		return encode(out, nil)
	}
}

// union converts one SDK union interface to and from its wire JSON, such as
// DynamoDB's AttributeValue and {"S":"x"}.
type union struct {
	// decode builds a member from the decoded JSON value.
	decode func(raw any, path string) (reflect.Value, error)
	// encode writes a non-nil member as a JSON value.
	encode func(member reflect.Value) (any, error)
}

// unions is keyed by the interface type. A service file registers its own.
var unions = map[reflect.Type]union{}

var (
	timeType      = reflect.TypeFor[time.Time]()
	marshalerType = reflect.TypeFor[json.Marshaler]()
)

// decode reads params into in, which points to an SDK input struct. A member
// name must match exactly at every depth, because Step Functions does not
// fold case. An unknown or mistyped member fails the task with States.Runtime.
func decode(params []byte, in any) error {
	trimmed := bytes.TrimSpace(params)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil
	}
	raw, err := parse(trimmed)
	if err != nil {
		return err
	}
	if _, ok := raw.(map[string]any); !ok {
		return paramErr("aws-sdk parameters must be a JSON object")
	}
	return fill(reflect.ValueOf(in).Elem(), raw, "")
}

func parse(text []byte) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(text))
	dec.UseNumber()
	var raw any
	if err := dec.Decode(&raw); err != nil {
		return nil, paramErr("aws-sdk parameters must be a JSON object")
	}
	return raw, nil
}

func paramErr(cause string) error {
	return &ServiceError{Name: "States.Runtime", Cause: cause}
}

func typeErr(path, want string) error {
	return paramErr(fmt.Sprintf("invalid aws-sdk parameter %q: expected %s", path, want))
}

// fill sets v from a JSON value decoded with UseNumber. path names the member
// in errors, such as Item.pk.
func fill(v reflect.Value, raw any, path string) error {
	if raw == nil {
		return nil
	}
	t := v.Type()
	if u, ok := unions[t]; ok {
		member, err := u.decode(raw, path)
		if err != nil {
			return err
		}
		v.Set(member)
		return nil
	}
	if t == timeType {
		return fillTime(v, raw, path)
	}
	switch t.Kind() {
	case reflect.Pointer:
		elem := reflect.New(t.Elem())
		if err := fill(elem.Elem(), raw, path); err != nil {
			return err
		}
		v.Set(elem)
	case reflect.Struct:
		obj, ok := raw.(map[string]any)
		if !ok {
			return typeErr(path, "an object")
		}
		keys := make([]string, 0, len(obj))
		for k := range obj {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			f, ok := t.FieldByName(k)
			child := join(path, k)
			if !ok || !f.IsExported() || len(f.Index) != 1 || k == "ResultMetadata" {
				return paramErr(fmt.Sprintf("unknown aws-sdk parameter %q", child))
			}
			if err := fill(v.Field(f.Index[0]), obj[k], child); err != nil {
				return err
			}
		}
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			s, ok := raw.(string)
			if !ok {
				return typeErr(path, "a base64 string")
			}
			b, err := base64.StdEncoding.DecodeString(s)
			if err != nil {
				return typeErr(path, "a base64 string")
			}
			v.SetBytes(b)
			return nil
		}
		arr, ok := raw.([]any)
		if !ok {
			return typeErr(path, "an array")
		}
		out := reflect.MakeSlice(t, len(arr), len(arr))
		for i, item := range arr {
			if err := fill(out.Index(i), item, fmt.Sprintf("%s[%d]", path, i)); err != nil {
				return err
			}
		}
		v.Set(out)
	case reflect.Map:
		obj, ok := raw.(map[string]any)
		if !ok || t.Key().Kind() != reflect.String {
			return typeErr(path, "an object")
		}
		out := reflect.MakeMapWithSize(t, len(obj))
		for k, item := range obj {
			elem := reflect.New(t.Elem()).Elem()
			if err := fill(elem, item, join(path, k)); err != nil {
				return err
			}
			out.SetMapIndex(reflect.ValueOf(k).Convert(t.Key()), elem)
		}
		v.Set(out)
	case reflect.String:
		s, ok := raw.(string)
		if !ok {
			return typeErr(path, "a string")
		}
		v.SetString(s)
	case reflect.Bool:
		b, ok := raw.(bool)
		if !ok {
			return typeErr(path, "a boolean")
		}
		v.SetBool(b)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n, ok := raw.(json.Number)
		i, err := n.Int64()
		if !ok || err != nil || v.OverflowInt(i) {
			return typeErr(path, "an integer")
		}
		v.SetInt(i)
	case reflect.Float32, reflect.Float64:
		n, ok := raw.(json.Number)
		f, err := n.Float64()
		if !ok || err != nil {
			return typeErr(path, "a number")
		}
		v.SetFloat(f)
	default:
		return typeErr(path, "a value this server can send")
	}
	return nil
}

// fillTime takes RFC 3339 or epoch seconds.
func fillTime(v reflect.Value, raw any, path string) error {
	switch t := raw.(type) {
	case string:
		parsed, err := time.Parse(time.RFC3339Nano, t)
		if err != nil {
			return typeErr(path, "an RFC 3339 timestamp")
		}
		v.Set(reflect.ValueOf(parsed))
	case json.Number:
		f, err := t.Float64()
		if err != nil {
			return typeErr(path, "a timestamp")
		}
		sec := int64(f)
		v.Set(reflect.ValueOf(time.Unix(sec, int64((f-float64(sec))*1e9)).UTC()))
	default:
		return typeErr(path, "a timestamp")
	}
	return nil
}

func join(path, name string) string {
	if path == "" {
		return name
	}
	return path + "." + name
}

// Opaque lists the exported members of t, at any depth, that the codec cannot
// carry: interfaces without a registered union (streams, documents), funcs,
// and channels. A path is dotted from t, such as Body.
func Opaque(t reflect.Type) []string {
	var out []string
	seen := map[reflect.Type]bool{}
	var walk func(t reflect.Type, path string)
	walk = func(t reflect.Type, path string) {
		if _, ok := unions[t]; ok {
			return
		}
		switch t.Kind() {
		case reflect.Pointer, reflect.Slice, reflect.Array:
			walk(t.Elem(), path)
		case reflect.Map:
			walk(t.Key(), path)
			walk(t.Elem(), path)
		case reflect.Interface, reflect.Func, reflect.Chan:
			out = append(out, path)
		case reflect.Struct:
			if seen[t] || t == timeType {
				return
			}
			seen[t] = true
			for i := 0; i < t.NumField(); i++ {
				f := t.Field(i)
				if !f.IsExported() || f.Name == "ResultMetadata" {
					continue
				}
				walk(f.Type, join(path, f.Name))
			}
		}
	}
	walk(t, "")
	sort.Strings(out)
	return out
}

// encode writes an SDK output struct as compact JSON. ResultMetadata, nil
// members, and unset enums are left out. A *string that is set to "" stays.
// extra sets top-level members, such as a body the struct holds as a stream.
func encode(out any, extra map[string]any) (string, error) {
	val, err := plain(reflect.ValueOf(out))
	if err != nil {
		return "", err
	}
	obj, ok := val.(map[string]any)
	if !ok {
		obj = map[string]any{}
	}
	for k, v := range extra {
		obj[k] = v
	}
	text, err := json.Marshal(obj)
	if err != nil {
		return "", err
	}
	return string(text), nil
}

// plain converts an SDK value to maps, slices, and scalars. It returns nil for
// a member to leave out. A struct also leaves out an enum (a non-pointer
// string) that is unset.
func plain(v reflect.Value) (any, error) {
	if !v.IsValid() {
		return nil, nil
	}
	if u, ok := unions[v.Type()]; ok {
		if v.IsNil() {
			return nil, nil
		}
		return u.encode(v.Elem())
	}
	if v.Type().Implements(marshalerType) && !(v.Kind() == reflect.Pointer && v.IsNil()) {
		raw, err := v.Interface().(json.Marshaler).MarshalJSON()
		if err != nil {
			return nil, err
		}
		var out any
		if err := json.Unmarshal(raw, &out); err != nil {
			return nil, err
		}
		return out, nil
	}
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface:
		if v.IsNil() {
			return nil, nil
		}
		return plain(v.Elem())
	case reflect.Struct:
		obj := map[string]any{}
		t := v.Type()
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			if !f.IsExported() || f.Name == "ResultMetadata" {
				continue
			}
			fv := v.Field(i)
			if fv.Kind() == reflect.String && fv.Len() == 0 {
				continue
			}
			child, err := plain(fv)
			if err != nil {
				return nil, err
			}
			if child != nil {
				obj[f.Name] = child
			}
		}
		return obj, nil
	case reflect.Slice:
		if v.IsNil() {
			return nil, nil
		}
		if v.Type().Elem().Kind() == reflect.Uint8 {
			return v.Bytes(), nil
		}
		fallthrough
	case reflect.Array:
		arr := make([]any, v.Len())
		for i := range arr {
			item, err := plain(v.Index(i))
			if err != nil {
				return nil, err
			}
			arr[i] = item
		}
		return arr, nil
	case reflect.Map:
		if v.IsNil() {
			return nil, nil
		}
		obj := map[string]any{}
		iter := v.MapRange()
		for iter.Next() {
			item, err := plain(iter.Value())
			if err != nil {
				return nil, err
			}
			obj[fmt.Sprint(iter.Key().Interface())] = item
		}
		return obj, nil
	case reflect.Func, reflect.Chan:
		return nil, nil
	default:
		return v.Interface(), nil
	}
}
