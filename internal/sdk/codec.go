package sdk

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"sync"
)

// op adapts one SDK client method. The parameters decode into the method's
// input struct and the output struct encodes back to JSON. Most actions are
// one op line. An action whose members do not survive encoding/json, such as
// a streaming body or a union, gets its own handler.
//
// op panics when I or O holds a member encoding/json cannot carry. Every
// table is built by the inventory test, so that panic fails CI, not a task.
func op[I, O, Opt any](f func(context.Context, *I, ...func(*Opt)) (*O, error)) handler {
	for _, t := range []reflect.Type{reflect.TypeFor[I](), reflect.TypeFor[O]()} {
		if members := Opaque(t); len(members) > 0 {
			panic(fmt.Sprintf("sdk: %s has %v, which encoding/json cannot carry; write a handler", t, members))
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

// decode reads params into in. A top-level key must match a field name
// exactly, because Step Functions does not fold case and encoding/json does.
// An unknown key fails the task with States.Runtime.
func decode(params []byte, in any) error {
	trimmed := bytes.TrimSpace(params)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(trimmed, &top); err != nil {
		return &ServiceError{Name: "States.Runtime", Cause: "aws-sdk parameters must be a JSON object"}
	}
	fields := fieldNames(reflect.TypeOf(in).Elem())
	var unknown []string
	for key := range top {
		if !fields[key] {
			unknown = append(unknown, key)
		}
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		return &ServiceError{Name: "States.Runtime", Cause: fmt.Sprintf("unknown aws-sdk parameter %q", unknown[0])}
	}
	dec := json.NewDecoder(bytes.NewReader(trimmed))
	dec.DisallowUnknownFields()
	if err := dec.Decode(in); err != nil {
		return &ServiceError{Name: "States.Runtime", Cause: "invalid aws-sdk parameters: " + err.Error()}
	}
	return nil
}

// Opaque lists the exported members of t, at any depth, that encoding/json
// cannot round-trip: interfaces (streams, unions, documents), funcs, and
// channels. A path is dotted from t, such as Body or Item.Value.
func Opaque(t reflect.Type) []string {
	var out []string
	seen := map[reflect.Type]bool{}
	var walk func(t reflect.Type, path string)
	walk = func(t reflect.Type, path string) {
		switch t.Kind() {
		case reflect.Pointer, reflect.Slice, reflect.Array:
			walk(t.Elem(), path)
		case reflect.Map:
			walk(t.Key(), path)
			walk(t.Elem(), path)
		case reflect.Interface, reflect.Func, reflect.Chan:
			out = append(out, path)
		case reflect.Struct:
			if seen[t] {
				return
			}
			seen[t] = true
			for i := 0; i < t.NumField(); i++ {
				f := t.Field(i)
				if !f.IsExported() || f.Name == "ResultMetadata" {
					continue
				}
				name := f.Name
				if path != "" {
					name = path + "." + f.Name
				}
				walk(f.Type, name)
			}
		}
	}
	walk(t, "")
	sort.Strings(out)
	return out
}

var fieldCache sync.Map

// fieldNames is the exported field set of an SDK input struct. It reads the
// type once. It is not used to choose a method.
func fieldNames(t reflect.Type) map[string]bool {
	if cached, ok := fieldCache.Load(t); ok {
		return cached.(map[string]bool)
	}
	names := map[string]bool{}
	for i := 0; i < t.NumField(); i++ {
		if f := t.Field(i); f.IsExported() {
			names[f.Name] = true
		}
	}
	fieldCache.Store(t, names)
	return names
}

// encode writes an SDK output struct as compact JSON. ResultMetadata, nil
// members, and unset enums are left out. A *string that is set to "" stays.
// extra sets top-level members, such as a body the struct holds as a stream.
func encode(out any, extra map[string]any) (string, error) {
	obj, ok := plain(reflect.ValueOf(out)).(map[string]any)
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

var marshalerType = reflect.TypeFor[json.Marshaler]()

// plain converts an SDK value to maps, slices, and scalars. It returns nil for
// a member to leave out. A struct also leaves out an enum (a non-pointer
// string) that is unset.
func plain(v reflect.Value) any {
	if !v.IsValid() {
		return nil
	}
	if v.Type().Implements(marshalerType) && !(v.Kind() == reflect.Pointer && v.IsNil()) {
		raw, err := v.Interface().(json.Marshaler).MarshalJSON()
		if err != nil {
			return nil
		}
		var out any
		if json.Unmarshal(raw, &out) != nil {
			return nil
		}
		return out
	}
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface:
		if v.IsNil() {
			return nil
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
			if child := plain(fv); child != nil {
				obj[f.Name] = child
			}
		}
		return obj
	case reflect.Slice:
		if v.IsNil() {
			return nil
		}
		if v.Type().Elem().Kind() == reflect.Uint8 {
			return v.Bytes()
		}
		fallthrough
	case reflect.Array:
		arr := make([]any, v.Len())
		for i := range arr {
			arr[i] = plain(v.Index(i))
		}
		return arr
	case reflect.Map:
		if v.IsNil() {
			return nil
		}
		obj := map[string]any{}
		iter := v.MapRange()
		for iter.Next() {
			obj[fmt.Sprint(iter.Key().Interface())] = plain(iter.Value())
		}
		return obj
	case reflect.Func, reflect.Chan:
		return nil
	default:
		return v.Interface()
	}
}
