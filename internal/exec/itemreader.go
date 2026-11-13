package exec

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"

	"github.com/catdevman/sfn-local/internal/asl"
	"github.com/catdevman/sfn-local/internal/jsonpath"
	"github.com/catdevman/sfn-local/internal/sdk"
)

// readerFailed is the error for any ItemReader failure. A service error
// keeps its name in the cause.
func readerFailed(err error) error {
	var named *jsonpath.EvalError
	if errors.As(err, &named) && named.Name == "States.ItemReaderFailed" {
		return err
	}
	cause := err.Error()
	var se *sdk.ServiceError
	if errors.As(err, &se) {
		cause = se.Name + ": " + se.Cause
	}
	return &jsonpath.EvalError{Name: "States.ItemReaderFailed", Cause: cause}
}

// readItems runs an ItemReader. Parameters and MaxItems are evaluated
// against the state input first, so a path or expression failure keeps its
// own error name.
func (r *Runner) readItems(ctx context.Context, st *asl.State, root, ctxObj any, vars map[string]any) ([]any, error) {
	rd := st.Dist.Reader
	var params string
	if st.JSONata {
		val, err := r.evalRaw(ctx, rd.Parameters, root, r.bindings(root, ctxObj, vars))
		if err != nil {
			return nil, err
		}
		if params, err = compactValue(val); err != nil {
			return nil, err
		}
	} else {
		var err error
		if _, params, err = evalParameters(rd.Parameters, root, ctxObj, vars); err != nil {
			return nil, err
		}
	}
	limit, err := r.distNumber(ctx, st, intToFloat(rd.MaxItems), rd.MaxItemsPath, rd.MaxItemsExpr, root, ctxObj, vars, "MaxItems", 0, 100000000)
	if err != nil {
		return nil, err
	}
	max := int64(0)
	if limit != nil {
		max = int64(*limit)
	}
	items, err := r.fetchItems(ctx, rd, []byte(params), max)
	if err != nil {
		return nil, readerFailed(err)
	}
	if max > 0 && int64(len(items)) > max {
		items = items[:max]
	}
	return items, nil
}

func (r *Runner) fetchItems(ctx context.Context, rd *asl.ItemReader, params []byte, max int64) ([]any, error) {
	if rd.InputType == "PARQUET" || rd.ManifestType == "ATHENA_DATA" {
		return nil, errors.New("this server does not read " + strings.ToLower(firstNonEmpty(rd.ManifestType, rd.InputType)) + " input")
	}
	if rd.Resource == "arn:aws:states:::s3:listObjectsV2" {
		listLimit := max
		if rd.Transformation == "LOAD_AND_FLATTEN" {
			listLimit = 0
		}
		objects, err := r.SDK.S3List(ctx, params, listLimit)
		if err != nil {
			return nil, err
		}
		if rd.Transformation != "LOAD_AND_FLATTEN" {
			out := make([]any, len(objects))
			for i, o := range objects {
				out[i] = map[string]any{"Etag": o.Etag, "Key": o.Key, "LastModified": o.LastModified, "Size": o.Size, "StorageClass": o.StorageClass}
			}
			return out, nil
		}
		var req struct{ Bucket string }
		_ = json.Unmarshal(params, &req)
		var out []any
		for _, o := range objects {
			if strings.HasSuffix(o.Key, "/") {
				continue
			}
			body, err := r.SDK.S3ReadKey(ctx, req.Bucket, o.Key)
			if err != nil {
				return nil, err
			}
			items, err := parseItems(rd, body)
			if err != nil {
				return nil, errors.New(o.Key + ": " + err.Error())
			}
			out = append(out, items...)
			if max > 0 && int64(len(out)) >= max {
				break
			}
		}
		return out, nil
	}
	body, err := r.SDK.S3Read(ctx, params)
	if err != nil {
		return nil, err
	}
	if rd.InputType == "MANIFEST" || rd.ManifestType == "S3_INVENTORY" {
		return r.readInventory(ctx, rd, body, max)
	}
	return parseItems(rd, body)
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

// parseItems splits one object's body by InputType.
func parseItems(rd *asl.ItemReader, body []byte) ([]any, error) {
	switch rd.InputType {
	case "JSON":
		return jsonItems(body, rd.ItemsPointer)
	case "JSONL":
		return jsonlItems(body)
	case "CSV":
		return csvItems(body, delimiter(rd.CSVDelimiter), rd.CSVHeaderLocation == "FIRST_ROW", rd.CSVHeaders)
	default:
		return nil, errors.New("this server does not read InputType " + rd.InputType)
	}
}

// jsonItems reads an array, at ItemsPointer when one is set.
func jsonItems(body []byte, pointer string) ([]any, error) {
	doc, err := parseJSONBytes(body)
	if err != nil {
		return nil, errors.New("the JSON file is not valid JSON")
	}
	node, err := jsonPointer(doc, pointer)
	if err != nil {
		return nil, err
	}
	arr, ok := node.([]any)
	if !ok {
		if pointer == "" {
			return nil, errors.New("the JSON file must contain an array, or set ItemsPointer to one")
		}
		return nil, errors.New("ItemsPointer " + pointer + " does not select an array")
	}
	return arr, nil
}

func parseJSONBytes(body []byte) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	if dec.More() {
		return nil, errors.New("trailing data")
	}
	return v, nil
}

// jsonPointer follows an RFC 6901 pointer.
func jsonPointer(doc any, pointer string) (any, error) {
	if pointer == "" {
		return doc, nil
	}
	node := doc
	for _, raw := range strings.Split(pointer[1:], "/") {
		token := strings.ReplaceAll(strings.ReplaceAll(raw, "~1", "/"), "~0", "~")
		switch v := node.(type) {
		case map[string]any:
			next, ok := v[token]
			if !ok {
				return nil, errors.New("ItemsPointer " + pointer + " was not found")
			}
			node = next
		case []any:
			i, err := strconv.Atoi(token)
			if err != nil || i < 0 || i >= len(v) {
				return nil, errors.New("ItemsPointer " + pointer + " was not found")
			}
			node = v[i]
		default:
			return nil, errors.New("ItemsPointer " + pointer + " was not found")
		}
	}
	return node, nil
}

// jsonlItems reads one JSON value per line. Blank lines are skipped.
func jsonlItems(body []byte) ([]any, error) {
	var out []any
	sc := bufio.NewScanner(bytes.NewReader(body))
	sc.Buffer(make([]byte, 0, 64<<10), 8<<20)
	line := 0
	for sc.Scan() {
		line++
		text := bytes.TrimSpace(sc.Bytes())
		if len(text) == 0 {
			continue
		}
		v, err := parseJSONBytes(text)
		if err != nil {
			return nil, errors.New("line " + strconv.Itoa(line) + " is not valid JSON")
		}
		out = append(out, v)
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func delimiter(name string) rune {
	switch name {
	case "PIPE":
		return '|'
	case "SEMICOLON":
		return ';'
	case "SPACE":
		return ' '
	case "TAB":
		return '\t'
	default:
		return ','
	}
}

// csvItems turns each record into an object of strings keyed by the header.
// A short record gets empty strings. Fields past the header are dropped.
func csvItems(body []byte, delim rune, headerRow bool, headers []string) ([]any, error) {
	records := csvRecords(string(body), delim)
	if headerRow {
		if len(records) == 0 {
			return nil, errors.New("the CSV file has no header row")
		}
		headers, records = records[0], records[1:]
	}
	out := make([]any, 0, len(records))
	for _, rec := range records {
		item := make(map[string]any, len(headers))
		for i, h := range headers {
			v := ""
			if i < len(rec) {
				v = rec[i]
			}
			item[h] = v
		}
		out = append(out, item)
	}
	return out, nil
}

// csvRecords follows the Step Functions rules. Quotes may wrap a field that
// holds delimiters or newlines, and "" inside them is one quote. A
// backslash escapes a backslash, a quote, or the delimiter, and is dropped
// before any other character. Empty lines are skipped.
func csvRecords(text string, delim rune) [][]string {
	var records [][]string
	var rec []string
	var field strings.Builder
	inQuotes := false
	started := false
	runes := []rune(text)
	endRecord := func() {
		if started || len(rec) > 0 {
			rec = append(rec, field.String())
			records = append(records, rec)
		}
		rec = nil
		field.Reset()
		started = false
	}
	for i := 0; i < len(runes); i++ {
		c := runes[i]
		switch {
		case c == '\\':
			started = true
			if i+1 < len(runes) {
				next := runes[i+1]
				if next == '\\' || next == '"' || next == delim {
					field.WriteRune(next)
					i++
				}
			}
		case c == '"':
			started = true
			if inQuotes && i+1 < len(runes) && runes[i+1] == '"' {
				field.WriteRune('"')
				i++
			} else {
				inQuotes = !inQuotes
			}
		case c == delim && !inQuotes:
			rec = append(rec, field.String())
			field.Reset()
			started = true
		case (c == '\n' || c == '\r') && !inQuotes:
			if c == '\r' && i+1 < len(runes) && runes[i+1] == '\n' {
				i++
			}
			endRecord()
		default:
			started = true
			field.WriteRune(c)
		}
	}
	endRecord()
	return records
}

// readInventory reads an S3 inventory manifest.json and then each CSV data
// file it lists, from the destination bucket. Items are keyed by fileSchema.
func (r *Runner) readInventory(ctx context.Context, rd *asl.ItemReader, body []byte, max int64) ([]any, error) {
	var manifest struct {
		DestinationBucket string `json:"destinationBucket"`
		FileFormat        string `json:"fileFormat"`
		FileSchema        string `json:"fileSchema"`
		Files             []struct {
			Key string `json:"key"`
		} `json:"files"`
	}
	if err := json.Unmarshal(body, &manifest); err != nil {
		return nil, errors.New("the inventory manifest is not valid JSON")
	}
	if manifest.FileFormat != "" && manifest.FileFormat != "CSV" {
		return nil, errors.New("the inventory format must be CSV, not " + manifest.FileFormat)
	}
	bucket := strings.TrimPrefix(manifest.DestinationBucket, "arn:aws:s3:::")
	var headers []string
	for _, h := range strings.Split(manifest.FileSchema, ",") {
		headers = append(headers, strings.TrimSpace(h))
	}
	var out []any
	for _, f := range manifest.Files {
		data, err := r.SDK.S3ReadKey(ctx, bucket, f.Key)
		if err != nil {
			return nil, err
		}
		items, err := csvItems(data, delimiter(rd.CSVDelimiter), false, headers)
		if err != nil {
			return nil, err
		}
		out = append(out, items...)
		if max > 0 && int64(len(out)) >= max {
			break
		}
	}
	return out, nil
}
