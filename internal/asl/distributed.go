package asl

import (
	"encoding/json"
	"sort"
	"strconv"
	"strings"
)

// DistMap is the Distributed Map part of a Map state whose ItemProcessor sets
// Mode DISTRIBUTED. Each item, or each batch, runs as a child execution.
type DistMap struct {
	// ExecutionType is STANDARD or EXPRESS, the type of each child execution.
	ExecutionType string
	// Definition is the child state machine: Comment, StartAt, and States from
	// ItemProcessor, plus QueryLanguage when the parent's is JSONata.
	Definition string
	// Label names the Map Run. Empty means one is generated per run.
	Label string

	MaxConcurrencyPath string

	TolerancePct     *float64
	TolerancePctPath string
	TolerancePctExpr string

	ToleranceCount     *int64
	ToleranceCountPath string
	ToleranceCountExpr string

	Batcher *ItemBatcher
	Writer  *ResultWriter
	Reader  *ItemReader
}

// ItemReader reads the items from S3 instead of the state input.
type ItemReader struct {
	// Resource is arn:aws:states:::s3:getObject or arn:aws:states:::s3:listObjectsV2.
	Resource string
	// Parameters is Parameters, or Arguments for JSONata.
	Parameters json.RawMessage
	// InputType is CSV, JSON, JSONL, PARQUET, or MANIFEST. Empty lists
	// object metadata, or reads an S3_INVENTORY manifest.
	InputType string
	// Transformation is NONE or LOAD_AND_FLATTEN, for listObjectsV2.
	Transformation    string
	ManifestType      string
	CSVHeaderLocation string
	CSVHeaders        []string
	CSVDelimiter      string
	ItemsPointer      string

	MaxItems     *int64
	MaxItemsPath string
	MaxItemsExpr string
}

const (
	resourceGetObject   = "arn:aws:states:::s3:getObject"
	resourceListObjects = "arn:aws:states:::s3:listObjectsV2"
)

// ItemBatcher groups items into one child input: {"Items":[...],"BatchInput":...}.
type ItemBatcher struct {
	MaxItems     *int64
	MaxItemsPath string
	MaxItemsExpr string

	MaxBytes     *int64
	MaxBytesPath string
	MaxBytesExpr string

	HasBatchInput bool
	BatchInput    json.RawMessage
}

// ResultWriter shapes the Map Run results. Resource and Parameters export them
// to S3. WriterConfig alone shapes the state output.
type ResultWriter struct {
	// Transformation is NONE, COMPACT, or FLATTEN. Empty is the default:
	// COMPACT for the state output, NONE for an export.
	Transformation string
	// OutputType is JSON or JSONL. It applies to an export.
	OutputType    string
	Resource      string
	HasParameters bool
	Parameters    json.RawMessage
}

// distributedOnly are the Map fields that need Mode DISTRIBUTED.
var distributedOnly = []string{
	"ItemReader", "ItemBatcher", "ResultWriter", "Label",
	"ToleratedFailurePercentage", "ToleratedFailurePercentagePath",
	"ToleratedFailureCount", "ToleratedFailureCountPath",
}

// labelForbidden is every character a Label may not contain, besides
// whitespace and control characters.
const labelForbidden = "?*<>{}[]:;,\\|^~$#%&`\""

// checkDistributed reads the Distributed Map fields onto st.Dist. dist is
// whether the ItemProcessor set Mode DISTRIBUTED.
func checkDistributed(st *State, fields map[string]json.RawMessage, ptr string, dist bool) error {
	if !dist {
		for _, name := range distributedOnly {
			if _, ok := fields[name]; ok {
				return defErr(codeSchema, "Field '"+name+"' requires ProcessorConfig Mode DISTRIBUTED", join(ptr, name))
			}
		}
		if _, ok := fields["MaxConcurrencyPath"]; ok {
			return defErr(codeSchema, "Field 'MaxConcurrencyPath' requires ProcessorConfig Mode DISTRIBUTED", join(ptr, "MaxConcurrencyPath"))
		}
		return nil
	}
	d := st.Dist
	for _, pair := range [][2]string{
		{"MaxConcurrency", "MaxConcurrencyPath"},
		{"ToleratedFailurePercentage", "ToleratedFailurePercentagePath"},
		{"ToleratedFailureCount", "ToleratedFailureCountPath"},
	} {
		if err := atMostOne(fields, pair[0], pair[1], ptr); err != nil {
			return err
		}
	}
	if raw, ok := fields["Label"]; ok {
		label, err := asString(raw)
		if err != nil || !validLabel(label) {
			return defErr(codeSchema, "Field 'Label' must be a string of 1 to 40 characters without whitespace, wildcards, brackets, or special characters", join(ptr, "Label"))
		}
		d.Label = label
	}
	var err error
	if d.MaxConcurrencyPath, err = pathField(fields, "MaxConcurrencyPath", ptr); err != nil {
		return err
	}
	if d.TolerancePctPath, err = pathField(fields, "ToleratedFailurePercentagePath", ptr); err != nil {
		return err
	}
	if d.ToleranceCountPath, err = pathField(fields, "ToleratedFailureCountPath", ptr); err != nil {
		return err
	}
	if raw, ok := fields["ToleratedFailurePercentage"]; ok {
		if body, isExpr := expressionBody(raw); isExpr && st.JSONata {
			if err := scanExpr(body, join(ptr, "ToleratedFailurePercentage"), false, false); err != nil {
				return err
			}
			d.TolerancePctExpr = body
		} else {
			f, err := asFloat(raw)
			if err != nil || f < 0 || f > 100 {
				return defErr(codeSchema, "Field 'ToleratedFailurePercentage' must be a number in 0..100", join(ptr, "ToleratedFailurePercentage"))
			}
			d.TolerancePct = &f
		}
	}
	if raw, ok := fields["ToleratedFailureCount"]; ok {
		if body, isExpr := expressionBody(raw); isExpr && st.JSONata {
			if err := scanExpr(body, join(ptr, "ToleratedFailureCount"), false, false); err != nil {
				return err
			}
			d.ToleranceCountExpr = body
		} else {
			n, err := asInt(raw, 0, int64(^uint64(0)>>1))
			if err != nil {
				return defErr(codeSchema, "Field 'ToleratedFailureCount' must be an integer >= 0", join(ptr, "ToleratedFailureCount"))
			}
			d.ToleranceCount = &n
		}
	}
	if raw, ok := fields["ItemBatcher"]; ok {
		b, err := checkBatcher(raw, join(ptr, "ItemBatcher"), st.JSONata)
		if err != nil {
			return err
		}
		d.Batcher = b
	}
	if raw, ok := fields["ResultWriter"]; ok {
		w, err := checkWriter(raw, join(ptr, "ResultWriter"), st.JSONata)
		if err != nil {
			return err
		}
		d.Writer = w
	}
	if raw, ok := fields["ItemReader"]; ok {
		if err := atMostOne(fields, "ItemReader", itemsField(st.JSONata), ptr); err != nil {
			return err
		}
		reader, err := checkReader(raw, join(ptr, "ItemReader"), st.JSONata)
		if err != nil {
			return err
		}
		d.Reader = reader
	}
	return nil
}

func itemsField(jsonata bool) string {
	if jsonata {
		return "Items"
	}
	return "ItemsPath"
}

func oneOf(raw json.RawMessage, values ...string) (string, bool) {
	text, err := asString(raw)
	if err != nil {
		return "", false
	}
	for _, v := range values {
		if text == v {
			return text, true
		}
	}
	return "", false
}

func checkReader(raw json.RawMessage, ptr string, jsonata bool) (*ItemReader, error) {
	fields, err := decodeObject(raw)
	if err != nil {
		return nil, defErr(codeSchema, "Field 'ItemReader' must be an object", ptr)
	}
	params := "Parameters"
	if jsonata {
		params = "Arguments"
	}
	if err := unknown(fields, map[string]bool{"Resource": true, params: true, "ReaderConfig": true}, ptr); err != nil {
		return nil, err
	}
	r := &ItemReader{}
	var ok bool
	if r.Resource, ok = oneOf(fields["Resource"], resourceGetObject, resourceListObjects); !ok {
		return nil, defErr(codeSchema, "ItemReader Resource must be "+resourceGetObject+" or "+resourceListObjects, join(ptr, "Resource"))
	}
	p, has := fields[params]
	if !has {
		return nil, defErr(codeSchema, "ItemReader needs "+params, ptr)
	}
	if _, err := decodeObject(p); err != nil {
		return nil, defErr(codeSchema, "Field '"+params+"' must be an object", join(ptr, params))
	}
	if !jsonata {
		if err := checkPayload(p, join(ptr, params)); err != nil {
			return nil, err
		}
	} else if err := scanRaw(p, join(ptr, params), false, false); err != nil {
		return nil, err
	}
	r.Parameters = append(json.RawMessage(nil), p...)
	cfgRaw, hasCfg := fields["ReaderConfig"]
	if hasCfg {
		if err := r.readConfig(cfgRaw, join(ptr, "ReaderConfig"), jsonata); err != nil {
			return nil, err
		}
	}
	cfgPtr := join(ptr, "ReaderConfig")
	switch r.Resource {
	case resourceGetObject:
		if r.InputType == "" && r.ManifestType != "S3_INVENTORY" {
			return nil, defErr(codeSchema, "ReaderConfig InputType is required for "+resourceGetObject, cfgPtr)
		}
		if r.Transformation != "" {
			return nil, defErr(codeSchema, "ReaderConfig Transformation requires "+resourceListObjects, cfgPtr)
		}
	case resourceListObjects:
		if r.Transformation == "LOAD_AND_FLATTEN" && r.InputType == "" {
			return nil, defErr(codeSchema, "ReaderConfig LOAD_AND_FLATTEN requires InputType", cfgPtr)
		}
		if r.Transformation != "LOAD_AND_FLATTEN" && r.InputType != "" {
			return nil, defErr(codeSchema, "ReaderConfig InputType with "+resourceListObjects+" requires Transformation LOAD_AND_FLATTEN", cfgPtr)
		}
		if r.ManifestType != "" {
			return nil, defErr(codeSchema, "ReaderConfig ManifestType requires "+resourceGetObject, cfgPtr)
		}
	}
	if r.ManifestType == "S3_INVENTORY" && r.InputType != "" {
		return nil, defErr(codeSchema, "ReaderConfig InputType is not allowed with ManifestType S3_INVENTORY", cfgPtr)
	}
	if r.InputType == "CSV" && r.CSVHeaderLocation == "" {
		return nil, defErr(codeSchema, "ReaderConfig CSVHeaderLocation is required for InputType CSV", cfgPtr)
	}
	if r.CSVHeaderLocation == "GIVEN" && len(r.CSVHeaders) == 0 {
		return nil, defErr(codeSchema, "ReaderConfig CSVHeaders is required for CSVHeaderLocation GIVEN", cfgPtr)
	}
	if r.CSVHeaderLocation != "GIVEN" && len(r.CSVHeaders) > 0 {
		return nil, defErr(codeSchema, "ReaderConfig CSVHeaders requires CSVHeaderLocation GIVEN", cfgPtr)
	}
	csvLike := r.InputType == "CSV" || r.InputType == "MANIFEST" || r.ManifestType != ""
	if (r.CSVHeaderLocation != "" || r.CSVDelimiter != "") && !csvLike {
		return nil, defErr(codeSchema, "ReaderConfig CSV fields require InputType CSV or MANIFEST", cfgPtr)
	}
	if r.ItemsPointer != "" && r.InputType != "JSON" {
		return nil, defErr(codeSchema, "ReaderConfig ItemsPointer requires InputType JSON", cfgPtr)
	}
	return r, nil
}

func (r *ItemReader) readConfig(raw json.RawMessage, ptr string, jsonata bool) error {
	cfg, err := decodeObject(raw)
	if err != nil {
		return defErr(codeSchema, "Field 'ReaderConfig' must be an object", ptr)
	}
	allowed := map[string]bool{"InputType": true, "Transformation": true, "ManifestType": true, "CSVHeaderLocation": true,
		"CSVHeaders": true, "CSVDelimiter": true, "ItemsPointer": true, "MaxItems": true}
	if !jsonata {
		allowed["MaxItemsPath"] = true
	}
	if err := unknown(cfg, allowed, ptr); err != nil {
		return err
	}
	for _, f := range []struct {
		name   string
		dest   *string
		values []string
	}{
		{"InputType", &r.InputType, []string{"CSV", "JSON", "JSONL", "PARQUET", "MANIFEST"}},
		{"Transformation", &r.Transformation, []string{"NONE", "LOAD_AND_FLATTEN"}},
		{"ManifestType", &r.ManifestType, []string{"ATHENA_DATA", "S3_INVENTORY"}},
		{"CSVHeaderLocation", &r.CSVHeaderLocation, []string{"FIRST_ROW", "GIVEN"}},
		{"CSVDelimiter", &r.CSVDelimiter, []string{"COMMA", "PIPE", "SEMICOLON", "SPACE", "TAB"}},
	} {
		v, has := cfg[f.name]
		if !has {
			continue
		}
		text, ok := oneOf(v, f.values...)
		if !ok {
			return defErr(codeSchema, "Field '"+f.name+"' must be one of "+strings.Join(f.values, ", "), join(ptr, f.name))
		}
		*f.dest = text
	}
	if v, has := cfg["CSVHeaders"]; has {
		headers, err := stringList(v, join(ptr, "CSVHeaders"))
		if err != nil || len(headers) == 0 {
			return defErr(codeSchema, "Field 'CSVHeaders' must be a non-empty array of strings", join(ptr, "CSVHeaders"))
		}
		r.CSVHeaders = headers
	}
	if v, has := cfg["ItemsPointer"]; has {
		text, err := asString(v)
		if err != nil || (text != "" && !strings.HasPrefix(text, "/")) || len(text) >= 2000 {
			return defErr(codeSchema, "Field 'ItemsPointer' must be a JSON Pointer", join(ptr, "ItemsPointer"))
		}
		r.ItemsPointer = text
	}
	if err := atMostOne(cfg, "MaxItems", "MaxItemsPath", ptr); err != nil {
		return err
	}
	if r.MaxItemsPath, err = pathField(cfg, "MaxItemsPath", ptr); err != nil {
		return err
	}
	if v, has := cfg["MaxItems"]; has {
		if body, isExpr := expressionBody(v); isExpr && jsonata {
			if err := scanExpr(body, join(ptr, "MaxItems"), false, false); err != nil {
				return err
			}
			r.MaxItemsExpr = body
		} else {
			n, err := asInt(v, 0, 100000000)
			if err != nil {
				return defErr(codeSchema, "Field 'MaxItems' must be an integer in 0..100000000", join(ptr, "MaxItems"))
			}
			r.MaxItems = &n
		}
	}
	return nil
}

// uniqueLabels rejects a Label used by two Distributed Map states anywhere
// in the definition, nested ones included. States are visited in name order.
func uniqueLabels(prog *Program, ptr string, seen map[string]bool) error {
	names := make([]string, 0, len(prog.States))
	for name := range prog.States {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		st := prog.States[name]
		at := ptr + "/States/" + escape(name)
		if st.Dist != nil && st.Dist.Label != "" {
			if seen[st.Dist.Label] {
				return defErr(codeSchema, "Label '"+st.Dist.Label+"' is already used by another Distributed Map state", at+"/Label")
			}
			seen[st.Dist.Label] = true
		}
		for i, b := range st.Branches {
			if err := uniqueLabels(b, at+"/Branches/"+strconv.Itoa(i), seen); err != nil {
				return err
			}
		}
		if st.Processor != nil {
			if err := uniqueLabels(st.Processor, at+"/ItemProcessor", seen); err != nil {
				return err
			}
		}
	}
	return nil
}

func validLabel(label string) bool {
	if label == "" || len([]rune(label)) > 40 {
		return false
	}
	for _, r := range label {
		if r <= 0x20 || (r >= 0x7f && r <= 0x9f) || r == 0x85 || r == 0xa0 || strings.ContainsRune(labelForbidden, r) {
			return false
		}
	}
	return true
}

func pathField(fields map[string]json.RawMessage, name, ptr string) (string, error) {
	raw, ok := fields[name]
	if !ok {
		return "", nil
	}
	text, err := asString(raw)
	if err != nil || !strings.HasPrefix(text, "$") {
		return "", defErr(codeSchema, "Field '"+name+"' must be a reference path", join(ptr, name))
	}
	return text, nil
}

func checkBatcher(raw json.RawMessage, ptr string, jsonata bool) (*ItemBatcher, error) {
	fields, err := decodeObject(raw)
	if err != nil {
		return nil, defErr(codeSchema, "Field 'ItemBatcher' must be an object", ptr)
	}
	allowed := map[string]bool{"MaxItemsPerBatch": true, "MaxInputBytesPerBatch": true, "BatchInput": true}
	if !jsonata {
		allowed["MaxItemsPerBatchPath"] = true
		allowed["MaxInputBytesPerBatchPath"] = true
	}
	if err := unknown(fields, allowed, ptr); err != nil {
		return nil, err
	}
	b := &ItemBatcher{}
	for _, f := range []struct {
		name, path string
		n          **int64
		p, expr    *string
		max        int64
	}{
		{"MaxItemsPerBatch", "MaxItemsPerBatchPath", &b.MaxItems, &b.MaxItemsPath, &b.MaxItemsExpr, int64(^uint64(0) >> 1)},
		{"MaxInputBytesPerBatch", "MaxInputBytesPerBatchPath", &b.MaxBytes, &b.MaxBytesPath, &b.MaxBytesExpr, 262144},
	} {
		if err := atMostOne(fields, f.name, f.path, ptr); err != nil {
			return nil, err
		}
		if *f.p, err = pathField(fields, f.path, ptr); err != nil {
			return nil, err
		}
		raw, ok := fields[f.name]
		if !ok {
			continue
		}
		if body, isExpr := expressionBody(raw); isExpr && jsonata {
			if err := scanExpr(body, join(ptr, f.name), false, false); err != nil {
				return nil, err
			}
			*f.expr = body
			continue
		}
		n, err := asInt(raw, 1, f.max)
		if err != nil {
			return nil, defErr(codeSchema, "Field '"+f.name+"' must be a positive integer", join(ptr, f.name))
		}
		*f.n = &n
	}
	if b.MaxItems == nil && b.MaxItemsPath == "" && b.MaxItemsExpr == "" && b.MaxBytes == nil && b.MaxBytesPath == "" && b.MaxBytesExpr == "" {
		return nil, defErr(codeSchema, "ItemBatcher needs MaxItemsPerBatch or MaxInputBytesPerBatch", ptr)
	}
	if raw, ok := fields["BatchInput"]; ok {
		if jsonata {
			if err := scanRaw(raw, join(ptr, "BatchInput"), false, false); err != nil {
				return nil, err
			}
		} else if err := checkPayload(raw, join(ptr, "BatchInput")); err != nil {
			return nil, err
		}
		b.HasBatchInput = true
		b.BatchInput = append(json.RawMessage(nil), raw...)
	}
	return b, nil
}

func checkWriter(raw json.RawMessage, ptr string, jsonata bool) (*ResultWriter, error) {
	fields, err := decodeObject(raw)
	if err != nil {
		return nil, defErr(codeSchema, "Field 'ResultWriter' must be an object", ptr)
	}
	params := "Parameters"
	if jsonata {
		params = "Arguments"
	}
	if err := unknown(fields, map[string]bool{"Resource": true, params: true, "WriterConfig": true}, ptr); err != nil {
		return nil, err
	}
	w := &ResultWriter{}
	if raw, ok := fields["WriterConfig"]; ok {
		cfg, err := decodeObject(raw)
		if err != nil {
			return nil, defErr(codeSchema, "Field 'WriterConfig' must be an object", join(ptr, "WriterConfig"))
		}
		if err := unknown(cfg, map[string]bool{"Transformation": true, "OutputType": true}, join(ptr, "WriterConfig")); err != nil {
			return nil, err
		}
		if raw, ok := cfg["Transformation"]; ok {
			w.Transformation, err = asString(raw)
			if err != nil || (w.Transformation != "NONE" && w.Transformation != "COMPACT" && w.Transformation != "FLATTEN") {
				return nil, defErr(codeSchema, "Field 'Transformation' must be NONE, COMPACT, or FLATTEN", join(ptr, "WriterConfig")+"/Transformation")
			}
		}
		if raw, ok := cfg["OutputType"]; ok {
			w.OutputType, err = asString(raw)
			if err != nil || (w.OutputType != "JSON" && w.OutputType != "JSONL") {
				return nil, defErr(codeSchema, "Field 'OutputType' must be JSON or JSONL", join(ptr, "WriterConfig")+"/OutputType")
			}
		}
	}
	_, hasResource := fields["Resource"]
	_, hasParams := fields[params]
	if hasResource != hasParams {
		return nil, defErr(codeSchema, "ResultWriter needs both Resource and "+params, ptr)
	}
	if !hasResource && w.Transformation == "" && w.OutputType == "" {
		return nil, defErr(codeSchema, "ResultWriter needs WriterConfig, or Resource and "+params, ptr)
	}
	if hasResource {
		w.Resource, err = asString(fields["Resource"])
		if err != nil || w.Resource != "arn:aws:states:::s3:putObject" {
			return nil, defErr(codeSchema, "ResultWriter Resource must be arn:aws:states:::s3:putObject", join(ptr, "Resource"))
		}
		w.HasParameters = true
		w.Parameters = append(json.RawMessage(nil), fields[params]...)
	}
	return w, nil
}

// childDefinition builds the child state machine text from an ItemProcessor.
func childDefinition(fields map[string]json.RawMessage, machineLang string) (string, error) {
	out := map[string]json.RawMessage{}
	for _, key := range []string{"Comment", "StartAt", "States"} {
		if raw, ok := fields[key]; ok {
			out[key] = raw
		}
	}
	if machineLang == langJSONata {
		out["QueryLanguage"] = json.RawMessage(`"JSONata"`)
	}
	text, err := json.Marshal(out)
	return string(text), err
}
