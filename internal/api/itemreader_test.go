package api

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/catdevman/sfn-local/internal/api/gen"
	"github.com/catdevman/sfn-local/internal/sdk"
	"github.com/catdevman/sfn-local/internal/store"
)

// memS3 is a path-style S3 that keeps objects in memory. It lists with
// max-keys and continuation tokens so pagination is exercised.
type memS3 struct {
	mu      sync.Mutex
	objects map[string][]byte
	pageMax int
}

func newMemS3(t *testing.T, s *Service) *memS3 {
	t.Helper()
	m := &memS3{objects: map[string][]byte{}, pageMax: 2}
	srv := httptest.NewServer(m)
	t.Cleanup(srv.Close)
	s.Runner.SDK = sdk.New(sdk.Config{Region: "us-east-1", AccessKeyID: "local", SecretAccessKey: "local", Endpoints: map[string]string{"s3": srv.URL}})
	return m
}

func (m *memS3) put(path string, body []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objects[path] = body
}

func (m *memS3) get(path string) ([]byte, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.objects[path]
	return b, ok
}

func (m *memS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()
	q := r.URL.Query()
	switch {
	case r.Method == http.MethodPut:
		body, _ := io.ReadAll(r.Body)
		m.objects[r.URL.Path] = body
		w.Header().Set("ETag", `"e"`)
	case r.Method == http.MethodGet && q.Get("list-type") == "2":
		bucket := strings.Trim(r.URL.Path, "/")
		var keys []string
		for path := range m.objects {
			key, ok := strings.CutPrefix(path, "/"+bucket+"/")
			if ok && strings.HasPrefix(key, q.Get("prefix")) {
				keys = append(keys, key)
			}
		}
		sort.Strings(keys)
		start, _ := strconv.Atoi(q.Get("continuation-token"))
		end := min(start+m.pageMax, len(keys))
		var b strings.Builder
		fmt.Fprintf(&b, `<ListBucketResult><Name>%s</Name><KeyCount>%d</KeyCount><IsTruncated>%v</IsTruncated>`, bucket, end-start, end < len(keys))
		if end < len(keys) {
			fmt.Fprintf(&b, `<NextContinuationToken>%d</NextContinuationToken>`, end)
		}
		for _, k := range keys[start:end] {
			fmt.Fprintf(&b, `<Contents><Key>%s</Key><LastModified>2024-01-02T03:04:05.000Z</LastModified><ETag>"etag-%s"</ETag><Size>%d</Size><StorageClass>STANDARD</StorageClass></Contents>`, k, k, len(m.objects["/"+bucket+"/"+k]))
		}
		b.WriteString(`</ListBucketResult>`)
		w.Header().Set("Content-Type", "application/xml")
		io.WriteString(w, b.String())
	case r.Method == http.MethodGet:
		body, ok := m.objects[r.URL.Path]
		if !ok {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusNotFound)
			io.WriteString(w, `<Error><Code>NoSuchKey</Code><Message>The specified key does not exist.</Message></Error>`)
			return
		}
		w.Write(body)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// echoChild returns its input.
const echoChild = `"ItemProcessor":{"ProcessorConfig":{"Mode":"DISTRIBUTED","ExecutionType":"EXPRESS"},"StartAt":"E","States":{"E":{"Type":"Pass","End":true}}}`

func TestItemReaderCSVAndJSON(t *testing.T) {
	s := newService()
	m := newMemS3(t, s)
	m.put("/data/ratings.csv", []byte("userId,movieId,rating\n1,307,3.5\n1,481,\"3,5\"\n2,1091\n"))
	csvDef := `{"StartAt":"M","States":{"M":{"Type":"Map",` + echoChild + `,
		"ItemReader":{"Resource":"arn:aws:states:::s3:getObject","ReaderConfig":{"InputType":"CSV","CSVHeaderLocation":"FIRST_ROW"},
			"Parameters":{"Bucket":"data","Key.$":"$.key"}},"End":true}}}`
	ex := runMachine(t, s, "csv", csvDef, "run", `{"key":"ratings.csv"}`)
	if ex.Status != store.ExecSucceeded {
		t.Fatalf("%s %s %s", ex.Status, ex.Error, ex.Cause)
	}
	jsonEqual(t, ex.Output, `[{"userId":"1","movieId":"307","rating":"3.5"},{"userId":"1","movieId":"481","rating":"3,5"},{"userId":"2","movieId":"1091","rating":""}]`)

	var gz bytes.Buffer
	zw := gzip.NewWriter(&gz)
	zw.Write([]byte("1|307\n2|481\n3|9\n"))
	zw.Close()
	m.put("/data/given.csv.gz", gz.Bytes())
	givenDef := `{"StartAt":"M","States":{"M":{"Type":"Map",` + echoChild + `,
		"ItemReader":{"Resource":"arn:aws:states:::s3:getObject","ReaderConfig":{"InputType":"CSV","CSVHeaderLocation":"GIVEN","CSVHeaders":["user","movie"],"CSVDelimiter":"PIPE","MaxItemsPath":"$.max"},
			"Parameters":{"Bucket":"data","Key":"given.csv.gz"}},"End":true}}}`
	gx := runMachine(t, s, "given", givenDef, "run", `{"max":2}`)
	if gx.Status != store.ExecSucceeded {
		t.Fatalf("%s %s %s", gx.Status, gx.Error, gx.Cause)
	}
	jsonEqual(t, gx.Output, `[{"user":"1","movie":"307"},{"user":"2","movie":"481"}]`)

	m.put("/data/nested.json", []byte(`{"inventory":{"featured":[{"sku":"pen"},{"sku":"desk"}]}}`))
	jsonDef := `{"QueryLanguage":"JSONata","StartAt":"M","States":{"M":{"Type":"Map",` + echoChild + `,
		"ItemSelector":{"sku":"{% $states.context.Map.Item.Value.sku %}","i":"{% $states.context.Map.Item.Index %}"},
		"ItemReader":{"Resource":"arn:aws:states:::s3:getObject","ReaderConfig":{"InputType":"JSON","ItemsPointer":"/inventory/featured"},
			"Arguments":{"Bucket":"data","Key":"{% $states.input.key %}"}},"End":true}}}`
	jx := runMachine(t, s, "json", jsonDef, "run", `{"key":"nested.json"}`)
	if jx.Status != store.ExecSucceeded {
		t.Fatalf("%s %s %s", jx.Status, jx.Error, jx.Cause)
	}
	jsonEqual(t, jx.Output, `[{"sku":"pen","i":0},{"sku":"desk","i":1}]`)

	missing := runMachine(t, s, "csv", csvDef, "missing", `{"key":"nope.csv"}`)
	if missing.Status != store.ExecFailed || missing.Error != "States.ItemReaderFailed" || !strings.HasPrefix(missing.Cause, "S3.NoSuchKeyException: The specified key does not exist.") {
		t.Fatalf("missing %s %s %s", missing.Status, missing.Error, missing.Cause)
	}

	none := newService()
	nx := runMachine(t, none, "csv", csvDef, "none", `{"key":"ratings.csv"}`)
	if nx.Status != store.ExecFailed || nx.Error != "States.ItemReaderFailed" || !strings.Contains(nx.Cause, "AWS_ENDPOINT_URL_S3") {
		t.Fatalf("no endpoint %s %s %s", nx.Status, nx.Error, nx.Cause)
	}
}

func TestItemReaderListObjects(t *testing.T) {
	s := newService()
	m := newMemS3(t, s)
	m.put("/logs/2024/a.jsonl", []byte("{\"n\":1}\n{\"n\":2}\n"))
	m.put("/logs/2024/b.jsonl", []byte("{\"n\":3}\n"))
	m.put("/logs/2024/c.jsonl", []byte("{\"n\":4}\n"))
	m.put("/logs/other/x.jsonl", []byte("{\"n\":99}\n"))
	listDef := `{"StartAt":"M","States":{"M":{"Type":"Map",` + echoChild + `,
		"ItemReader":{"Resource":"arn:aws:states:::s3:listObjectsV2","Parameters":{"Bucket":"logs","Prefix":"2024/"}},"End":true}}}`
	ex := runMachine(t, s, "list", listDef, "run", `{}`)
	if ex.Status != store.ExecSucceeded {
		t.Fatalf("%s %s %s", ex.Status, ex.Error, ex.Cause)
	}
	var objs []map[string]any
	json.Unmarshal([]byte(ex.Output), &objs)
	if len(objs) != 3 || objs[0]["Key"] != "2024/a.jsonl" || objs[0]["Etag"] != `"etag-2024/a.jsonl"` || objs[0]["LastModified"] != float64(1704164645) || objs[0]["StorageClass"] != "STANDARD" || objs[2]["Key"] != "2024/c.jsonl" {
		t.Fatalf("objects %s", ex.Output)
	}

	flatDef := `{"StartAt":"M","States":{"M":{"Type":"Map",` + echoChild + `,
		"ItemReader":{"Resource":"arn:aws:states:::s3:listObjectsV2","ReaderConfig":{"InputType":"JSONL","Transformation":"LOAD_AND_FLATTEN","MaxItems":3},
			"Parameters":{"Bucket":"logs","Prefix":"2024/"}},"End":true}}}`
	fx := runMachine(t, s, "flat", flatDef, "run", `{}`)
	if fx.Status != store.ExecSucceeded {
		t.Fatalf("%s %s %s", fx.Status, fx.Error, fx.Cause)
	}
	jsonEqual(t, fx.Output, `[{"n":1},{"n":2},{"n":3}]`)
}

func TestItemReaderInventory(t *testing.T) {
	s := newService()
	m := newMemS3(t, s)
	var gz bytes.Buffer
	zw := gzip.NewWriter(&gz)
	zw.Write([]byte("\"src\",\"csvDataset/titles.csv\",\"3399671\",\"2022-11-16T00:29:32.000Z\"\n\"src\",\"img/a.jpg\",\"27034\",\"2022-11-15T20:02:16.000Z\"\n"))
	zw.Close()
	m.put("/inv/data/part-1.csv.gz", gz.Bytes())
	m.put("/inv/manifest.json", []byte(`{"sourceBucket":"src","destinationBucket":"arn:aws:s3:::inv","fileFormat":"CSV","fileSchema":"Bucket, Key, Size, LastModifiedDate","files":[{"key":"data/part-1.csv.gz"}]}`))
	def := `{"StartAt":"M","States":{"M":{"Type":"Map",` + echoChild + `,
		"ItemReader":{"Resource":"arn:aws:states:::s3:getObject","ReaderConfig":{"InputType":"MANIFEST"},"Parameters":{"Bucket":"inv","Key":"manifest.json"}},"End":true}}}`
	ex := runMachine(t, s, "inv", def, "run", `{}`)
	if ex.Status != store.ExecSucceeded {
		t.Fatalf("%s %s %s", ex.Status, ex.Error, ex.Cause)
	}
	jsonEqual(t, ex.Output, `[{"Bucket":"src","Key":"csvDataset/titles.csv","Size":"3399671","LastModifiedDate":"2022-11-16T00:29:32.000Z"},{"Bucket":"src","Key":"img/a.jpg","Size":"27034","LastModifiedDate":"2022-11-15T20:02:16.000Z"}]`)
}

func TestResultWriterExport(t *testing.T) {
	s := newService()
	m := newMemS3(t, s)
	def := `{"StartAt":"M","States":{"M":{"Type":"Map","ToleratedFailurePercentage":50,` + failingChild + `,
		"ResultWriter":{"Resource":"arn:aws:states:::s3:putObject","Parameters":{"Bucket":"out","Prefix":"jobs/"}},"End":true}}}`
	ex := runMachine(t, s, "export", def, "run", `[{"bad":false},{"bad":true},{"bad":false}]`)
	if ex.Status != store.ExecSucceeded {
		t.Fatalf("%s %s %s", ex.Status, ex.Error, ex.Cause)
	}
	run := s.Store.MapRuns(ex.ARN)[0]
	runID := run.ARN[strings.LastIndex(run.ARN, ":")+1:]
	manifestKey := "jobs/" + runID + "/manifest.json"
	jsonEqual(t, ex.Output, `{"MapRunArn":"`+run.ARN+`","ResultWriterDetails":{"Bucket":"out","Key":"`+manifestKey+`"}}`)
	raw, ok := m.get("/out/" + manifestKey)
	if !ok {
		t.Fatalf("no manifest in %v", m.objects)
	}
	var manifest struct {
		DestinationBucket string
		MapRunArn         string
		ResultFiles       map[string][]struct {
			Key  string
			Size int
		}
	}
	if err := json.Unmarshal(raw, &manifest); err != nil || manifest.DestinationBucket != "out" || manifest.MapRunArn != run.ARN || len(manifest.ResultFiles["SUCCEEDED"]) != 1 || len(manifest.ResultFiles["FAILED"]) != 1 || len(manifest.ResultFiles["PENDING"]) != 0 {
		t.Fatalf("manifest %s %v", raw, err)
	}
	succ, _ := m.get("/out/" + manifest.ResultFiles["SUCCEEDED"][0].Key)
	var records []map[string]any
	if err := json.Unmarshal(succ, &records); err != nil || len(records) != 2 || records[0]["Status"] != "SUCCEEDED" || records[0]["Output"] != `"ok"` || manifest.ResultFiles["SUCCEEDED"][0].Size != len(succ) {
		t.Fatalf("succeeded %s %v", succ, err)
	}
	failed, _ := m.get("/out/jobs/" + runID + "/FAILED_0.json")
	if !strings.Contains(string(failed), `"Error":"Item.Bad"`) {
		t.Fatalf("failed %s", failed)
	}
	if run.Items.ResultsWritten != 3 || run.Executions.ResultsWritten != 3 {
		t.Fatalf("written %+v", run.Items)
	}
	desc, _ := s.DescribeMapRun(context.Background(), &gen.DescribeMapRunInput{MapRunArn: &run.ARN})
	if *desc.ItemCounts.ResultsWritten != 3 {
		t.Fatalf("describe written %d", *desc.ItemCounts.ResultsWritten)
	}

	// A failed run still exports. MaxConcurrency 1 leaves the items after
	// the first failure pending.
	strict := `{"StartAt":"M","States":{"M":{"Type":"Map","MaxConcurrency":1,` + failingChild + `,
		"ResultWriter":{"Resource":"arn:aws:states:::s3:putObject","Parameters":{"Bucket":"out","Prefix":"strict"}},"End":true}}}`
	sx := runMachine(t, s, "strict-export", strict, "run", `[{"bad":true},{"bad":false},{"bad":false}]`)
	if sx.Status != store.ExecFailed || sx.Error != "States.ExceedToleratedFailureThreshold" {
		t.Fatalf("strict %s %s", sx.Status, sx.Error)
	}
	srun := s.Store.MapRuns(sx.ARN)[0]
	sdir := "/out/strict/" + srun.ARN[strings.LastIndex(srun.ARN, ":")+1:]
	pend, ok := m.get(sdir + "/PENDING_0.json")
	if _, hasFailed := m.get(sdir + "/FAILED_0.json"); !ok || !hasFailed || string(pend) != `[{"Input":"{\"bad\":false}","Status":"PENDING"},{"Input":"{\"bad\":false}","Status":"PENDING"}]` {
		t.Fatalf("pending %s", pend)
	}
	if _, ok := m.get(sdir + "/manifest.json"); !ok {
		t.Fatal("no manifest for the failed run")
	}

	compact := `{"StartAt":"M","States":{"M":{"Type":"Map",` + echoChild + `,
		"ResultWriter":{"WriterConfig":{"Transformation":"COMPACT","OutputType":"JSONL"},"Resource":"arn:aws:states:::s3:putObject","Parameters":{"Bucket":"out"}},"End":true}}}`
	cx := runMachine(t, s, "compact", compact, "run", `[{"a":1},{"a":2}]`)
	if cx.Status != store.ExecSucceeded {
		t.Fatalf("%s %s %s", cx.Status, cx.Error, cx.Cause)
	}
	crun := s.Store.MapRuns(cx.ARN)[0]
	lines, ok := m.get("/out/" + crun.ARN[strings.LastIndex(crun.ARN, ":")+1:] + "/SUCCEEDED_0.jsonl")
	if !ok || string(lines) != "{\"a\":1}\n{\"a\":2}\n" {
		t.Fatalf("jsonl %q", lines)
	}
}

func TestItemReaderValidation(t *testing.T) {
	s := newService()
	child := `"ItemProcessor":{"ProcessorConfig":{"Mode":"DISTRIBUTED","ExecutionType":"STANDARD"},"StartAt":"P","States":{"P":{"Type":"Pass","End":true}}}`
	for name, tc := range map[string]struct{ reader, want string }{
		"resource":  {`{"Resource":"arn:aws:states:::s3:headObject","Parameters":{}}`, "ItemReader Resource"},
		"no params": {`{"Resource":"arn:aws:states:::s3:getObject","ReaderConfig":{"InputType":"JSON"}}`, "needs Parameters"},
		"no type":   {`{"Resource":"arn:aws:states:::s3:getObject","Parameters":{"Bucket":"b","Key":"k"}}`, "InputType is required"},
		"csv":       {`{"Resource":"arn:aws:states:::s3:getObject","ReaderConfig":{"InputType":"CSV"},"Parameters":{"Bucket":"b","Key":"k"}}`, "CSVHeaderLocation"},
		"given":     {`{"Resource":"arn:aws:states:::s3:getObject","ReaderConfig":{"InputType":"CSV","CSVHeaderLocation":"GIVEN"},"Parameters":{"Bucket":"b","Key":"k"}}`, "CSVHeaders"},
		"flatten":   {`{"Resource":"arn:aws:states:::s3:listObjectsV2","ReaderConfig":{"Transformation":"LOAD_AND_FLATTEN"},"Parameters":{"Bucket":"b"}}`, "requires InputType"},
		"pointer":   {`{"Resource":"arn:aws:states:::s3:getObject","ReaderConfig":{"InputType":"CSV","CSVHeaderLocation":"FIRST_ROW","ItemsPointer":"/a"},"Parameters":{"Bucket":"b","Key":"k"}}`, "ItemsPointer requires"},
		"max":       {`{"Resource":"arn:aws:states:::s3:getObject","ReaderConfig":{"InputType":"JSON","MaxItems":-1},"Parameters":{"Bucket":"b","Key":"k"}}`, "MaxItems"},
	} {
		def := `{"StartAt":"M","States":{"M":{"Type":"Map","ItemReader":` + tc.reader + `,` + child + `,"End":true}}}`
		_, err := s.CreateStateMachine(context.Background(), &gen.CreateStateMachineInput{Name: sp(strings.ReplaceAll(name, " ", "-")), Definition: sp(def), RoleArn: sp(roleARN)})
		if api := mustAPI(t, err); api.ErrorName() != "InvalidDefinition" || !strings.Contains(api.Error(), tc.want) {
			t.Errorf("%s: %v", name, err)
		}
	}
}
