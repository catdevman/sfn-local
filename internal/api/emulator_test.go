package api

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"os"
	osexec "os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/apigateway"
	agtypes "github.com/aws/aws-sdk-go-v2/service/apigateway/types"
	"github.com/aws/aws-sdk-go-v2/service/ec2"

	"github.com/catdevman/sfn-local/internal/sdk"
	"github.com/catdevman/sfn-local/internal/store"
)

// TestAWSEmulator runs integrations against the AWS Emulator
// (github.com/aws/aws-emulator) when it is installed: SFN_AWS_EMULATOR names
// the binary, or aws-emulator is on PATH. This server is configured with the
// environment the emulator's Getting Started page exports, read through
// sdk.LoadOutbound, with authentication on. The emulator is not AWS. It checks
// that the calls work end to end, not AWS's exact answers. The AWS recordings
// do that.
func TestAWSEmulator(t *testing.T) {
	bin := os.Getenv("SFN_AWS_EMULATOR")
	if bin == "" {
		bin, _ = osexec.LookPath("aws-emulator")
	}
	if bin == "" {
		t.Skip("SKIPPING AWS Emulator checks: install aws-emulator or set SFN_AWS_EMULATOR to run ECS, API Gateway, and aws-sdk:sfn against it")
	}
	env := startEmulator(t, bin)
	out, err := sdk.LoadOutbound(context.Background(), envOf(env))
	if err != nil {
		t.Fatal(err)
	}
	if out.Credentials == nil || !out.DisableHostPrefix || out.HTTPClient == nil {
		t.Fatalf("outbound %+v", out)
	}
	endpoint := env["AWS_ENDPOINT_URL"]
	newEmulatorService := func() *Service {
		s := newService()
		s.Runner.SDK = sdk.New(sdk.Config{
			Region:            "us-east-1",
			Endpoints:         map[string]string{"ecs": endpoint, "sfn": endpoint},
			Credentials:       out.Credentials,
			HTTPClient:        out.HTTPClient,
			DisableHostPrefix: out.DisableHostPrefix,
		})
		s.Runner.HTTPClient = out.HTTPClient
		s.Runner.APIGatewayEndpoint = endpoint
		s.Runner.ECSPoll = 500 * time.Millisecond
		return s
	}
	awsCfg := aws.Config{Region: "us-east-1", Credentials: out.Credentials, HTTPClient: out.HTTPClient, BaseEndpoint: aws.String(endpoint)}
	ctx := context.Background()

	t.Run("APIGatewayRoutesByHost", func(t *testing.T) {
		s := newEmulatorService()
		gw := apigateway.NewFromConfig(awsCfg)
		ids := map[string]string{}
		for _, name := range []string{"one", "two"} {
			ids[name] = mockRestAPI(t, gw, name)
		}
		for name, id := range ids {
			ex := runMachine(t, s, "emu-apigw-"+name, apiGatewayMachine(`{"ApiEndpoint":"`+id+`.execute-api.us-east-1.amazonaws.com","Method":"GET","Stage":"prod","Path":"bills"}`), "run", `{}`)
			if ex.Status != store.ExecSucceeded {
				t.Fatalf("%s: %s %s %s", name, ex.Status, ex.Error, ex.Cause)
			}
			var got struct {
				ResponseBody struct{ API string }
				StatusCode   int
				StatusText   string
			}
			if err := json.Unmarshal([]byte(ex.Output), &got); err != nil || got.ResponseBody.API != name || got.StatusCode != 200 {
				t.Fatalf("%s: %s", name, ex.Output)
			}
		}
		ex := runMachine(t, s, "emu-apigw-missing", apiGatewayMachine(`{"ApiEndpoint":"`+ids["one"]+`.execute-api.us-east-1.amazonaws.com","Method":"GET","Stage":"prod","Path":"nope"}`), "run", `{}`)
		if ex.Status != store.ExecFailed || !strings.HasPrefix(ex.Error, "ApiGateway.4") {
			t.Fatalf("missing route: %s %s %s", ex.Status, ex.Error, ex.Cause)
		}
	})

	t.Run("ECSRunTaskSync", func(t *testing.T) {
		s := newEmulatorService()
		subnet := emulatorSubnet(t, ec2.NewFromConfig(awsCfg))
		call := func(action, params string) string {
			t.Helper()
			res, err := s.Runner.SDK.Call(ctx, "ecs", action, []byte(params))
			if err != nil {
				t.Fatalf("%s: %v", action, err)
			}
			return res
		}
		call("createCluster", `{"ClusterName":"sfn"}`)
		td := call("registerTaskDefinition", `{"Family":"sfn-exit","RequiresCompatibilities":["FARGATE"],"NetworkMode":"awsvpc","Cpu":"256","Memory":"512",`+
			`"ContainerDefinitions":[{"Name":"app","Image":"public.ecr.aws/docker/library/busybox:latest","Essential":true,"Command":["sh","-c","exit 3"]}]}`)
		var reg struct {
			TaskDefinition struct{ TaskDefinitionArn string }
		}
		json.Unmarshal([]byte(td), &reg)
		params := `"Parameters":{"Cluster":"sfn","TaskDefinition":"` + reg.TaskDefinition.TaskDefinitionArn + `","LaunchType":"FARGATE",` +
			`"NetworkConfiguration":{"AwsvpcConfiguration":{"Subnets":["` + subnet + `"]}}}`
		def := func(extra string) string {
			return `{"StartAt":"T","States":{"T":{"Type":"Task","Resource":"arn:aws:states:::ecs:runTask.sync",` + params + extra + `,"End":true}}}`
		}

		// The task stops without a zero exit: it exits 3, or its image cannot
		// be pulled. Either way the .sync state fails with the stopped task.
		ex := runSlow(t, s, "emu-ecs-sync", def(""))
		if ex.Status != store.ExecFailed || ex.Error != "States.TaskFailed" || !strings.Contains(ex.Cause, `"LastStatus":"STOPPED"`) {
			t.Fatalf("%s %s %s %v", ex.Status, ex.Error, ex.Cause, typesOf(ex))
		}
		assertTypes(t, ex, "ExecutionStarted", "TaskStateEntered", "TaskScheduled", "TaskStarted", "TaskSubmitted", "TaskFailed", "ExecutionFailed")

		// A task that outlives TimeoutSeconds is stopped with Step Functions' reason.
		ex = runSlow(t, s, "emu-ecs-timeout", def(`,"TimeoutSeconds":1`))
		if ex.Status != store.ExecFailed || ex.Error != "States.Timeout" {
			t.Fatalf("%s %s %s", ex.Status, ex.Error, ex.Cause)
		}
		var submitted struct{ Tasks []struct{ TaskArn string } }
		json.Unmarshal([]byte(findEventMust(t, ex, "TaskSubmitted").Output), &submitted)
		if len(submitted.Tasks) != 1 {
			t.Fatalf("submitted %v", submitted)
		}
		desc := call("describeTasks", `{"Cluster":"sfn","Tasks":["`+submitted.Tasks[0].TaskArn+`"]}`)
		if !strings.Contains(desc, `"StoppedReason":"Task stopped by AWS Step Functions"`) {
			t.Fatalf("not stopped by the timeout: %s", desc)
		}
	})

	t.Run("SDKStartSyncExecutionKeepsHost", func(t *testing.T) {
		s := newEmulatorService()
		def := `{"StartAt":"Create","States":{` +
			`"Create":{"Type":"Task","Resource":"arn:aws:states:::aws-sdk:sfn:createStateMachine","Parameters":{"Name":"emu-express","Type":"EXPRESS",` +
			`"RoleArn":"arn:aws:iam::000000000000:role/sfn","Definition":"{\"StartAt\":\"P\",\"States\":{\"P\":{\"Type\":\"Pass\",\"Result\":{\"ok\":true},\"End\":true}}}"},` +
			`"ResultSelector":{"arn.$":"$.StateMachineArn"},"Next":"Start"},` +
			`"Start":{"Type":"Task","Resource":"arn:aws:states:::aws-sdk:sfn:startSyncExecution","Parameters":{"StateMachineArn.$":"$.arn"},` +
			`"ResultSelector":{"status.$":"$.Status","output.$":"States.StringToJson($.Output)"},"End":true}}}`
		ex := runMachine(t, s, "emu-sync", def, "run", `{}`)
		if ex.Status != store.ExecSucceeded {
			t.Fatalf("%s %s %s", ex.Status, ex.Error, ex.Cause)
		}
		jsonEqual(t, ex.Output, `{"status":"SUCCEEDED","output":{"ok":true}}`)
	})
}

// startEmulator runs the emulator on a free port with authentication on and
// telemetry off, and returns the environment its Getting Started page exports.
func startEmulator(t *testing.T, bin string) map[string]string {
	t.Helper()
	home := t.TempDir()
	cmd := osexec.Command(bin, "-p", "0", "--telemetry", "off", "--idle-timeout", "900")
	cmd.Env = append(os.Environ(), "HOME="+home, "EMULATOR_TELEMETRY=off")
	pr, pw := io.Pipe()
	cmd.Stdout, cmd.Stderr = pw, pw
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cmd.Process.Signal(os.Interrupt)
		done := make(chan struct{})
		go func() { cmd.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			cmd.Process.Kill()
		}
		pw.Close()
	})
	type info struct {
		Fields struct {
			Message, EndpointURL, ConfigFile, CAFile string
		} `json:"fields"`
	}
	found := make(chan info, 1)
	var once sync.Once
	go func() {
		sc := bufio.NewScanner(pr)
		for sc.Scan() {
			var line struct {
				Fields map[string]string `json:"fields"`
			}
			if json.Unmarshal(sc.Bytes(), &line) != nil || line.Fields["message"] != "emulator server information" {
				continue
			}
			var i info
			i.Fields.EndpointURL, i.Fields.ConfigFile, i.Fields.CAFile = line.Fields["endpoint_url"], line.Fields["config_file"], line.Fields["ca_file"]
			once.Do(func() { found <- i })
		}
	}()
	var i info
	select {
	case i = <-found:
	case <-time.After(60 * time.Second):
		t.Fatal("aws-emulator did not report its endpoint")
	}
	dir := filepath.Dir(i.Fields.ConfigFile)
	return map[string]string{
		"AWS_ENDPOINT_URL":                  i.Fields.EndpointURL,
		"AWS_SHARED_CREDENTIALS_FILE":       filepath.Join(dir, "emulator_credentials"),
		"AWS_CONFIG_FILE":                   i.Fields.ConfigFile,
		"AWS_CA_BUNDLE":                     i.Fields.CAFile,
		"AWS_EC2_METADATA_DISABLED":         "true",
		"AWS_DISABLE_HOST_PREFIX_INJECTION": "true",
		"AWS_REGION":                        "us-east-1",
		"AWS_DEFAULT_REGION":                "us-east-1",
	}
}

// runSlow is runMachine with a wait long enough for an emulated ECS task.
func runSlow(t *testing.T, s *Service, name, def string) store.Execution {
	t.Helper()
	sm := createMachine(t, s, name, def, roleARN)
	out := startExec(t, s, *sm.StateMachineArn, "run", `{}`)
	select {
	case <-s.Store.Finished(*out.ExecutionArn):
	case <-time.After(90 * time.Second):
		t.Fatal("execution did not finish")
	}
	ex, _ := s.Store.Execution(*out.ExecutionArn)
	return ex
}

func envOf(m map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) {
		v, ok := m[k]
		return v, ok
	}
}

// mockRestAPI deploys a REST API whose GET /bills on stage prod returns
// {"api":"<name>"}, and returns its id.
func mockRestAPI(t *testing.T, gw *apigateway.Client, name string) string {
	t.Helper()
	ctx := context.Background()
	api, err := gw.CreateRestApi(ctx, &apigateway.CreateRestApiInput{Name: aws.String("sfn-" + name)})
	if err != nil {
		t.Fatal(err)
	}
	res, err := gw.GetResources(ctx, &apigateway.GetResourcesInput{RestApiId: api.Id})
	if err != nil || len(res.Items) == 0 {
		t.Fatal(err)
	}
	child, err := gw.CreateResource(ctx, &apigateway.CreateResourceInput{RestApiId: api.Id, ParentId: res.Items[0].Id, PathPart: aws.String("bills")})
	if err != nil {
		t.Fatal(err)
	}
	get := aws.String("GET")
	steps := []func() error{
		func() error {
			_, err := gw.PutMethod(ctx, &apigateway.PutMethodInput{RestApiId: api.Id, ResourceId: child.Id, HttpMethod: get, AuthorizationType: aws.String("NONE")})
			return err
		},
		func() error {
			_, err := gw.PutIntegration(ctx, &apigateway.PutIntegrationInput{RestApiId: api.Id, ResourceId: child.Id, HttpMethod: get, Type: agtypes.IntegrationTypeMock,
				RequestTemplates: map[string]string{"application/json": `{"statusCode": 200}`}})
			return err
		},
		func() error {
			_, err := gw.PutMethodResponse(ctx, &apigateway.PutMethodResponseInput{RestApiId: api.Id, ResourceId: child.Id, HttpMethod: get, StatusCode: aws.String("200")})
			return err
		},
		func() error {
			_, err := gw.PutIntegrationResponse(ctx, &apigateway.PutIntegrationResponseInput{RestApiId: api.Id, ResourceId: child.Id, HttpMethod: get, StatusCode: aws.String("200"),
				ResponseTemplates: map[string]string{"application/json": `{"api":"` + name + `"}`}})
			return err
		},
		func() error {
			_, err := gw.CreateDeployment(ctx, &apigateway.CreateDeploymentInput{RestApiId: api.Id, StageName: aws.String("prod")})
			return err
		},
	}
	for _, step := range steps {
		if err := step(); err != nil {
			t.Fatal(err)
		}
	}
	return *api.Id
}

// emulatorSubnet returns a subnet the emulator's ECS accepts.
func emulatorSubnet(t *testing.T, c *ec2.Client) string {
	t.Helper()
	ctx := context.Background()
	out, err := c.DescribeSubnets(ctx, &ec2.DescribeSubnetsInput{})
	if err == nil && len(out.Subnets) > 0 {
		return *out.Subnets[0].SubnetId
	}
	vpc, err := c.CreateVpc(ctx, &ec2.CreateVpcInput{CidrBlock: aws.String("10.0.0.0/16")})
	if err != nil {
		t.Fatal(err)
	}
	sn, err := c.CreateSubnet(ctx, &ec2.CreateSubnetInput{VpcId: vpc.Vpc.VpcId, CidrBlock: aws.String("10.0.1.0/24")})
	if err != nil {
		t.Fatal(err)
	}
	return *sn.Subnet.SubnetId
}
