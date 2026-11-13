# stepfunctions-local

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).

A tag `v*` publishes `sfnlocal` for Linux, macOS, and Windows on amd64 and arm64. The archive is `sfnlocal_<version>_<os>_<arch>.tar.gz` (`.zip` on Windows). `sfnlocal_<version>_checksums.txt` lists the SHA-256 sums. The same release pushes a Homebrew formula named `sfnlocal` to [catdevman/homebrew-tools](https://github.com/catdevman/homebrew-tools):

```
brew install catdevman/tools/sfnlocal
```

The local server is the roadmap in [ROADMAP.md](ROADMAP.md). How a state runs is [docs/spec.md](docs/spec.md). Phase G speaks AWS JSON 1.0. Phase 0 is the control plane. Phase 0b checks create-time validator messages. Phase 1 runs Pass, Succeed, and Fail. Phase 2 runs JSONPath data flow, Choice, and Wait. Phase 3 runs mocked tasks, direct Lambda invoke, Retry, and Catch. Phase 4 runs Parallel, inline Map, and the eighteen intrinsic functions. Phase V validates definitions and serves state machine versions and aliases. `TestState` runs one state. Activities and `.waitForTaskToken` callbacks run. A timed-out `SendTask*` reports `Task Timed Out: '<resource>'`. JSONata and JSONPath `Assign` run. `RedriveExecution` resumes a failed Standard execution. Optimized integrations run when their endpoint is set, including nested `.sync`. `aws-sdk:` tasks run through the AWS SDK for Go v2 when `AWS_ENDPOINT_URL` or `AWS_ENDPOINT_URL_<SERVICE>` is set. S3, DynamoDB, SQS, SNS, and EventBridge run, and the DynamoDB, SQS, SNS, and EventBridge optimized integrations use the same clients. [internal/invoke/operations.toml](internal/invoke/operations.toml) lists which operations run. Customer-managed KMS and tracing stay deferred. Distributed Map runs each item or batch as a child execution, with the Map Run API. `ItemReader` and `ResultWriter` export to S3 do not run yet.

Listen on `0.0.0.0:8083`. Point the AWS CLI at `--endpoint-url http://127.0.0.1:8083`. SigV4 is not checked. The store is in memory. Bind `127.0.0.1` unless the port is published on purpose, and do not expose the port beyond the host or the compose network. Open http://127.0.0.1:8083/dashboard for a console of state machines, executions, and history. API calls stay POST.

The default error profile is the public Smithy shape id. `__type` and `X-Amzn-Errortype` are `com.amazonaws.sfn#<ShapeName>`. `--compat=sfn-local` selects the Step Functions Local profile. `MockConfigFile.json` and the `#TestCase` suffix on an execution name stay compatible with Step Functions Local and LocalStack. The process reads `SFN_MOCK_CONFIG` when a StartExecution state machine ARN contains one `#` test suffix.

Step Functions Local 2.0.0 constructs API Gateway and EventBridge endpoint options and then leaves them off the list `parseArgs` walks, so `-apiGatewayEndpoint`, `--api-gateway-endpoint`, `API_GATEWAY_ENDPOINT`, and the EventBridge equivalents do nothing in that jar. This server accepts them.

Regenerate the Go surface with `go generate ./...`.

Three specifications sit next to each other. They are not the same document.

| Spec | Path | Who controls it | How it updates |
| --- | --- | --- | --- |
| Local runner | [docs/spec.md](docs/spec.md) | This project | Edited here. The AWS sync does not touch it. |
| Step Functions API | [aws-spec/states-api/](aws-spec/states-api/) | AWS, via [api-models-aws](https://github.com/aws/api-models-aws/blob/main/models/sfn/service/2016-11-23/sfn-2016-11-23.json) | `.github/workflows/sync-aws-spec.yml` replaces the Smithy snapshot and regenerates `operations.json`. |
| Amazon States Language | [aws-spec/asl/spec.html](aws-spec/asl/spec.html) | AWS, via [states-language.net/spec.html](https://states-language.net/spec.html) | The same workflow replaces `spec.html` when the published page changes. The copy is kept verbatim. |

Adjacent material lives under `aws-spec/` as well:

- `aws-spec/jsonata/` is the JSONata 2.0.6 language reference. The local runtime is `github.com/recolabs/gnata` v0.6.2. Step Functions names 2.0.6. The language is not part of the States Language spec, and the sync job does not move this pin. It does hash the AWS page that states the version.
- `aws-spec/page-watch.json` is the SHA-256 of the developer-guide pages for quotas, intrinsics, TestState, redrive, activities, and the other product behavior. Those pages are not copied.
- `aws-spec/release-notes.json`, `document-history.json`, `recent-launches.json`, and `sdk-integrations.json` are the changelog feeds the same workflow refreshes.

Run the fetch locally with `python3 aws-spec/tools/sync_aws_spec.py`, then `go generate ./...`. On GitHub the same workflow runs daily. After the fetch it regenerates Go and opens a pull request when a snapshot changes. The bot may commit `aws-spec/**`, `internal/api/gen/**`, and `internal/asl/specgen/**`. It refuses every other path, including handlers, `docs/spec.md`, `surface.toml`, and `catalog.toml`. The pull request body lists new operations, members, ASL fields, and error names.
