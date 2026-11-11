# AWS Step Functions spec snapshots

These files are the AWS-controlled specifications and the notes beside them. The local runner design is [docs/spec.md](../docs/spec.md). The sync job leaves that file alone.

The two snapshots AWS publishes, and this job replaces, are:

- `states-api/sfn-2016-11-23.json`, the Smithy model of the HTTP API, from api-models-aws.
- `asl/spec.html`, the Amazon States Language specification, from https://states-language.net/spec.html. The file is copied verbatim. The specification forbids modifying it.

GitHub Actions has no RSS subscription. `.github/workflows/sync-aws-spec.yml` polls the sources below once a day and on `workflow_dispatch`. When a snapshot changes, it rebuilds branch `aws-spec/sync` from the default branch and opens or updates one pull request. That branch belongs to the job. Merging the pull request is the review step. The job does not change interpreter behavior, and it does not turn a changelog row into an executable rule.

Run the same fetch locally:

```bash
python3 aws-spec/tools/sync_aws_spec.py
```

A second run with the same upstream bytes prints `no file changes`.

## Where AWS publishes changes

There is no machine-readable feed of Amazon States Language semantics. The useful public sources are these:

1. **Release notes RSS.** https://docs.aws.amazon.com/step-functions/latest/dg/recent-updates.rss
   The document history page points at it with `<meta name="rss" content="recent-updates.rss">`. The channel title is "Amazon Step Functions Release Notes". Items cover product changes and documentation updates. SDK integration batches show up here. Parsed items are stored in `release-notes.json`.

2. **Developer Guide document history.** https://docs.aws.amazon.com/step-functions/latest/dg/document-history.html
   This is the human changelog. Each row has a change label such as "New feature", "Updates", or "Documentation-only update". The markdown table is parsed into `document-history.json`. The canonical page stays on docs.aws.amazon.com.

3. **Recent feature launches.** https://docs.aws.amazon.com/step-functions/latest/dg/recent-launches.html
   Short dated list of launches with links to announcement posts. Parsed into `recent-launches.json`.

4. **SDK integration release history.** https://docs.aws.amazon.com/step-functions/latest/dg/awssdk-release-history.html
   Dates and service names for AWS SDK integrations, including services AWS has deprecated. Parsed into `sdk-integrations.json` as `added` and `deprecated`. A row that is a sentence rather than a service list is stored as `summary`. This list is integration coverage. It is not the States Language.

5. **Developer guide pages that define behavior.** State types, JSONata, variables, intrinsics, errors, quotas, TestState, redrive, activities, integration patterns, and the Step Functions Local pages. `page-watch.json` stores the URL, a one-line reason, and the SHA-256 of the page. The page text is not copied into this repo. When a hash changes, open the URL.

6. **States API model.** https://github.com/aws/api-models-aws/blob/main/models/sfn/service/2016-11-23/sfn-2016-11-23.json
   Smithy 2.0 model of the control-plane API (`AWSStepFunctions.*` over AWS JSON 1.0). It is Apache-2.0. `states-api/sfn-2016-11-23.json` is an exact snapshot. `states-api/operations.json` is a generated member and error list with documentation strings left out. This model is the HTTP API. It is not the workflow language inside a state machine definition.

7. **Amazon States Language.** https://states-language.net/spec.html
   `asl/spec.html` is that published specification, copied verbatim. It includes `QueryLanguage`, JSONata, and variables. The older file in `awslabs/states-language` can lag this page, so the job stores the published page and only hashes the repository file.

8. **Botocore witness.** https://github.com/boto/botocore/blob/develop/botocore/data/stepfunctions/2016-11-23/service-2.json
   The same API as published for the AWS SDK, also Apache-2.0. `witnesses.json` stores its hash so a client-model change is visible. The file itself is not vendored, because the Smithy snapshot is the copy this repo keeps.

9. **JSONata 2.0.6 language.** The developer guide says Step Functions implements JSONata 2.0.6 and links to the JSONata 2.0 documentation. `jsonata/2.0.6/docs/` is that language reference, taken from jsonata-js tag `v2.0.6` under the MIT license. See `jsonata/README.md` for the Step Functions differences (`$eval` removed, `$parse` and a few other functions added). The sync job does not refresh this pin. It does hash `transforming-data.md`, which is where AWS states the version.

The guide index at https://docs.aws.amazon.com/step-functions/latest/dg/llms.txt is how the watched page list was chosen. `sources.json` is the list the script fetches.

## Sources this job ignores

- https://status.aws.amazon.com/rss/all.rss reports outages. It does not describe the language or the API.
- https://aws.amazon.com/about-aws/whats-new/recent/feed/ is every AWS announcement. Adding `?tag=aws-step-functions` still returns the global feed, so it is not a Step Functions feed. Launch posts that matter are already linked from `recent-launches.json` and the release-notes RSS.

## Hints

`local_runtime_hint` is a keyword guess from this script. AWS does not publish it.

- `behavior` means the row mentions execution behavior, such as JSONata, variables, Map readers, errors, quotas, TestState, or redrive. Read it before changing a local interpreter.
- `docs` means the row looks like a documentation edit.
- `unclassified` means the keyword lists matched neither. IAM, console, metrics, and new SDK service names often land here. A new service integration still matters when the local runner grows past mocks.

## What a pull request means

A green sync that opens a pull request means AWS changed one of the watched sources. The diff is the spec change. Promoting a row into the local runner is a separate change, and it belongs in the design in `docs/spec.md` when that design should move.
