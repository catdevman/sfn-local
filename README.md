# stepfunctions-local

Three specifications sit next to each other. They are not the same document.

| Spec | Path | Who controls it | How it updates |
| --- | --- | --- | --- |
| Local runner | [docs/spec.md](docs/spec.md) | This project | Edited here. The AWS sync does not touch it. |
| Step Functions API | [aws-spec/states-api/](aws-spec/states-api/) | AWS, via [api-models-aws](https://github.com/aws/api-models-aws/blob/main/models/sfn/service/2016-11-23/sfn-2016-11-23.json) | `.github/workflows/sync-aws-spec.yml` replaces the Smithy snapshot and regenerates `operations.json`. |
| Amazon States Language | [aws-spec/asl/spec.html](aws-spec/asl/spec.html) | AWS, via [states-language.net/spec.html](https://states-language.net/spec.html) | The same workflow replaces `spec.html` when the published page changes. The copy is kept verbatim. |

Adjacent material lives under `aws-spec/` as well:

- `aws-spec/jsonata/` is the JSONata 2.0.6 language reference. Step Functions names that version. The language is not part of the States Language spec, and the sync job does not move this pin. It does hash the AWS page that states the version.
- `aws-spec/page-watch.json` is the SHA-256 of the developer-guide pages for quotas, intrinsics, TestState, redrive, activities, and the other product behavior. Those pages are not copied.
- `aws-spec/release-notes.json`, `document-history.json`, `recent-launches.json`, and `sdk-integrations.json` are the changelog feeds the same workflow refreshes.

Run the fetch locally with `python3 aws-spec/tools/sync_aws_spec.py`. On GitHub it runs daily and opens a pull request when an AWS-controlled snapshot changes.
