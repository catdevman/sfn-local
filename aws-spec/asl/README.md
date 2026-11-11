# Amazon States Language specification

`spec.html` is an unmodified copy of https://states-language.net/spec.html. The specification permits copying and distributing it, and it forbids modifying the copy. Notes about the file belong in this README.

The published spec includes the AWS product extensions that the older source file does not: `QueryLanguage`, JSONata, and state-machine variables. The source repository at https://github.com/awslabs/states-language/blob/master/spec/States.html was still the pre-JSONata text when this copy was taken. `.github/workflows/sync-aws-spec.yml` replaces `spec.html` when the published page changes, and it records the source file's hash in `aws-spec/witnesses.json`.

JSONata the expression language is not defined here. Step Functions names version 2.0.6, and that language reference is `aws-spec/jsonata/`. The local runner design is `docs/spec.md`. The sync job does not edit that file.
