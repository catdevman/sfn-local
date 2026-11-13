# JSONata as Step Functions uses it

Amazon States Language names `QueryLanguage` and `{% %}` expressions. It does not define JSONata. The Step Functions developer guide does, on [Transforming data with JSONata](https://docs.aws.amazon.com/step-functions/latest/dg/transforming-data.html):

> Step Functions implements JSONata based on the 2.0.6 specification. All built-in JSONata functions and operators in the 2.0.6 specification are supported, with one exception: `$eval` is not available — use `$parse` instead.

That page links the language reference at https://docs.jsonata.org/2.0.0/overview. The `2.0.0` path is the documentation line for JSONata 2.0. The implementation AWS names is [jsonata-js](https://github.com/jsonata-js/jsonata) **2.0.6**, tag `v2.0.6`, commit `6f1fc886aa1e3e6bdbfffcdd80e63ede1d7d6b6b`, released 29 November 2024. The files under `2.0.6/docs/` are that tag's language docs. The license is MIT (`2.0.6/LICENSE`). `upstream.json` records the pin.

JSONata 2.1.0 (31 July 2025) added the `?:` and `??` operators. They are not in these 2.0.6 docs. A later jsonata-js release is a different language from the one this page names.

The Step Functions feature announcement is 22 November 2024, a week before the 2.0.6 tag. The guide as published now says 2.0.6. It does not say which build was embedded on the announcement day.

The local runtime is `github.com/recolabs/gnata` v0.6.2. This directory stays the language pin. It is not copied into the binary. gnata accepts the 2.1 operators `?:` and `??`. The validator rejects them so a definition stays inside the 2.0.6 language.

`aws-spec/page-watch.json` already hashes `transforming-data.md`. A change to the version sentence shows up there. This directory is the pinned language text. The sync job does not replace it.

## What Step Functions adds and removes

Expressions are strings that start with `{%` and end with `%}`, with no space before `{%` or after `%}`. `$states` is a reserved binding (`input`, `result`, `errorOutput`, `context`). Assign and Output are evaluated from the same pre-assignment snapshot. A type error, an undefined result, a one-second timeout, a memory limit, or a stack overflow is `States.QueryEvaluationError`.

`$eval` exists in the JSONata library and is rejected by Step Functions. The replacement that only parses a JSON string is `$parse`. The other functions Step Functions adds, which are not in the 2.0.6 library, are `$partition`, `$range`, `$hash`, `$random` with an optional seed, and `$uuid`. Integer-taking built-ins round a non-integer down. The embedding API in `embedding-extending.md` is the host API of jsonata-js. Step Functions does not expose it.
