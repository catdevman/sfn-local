# Examples

State machine definitions for this server. Each directory has a definition, one input, and the output that input produces. `go test ./examples` runs all eight and checks those outputs.

The small, medium, and large machines use Pass, Choice, and Fail. They change the document as it moves: `Parameters` replaces it, `ResultPath` adds a field, `InputPath` narrows what a state sees, and the last Pass publishes a smaller result. Change a field in `input.json` to take another branch.

Parallel, Map, JSONata, and Catch are the machines that use those pieces. Workflow runs them in one order, then waits zero seconds and succeeds. Each one still finishes without a worker or a service endpoint.

Money fields are integer cents. `States.MathAdd` adds two numbers, so a price is a base plus a flat addon.

Start the server with `go run ./cmd/sfnlocal --bind-address 127.0.0.1`. It ignores SigV4. The AWS CLI still looks up credentials before it sends the request. If `aws` is the 1Password plugin (`op plugin run -- aws`), that lookup waits for an approval and exits with `authorization timeout` before anything reaches port 8083. `command aws` skips the alias. Dummy keys satisfy the CLI:

```bash
export AWS_ACCESS_KEY_ID=local AWS_SECRET_ACCESS_KEY=local AWS_DEFAULT_REGION=us-east-1

command aws stepfunctions create-state-machine \
  --endpoint-url http://127.0.0.1:8083 \
  --name small \
  --definition file://examples/small/definition.json \
  --role-arn arn:aws:iam::123456789012:role/local

command aws stepfunctions start-execution \
  --endpoint-url http://127.0.0.1:8083 \
  --state-machine-arn arn:aws:states:us-east-1:123456789012:stateMachine:small \
  --name small-run \
  --input file://examples/small/input.json
```

The default account is `123456789012` and the default region is `us-east-1`. Open http://127.0.0.1:8083/dashboard to see the execution and its history.

## small

A flat order is nested, then a choice picks a lane.

The sample amount is 120, so the lane is `premium`. An amount of 100 or less with `priority: true` takes `rush`. Anything else takes `standard`. `AddReceipt` writes `receipt.label` onto that lane document. The currency field is dropped at the first Pass.

Output: `premium:A-100` on the receipt, lane `premium`.

## medium

An account, a seat count, and an addon list become an invoice.

The sample is a pro plan in `eu` with the `audit` addon, so the path is pro price, EU tax, and manual review. `NameCustomer` adds `$.customer` and leaves seats and addons in place. `Greet` reads only `$.customer`, then `ResultPath` puts the greeting back on the full document. `ShapeInvoice` publishes the flat invoice and drops the raw account.

A missing email fails with `MissingEmail`. `enterprise` uses a base of 50000. Any other plan uses the free price. `us` tax is 800 cents. No `audit` addon marks the invoice `auto`.

Output: amount 11500, tax 2000, review `review`, greeting `Hello, Northwind`.

## large

A fulfillment request is copied into `$.work`. Later choices fill coupon, tier, shipping, duty, gift wrap, hold, and channel fee. The last Pass drops the raw request and `$.work`.

The sample is a gold web order, coupon `SAVE10`, express shipping to CA, with a gift and no hold. Two cart rows keep it off the empty-cart failure. Fees are 1500 + 300 + 250 + 0 = 2050, which is over 2000, so the decision is `review` on the `floor` queue. A non-gold customer with `flags.hold: true` is held. `pickup` ships for 0. `FREESHIP` and `none` are the other coupons.

Output: `req-42 review via express`, fee 2050, discount 1000, first SKU `pen`.

## parallel

Two branches quote the same order. The ground branch chooses on country. The express branch adds 500 cents when `weight` is over 10. `ResultSelector` names the two branch results, and `ResultPath` keeps them on the original input. The sample weight is 5, so express has no surcharge.

Output: ground 700, express 1500, surcharge false.

## map

An inline Map chooses a lane for each cart line. A back-ordered line is `backorder` even when it is expensive. A line over 5000 cents is `bulky`. The rest are `pick`. The parent Pass publishes the line count and the lines.

Output: pen `pick`, desk `backorder`, book `pick`.

## jsonata

`QueryLanguage` is JSONata. `Normalize` builds name, score, and the sum of item quantities, and Assign stores the name and the unit count. `ChooseLane` uses a `Condition`. Score 95 with at least one unit takes `gold`. A lower score takes `standard`. The envelope reads the assigned name and units, not a field copied by hand.

Output: `ada / gold / 3`.

## catch

A Parallel checks stock. The sample has `inStock: false`, so one branch fails with `InventoryMissing`. The Catch writes that error onto `$.failure`, and `Explain` turns it into a stock response. `inStock: true` takes `BothOk` and returns status `reserved`.

Output: status `needs-stock`, detail `pen is unavailable`.

## workflow

One order uses the pieces above together. `Prepare` copies the order and Assign stores the order id. `Plan` is a Parallel: one branch chooses ground shipping from the country, and the other chooses rush, standard, or review. Rush is priority plus a channel matching `web*`. `Stamp` reads that assigned id through `States.Format` and writes the label back with `ResultPath`.

`Lines` is an inline Map at concurrency 2. A back-ordered line enters a Parallel whose second branch fails with `InventoryMissing`. Catch records the error on that line and the Map still finishes. A line over 5000 cents is `bulky`. A line with `gift: true` is `gift`. Anything else is `pick`.

`Choose` sends a non-priority order to `manual`. The sample is priority, shipping to CA, and rush, so the status is `ready`. `Publish` merges the ship quote and the service fee with `States.JsonMerge`, adds those cents, and `OutputPath` drops a scratch field. `Pause` waits zero seconds. `Done` is Succeed.

A country other than `CA` ships for 900 cents. Priority false, or channel `store`, selects standard and then `manual` because the Not rule is first. Any other channel with priority still set is `review`. Clear `backorder` on the desk and its 12000 cents takes `bulky`. Clear `gift` on the book and it is `pick`.

Output: label `order ord-7`, status `ready`, handling 1200, four lines. Desk is `backorder` with detail `desk is unavailable`. Lamp is `bulky`, book is `gift`, pen is `pick`.
