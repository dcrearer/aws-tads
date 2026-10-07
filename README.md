# aws-tads

A Rust implementation of the [BBC TADS](https://github.com/bbc/tads) (Time-Addressable Data
Store) API, v0.1, designed to run on AWS Lambda behind API Gateway.

TAMS stores media *essence* (video, audio, caption files); **TADS stores the timeline-linked
metadata about that content** — logging, transcripts, shot-changes, person identification, and
other machine- or human-generated data from AI/ML enrichment, live content analysis, data-stream
ingest, and analytics. Each Event is a JSON payload pinned to a point/range on a content
timeline, validated against the Track's managed Payload Schema.

## Architecture

Each top-level resource is its own Lambda (independent scaling + isolation), all sharing a set
of library crates. Event payloads are stored **inline** in DynamoDB and returned directly in
listings. TADS holds small metadata, so an Event larger than the configured limit is rejected
with `413` rather than offloaded — large data belongs in TAMS, not TADS.

A separable search index ("search by the data itself", fed asynchronously from DynamoDB Streams
into OpenSearch Serverless) is **designed and coded but not currently deployed** — the query
surface is undecided. See [Deferred: search](#deferred-search).

```
                       API Gateway HTTP API
   service   schemas   tracks   events   webhooks   delete-requests     (request Lambdas)
      └─────────┴─────────┴── tads-store (DynamoDB repos) ──┴─────────┘
                         │                     │
                     DynamoDB                  SQS ─▶ delete-worker
                         │
                    DynamoDB Streams ──▶ webhook-dispatcher ─▶ subscriber URLs
                         (└─▶ search-indexer ─▶ OpenSearch — deferred, not deployed)
```

### Workspace crates

| Crate | Role |
|-------|------|
| `tads-domain` | Pure models, the TAI `TimeRange` grammar, JSON-Schema payload validation. No I/O. |
| `tads-store` | DynamoDB repositories, opaque cursor pagination, Event size-limit enforcement, SQS enqueue. |
| `tads-telemetry` | `tracing` JSON logs + X-Ray trace-id capture + EMF metrics. |
| `tads-http` | Central `AppError` → `IntoResponse`, auth extractor, paging headers, config/state, observability middleware, Lambda bootstrap. |

### Lambdas (`lambdas/*`)

Request handlers: `service`, `schemas`, `tracks`, `events`, `webhooks`, `delete-requests`.
Async workers (event-driven): `delete-worker` (SQS), `webhook-dispatcher` (DynamoDB Streams).
`authorizer` is the HTTP API's custom Lambda authorizer (verifies the Cognito access token).
The `search-indexer` crate (also Streams-driven) is built but not deployed — see
[Deferred: search](#deferred-search). Each binary is named `bootstrap` for the Lambda custom runtime.

## Status

**Phase 1 (implemented):** schemas CRUD + list, tracks get/upsert/list + async delete, events
upsert (single/batch, per-event schema validation) + list + async delete, service endpoints,
cursor pagination with `X-Paging-*`/`Link` headers, central error handling, observability.

**Phase 2 (in progress):**
- **`delete-worker` — implemented.** SQS-triggered; advances delete requests
  `created→started→done|error`, deletes Events wholly within `timerange_to_delete` (eternity ⇒
  all Events on the track) via batched `BatchWriteItem`, deletes the Track if `delete_track`.
  Idempotent on SQS redelivery.
- **`webhooks` CRUD — implemented.** `POST/GET/PUT/DELETE /service/webhooks[/{id}]` with tag
  filters + pagination. `api_key_value` is write-only (accepted, stored out-of-band, never
  returned).
- **`webhook-dispatcher` — implemented.** DynamoDB-Streams-triggered; maps each change to a
  webhook event, matches subscription filters (`events`, `track_ids`, `schema_ids`,
  `editable_events`, `timeline_reference.entity_id`, `tags`), POSTs the callback to the
  subscriber `url` with the `api_key_name` header (reqwest), retries, and drives the webhook to
  `error` on persistent failure.
- **`search-indexer` — implemented, deployment deferred.** DynamoDB-Streams-triggered; maps
  INSERT/MODIFY→upsert and REMOVE→delete into per-resource OpenSearch indices (events enriched
  with `track_id`/`start_ns`/`end_ns` for range search), writing via a SigV4-signed OpenSearch
  (Serverless, `aoss`) client. Reads `TADS_OPENSEARCH_ENDPOINT`. **Not provisioned by the CDK
  stack** — see [Deferred: search](#deferred-search).
- **Granular property sub-resources — implemented.** `/tags`, `/tags/{name}`, `/description`,
  `/label` (and `/external_id` for schemas; `/read_only`, `/allow_overlaps`, `/editable_events`
  for tracks) GET/PUT/DELETE. Track mutations respect `read_only` (except the `read_only` toggle).

**Phase 2 is feature-complete.** Remaining: the **CDK infra stack** (deployment) and two noted
refinements — an event-id GSI so moving an Event's timerange doesn't orphan the old key, and the
`allow_overlaps=true` overlap-conflict check.

## Persistent store (DynamoDB)

One table per aggregate; all with Streams enabled (`NEW_AND_OLD_IMAGES`).

| Table | PK | SK | Notes |
|-------|----|----|-------|
| `tads-schemas` | `schema_id` | — | GSI `external_id-index`. |
| `tads-tracks` | `track_id` | — | GSI `schema_id-index`, `entity_id-index`. |
| `tads-events` | `track_id` | `<sortable(start_ns)>#<event_id>` | `start_ns`/`end_ns` numbers for timerange+overlap queries; Events over `max_payload_bytes` are rejected (413). |
| `tads-webhooks` | `webhook_id` | — | |
| `tads-delete-requests` | `request_id` | — | TTL on `expiry`. |

Timerange query returns every Event overlapping `[a, b)` via `sk < encode(b)` + `end_ns > a`.

## Configuration

`config/default.yaml` holds defaults; every value is overridable by env var
`TADS__<SECTION>__<KEY>` (double-underscore nesting), which is how each Lambda receives its
table/queue/bucket names at runtime. Example: `TADS__TABLES__EVENTS=prod-tads-events`.

## Observability

`tracing` structured JSON logs → CloudWatch Logs; each request runs in a span carrying method,
route, and the X-Ray trace id (`_X_AMZN_TRACE_ID`) for cross-Lambda correlation; EMF metrics →
CloudWatch Metrics. The OTLP → ADOT-layer → X-Ray *span export* is enabled when the ADOT Lambda
layer is provisioned (infra task); the trace-id correlation already works without it.

## Build, test, lint

Build/test/lint for the Rust workspace:

```sh
cargo build --release          # release profile is Lambda-tuned (size + cold start)
cargo test                     # unit tests + the events/webhooks end-to-end smoke tests
cargo clippy --all-targets -- -D warnings
cargo fmt --all --check
cargo audit                    # advisory check against Cargo.lock
```

AWS service mocking in tests uses `aws-smithy-mocks` at the SDK-client level (see
`lambdas/events/tests/smoke.rs`).

## Deployment

Infrastructure is defined as **AWS CDK (Python, uv-managed)** in [`cdk/`](cdk/). It provisions
the 5 DynamoDB tables (Streams on), the SQS queue + DLQ + shared Lambda async-DLQ, the 9 deployed
Lambdas (arm64 `provided.al2023`, X-Ray active tracing — 6 HTTP handlers, 2 workers, 1 authorizer),
an HTTP API Gateway behind a **custom Lambda authorizer** (verifies the Cognito access token), a
Cognito **resource server + hosted domain + machine-to-machine OAuth2 client** (`client_credentials`),
a customer-managed KMS CMK encrypting all data at rest, least-privilege IAM, and the Streams/SQS
event-source mappings. OpenSearch Serverless is **not** provisioned yet —
see [Deferred: search](#deferred-search).

```sh
# 1. Build the Lambda binaries (one `bootstrap` per function under target/lambda/<name>/)
cargo lambda build --release --arm64

# 2. Synthesize / test / deploy the stack
cd cdk
uv sync
uv run pytest                       # stack assertions
uv run cdk synth                    # requires the binaries from step 1
uv run cdk deploy                   # creates real AWS resources
```

### Calling the deployed API

Auth is **OAuth2 `client_credentials`** (machine-to-machine): a client exchanges its id/secret at
the Cognito token endpoint for a bearer **access token**, which the custom Lambda authorizer
verifies (JWKS signature + issuer + `client_id` + a `tads-api/*` scope). No interactive users are
involved. `cdk deploy` prints the client id/secret and token endpoint directly as outputs
(`UserPoolClientId`, `ClientSecret`, `TokenUrl`) — paste them into Insomnia, or use the ready-made
`GetTokenCommand` one-liner:

```sh
# All values below are printed by `cdk deploy` (ClientSecret / TokenUrl / UserPoolClientId / ApiEndpoint).
TOKEN=$(curl -s -X POST "<TokenUrl>" \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -u "<UserPoolClientId>:<ClientSecret>" \
  --data-urlencode grant_type=client_credentials \
  --data-urlencode 'scope=tads-api/read tads-api/write tads-api/delete' \
  | python3 -c 'import json,sys;print(json.load(sys.stdin)["access_token"])')

curl -H "Authorization: Bearer $TOKEN" "<ApiEndpoint>/service"   # -> 200 service.json
```

Relevant stack outputs: `ApiEndpoint`, `TokenUrl`, `UserPoolClientId`, `ClientSecret`,
`OAuthScopes`, `JwtIssuer`, `UserPoolId`, and the ready-to-run `GetTokenCommand`. Override the
Cognito domain prefix with `-c cognito_domain_prefix=<prefix>`. (The `ClientSecret` output is a dev
convenience — remove it for anything beyond a sandbox.)

An **Insomnia collection** covering every endpoint lives at
[`insomnia/TADS.insomnia.yaml`](insomnia/TADS.insomnia.yaml). Import it and set the environment's
`base_url`, `token_url`, `client_id`, and `client_secret` (from the `ApiEndpoint` / `TokenUrl` /
`UserPoolClientId` / `ClientSecret` outputs). Every request uses OAuth2 `client_credentials`, so
Insomnia fetches and refreshes the bearer token automatically.

### Building the Lambdas on macOS

Step 1 above (`cargo lambda build --release --arm64`) works on Linux (and in CI), but **fails on
macOS**: cross-compiling to Linux uses `zig` as the linker, which cannot link the native crypto
crates `aws-lc-sys`/`ring` pulled in by the AWS SDK (symptoms: `undefined symbol: aws_lc_*` /
`ring_core_*`, or `unsupported linker arg: --fix-cortex-a53-843419`). Build the Linux artifacts
natively inside a container instead:

```sh
# Replaces step 1 only; steps 2 (cd cdk; uv sync; …) are unchanged.
./scripts/build-lambdas.sh          # builds in the cargo-lambda container, extracts to target/lambda/<name>/
```

Requires a running container engine (`podman machine start`). The script compiles every bin in the
official `cargo-lambda` Linux image and copies each `bootstrap` to `target/lambda/<name>/` where CDK
reads it. Give the podman VM ≥8 GiB (`podman machine set --memory 8192`) — release+LTO builds
OOM-kill `rustc` on the 4 GiB default. On a Linux host, skip the script and run step 1 directly.

Each Lambda receives its table/queue/endpoint names as `TADS__*` env vars (the `config` crate's
env overrides), so no YAML is bundled into the images. To synth without building the binaries
(CI/validation), set `CDK_PLACEHOLDER=1`. An ADOT layer for OTLP→X-Ray span export is wired when
`-c adot_layer_arn=<arn>` is supplied.

## Deferred: search

The TADS "search by the data itself" pillar is designed as a **separable** OpenSearch Serverless
index fed from DynamoDB Streams by the `search-indexer` Lambda. The indexer and its index
projections are implemented in Rust, but the collection and the indexer Lambda are **not
provisioned by the CDK stack** — because the *query* side is undecided: TADS v0.1 defines no
search endpoint, and nothing in the API reads from the index yet. The open decision is whether
clients query through a new TADS API endpoint (Cognito-authed, OpenSearch stays private) or
directly against OpenSearch/Dashboards (IAM/SigV4, suits ops/analysts, not a browser UI).

Nothing is lost by deferring: DynamoDB remains the system-of-record and serves all key-based and
timerange reads. Re-enabling search is a CDK-only change — restore `_build_search`, the
`search-indexer` Lambda, and its Streams event-source wiring in `cdk/tads_infra/stack.py` (the
Streams are already enabled on every table), then pick and build the query surface.
