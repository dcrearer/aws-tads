# aws-tads

A Rust implementation of the [BBC TADS](https://github.com/bbc/tads) (Time-Addressable Data
Store) API, v0.1, designed to run on AWS Lambda behind API Gateway.

TAMS stores media *essence* (video, audio, caption files); **TADS stores the timeline-linked
metadata about that content** — logging, transcripts, shot-changes, person identification, and
other machine- or human-generated data from AI/ML enrichment, live content analysis, data-stream
ingest, and analytics. Each Event is a JSON payload pinned to a point/range on a content
timeline, validated against the Track's managed Payload Schema.

## Architecture

Each top-level resource is its own Lambda (independent scaling + isolation), all sharing a set of
library crates. The system-of-record is **Aurora PostgreSQL Serverless v2** (reached through RDS
Proxy); Event payloads are stored inline in a `jsonb` column and returned directly in listings.
TADS holds small metadata, so an Event larger than the configured limit is rejected with `413`
rather than offloaded — large data belongs in TAMS, not TADS.

"Search by the data itself" is served **natively** by Postgres — timerange overlap via an
`int8range` GiST index (`tr && window`) and payload/tag/entity filtering via SQL/JSONB — so no
separate search system is needed. Change notifications (webhooks) ride a **transactional outbox**:
each mutation writes an outbox row in the same transaction and nudges a **poke** queue; a `relay`
drains the outbox and fans each change out to a **delivery** queue (one message per subscriber),
POSTed out-of-VPC with SQS retries + a DLQ — event-driven, no schedule (replacing the
DynamoDB-Streams design).

```
                        API Gateway HTTP API  ── Lambda authorizer (Cognito access token)
   service   schemas   tracks   events   webhooks   delete-requests     (request Lambdas)
      └─────────┴─────────┴── tads-store (sqlx/Postgres repos) ──┴────────┘
                         │                         │              │
              Aurora PG (via RDS Proxy)            SQS ─▶ delete-worker
                         │  (writes also append an `outbox` row, in-tx)
   outbox ─(poke)─▶ relay ─▶ delivery queue ─▶ delivery (out-of-VPC) ─▶ subscriber URLs
                                                     └─(retries exhausted)─▶ DLQ ─▶ dlq handler
```

Everything DB-touching runs inside an isolated VPC (no NAT) with interface endpoints; the
authorizer stays outside the VPC (it only needs Cognito JWKS over the internet).

### Workspace crates

| Crate | Role |
|-------|------|
| `tads-domain` | Pure models, the TAI `TimeRange` grammar, JSON-Schema payload validation. No I/O. |
| `tads-store` | sqlx/Postgres repositories, `TimeRange`↔`int8range` mapping, keyset cursor pagination, Event size-limit enforcement, the transactional outbox, SQS enqueue. |
| `tads-telemetry` | `tracing` JSON logs + X-Ray trace-id capture + EMF metrics. |
| `tads-http` | Central `AppError` → `IntoResponse`, auth extractor, paging headers, config/state (builds the Postgres pool from the Secrets Manager secret), observability middleware, Lambda bootstrap. |

### Lambdas (`lambdas/*`)

Request handlers: `service`, `schemas`, `tracks`, `events`, `webhooks`, `delete-requests`.
Async workers: `delete-worker` (SQS-triggered); and the webhook pipeline — `relay` (poke-triggered,
drains the outbox), `delivery` (delivery-queue-triggered, out-of-VPC POST), `dlq` (marks a webhook
`error` after retries exhaust). `authorizer` is the HTTP API's custom Lambda authorizer (verifies the
Cognito access token). `migrate` runs the sqlx migrations via a CloudFormation custom resource on deploy.
Each binary is named `bootstrap` for the Lambda custom runtime.

## Status

**Phase 1 (implemented):** schemas CRUD + list, tracks get/upsert/list + async delete, events
upsert (single/batch, per-event schema validation) + list + async delete, service endpoints,
cursor pagination with `X-Paging-*`/`Link` headers, central error handling, observability.

**Phase 2 (implemented):**
- **`delete-worker`.** SQS-triggered; advances delete requests `created→started→done|error`,
  deletes Events wholly within `timerange_to_delete` (`DELETE … WHERE tr <@ window`; eternity ⇒ all
  Events on the track), deletes the Track if `delete_track`. Idempotent on SQS redelivery.
- **`webhooks` CRUD.** `POST/GET/PUT/DELETE /service/webhooks[/{id}]` with tag filters +
  pagination. `api_key_value` is write-only (accepted, stored out-of-band, never returned).
- **Webhook delivery pipeline (poke-driven).** After a mutation commits, handlers nudge a poke
  queue; the `relay` claims undispatched outbox rows (`FOR UPDATE SKIP LOCKED`, marked dispatched
  only *after* enqueue), maps each to a webhook event, matches subscription filters (`events`,
  `track_ids`, `schema_ids`, `editable_events`, `timeline_reference.entity_id`, `tags`), and fans
  out one **delivery-queue** message per matching subscriber. The out-of-VPC `delivery` worker POSTs
  the callback with the `api_key_name` header (reqwest); SQS retries with backoff and routes
  exhausted messages to a DLQ, where the `dlq` handler drives that webhook to `error`. No schedule.
- **Granular property sub-resources.** `/tags`, `/tags/{name}`, `/description`, `/label` (and
  `/external_id` for schemas; `/read_only`, `/allow_overlaps`, `/editable_events` for tracks)
  GET/PUT/DELETE. Track mutations respect `read_only` (except the `read_only` toggle).

**Deployed and validated end-to-end on AWS.** The `allow_overlaps=false` conflict check (409 when a
track with overlapping Events disables overlaps) and the moved-timerange handling are both resolved —
the `track_event` primary key is `(track_id, event_id)`, so re-posting at a new timerange updates in
place.

## Persistent store (PostgreSQL)

Aurora PostgreSQL (via RDS Proxy). The authoritative column on every table is `doc jsonb` (the
serialized domain object); scalar/`int8range` columns are projected for keys, filters, and the
overlap index. Migrations live in [`crates/tads-store/migrations`](crates/tads-store/migrations).

| Table | Key | Notes |
|-------|-----|-------|
| `payload_schema` | `schema_id` | unique `external_id`; `tags` jsonb |
| `track` | `track_id` | `schema_id`/`entity_id` indexed; `tags` GIN-indexed |
| `track_event` | `(track_id, event_id)` | `tr int8range` with a **GiST** `(track_id, tr)` overlap index; `start_ns`/`end_ns`; Events over `max_payload_bytes` → 413 |
| `webhook` | `webhook_id` | write-only `api_key_value` column |
| `track_delete_request` | `request_id` | `expiry` swept by the delete-worker |
| `outbox` | `id` | change-capture rows drained by the webhook relay |

Timerange query returns every Event overlapping `[a, b)` with a single GiST index scan:
`WHERE track_id = $1 AND tr && int8range($lo, $hi, '[)')`. Listings keyset-paginate on
`(start_ns, event_id)` (events) or the id (other resources).

## Configuration

`config/default.yaml` holds defaults; every value is overridable by env var
`TADS__<SECTION>__<KEY>` (double-underscore nesting), which is how each Lambda receives its
runtime settings. The database block is set per-environment by the CDK stack:
`TADS__DATABASE__PROXY_ENDPOINT`, `TADS__DATABASE__DB_NAME`, `TADS__DATABASE__SECRET_ARN`
(the DB password is fetched from that Secrets Manager secret at cold start), plus
`TADS__DELETE_QUEUE_URL`.

## Observability

`tracing` structured JSON logs → CloudWatch Logs; each request runs in a span carrying method,
route, and the X-Ray trace id (`_X_AMZN_TRACE_ID`) for cross-Lambda correlation; EMF metrics →
CloudWatch Metrics. The OTLP → ADOT-layer → X-Ray *span export* is enabled when the ADOT Lambda
layer is provisioned (infra task); the trace-id correlation already works without it.

## Build, test, lint

Build/test/lint for the Rust workspace:

```sh
cargo build --release          # release profile is Lambda-tuned (size + cold start)
cargo test                     # unit tests always; DB smoke tests skip without TADS_TEST_DB
cargo clippy --all-targets -- -D warnings
cargo fmt --all --check
cargo audit                    # advisory check against Cargo.lock
```

Unit tests (TimeRange↔`int8range`, keyset cursor, outbox adapter) run anywhere. The end-to-end
smoke tests (`lambdas/{events,webhooks}/tests/smoke.rs`) run the real router against a Postgres and
are **gated on `TADS_TEST_DB`** (a Postgres connection string) — they skip when it is unset. Spin a
local one and point the tests at it:

```sh
podman run -d --name tads-pg -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=tads -p 55432:5432 postgres:16
TADS_TEST_DB="postgres://postgres:postgres@localhost:55432/tads" cargo test
```

## Deployment

Infrastructure is defined as **AWS CDK (Python, uv-managed)** in [`cdk/`](cdk/). It provisions an
**isolated VPC** (no NAT) with interface endpoints (Secrets Manager, SQS, KMS, STS); **Aurora
PostgreSQL Serverless v2** (CMK-encrypted) behind **RDS Proxy** (TLS); the SQS delete queue + DLQ,
the webhook poke/delivery queues + DLQs, and the shared Lambda async-DLQ; 12 Rust Lambdas (arm64
`provided.al2023`, X-Ray active tracing — 6 HTTP handlers, `delete-worker`, the webhook
`relay`/`delivery`/`dlq`, `authorizer`, `migrate`); an HTTP API Gateway behind the **custom Lambda
authorizer**; a Cognito **resource server + hosted domain + machine-to-machine OAuth2 client**
(`client_credentials`); a customer-managed KMS CMK; least-privilege IAM; SQS event-source mappings
wiring the webhook pipeline (no EventBridge schedule); and a custom resource that runs the DB
migrations. The DB-touching Lambdas run in the VPC; the authorizer and the webhook `delivery` worker
stay outside it (both need public egress and touch no DB). (Aurora **16.4 is not offered
in us-east-1** — the stack pins **16.15**.)

```sh
# 1. Build the Lambda binaries (see "Building the Lambdas on macOS" below for the container build)
cargo lambda build --release --arm64

# 2. Synthesize / test / deploy the stack
cd cdk
uv sync
uv run pytest                       # stack assertions
uv run cdk synth                    # requires the binaries from step 1
uv run cdk deploy                   # creates real AWS resources (Aurora + Proxy ~15-20 min)
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

Each Lambda receives its runtime settings as `TADS__*` env vars (the `config` crate's env
overrides), so no YAML is bundled into the images; the DB password is fetched from Secrets Manager
at cold start. To synth without building the binaries (CI/validation), set `CDK_PLACEHOLDER=1`. An
ADOT layer for OTLP→X-Ray span export is wired when `-c adot_layer_arn=<arn>` is supplied.
Override the Aurora idle floor by editing `serverless_v2_min_capacity` (set to 0 to auto-pause).

## Search

"Search by the data itself" is served natively by Postgres — no separate search system. Timerange
overlap is a single GiST index scan (`tr && window`); listings filter on `tags` (JSONB `@>` / `?`),
`timeline_reference.entity_id`, `schema_id`, and `label` directly in SQL. (An earlier design used a
separable OpenSearch index fed from DynamoDB Streams; it was retired when the store moved to
PostgreSQL, which covers the same pillar in one strongly-consistent store.)
