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
listings; OpenSearch (fed asynchronously from DynamoDB Streams) provides "search by the data
itself". TADS holds small metadata, so an Event larger than the configured limit is rejected
with `413` rather than offloaded — large data belongs in TAMS, not TADS.

```
                       API Gateway HTTP API
   service   schemas   tracks   events   webhooks   delete-requests     (request Lambdas)
      └─────────┴─────────┴── tads-store (DynamoDB repos) ──┴─────────┘
                         │                     │
                     DynamoDB                  SQS ─▶ delete-worker
                         │
                    DynamoDB Streams ─┬─▶ webhook-dispatcher ─▶ subscriber URLs
                                      └─▶ search-indexer ─▶ OpenSearch Serverless
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
Async workers (event-driven): `delete-worker` (SQS), `webhook-dispatcher` + `search-indexer`
(DynamoDB Streams). Each binary is named `bootstrap` for the Lambda custom runtime.

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
- **Remaining stubs (no-op):** search-indexer, and the granular property sub-resources
  (`/tracks/{id}/label`, `/schemas/{id}/tags/{name}`, …).

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

```sh
cargo build --release          # release profile is Lambda-tuned (size + cold start)
cargo test                     # unit tests + the events end-to-end smoke test
cargo clippy --all-targets -- -D warnings
cargo fmt --all --check
cargo audit                    # advisory check against Cargo.lock
```

AWS service mocking in tests uses `aws-smithy-mocks` at the SDK-client level (see
`lambdas/events/tests/smoke.rs`).

## Deployment

Infrastructure-as-Code is a separate task (not in this repo yet). The Lambdas read all resource
names from config/env, so no code change is needed when infra lands. Build each function with
[`cargo lambda`](https://cargo-lambda.info):

```sh
cargo lambda build --release --arm64
```

then wire API Gateway routes to each function, create the DynamoDB tables (Streams on), the SQS
queue, and the ADOT layer, and enable Lambda active tracing.
