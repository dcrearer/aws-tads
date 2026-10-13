# Video-Understanding Inference — Implementation Plan

**A Rust TAMS → Video-Understanding → TADS bridge.** Consumes TAMS media (event-driven),
runs model inference, and writes typed, schema-validated, time-addressable **data** to TADS.

This is the **video-understanding (`vu-*`) side of the `aws-tads` workspace** — the write path that
produces annotations, living alongside the TADS store (`tads-*`) it writes to. (Formerly the separate
`aws-tads-inference` repo; folded in so the `vu-*` crates share the workspace and reuse `tads-domain`.)

- Design diagram: [`tads-inference.png`](tads-inference.png)
- Payload-schema contracts: [`inference-schemas.md`](inference-schemas.md)
- Store it writes to: the `tads-*` crates/lambdas in this same workspace (TADS HTTP API).

> Reference prior art (read-only, do not depend on): `/Users/crearerd/Dev/aws/tams-inference`
> (Rust HLS→TAMS ingest + Python video-understanding) and `tams-live-inference` (Nova/Whisper +
> React UI). This project is the clean, all-Rust successor to their analysis-write path.

---

## 1. Goal & scope

**Goal:** a deterministic, near-live pipeline that turns TAMS media into TADS Track Events —
transcripts and key moments — each carrying an absolute timerange, confidence, and model
provenance, validated against a managed TADS Payload Schema.

> **Scene-change is out of scope** (dropped 2026-10-13): shot-boundary detection is a
> frame-differential signal — a cut is defined by comparing a frame to its predecessor, so a cut on
> a segment boundary is undetectable without the previous segment's trailing frames (a look-back /
> overlap window). Per-segment analysis with no overlap can't do it correctly. Transcript and
> key-moment are *semantic* units that degrade gracefully across boundaries (buffering + idempotent
> ids), so they stay. Revisit scene-change only with an overlap-window design.

**In scope (this repo):**
- **Phase A** — resolve TADS schemas by `external_id`; create/ensure the TADS Track(s) — **one per
  analysis type** — per TAMS Source.
- **Phase B** — per-segment loop: TAMS `flows/segments_added` webhook → fetch media → analyse →
  `POST` Track Events (idempotent, schema-validated).

**Out of scope (separate project, deferred):**
- **Phase C** — the agentic query/UX consumer. It reads *from* TADS (query + `tracks/events_added`
  webhooks), shares no runtime with this repo, and may be built in **Python + Strands** later. The
  only shared contract is the `vu:*:v1` schemas. See §12.

**Non-goals:** re-implementing schema storage or validation (TADS owns both); building a TAMS;
writeback-to-TAMS (a Phase-C concern); any UI.

---

## 2. Architecture

```
                         EventBridge / SQS (orchestration, retries)
TAMS ──webhook──▶  Webhook Receiver λ ──▶ queue ──▶  Analyzer λ ──┬─▶ Bedrock (Pegasus / Nova)
 (flows/created,         (verify, enqueue)                        ├─▶ SageMaker (Whisper)  [optional]
  flows/segments_added)                                           ├─▶ ffmpeg (frame extract / probe)
                                                                  └─▶ TADS  POST /tracks/{id}/events
Storage (S3) ◀── GET media object (presigned get_urls) ───────────┘        (schema-validated, idempotent)
```

- **Deterministic pipeline, not an agent** (see §4). Each analysis = one Bedrock/SageMaker call with
  **structured output**; control flow lives in code/Step Functions/SQS, never in a model.
- **TADS is the sink and the boundary.** This repo only ever writes via the TADS HTTP API.

AWS topology (mirrors `aws-tads` where sensible): Rust Lambdas (`provided.al2023`, arm64); SQS work
queue + DLQ; Secrets Manager for TAMS/TADS client credentials; KMS CMK; CloudWatch structured logs.
VPC only if a SageMaker endpoint or private TADS requires it — default is public egress to Bedrock,
TAMS, TADS (no NAT needed for the Bedrock/HTTP path).

---

## 2a. Live processing flow (per segment)

Live media is already ingested into TAMS (e.g. an HLS→TAMS feed) and chopped into **flow segments**;
TAMS fires `flows/segments_added` as each lands. **That webhook is the clock — one segment = one unit
of work.** Phase A (ensure the per-type Tracks) has already run on `flows/created`.

```
segment lands in TAMS
   │  flows/segments_added  { flow_id, source_id, segment: { timerange, get_urls } }
   ▼
webhook-receiver λ ── verify ──▶ SQS ──▶ analyzer λ:
   1. read segment_start (TAI) + duration from the segment timerange   ← authoritative absolute clock (§4.3)
   2. presigned GET the segment object (media bytes)
   3. ffmpeg/ffprobe demux ONCE → audio.pcm (16k mono) + frames/*.jpg (sampled)
   4. run the analyses enabled for this flow (routing §13), each emitting RELATIVE seconds
   5. map relative → absolute:  abs = segment_start + relative   (vu-time)
   6. build typed payloads; assign event_id = uuid5(track, segment_anchor, unit_ordinal)
   7. POST events to each type's Track (batch array); inspect per-event 201/400
```

Step 3 decodes each segment **once** and fans the artifacts out to all enabled analyses — ffmpeg cost
is per segment, not per type.

**Per analysis type** (each writes to its own Track / `schema_id`):

| Type | Input artifact | Producer | Payload (`vu:*:v1`) | Timerange |
|---|---|---|---|---|
| **transcript** | `audio.pcm` | Whisper (SageMaker endpoint or in-process `whisper-rs`) | text, speaker?, confidence | span per utterance `[start_end)` |
| **key-moment** | sampled frames (+ audio ctx) | Bedrock video-understanding (Pegasus/Nova), structured output | title, category, confidence | span `[start_end)` |

- **transcript** emits one event per utterance; overlapping speakers are why Tracks are `allow_overlaps = true`.
- **key-moment** is the simplest payload → wired end-to-end first (§11.4). It is a *semantic* judgment,
  so it runs per-segment without a frame-accurate look-back; boundary-spanning moments are handled by
  the cross-segment buffer, and an optional soft-context prefix (previous segment summary) can be fed
  into the prompt if boundary detection quality needs it.

**Three mechanics that keep live output correct:**
1. **Absolute time comes from TAMS, not the file.** Models emit "3.2s into this clip"; the segment's TAMS
   `timerange` gives the true TAI start. `vu-time` does `abs = segment_start + relative` (§4.3).
2. **Cross-segment buffering (§4.8).** A unit (sentence / moment) that *starts* in segment S but completes
   in S+1 is anchored to S, buffered, and flushed once complete with its full span — which is also why
   `unit_ordinal` anchors on the start segment (stable identity regardless of which invocation finishes it).
3. **Idempotent on redelivery / reprocess (§4.4).** Same (track, segment_anchor, unit_ordinal) → same id →
   upsert; count-changing reanalysis uses `DELETE …/events?ids=…` then re-POST.

**Near-live latency:** per-segment lag ≈ fetch + decode + inference + POST, dominated by inference. Tune
segment size, `analyzer` reserved concurrency (also the backpressure bound), and provisioned concurrency
if cold starts dominate. The transcript buffer adds up to ~one segment of latency for boundary-spanning
sentences (expected).

---

## 3. Workspace layout

Add the `vu-*` crates and the two inference lambdas as **new members of the existing `aws-tads`
Cargo workspace** (resolver 3, `edition = "2024"`, shared `[workspace.dependencies]`). No new
workspace — this is an extension of the one already hosting the `tads-*` store crates.

```
aws-tads/                        # existing workspace (store + inference share it)
├── Cargo.toml                   # existing [workspace]; ADD the vu-* + lambda members below
├── config/                      # add inference keys (model ids, schema external_ids, endpoints)
├── crates/
│   ├── tads-domain/            # EXISTING store domain — vu-* reuses its TimeRange/Timestamp
│   ├── tads-store/  tads-http/  tads-telemetry/   # EXISTING store crates
│   ├── vu-domain/     NEW       # analysis types + payload builders (depends on tads-domain for time)
│   ├── vu-time/       NEW       # segment→absolute timerange mapping (thin over tads-domain time)
│   ├── vu-tads/       NEW       # TADS HTTP client: schema resolve+cache, PUT track, POST/DELETE events
│   ├── vu-tams/       NEW       # TAMS client: webhook verify, GET flow/segments, fetch media object
│   ├── vu-infer/      NEW       # Bedrock/SageMaker invokers + structured-output → payload mapping
│   └── vu-media/      NEW       # ffmpeg/ffprobe wrapper (shell-out to bundled static binary)
│   # vu-telemetry dropped — reuse the existing tads-telemetry crate directly
├── lambdas/
│   ├── service/ schemas/ tracks/ events/ webhooks/ …   # EXISTING store lambdas
│   ├── webhook-receiver/  NEW   # axum/lambda_http: verify TAMS webhook → enqueue work
│   └── analyzer/          NEW   # SQS-triggered: fetch → analyse → POST events
├── scripts/build-lambdas.sh     # EXISTING podman arm64 cross-compile (extend for ffmpeg bundling)
├── cdk/                         # EXISTING CDK Python/uv infra (add SQS/DLQ, the 2 lambdas, Bedrock IAM)
└── docs/                        # inference-plan.md, tads-inference.png, inference-schemas.md
```

Rationale: the `vu-*` libs stay pure and unit-testable (`thiserror`); the two lambda bins are thin
`anyhow` applications wiring them together. Each lambda bin keeps a unique `[[bin]]` name so
cargo-lambda emits distinct `target/lambda/<name>/` assets. Folding into the store workspace means
one `Cargo.lock`, one release profile, one build/CI pipeline, and direct reuse of `tads-domain` and
`tads-telemetry` instead of copies.

**Reuse `tads-domain`'s `TimeRange`/`Timestamp` directly** (now that both live in one workspace) — the
TAI-ns timerange logic is identical, so `vu-time`/`vu-domain` depend on `tads-domain` rather than
re-implementing it. `vu-time` becomes a thin segment→absolute mapping layer over those types. The
earlier copy-vs-extract question is moot: it's a shared in-workspace dependency.

---

## 4. Key design decisions (locked)

1. **Deterministic pipeline.** No LLM-driven control flow. One structured-output inference call per
   analysis. Reliability, bounded cost, low latency, idempotency over agentic flexibility.
2. **Schemas are TADS's; VU looks them up.** At startup resolve each `vu:*:v1` by `external_id`
   (`GET /schemas?external_id=…`), cache the `schema_id`. **Startup self-check: fail fast if any is
   missing** (schemas are seeded into TADS out of band — not created here).
3. **Absolute timerange from segment metadata.** Models emit *relative* seconds;
   `absolute = segment_start + relative`. The `vu-time` crate owns this mapping, round-trip tested.
   Misalignment here = data that doesn't match the media.
4. **Idempotent events — keyed on logical unit, never on timerange.** Derive
   `event_id = uuid5(NS_VU, "{track_id}:{segment_anchor}:{unit_ordinal}")`, where `segment_anchor`
   is the **immutable TAMS start timestamp** (TAI-ns) of the segment the unit *begins* in and
   `unit_ordinal` is the model's deterministic 0-based index of that unit within the segment. TADS
   treats a supplied `id` as upsert (PUT-with-id = replace), so redelivery **and timerange
   refinements** replace in place instead of orphaning a duplicate. The timerange is never part of
   the id. For genuine reanalysis that changes the unit *count* for a segment (e.g. a new model), do
   a **segment-scoped delete+recreate** — `DELETE /tracks/{id}/events?ids=…` for the deterministic
   id set you're about to rewrite (prefer `ids` over the `timerange` filter: its "wholly overlap"
   semantics would miss a buffered unit that starts in-segment but spills past it, and could
   over-delete a neighbour), then POST the fresh set. Relies on `editable_events`/deletes (TADS
   default `true`); handle the `202 Accepted` async-delete `Location` response. See §13.
5. **One Track per (Source × analysis-type); Track references the whole Source.** A TADS Track
   carries a single `schema_id` and validates every Event against it, so each analysis type gets its
   own Track. Derive a deterministic, client-supplied `track_id = uuid5(NS_VU, "{source_id}:{type}")`
   so `PUT /tracks/{track_id}` is itself idempotent. Each Track sets
   `timeline_reference.entity_id = tams://…/sources/{id}` (never a `?timerange=` variant — exact
   string match, TADS rejects it 400), the resolved `schema_id`, and `allow_overlaps = true`.
   Rendition-independent (analyse the proxy, map onto the co-timed broadcast via shared `source_id`).
   Phase A therefore ensures **N Tracks** per Source — one per analysis type the flow routes to (§13).
6. **`allow_overlaps = true`** on VU Tracks (actor-visibility / multi-speaker overlap). A
   non-overlapping snapshot would only be needed for a future TAMS writeback.
7. **Validation stays in TADS.** VU maps model output into the payload shape and POSTs; it does **not**
   re-validate (TADS returns 201 / per-event 400). A local self-check against the schema is optional
   belt-and-suspenders only.
8. **Batch events.** Buffer across segments where an item spans segments (sentence/topic), then POST a
   batch — fewer round-trips, correct spans.
9. **Deterministic decoding (locked).** Every inference call runs at temperature 0 with forced
   structured output. This is not just a quality knob: `unit_ordinal` idempotency (§4.4) assumes the
   model returns the *same ordered list* for the same input, so non-deterministic decoding would
   re-key events on reprocess.

---

## 4a. Inference prompt contract

The prompt is the contract that makes model output map onto a typed payload and pass the TADS gate.
Owned by `vu-infer` (`key_moment_prompt` + `key_moment_tool_schema`). It must encode:

- **Task + taxonomy** — what a key moment is, constrained to the exact `vu:key-moment:v1` enum
  (`notable_event | speaker_change | highlight | scene_cut | text_on_screen`). A free-text category
  fails validation.
- **Structured output, not prose** — Bedrock Converse with **forced tool-use** (`record_key_moments`,
  `tool_choice = that tool`); the tool's input schema is the response contract. The returned tool
  input deserializes straight into the typed payload.
- **Relative timing** — the model emits `start`/`end` in **seconds from the start of this clip**
  (`0..segment_duration`); `vu-time` maps them to absolute. Timing is split off to the TADS
  `timerange`; the rest becomes `payload`. `provenance` is stamped by `vu-infer`, not the model.
- **Constraints** — confidence ∈ [0,1]; required fields; no extras; empty list when nothing notable;
  no duplicate moments.
- **Soft look-back (optional)** — a one-line recap of the previous segment (`prior_summary`) for
  continuity on boundary-spanning moments; no frame precision needed (contrast scene-change, §1).
- **Provenance = model + version (+ prompt version)** — a prompt/model change can shift what's
  detected, which is exactly the "unit count changed → segment-scoped delete+recreate" reanalysis
  case (§4.4).

---

## 5. TADS contract

Endpoints this repo calls (all Bearer-authenticated):
- `GET /schemas?external_id=vu:<type>:v1` → resolve `schema_id` (Phase A, cached).
- `PUT /tracks/{track_id}` → create/ensure a Track, one per analysis type, with a deterministic
  `track_id = uuid5(NS_VU, "{source_id}:{type}")` (Phase A, on TAMS `flows/created`).
- `POST /tracks/{track_id}/events` (batch array) → write analysis; upsert on supplied `id` (Phase B).
- `DELETE /tracks/{track_id}/events?ids=…` → segment-scoped reanalysis (§4.4); may return `202` with
  an async delete-request `Location`. (`?timerange=` filter also exists but uses "wholly overlap".)

Schema contracts (seeded into TADS separately; see `docs/inference-schemas.md`):
`vu:transcript:v1`, `vu:key-moment:v1`. The JSON Schema validates only the event **`payload`**;
`timerange`/`id` are first-class TADS fields.

---

## 6. Dependencies (workspace)

- Runtime/web: `tokio`, `lambda_runtime`, `lambda_http`, `axum` (webhook receiver), `tower`, `http`.
- AWS: `aws-config`, `aws-sdk-bedrockruntime`, `aws-sdk-sqs`, `aws-sdk-secretsmanager`,
  `aws-sdk-s3` (if not using presigned URLs directly), `aws-sdk-sagemakerruntime` (optional, Whisper).
- HTTP clients: `reqwest` (`rustls-tls`) for TAMS/TADS/presigned media.
- Serde/validation: `serde`, `serde_json`, `jsonschema` (optional local self-check), `uuid` (v4 + v5),
  `chrono`.
- Config/CLI: `config` (YAML), `clap` (derive) if any CLI tooling.
- Errors/obs: `thiserror` (libs), `anyhow` (bins), `tracing`, `tracing-subscriber`.
- Tests: `aws-smithy-mocks` (`test-util`) for AWS mocks; `criterion` if benchmarking.
- **ffmpeg:** no crate — bundle a **static arm64 `ffmpeg`/`ffprobe`** in the Lambda (or a layer) and
  shell out via `std::process::Command` (`vu-media`). Avoids linking C libs; matches `tams-ingest`.
- **Optional:** `whisper-rs` (whisper.cpp bindings) if you want in-process transcription instead of a
  SageMaker endpoint.

Pin versions in `Cargo.toml`. Release profile tuned for Lambda cold starts (`opt-level="z"`, `lto`,
`codegen-units=1`, `strip`, `panic="abort"`) — copy from `aws-tads`.

---

## 7. Configuration & auth

- **Config (`config` crate, YAML + `AWS_TADS_INFERENCE__*` env overrides):** TAMS base URL + token
  URL, TADS base URL + token URL, Bedrock model ids (Pegasus/Nova), SageMaker endpoint name (opt),
  the three schema `external_id`s, work-queue URL, webhook shared-secret name, batch sizes.
- **Auth to TADS and TAMS:** OAuth2 **client-credentials** per service; client id/secret from
  **Secrets Manager**, fetched at cold start, token cached to `expires_in − 30s`. Use a **read+write**
  scope for TADS (`tads-api/write`); TAMS scope per that service.
- **Inbound webhook auth:** verify the TAMS callback (shared `x-api-key` / signature) in
  `webhook-receiver` before enqueuing.
- **No secrets in config or repo** (ASH/gitleaks enforce).

---

## 8. Build, deploy, CI

- **Build:** `scripts/build-lambdas.sh` — podman arm64 Linux cross-compile (reuse the `aws-tads`
  script + `Dockerfile.lambda`; macOS can't link aws-lc/ring for arm64). Add the static ffmpeg binary
  into the artifact/layer.
- **Infra:** CDK **Python + uv** (`uv run cdk deploy`). SQS + DLQ, Lambdas, Secrets, KMS CMK, log
  groups, EventBridge/SQS wiring; SageMaker endpoint only if used.
- **CI (GitHub Actions, pinned SHAs):** `cargo fmt --check`, `cargo clippy --all-targets -D warnings`,
  `cargo test`, `cargo audit`; `uv run cdk synth` + **ASH** (`--mode local`); `.ash/ash.yaml` for any
  documented suppressions.

---

## 9. Testing

- **`vu-time`:** exhaustive round-trip of segment→absolute timerange and TAI-ns conversions
  (eternity/never sentinels) — load-bearing, test first.
- **Payload builders (`vu-domain`):** model-output → payload mapping produces objects that **validate
  against the committed `vu:*:v1` schemas** (vendor the schema JSON into test fixtures; assert
  conforming + a deliberately-malformed case). This is the local mirror of TADS's gate.
- **`event_id` determinism:** same (track, timerange, type) → same id.
- **AWS calls:** `aws-smithy-mocks` for Bedrock/SQS/Secrets; no live calls in unit tests.
- **Lambda smoke tests:** `webhook-receiver` (valid/invalid signature → 2xx/401) and `analyzer`
  (fixture segment event → asserts the TADS POST body) with the HTTP clients behind a seam.
- **Integration (gated, opt-in):** against the live TADS dev stack — resolve schemas, PUT a track,
  POST valid + invalid events (201 / per-event 400), confirm idempotent re-POST.

---

## 10. Observability

`tracing` with structured fields (never `println!`). Span per webhook + per segment, fields:
`flow_id`, `source_id`, `track_id`, `segment_timerange`, `analysis_type`, `event_count`, `model_id`,
latency. Emit a metric/log on inference failure and on TADS 4xx (per-event validation errors).

---

## 11. Phased execution

1. **Add crates to the workspace + skeleton.** Add the `vu-*` crates and the two lambdas as members of
   the existing `aws-tads` workspace; wire config and reuse `tads-telemetry` + the shared release
   profile; stub both lambdas building green through the existing podman cross-compile.
2. **`vu-time` + `vu-domain` (TDD).** Timerange mapping + payload builders validated against the
   `vu:*:v1` schemas. Pure, no AWS — fastest feedback.
3. **`vu-tads` + `vu-tams`.** Clients: OAuth token cache, schema resolve+cache + startup self-check,
   PUT the per-type Track(s) with deterministic `track_id`, POST events; TAMS webhook verify,
   GET segments, fetch media.
4. **`vu-infer` + `vu-media`.** Bedrock invoker (structured output), ffmpeg shell-out; wire one
   analysis type end-to-end (recommend **key-moment** first — simplest payload).
5. **`analyzer` lambda.** Full Phase B loop with unit-keyed idempotent `event_id` (§4.4), batching,
   retries; then add `transcript`.
6. **`webhook-receiver` + CDK + orchestration.** Deploy; validate live against the TADS dev stack.
7. **CI + ASH 0-actionable;** load/latency check on the per-segment path.

---

## 12. Phase C (separate, deferred)

Standalone read-side consumer — natural-language search / summarisation over TADS data. Reads via the
TADS API (`GET /tracks`/`events`, `tracks/events_added` webhooks) with its own **read-only** TADS
token. Likely **Python + Strands** (agent with a `query_tads` tool); Rust + Bedrock `Converse` is also
viable. Shares nothing with this repo except the `vu:*:v1` schema vocabulary. Build only if/when a
query UX is needed.

---

## 13. Risks / open questions

- **ffmpeg in Lambda** — static arm64 binary size + the 250 MB unzipped limit; use a **layer** if
  tight. Cold-start cost of shelling out.
- **Bedrock model availability/region** — confirm Pegasus/Nova are enabled in the target region;
  model-access request may be needed.
- **Latency** — per-segment inference + media fetch vs near-live expectations; tune batch size and
  analyzer concurrency; consider provisioned concurrency if cold starts dominate.
- **Cross-segment buffering** — where the sentence/topic buffer lives and how it flushes
  (in-memory per invocation vs a small state store).
- **Backpressure / cost** — a busy flow can fan out many segments; SQS + reserved concurrency bound it.
- **Routing** — if a single VU handles multiple analysis types, how a flow maps to type(s)
  (flow `format`/`tags`) → which `external_id`(s).
- **Idempotency vs edits** *(resolved, §4.4)* — unit-keyed `event_id` makes timerange refinements
  replace in place via upsert; only unit-*count* changes on reanalysis need segment-scoped
  delete+recreate via `DELETE …/events?ids=…` (confirmed in spec; async `202` possible). Keep
  `editable_events = true`. Remaining nuance: where `unit_ordinal` is assigned when a unit spans the
  cross-segment buffer (anchor on the start segment).
