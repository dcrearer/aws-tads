# Conformance fixtures

Read-only exports of the four top-level collections from a live BBC-TADS v0.1 deployment
("Phrame TADS", `mxl-tams.phrame.live`), captured 2026-10-06. They back
`crates/tads-domain/tests/conformance.rs`, which asserts our domain models round-trip real
production data and that our JSON-Schema validation accepts/rejects payloads against the real
Payload Schemas.

| File | `GET` source | Contents |
|------|--------------|----------|
| `schemas.json` | `/schemas` | 8 Payload Schemas (JSON Schema draft-07 and 2020-12) |
| `tracks.json` | `/tracks` | 94 Tracks (bound to TAMS flow timelines) |
| `track-delete-requests.json` | `/track-delete-requests` | 14 completed delete requests |
| `webhooks.json` | `/service/webhooks` | empty (no subscriptions registered) |

Public, non-secret timeline metadata — no credentials or payload bodies. Not a golden snapshot:
update only if re-exported from a known-good instance.
