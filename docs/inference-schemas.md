# VU → TADS Payload Schemas

Here are the two schemas, ready to paste into Insomnia. All use JSON Schema draft 2020-12,
strict `additionalProperties: false` (so the validation gate is meaningful), a stable `external_id`
for VU lookup, and a shared provenance block.

## Insomnia request (same for all three)

- **Method/URL:** `PUT {{ base_url }}/schemas/{id}` — put the schema's `id` in the path (and it matches the body `id`).
- **Auth:** Bearer `{{ access_token }}` (your existing OAuth2 client-credentials setup).
- **Body:** JSON (below). Expect **201** (first time) / **200** (idempotent re-PUT with identical `schema`).

---

### 1. Transcript — `PUT {{base_url}}/schemas/d4ba1f7d-3d88-4c19-b693-1e701aa853d8`

```json
{
  "id": "d4ba1f7d-3d88-4c19-b693-1e701aa853d8",
  "external_id": "vu:transcript:v1",
  "label": "VU Transcript Segment",
  "description": "One transcript utterance/sentence. Event timerange = the span it covers on the media timeline.",
  "tags": { "producer": "video-understanding", "analysis": "transcript" },
  "schema": {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "VU Transcript Segment",
    "type": "object",
    "additionalProperties": false,
    "required": ["text"],
    "properties": {
      "text": { "type": "string", "minLength": 1 },
      "language": { "type": "string", "description": "BCP-47, e.g. en-US" },
      "speaker": { "type": "string" },
      "confidence": { "type": "number", "minimum": 0, "maximum": 1 },
      "words": {
        "type": "array",
        "items": {
          "type": "object",
          "additionalProperties": false,
          "required": ["text", "start", "end"],
          "properties": {
            "text": { "type": "string" },
            "start": { "type": "number", "description": "seconds on the media timeline" },
            "end": { "type": "number", "description": "seconds on the media timeline" },
            "confidence": { "type": "number", "minimum": 0, "maximum": 1 }
          }
        }
      },
      "provenance": { "$ref": "#/$defs/provenance" }
    },
    "$defs": {
      "provenance": {
        "type": "object",
        "additionalProperties": false,
        "properties": {
          "model": { "type": "string" },
          "model_version": { "type": "string" },
          "generated_at": { "type": "string", "format": "date-time" },
          "source": { "type": "string", "description": "e.g. tams:// proxy flow analysed" }
        }
      }
    }
  }
}
```

---

### 2. Key Moment — `PUT {{base_url}}/schemas/ff85b5b7-f6df-4145-842a-b22746a238a4`

```json
{
  "id": "ff85b5b7-f6df-4145-842a-b22746a238a4",
  "external_id": "vu:key-moment:v1",
  "label": "VU Key Moment",
  "description": "A detected moment of interest. Event timerange = the moment's span.",
  "tags": { "producer": "video-understanding", "analysis": "key-moment" },
  "schema": {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "VU Key Moment",
    "type": "object",
    "additionalProperties": false,
    "required": ["title", "type", "confidence"],
    "properties": {
      "title": { "type": "string", "minLength": 1 },
      "type": {
        "type": "string",
        "enum": ["notable_event", "speaker_change", "highlight", "scene_cut", "text_on_screen"]
      },
      "description": { "type": "string" },
      "confidence": { "type": "number", "minimum": 0, "maximum": 1 },
      "evidence": {
        "type": "array",
        "items": { "type": "string" },
        "description": "supporting signals/quotes the model cited"
      },
      "provenance": { "$ref": "#/$defs/provenance" }
    },
    "$defs": {
      "provenance": {
        "type": "object",
        "additionalProperties": false,
        "properties": {
          "model": { "type": "string" },
          "model_version": { "type": "string" },
          "generated_at": { "type": "string", "format": "date-time" },
          "source": { "type": "string" }
        }
      }
    }
  }
}
```

---

> **Scene-change (`vu:scene-change:v1`) was dropped** (2026-10-13) — shot-boundary detection needs a
> look-back/overlap window the per-segment pipeline doesn't have (see docs/inference-plan.md §1). If
> it was already seeded into TADS, the schema is simply left unused; nothing writes to it.

---

## Notes / knobs

- **Validation only touches `payload`.** A transcript event you POST looks like `{ "timerange": "[...)", "payload": { "text": "...", "confidence": 0.98 } }` — the `payload` object is what these schemas check.
- **`additionalProperties: false` is deliberate** — it makes the gate catch malformed/unexpected model output. If you'd rather allow forward-compatible extra fields without a new version, flip the top-level to `true` (keep required fields). Your call; strict is the stronger contract.
- **Evolving a schema:** since `schema` is immutable, a change = new `id` + `external_id: vu:transcript:v2`. VU resolving `vu:transcript:v1` keeps working against old data.
- **`format: date-time`** is annotative in most JSON Schema validators (not enforced unless format-assertion is on) — fine for provenance.
