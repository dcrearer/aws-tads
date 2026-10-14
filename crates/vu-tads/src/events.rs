//! Interpretation of the per-event `POST /tracks/{id}/events` response.
//!
//! The endpoint always returns HTTP 200 wrapping a per-event result list. On a TAMS-compatible
//! Track (`editable_events=false`) re-POSTing an existing deterministic id yields a per-event
//! **403**. Because inference is temperature-0 deterministic, such an Event is already present and
//! correct, so a redelivery is an **idempotent success** — not a failure. This module encodes that
//! classification purely, so it is unit-tested without a live server.

use tads_domain::{EventPostResponses, TadsId};

/// Outcome of a single posted Event.
#[derive(Debug, Clone, PartialEq)]
pub enum EventOutcome {
    /// 201 — newly created.
    Created(TadsId),
    /// 200 — replaced an existing Event (only possible on editable Tracks).
    Replaced(TadsId),
    /// 403 for `editable_events=false` on an already-present id — treated as idempotent success.
    AlreadyPresent(Option<TadsId>),
    /// Any other non-success per-event result (e.g. 400 overlap, 413 too large, 500).
    Rejected {
        id: Option<TadsId>,
        status: u16,
        summary: String,
    },
}

impl EventOutcome {
    /// Whether this outcome counts as success for the write (created, replaced, or already-present).
    pub fn is_success(&self) -> bool {
        !matches!(self, EventOutcome::Rejected { .. })
    }
}

/// Classify every per-event response. `editable_events`-driven 403s are mapped to
/// [`EventOutcome::AlreadyPresent`]; everything else non-2xx is [`EventOutcome::Rejected`].
pub fn interpret(responses: &EventPostResponses) -> Vec<EventOutcome> {
    responses
        .responses
        .iter()
        .map(|r| match r.http_status_code {
            201 => EventOutcome::Created(r.id.unwrap_or_else(TadsId::new_v4)),
            200 => EventOutcome::Replaced(r.id.unwrap_or_else(TadsId::new_v4)),
            403 if is_editable_events_block(r) => EventOutcome::AlreadyPresent(r.id),
            status => EventOutcome::Rejected {
                id: r.id,
                status,
                summary: r
                    .error
                    .as_ref()
                    .map(|e| e.summary.clone())
                    .unwrap_or_default(),
            },
        })
        .collect()
}

/// A 403 whose error summary points at the `editable_events` rule (vs. e.g. a read-only Track).
fn is_editable_events_block(r: &tads_domain::EventPostResponse) -> bool {
    r.error
        .as_ref()
        .map(|e| e.summary.to_ascii_lowercase().contains("editable_events"))
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn responses(v: serde_json::Value) -> EventPostResponses {
        serde_json::from_value(v).unwrap()
    }

    #[test]
    fn classifies_created_and_replaced() {
        let id = TadsId::new_v4();
        let r = responses(json!({
            "all_successful": true,
            "responses": [
                { "http_status_code": 201, "id": id.to_string() },
                { "http_status_code": 200, "id": id.to_string() }
            ]
        }));
        let out = interpret(&r);
        assert_eq!(out[0], EventOutcome::Created(id));
        assert_eq!(out[1], EventOutcome::Replaced(id));
        assert!(out.iter().all(EventOutcome::is_success));
    }

    #[test]
    fn editable_events_403_is_idempotent_success() {
        let id = TadsId::new_v4();
        let r = responses(json!({
            "all_successful": false,
            "responses": [
                {
                    "http_status_code": 403,
                    "id": id.to_string(),
                    "error": {
                        "type": "forbidden",
                        "summary": "editable_events is false; cannot modify an existing Event",
                        "time": "2026-10-13T00:00:00Z"
                    }
                }
            ]
        }));
        let out = interpret(&r);
        assert_eq!(out[0], EventOutcome::AlreadyPresent(Some(id)));
        assert!(out[0].is_success(), "already-present must count as success");
    }

    #[test]
    fn overlap_400_is_rejected() {
        let r = responses(json!({
            "all_successful": false,
            "responses": [
                {
                    "http_status_code": 400,
                    "error": {
                        "type": "bad_request",
                        "summary": "allow_overlaps is false; Event overlaps an existing Event",
                        "time": "2026-10-13T00:00:00Z"
                    }
                }
            ]
        }));
        let out = interpret(&r);
        assert!(matches!(out[0], EventOutcome::Rejected { status: 400, .. }));
        assert!(!out[0].is_success());
    }

    #[test]
    fn non_editable_403_is_rejected() {
        // A read-only Track also returns 403, but not an editable_events block -> real rejection.
        let r = responses(json!({
            "all_successful": false,
            "responses": [
                {
                    "http_status_code": 403,
                    "error": {
                        "type": "forbidden",
                        "summary": "track is read_only",
                        "time": "2026-10-13T00:00:00Z"
                    }
                }
            ]
        }));
        assert!(matches!(
            interpret(&r)[0],
            EventOutcome::Rejected { status: 403, .. }
        ));
    }
}
