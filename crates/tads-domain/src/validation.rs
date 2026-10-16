//! JSON-Schema validation of Event payloads against a Track's Payload Schema, plus
//! timeline-reference and webhook-URL validation.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use serde_json::Value;
use url::{Host, Url};

use crate::error::DomainError;
use crate::models::TimelineReference;

/// A compiled JSON Schema validator, reusable across many payloads.
///
/// Compiling a schema is ~150× more expensive than validating one payload against an
/// already-compiled validator, so batch paths (e.g. a multi-Event POST, all against one Track's
/// schema) should [`compile_schema`] once and [`validate_with`] per payload rather than calling
/// [`validate_payload`] in a loop.
pub type PayloadValidator = jsonschema::Validator;

/// Compile a JSON Schema into a reusable [`PayloadValidator`].
pub fn compile_schema(schema: &Value) -> Result<PayloadValidator, DomainError> {
    jsonschema::validator_for(schema).map_err(|e| DomainError::InvalidSchema(e.to_string()))
}

/// Validate `payload` against an already-compiled validator, collecting any violations.
pub fn validate_with(validator: &PayloadValidator, payload: &Value) -> Result<(), DomainError> {
    let errors: Vec<String> = validator
        .iter_errors(payload)
        .map(|e| e.to_string())
        .collect();
    if errors.is_empty() {
        Ok(())
    } else {
        Err(DomainError::PayloadValidation(errors.join("; ")))
    }
}

/// Validate `payload` against the JSON Schema `schema`, compiling the schema on the fly.
///
/// Convenience for one-off validation. In a loop over many payloads sharing one schema, prefer
/// [`compile_schema`] + [`validate_with`] to avoid recompiling per payload.
pub fn validate_payload(payload: &Value, schema: &Value) -> Result<(), DomainError> {
    validate_with(&compile_schema(schema)?, payload)
}

/// Ensure `schema` is itself a usable JSON Schema (compiles). Used when a Payload Schema is
/// created, before it is stored, so Tracks can rely on it later.
pub fn ensure_valid_schema(schema: &Value) -> Result<(), DomainError> {
    jsonschema::validator_for(schema)
        .map(|_| ())
        .map_err(|e| DomainError::InvalidSchema(e.to_string()))
}

/// Enforce TADS AppNote 0001's rule that a TAMS (`tams://`) `timeline_reference` MUST NOT point at
/// a specific TimeRange of a Source or Flow (the TAMS AppNote 0014 TimeRange URI variants). A
/// Track's timing comes from its Events, so the reference identifies the whole Source/Flow timeline.
///
/// Scope is deliberately limited to the `tams://` pseudo-protocol: TADS also annotates non-TAMS,
/// file-based media whose `entity_id` may be an arbitrary URI that legitimately carries a query
/// string. We detect the variant by a `timerange` query parameter (covers both the Source form
/// `.../sources/{id}?timerange=...` and the Flow form `.../flows/{id}/segments?timerange=...`).
pub fn validate_timeline_reference(tr: &TimelineReference) -> Result<(), DomainError> {
    let id = tr.entity_id.trim();
    let is_tams = id.len() >= 7 && id.as_bytes()[..7].eq_ignore_ascii_case(b"tams://");
    if is_tams && let Some((_, query)) = id.split_once('?') {
        let has_timerange = query.split('&').any(|param| {
            param
                .split('=')
                .next()
                .is_some_and(|key| key.eq_ignore_ascii_case("timerange"))
        });
        if has_timerange {
            return Err(DomainError::InvalidTimelineReference(format!(
                "`{}`: a tams:// reference MUST NOT point at a specific TimeRange of a Source/Flow \
                 (TADS AppNote 0001); reference the whole Source/Flow instead",
                tr.entity_id
            )));
        }
    }
    Ok(())
}

/// True if `ip` must never be a webhook delivery destination: loopback, private (RFC1918),
/// link-local (incl. the cloud metadata endpoint `169.254.169.254`), carrier-grade NAT, the
/// unspecified/broadcast addresses, multicast, documentation/benchmark ranges, or the IPv6
/// equivalents (ULA `fc00::/7`, link-local `fe80::/10`). This is the core SSRF control: it is
/// applied to IP literals at registration and to every resolved address at delivery time.
pub fn is_blocked_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => is_blocked_v4(v4),
        // Treat an IPv4-mapped v6 address (`::ffff:a.b.c.d`) as its underlying v4 address.
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => is_blocked_v4(v4),
            None => is_blocked_v6(v6),
        },
    }
}

fn is_blocked_v4(ip: Ipv4Addr) -> bool {
    let [a, b, ..] = ip.octets();
    ip.is_private()
        || ip.is_loopback()
        || ip.is_link_local()
        || ip.is_unspecified()
        || ip.is_broadcast()
        || ip.is_multicast()
        || ip.is_documentation()
        // Carrier-grade NAT 100.64.0.0/10 (no stable std predicate).
        || (a == 100 && (64..=127).contains(&b))
}

fn is_blocked_v6(ip: Ipv6Addr) -> bool {
    let first = ip.segments()[0];
    ip.is_loopback()
        || ip.is_unspecified()
        || ip.is_multicast()
        || (first & 0xfe00) == 0xfc00 // unique-local fc00::/7
        || (first & 0xffc0) == 0xfe80 // link-local fe80::/10
}

/// Validate a webhook subscriber URL at registration (SSRF control, step 1 of 2).
///
/// Enforces **https** and rejects URLs whose host is an IP literal in a [blocked range](is_blocked_ip)
/// or an obvious loopback name. Hostnames are not resolved here (DNS can change, and resolution is
/// I/O) — the delivery worker re-resolves and re-checks every address before each POST, which also
/// defends against DNS-rebinding. Returns [`DomainError::InvalidWebhookUrl`] (→ `400`).
pub fn validate_webhook_url(raw: &str) -> Result<(), DomainError> {
    let blocked = |msg: String| Err(DomainError::InvalidWebhookUrl(msg));
    let url =
        Url::parse(raw).map_err(|e| DomainError::InvalidWebhookUrl(format!("`{raw}`: {e}")))?;

    if url.scheme() != "https" {
        return blocked(format!("`{raw}`: webhook url must use https"));
    }
    match url.host() {
        None => blocked(format!("`{raw}`: url has no host")),
        Some(Host::Ipv4(ip)) if is_blocked_ip(IpAddr::V4(ip)) => blocked(format!(
            "`{raw}`: host {ip} is in a disallowed (private/link-local) range"
        )),
        Some(Host::Ipv6(ip)) if is_blocked_ip(IpAddr::V6(ip)) => blocked(format!(
            "`{raw}`: host {ip} is in a disallowed (private/link-local) range"
        )),
        Some(Host::Domain(d)) => {
            let d = d.to_ascii_lowercase();
            if d == "localhost" || d.ends_with(".localhost") || d == "metadata.google.internal" {
                return blocked(format!("`{raw}`: host {d} is not a permitted destination"));
            }
            Ok(())
        }
        Some(_) => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn accepts_conforming_payload() {
        let schema = json!({
            "type": "object",
            "required": ["text"],
            "properties": { "text": { "type": "string" } }
        });
        assert!(validate_payload(&json!({ "text": "hello" }), &schema).is_ok());
    }

    #[test]
    fn rejects_nonconforming_payload() {
        let schema = json!({
            "type": "object",
            "required": ["text"],
            "properties": { "text": { "type": "string" } }
        });
        let err = validate_payload(&json!({ "text": 42 }), &schema).unwrap_err();
        assert!(matches!(err, DomainError::PayloadValidation(_)));
    }

    #[test]
    fn rejects_invalid_schema() {
        // `type` must be a string/array, not a number.
        let bad_schema = json!({ "type": 123 });
        let err = validate_payload(&json!({}), &bad_schema).unwrap_err();
        assert!(matches!(err, DomainError::InvalidSchema(_)));
    }

    use crate::models::TimelineReference;

    fn tref(entity_id: &str) -> TimelineReference {
        TimelineReference {
            entity_id: entity_id.to_string(),
            timeline_selected: None,
        }
    }

    // TADS AppNote 0001: the TAMS (AppNote 0014) URI variants referring to a specific TimeRange of
    // a Source or Flow MUST NOT be used as a `tams://` timeline_reference — the Track's Events
    // carry timing. We reject them with a 400 at write time.
    #[test]
    fn rejects_tams_source_timerange_variant() {
        let err = validate_timeline_reference(&tref(
            "tams://tams.example.com/sources/7abe4d59-7d1c-432d-ba2a-6180e12137cc?timerange=[100:0_200:0)",
        ))
        .unwrap_err();
        assert!(matches!(err, DomainError::InvalidTimelineReference(_)));
    }

    #[test]
    fn rejects_tams_flow_segments_timerange_variant() {
        let err = validate_timeline_reference(&tref(
            "tams://tams.example.com/flows/f87f7f32-a34d-4a6f-b085-a5ee49b4b2f8/segments?timerange=[100:0_200:0)",
        ))
        .unwrap_err();
        assert!(matches!(err, DomainError::InvalidTimelineReference(_)));
    }

    #[test]
    fn rejects_timerange_variant_case_insensitive_scheme() {
        let err = validate_timeline_reference(&tref(
            "TAMS://tams.example.com/sources/7abe4d59-7d1c-432d-ba2a-6180e12137cc?timerange=[0:0_1:0)",
        ))
        .unwrap_err();
        assert!(matches!(err, DomainError::InvalidTimelineReference(_)));
    }

    #[test]
    fn allows_whole_tams_source_reference() {
        assert!(
            validate_timeline_reference(&tref(
                "tams://tams.example.com/sources/7abe4d59-7d1c-432d-ba2a-6180e12137cc"
            ))
            .is_ok()
        );
    }

    #[test]
    fn allows_whole_tams_flow_reference() {
        assert!(
            validate_timeline_reference(&tref(
                "tams://tams.example.com/flows/f87f7f32-a34d-4a6f-b085-a5ee49b4b2f8"
            ))
            .is_ok()
        );
    }

    // Only `tams://` references are in scope; a non-TAMS URI may legitimately carry a query string
    // (including a `timerange` param of its own semantics), so we must not reject it.
    #[test]
    fn allows_non_tams_uri_with_timerange_query() {
        assert!(
            validate_timeline_reference(&tref(
                "https://other.example.com/thing?timerange=[0:0_1:0)"
            ))
            .is_ok()
        );
    }

    // A `tams://` reference with some other (non-timerange) query is not a TimeRange variant.
    #[test]
    fn allows_tams_reference_with_unrelated_query() {
        assert!(
            validate_timeline_reference(&tref(
                "tams://tams.example.com/sources/7abe4d59-7d1c-432d-ba2a-6180e12137cc?foo=bar"
            ))
            .is_ok()
        );
    }

    // Non-URI opaque ids (TADS also annotates non-TAMS, file-based media) pass through.
    #[test]
    fn allows_opaque_non_uri_entity_id() {
        assert!(validate_timeline_reference(&tref("my-local-asset-42")).is_ok());
    }

    // --- webhook URL / SSRF validation ---

    #[test]
    fn accepts_public_https_webhook_url() {
        assert!(validate_webhook_url("https://hooks.example.com/tads").is_ok());
    }

    #[test]
    fn rejects_non_https_webhook_url() {
        let err = validate_webhook_url("http://hooks.example.com/tads").unwrap_err();
        assert!(matches!(err, DomainError::InvalidWebhookUrl(_)));
    }

    #[test]
    fn rejects_metadata_and_private_ip_literals() {
        for url in [
            "https://169.254.169.254/latest/meta-data/", // cloud metadata (link-local)
            "https://127.0.0.1/hook",                    // loopback
            "https://10.0.0.5/hook",                     // RFC1918
            "https://192.168.1.1/hook",                  // RFC1918
            "https://172.16.0.1/hook",                   // RFC1918
            "https://[::1]/hook",                        // IPv6 loopback
            "https://[fd00::1]/hook",                    // IPv6 ULA
            "https://100.64.0.1/hook",                   // CGNAT
            "https://[::ffff:127.0.0.1]/hook",           // IPv4-mapped loopback
        ] {
            assert!(
                matches!(
                    validate_webhook_url(url),
                    Err(DomainError::InvalidWebhookUrl(_))
                ),
                "expected {url} to be rejected"
            );
        }
    }

    #[test]
    fn rejects_localhost_name() {
        assert!(matches!(
            validate_webhook_url("https://localhost/hook"),
            Err(DomainError::InvalidWebhookUrl(_))
        ));
    }

    #[test]
    fn is_blocked_ip_classifies_ranges() {
        use std::net::IpAddr;
        assert!(is_blocked_ip("169.254.169.254".parse::<IpAddr>().unwrap()));
        assert!(is_blocked_ip("10.1.2.3".parse::<IpAddr>().unwrap()));
        assert!(is_blocked_ip("::1".parse::<IpAddr>().unwrap()));
        assert!(!is_blocked_ip("8.8.8.8".parse::<IpAddr>().unwrap()));
        assert!(!is_blocked_ip(
            "2606:4700:4700::1111".parse::<IpAddr>().unwrap()
        ));
    }
}
