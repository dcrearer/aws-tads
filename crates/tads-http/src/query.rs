//! Shared query-string parsing for the list endpoints (pagination, `tag.{name}` /
//! `tag_exists.{name}` filters, CSV/boolean helpers). Handlers extract
//! `Query<HashMap<String, String>>` and lean on these so parsing lives in one place.

use std::collections::HashMap;

use percent_encoding::percent_decode_str;
use tads_store::PageParams;

/// Pull `limit`/`page` into [`PageParams`]. A non-numeric `limit` is ignored (falls back to the
/// server default) rather than failing the request.
pub fn page_params(q: &HashMap<String, String>) -> PageParams {
    PageParams {
        limit: q.get("limit").and_then(|v| v.parse().ok()),
        page: q.get("page").cloned(),
    }
}

/// Parse `tag.{name}` (value matches) and `tag_exists.{name}` (presence) filters.
/// Returns `(matches, exists)`.
pub fn tag_filters(q: &HashMap<String, String>) -> (Vec<(String, String)>, Vec<String>) {
    let mut matches = Vec::new();
    let mut exists = Vec::new();
    for (k, v) in q {
        if let Some(name) = k.strip_prefix("tag_exists.") {
            if parse_bool(v) {
                exists.push(name.to_string());
            }
        } else if let Some(name) = k.strip_prefix("tag.") {
            matches.push((name.to_string(), v.clone()));
        }
    }
    (matches, exists)
}

/// Parse a boolean query flag (`true`/`1` are true).
pub fn parse_bool(v: &str) -> bool {
    matches!(v.trim().to_ascii_lowercase().as_str(), "true" | "1")
}

/// Split a comma-separated list value (e.g. the `ids` param), trimming empties.
pub fn parse_csv(v: &str) -> Vec<String> {
    v.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
}

/// Parse the `timeline_reference.entity_id` query parameter per TADS AppNote 0001.
///
/// Clients individually percent-encode each entity-id URI (so an embedded comma can't be mistaken
/// for a list separator), join them with commas, then percent-encode the whole list a second time.
/// The HTTP layer strips that outer layer for us, leaving comma-separated *still-encoded* URIs.
/// We therefore split on the literal separators and percent-decode each entry back to its raw URI
/// (e.g. `tams://...`). Decoding is a no-op for already-decoded values, so plain and single-encoded
/// inputs keep working.
pub fn parse_entity_ids(v: &str) -> Vec<String> {
    v.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| percent_decode_str(s).decode_utf8_lossy().into_owned())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const RAW: &str = "tams://tams.example.com/sources/8d7227aa-6bf2-4e84-881c-89eb046c3b48";
    // The RAW URI after a single percent-encode (`:` -> %3A, `/` -> %2F). This is what the HTTP
    // layer hands us for the appnote's double-encoded form, after it strips the outer layer.
    const ONCE_ENCODED: &str =
        "tams%3A%2F%2Ftams.example.com%2Fsources%2F8d7227aa-6bf2-4e84-881c-89eb046c3b48";

    // AppNote 0001: each entity_id is individually percent-encoded, so after the HTTP layer's
    // single outer-decode we receive encoded URIs separated by literal commas. We must decode each.
    #[test]
    fn decodes_appnote_double_encoded_single_value() {
        assert_eq!(parse_entity_ids(ONCE_ENCODED), vec![RAW.to_string()]);
    }

    // A raw/already-decoded URI (no percent escapes) must pass through unchanged: decoding is a
    // no-op, so the previously-working single-encoded and plain cases keep matching.
    #[test]
    fn passes_through_already_decoded_value() {
        assert_eq!(parse_entity_ids(RAW), vec![RAW.to_string()]);
    }

    // A comma-separated list of individually-encoded URIs splits on the separators, then decodes.
    #[test]
    fn splits_then_decodes_each_entry() {
        let input = format!("{ONCE_ENCODED},{ONCE_ENCODED}");
        assert_eq!(
            parse_entity_ids(&input),
            vec![RAW.to_string(), RAW.to_string()]
        );
    }

    // A comma *inside* a URI is encoded as %2C by the inner layer, so it is NOT a separator and
    // must survive splitting and be restored by the decode (the whole reason for double-encoding).
    #[test]
    fn does_not_split_on_encoded_comma() {
        let input = "urn%3Aexample%3Aa%2Cb"; // "urn:example:a,b" inner-encoded
        assert_eq!(parse_entity_ids(input), vec!["urn:example:a,b".to_string()]);
    }

    #[test]
    fn trims_and_drops_empties() {
        assert_eq!(parse_entity_ids(" , "), Vec::<String>::new());
    }
}
