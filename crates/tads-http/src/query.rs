//! Shared query-string parsing for the list endpoints (pagination, `tag.{name}` /
//! `tag_exists.{name}` filters, CSV/boolean helpers). Handlers extract
//! `Query<HashMap<String, String>>` and lean on these so parsing lives in one place.

use std::collections::HashMap;

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
