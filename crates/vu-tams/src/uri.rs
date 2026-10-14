//! The `tams://` pseudo-protocol mapping.
//!
//! Per TADS AppNote 0001 a Track's `timeline_reference.entity_id` uses the `tams://` scheme (and
//! points at a whole Source), while actual HTTP fetches swap the scheme to `https://`.

/// Swap a leading `tams://` (case-insensitive) for `https://`. Non-TAMS URIs pass through unchanged.
pub fn tams_to_https(uri: &str) -> String {
    if uri.len() >= 7 && uri[..7].eq_ignore_ascii_case("tams://") {
        format!("https://{}", &uri[7..])
    } else {
        uri.to_string()
    }
}

/// Swap a leading `https://` (case-insensitive) for `tams://`. Non-HTTPS URIs pass through.
pub fn https_to_tams(uri: &str) -> String {
    if uri.len() >= 8 && uri[..8].eq_ignore_ascii_case("https://") {
        format!("tams://{}", &uri[8..])
    } else {
        uri.to_string()
    }
}

/// Normalise a configured TAMS host/base-url to a bare host (no scheme, no trailing slash).
fn normalize_host(tams_host: &str) -> &str {
    tams_host
        .trim()
        .trim_end_matches('/')
        .trim_start_matches("tams://")
        .trim_start_matches("https://")
        .trim_start_matches("http://")
}

/// Build the whole-Source `tams://` entity_id for a Track, given the TAMS host (with or without a
/// scheme) and a source id. Never carries a `?timerange=` query — TADS rejects that (AppNote 0001).
pub fn source_entity_id(tams_host: &str, source_id: &str) -> String {
    format!("tams://{}/sources/{source_id}", normalize_host(tams_host))
}

/// Build a `tams://` reference to a specific flow media object (segment). Used as per-Event
/// provenance so a key moment traces directly to the exact media chunk it came from — unlike the
/// Track's `entity_id`, which points at the whole Source.
pub fn flow_media_object_uri(tams_host: &str, flow_id: &str, object_id: &str) -> String {
    format!(
        "tams://{}/flows/{flow_id}/media-objects/{object_id}",
        normalize_host(tams_host)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_tams_to_https_and_back() {
        assert_eq!(
            tams_to_https("tams://tams.example.com/sources/abc"),
            "https://tams.example.com/sources/abc"
        );
        assert_eq!(
            https_to_tams("https://tams.example.com/flows/x"),
            "tams://tams.example.com/flows/x"
        );
    }

    #[test]
    fn case_insensitive_scheme() {
        assert_eq!(tams_to_https("TAMS://h/x"), "https://h/x");
    }

    #[test]
    fn non_matching_uris_pass_through() {
        assert_eq!(tams_to_https("s3://bucket/key"), "s3://bucket/key");
        assert_eq!(https_to_tams("ftp://h/x"), "ftp://h/x");
    }

    #[test]
    fn builds_source_entity_id_from_various_hosts() {
        assert_eq!(
            source_entity_id("https://tams.example.com", "s-1"),
            "tams://tams.example.com/sources/s-1"
        );
        assert_eq!(
            source_entity_id("tams://tams.example.com/", "s-1"),
            "tams://tams.example.com/sources/s-1"
        );
        assert_eq!(
            source_entity_id("tams.example.com", "s-1"),
            "tams://tams.example.com/sources/s-1"
        );
    }

    #[test]
    fn builds_flow_media_object_uri() {
        assert_eq!(
            flow_media_object_uri("https://tams.example.com/", "f-1", "obj-9"),
            "tams://tams.example.com/flows/f-1/media-objects/obj-9"
        );
    }
}
