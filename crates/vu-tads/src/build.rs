//! Pure builders for the Payload Schema and Track resources the pipeline writes.

use serde_json::Value;
use tads_domain::{PayloadSchema, TadsId, TimelineReference, Track};

/// Build the key-moment [`PayloadSchema`] resource for a `PUT /schemas/{id}`.
pub fn key_moment_schema(
    schema_id: TadsId,
    external_id: &str,
    schema_json: &str,
) -> Result<PayloadSchema, serde_json::Error> {
    let schema: Value = serde_json::from_str(schema_json)?;
    Ok(PayloadSchema {
        id: schema_id,
        external_id: Some(external_id.to_string()),
        label: Some("VU Key Moment".to_string()),
        description: Some("Key moments extracted from video by the VU pipeline.".to_string()),
        created_by: None,
        updated_by: None,
        created: None,
        updated: None,
        tags: Default::default(),
        schema,
    })
}

/// Build the key-moment [`Track`] resource for a `PUT /tracks/{id}`.
///
/// TAMS-compatible writeback: `allow_overlaps=false`, `editable_events=false`, the Track references
/// the **whole** Source via a `tams://` `entity_id` (never a `?timerange=` variant), and
/// `timeline_selected` is left unset.
pub fn key_moment_track(track_id: TadsId, schema_id: TadsId, source_entity_id: &str) -> Track {
    Track {
        id: track_id,
        label: Some("VU Key Moments".to_string()),
        description: None,
        created_by: None,
        updated_by: None,
        timeline_reference: TimelineReference {
            entity_id: source_entity_id.to_string(),
            timeline_selected: None,
        },
        tags: Default::default(),
        metadata_version: None,
        created: None,
        metadata_updated: None,
        events_updated: None,
        read_only: false,
        allow_overlaps: false,
        editable_events: false,
        timerange: None,
        schema_id,
        edit_rate: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vu_domain::{KEY_MOMENT_SCHEMA_EXTERNAL_ID, KEY_MOMENT_SCHEMA_JSON};

    #[test]
    fn schema_resource_carries_external_id_and_schema() {
        let id = TadsId::new_v4();
        let s =
            key_moment_schema(id, KEY_MOMENT_SCHEMA_EXTERNAL_ID, KEY_MOMENT_SCHEMA_JSON).unwrap();
        assert_eq!(s.id, id);
        assert_eq!(s.external_id.as_deref(), Some("vu:key-moment:v1"));
        // The vendored schema requires title/type/confidence.
        assert_eq!(s.schema["title"], serde_json::json!("VU Key Moment"));
        assert!(
            s.schema["required"]
                .as_array()
                .unwrap()
                .contains(&serde_json::json!("confidence"))
        );
    }

    #[test]
    fn track_is_tams_compatible_and_references_whole_source() {
        let track_id = TadsId::new_v4();
        let schema_id = TadsId::new_v4();
        let t = key_moment_track(track_id, schema_id, "tams://tams.example.com/sources/s-1");

        assert!(!t.allow_overlaps, "TAMS-compatible tracks forbid overlaps");
        assert!(
            !t.editable_events,
            "TAMS-compatible tracks are not editable"
        );
        assert_eq!(t.schema_id, schema_id);
        assert_eq!(
            t.timeline_reference.entity_id,
            "tams://tams.example.com/sources/s-1"
        );
        assert!(t.timeline_reference.timeline_selected.is_none());
        // The entity_id must survive the server's AppNote-0001 check (no `?timerange=`).
        assert!(tads_domain::validate_timeline_reference(&t.timeline_reference).is_ok());
    }
}
