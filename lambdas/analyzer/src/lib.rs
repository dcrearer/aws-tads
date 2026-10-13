//! Analyzer: the Phase B per-segment loop (docs/inference-plan.md §2a).
//!
//! Wires the `vu-*` crates together — fetch segment media (vu-tams), decode once (vu-media),
//! run key-moment inference (vu-infer), map relative→absolute timing (vu-time), build idempotent
//! events (vu-domain), and POST them (vu-tads). The pure mapping (`build_key_moment_events`) and
//! job parsing are unit-tested here; the client-driven `Analyzer` is exercised by integration tests.

use std::path::PathBuf;

use tads_domain::{TadsId, Timestamp, TrackEventPost};
use vu_domain::{AnalysisType, Provenance, derive_track_id, to_event_post};
use vu_infer::{AnalysisContext, DetectedUnit, KeyMomentAnalyzer};
use vu_media::Ffmpeg;
use vu_tads::TadsClient;
use vu_tams::{FlowSegment, SegmentsAddedEvent, TamsClient};

#[derive(Debug, thiserror::Error)]
pub enum AnalyzerError {
    #[error("segment is missing a media URL")]
    NoMediaUrl,
    #[error("segment timerange has no bounded start (no segment_anchor)")]
    NoAnchor,
    #[error("event has no source_id to derive its Track from")]
    NoSourceId,
    #[error("relative→absolute mapping failed: {0}")]
    Time(#[from] vu_time::VuTimeError),
    #[error("payload build failed: {0}")]
    Domain(#[from] vu_domain::VuDomainError),
    #[error("media fetch/decode failed: {0}")]
    Tams(#[from] vu_tams::VuTamsError),
    #[error("media decode failed: {0}")]
    Media(#[from] vu_media::VuMediaError),
    #[error("inference failed: {0}")]
    Infer(#[from] vu_infer::VuInferError),
    #[error("TADS write failed: {0}")]
    Tads(#[from] vu_tads::VuTadsError),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

pub use vu_config::InferenceConfig;

/// Parse an SQS record body into the TAMS `flows/segments_added` event the analyzer processes.
pub fn parse_job(body: &str) -> Result<SegmentsAddedEvent, serde_json::Error> {
    serde_json::from_str(body)
}

/// Map detected key moments onto idempotent [`TrackEventPost`]s — the load-bearing step-10/11 core.
///
/// Each unit's relative timing is made absolute against `segment_anchor`; the event id is derived
/// from `(track_id, segment_anchor, ordinal)` so a refined timerange upserts (§4.4). `unit_ordinal`
/// is the unit's index in the model's returned order (deterministic decoding — §4.9).
pub fn build_key_moment_events(
    track_id: TadsId,
    segment_anchor: Timestamp,
    units: &[DetectedUnit],
) -> Result<Vec<TrackEventPost>, AnalyzerError> {
    units
        .iter()
        .enumerate()
        .map(|(ordinal, u)| {
            let timerange = vu_time::relative_to_absolute(segment_anchor, u.rel_start, u.rel_end)?;
            Ok(to_event_post(
                track_id,
                segment_anchor,
                ordinal as u32,
                timerange,
                &u.payload,
            )?)
        })
        .collect()
}

/// Holds the cold-start clients and drives the per-segment loop.
pub struct Analyzer {
    pub tams: TamsClient,
    pub tads: TadsClient,
    pub infer: KeyMomentAnalyzer,
    pub ffmpeg: Ffmpeg,
    pub model_id: String,
    pub frames_per_second: f32,
}

impl Analyzer {
    /// Process every segment in one `flows/segments_added` event. Returns the number of events
    /// successfully written. A per-event TADS `400` is logged, not raised (retrying won't fix a bad
    /// payload); transport/decode/inference errors propagate so SQS can retry the message.
    #[tracing::instrument(skip_all, fields(flow_id = %event.flow_id, segments = event.segments.len()))]
    pub async fn process_event(&self, event: &SegmentsAddedEvent) -> Result<usize, AnalyzerError> {
        let source_id = event
            .source_id
            .as_deref()
            .ok_or(AnalyzerError::NoSourceId)?;
        let track_id = derive_track_id(source_id, AnalysisType::KeyMoment);
        let mut written = 0;
        for segment in &event.segments {
            written += self.process_segment(source_id, track_id, segment).await?;
        }
        Ok(written)
    }

    #[tracing::instrument(skip_all, fields(object_id = %segment.object_id))]
    async fn process_segment(
        &self,
        source_id: &str,
        track_id: TadsId,
        segment: &FlowSegment,
    ) -> Result<usize, AnalyzerError> {
        let url = segment.first_get_url().ok_or(AnalyzerError::NoMediaUrl)?;
        let anchor = segment.segment_anchor().ok_or(AnalyzerError::NoAnchor)?;

        let bytes = self.tams.fetch_media(url).await?;
        let work = Workspace::create(&segment.object_id).await?;
        let media_path = work.dir.join("segment.media");
        tokio::fs::write(&media_path, &bytes).await?;

        let info = self.ffmpeg.probe(&media_path).await?;
        let frame_paths = self
            .ffmpeg
            .extract_frames(&media_path, self.frames_per_second, &work.dir)
            .await?;
        let mut frames = Vec::with_capacity(frame_paths.len());
        for p in &frame_paths {
            frames.push(tokio::fs::read(p).await?);
        }

        let ctx = AnalysisContext {
            segment_duration_secs: info.duration_secs,
            prior_summary: None,
        };
        let provenance = self.provenance(source_id);
        let units = self.infer.analyze(&frames, &ctx, provenance).await?;
        let events = build_key_moment_events(track_id, anchor, &units)?;

        work.cleanup().await;

        if events.is_empty() {
            return Ok(0);
        }
        let outcome = self.tads.post_events(track_id, &events).await?;
        let failures = outcome.failures();
        if !failures.is_empty() {
            tracing::warn!(
                track_id = %track_id,
                rejected = failures.len(),
                total = events.len(),
                "TADS rejected some events (per-event 400); not retrying"
            );
        }
        Ok(events.len() - failures.len())
    }

    fn provenance(&self, source_id: &str) -> Provenance {
        Provenance {
            model: Some(self.model_id.clone()),
            model_version: None,
            generated_at: Some(chrono::Utc::now().to_rfc3339()),
            source: Some(format!("tams://sources/{source_id}")),
        }
    }
}

/// A unique scratch dir under the temp root for one segment's media + frames, cleaned up after use.
struct Workspace {
    dir: PathBuf,
}

impl Workspace {
    async fn create(object_id: &str) -> Result<Self, std::io::Error> {
        let slug: String = object_id
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
            .collect();
        let dir = std::env::temp_dir()
            .join("vu-analyzer")
            .join(format!("{slug}-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&dir).await?;
        Ok(Self { dir })
    }

    async fn cleanup(&self) {
        let _ = tokio::fs::remove_dir_all(&self.dir).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vu_domain::{KeyMomentPayload, KeyMomentType};

    fn unit(start: f64, end: f64, title: &str) -> DetectedUnit {
        DetectedUnit {
            rel_start: start,
            rel_end: end,
            payload: KeyMomentPayload {
                title: title.into(),
                kind: KeyMomentType::Highlight,
                description: None,
                confidence: 0.9,
                evidence: None,
                provenance: None,
            },
        }
    }

    #[test]
    fn builds_absolute_idempotent_events() {
        let track = derive_track_id("source-1", AnalysisType::KeyMoment);
        let anchor = Timestamp::new(1_694_429_247, 0);
        let units = vec![unit(1.0, 3.0, "a"), unit(4.0, 4.0, "b")];

        let events = build_key_moment_events(track, anchor, &units).unwrap();
        assert_eq!(events.len(), 2);
        // First unit → absolute half-open span anchored at segment_start + 1s .. +3s.
        assert_eq!(
            events[0].timerange.to_string(),
            "[1694429248:0_1694429250:0)"
        );
        // Equal bounds collapse to an instant.
        assert_eq!(events[1].timerange.to_string(), "[1694429251:0]");
        // Ids are ordinal-stable and distinct per unit.
        assert_eq!(
            events[0].id.unwrap(),
            vu_domain::derive_event_id(track, anchor, 0)
        );
        assert_ne!(events[0].id, events[1].id);
    }

    #[test]
    fn empty_units_build_no_events() {
        let track = derive_track_id("s", AnalysisType::KeyMoment);
        let events = build_key_moment_events(track, Timestamp::new(100, 0), &[]).unwrap();
        assert!(events.is_empty());
    }

    #[test]
    fn parse_job_reads_segments_added() {
        let body = r#"{ "flow_id": "f1", "source_id": "s1", "segments": [] }"#;
        let ev = parse_job(body).unwrap();
        assert_eq!(ev.flow_id, "f1");
        assert_eq!(ev.source_id.as_deref(), Some("s1"));
    }
}
