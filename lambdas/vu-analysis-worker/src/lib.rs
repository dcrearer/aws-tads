//! SQS-triggered video-understanding analysis worker.
//!
//! Per message (a TAMS `flows/segments_added` payload): resolve the Source, ensure the key-moment
//! Payload Schema and Track exist, assemble a look-back window clip, run Pegasus, map the moments
//! anchored in the newly-added segment(s) onto non-overlapping Track Events, and POST them. The
//! window is inference *context* only; re-analysis uses an explicit delete→poll→recreate path.
//!
//! All external effects sit behind traits ([`vu_tams::TamsApi`], [`vu_infer::KeyMomentModel`],
//! [`vu_tads::TadsApi`], [`MediaAssembler`]), so [`Worker::process_message`] is unit-tested with
//! fakes and the binary wires in the reqwest/Bedrock/ffmpeg edges.

// Building the production worker nests reqwest + AWS SDK + lambda_runtime futures deep enough to
// exceed the default type-layout recursion limit during monomorphization.
#![recursion_limit = "512"]

pub mod config;
pub mod media;
pub mod window;

use std::collections::HashSet;

use base64::Engine as _;
use tads_domain::{TadsId, TrackEvent};
use thiserror::Error;
use vu_domain::{KEY_MOMENT_ANALYSIS_TYPE, derive_track_id};
use vu_infer::KeyMomentModel;
use vu_tads::{EventOutcome, TadsApi, key_moment_track};

use crate::window::ProvenanceCtx;

pub use vu_tads::EventOutcome as WriteOutcome;
use vu_tams::{SegmentsAddedEvent, TamsApi, source_entity_id};

pub use config::WorkerConfig;
pub use media::{FfmpegAssembler, MediaAssembler};

/// The production worker type: reqwest TAMS/TADS clients, Bedrock Pegasus, and ffmpeg assembly.
pub type DefaultWorker =
    Worker<vu_tams::TamsClient, vu_infer::PegasusClient, vu_tads::TadsClient, FfmpegAssembler>;

/// Build the production worker from config. OAuth credentials for both TAMS and TADS are read
/// inline from config (no Secrets Manager); an absent `oauth` block leaves that client
/// unauthenticated. Shared by the Lambda binary and the local dry-run CLI.
pub async fn build_worker(cfg: WorkerConfig) -> Result<DefaultWorker, String> {
    let aws = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let http = reqwest::Client::builder()
        .build()
        .map_err(|e| e.to_string())?;

    let mut tams = vu_tams::TamsClient::new(http.clone(), cfg.tams.base_url.clone());
    if let Some(o) = &cfg.tams.oauth {
        let creds = vu_auth::ClientCredentials::from_values(
            o.token_url.clone(),
            o.client_id.clone(),
            o.client_secret.clone(),
            o.scope(),
        );
        tams = tams.with_token(vu_auth::TokenProvider::new(creds, http.clone()));
    }

    let mut tads = vu_tads::TadsClient::new(http.clone(), cfg.tads.base_url.clone());
    if let Some(o) = &cfg.tads.oauth {
        let creds = vu_auth::ClientCredentials::from_values(
            o.token_url.clone(),
            o.client_id.clone(),
            o.client_secret.clone(),
            o.scope(),
        );
        tads = tads.with_token(vu_auth::TokenProvider::new(creds, http.clone()));
    }

    let model = vu_infer::PegasusClient::new(
        aws_sdk_bedrockruntime::Client::new(&aws),
        vu_infer::InferenceConfig {
            model_id: cfg.bedrock.model_id.clone(),
            max_output_tokens: cfg.bedrock.max_output_tokens,
        },
    );

    Ok(Worker::new(
        tams,
        model,
        tads,
        FfmpegAssembler::default(),
        cfg,
    ))
}

/// Hard cap on the inline base64 clip Pegasus accepts (`mediaSource.base64String`): 25 MB.
const MAX_BASE64_BYTES: usize = 25 * 1024 * 1024;

/// Errors from processing one message.
#[derive(Debug, Error)]
pub enum WorkerError {
    #[error("invalid segments_added payload: {0}")]
    Payload(String),
    #[error("flow {0} has no resolvable source")]
    NoSource(String),
    #[error("segment {0} has no media url")]
    NoMedia(String),
    #[error("assembled clip too large for inline base64: {0} bytes")]
    ClipTooLarge(usize),
    #[error("media assembly failed: {0}")]
    Media(String),
    #[error("payload schema `{0}` is not seeded in TADS")]
    SchemaNotFound(String),
    #[error("writeback rejected: {0}")]
    Writeback(String),
    #[error(transparent)]
    Tams(#[from] vu_tams::TamsError),
    #[error(transparent)]
    Infer(#[from] vu_infer::InferError),
    #[error(transparent)]
    Tads(#[from] vu_tads::TadsError),
}

/// The outcome of analysing one message, before any TADS writes.
#[derive(Debug, Clone)]
pub struct AnalysisPlan {
    pub track_id: TadsId,
    pub source_entity_id: String,
    pub events: Vec<TrackEvent>,
}

/// Orchestrates one analysis message end to end.
pub struct Worker<T, M, D, A> {
    tams: T,
    model: M,
    tads: D,
    assembler: A,
    cfg: WorkerConfig,
}

impl<T, M, D, A> Worker<T, M, D, A>
where
    T: TamsApi,
    M: KeyMomentModel,
    D: TadsApi,
    A: MediaAssembler,
{
    pub fn new(tams: T, model: M, tads: D, assembler: A, cfg: WorkerConfig) -> Self {
        Self {
            tams,
            model,
            tads,
            assembler,
            cfg,
        }
    }

    /// Resolve, window, assemble, infer, and map — with **no** TADS writes. Shared by the live and
    /// dry-run paths (and used by the local dry-run CLI to preview Events).
    #[tracing::instrument(skip_all)]
    pub async fn analyze(&self, body: &str) -> Result<AnalysisPlan, WorkerError> {
        let event: SegmentsAddedEvent =
            serde_json::from_str(body).map_err(|e| WorkerError::Payload(e.to_string()))?;

        // Resolve the Source and derive deterministic schema/track ids.
        let source_id = match event.source_id.clone() {
            Some(s) => s,
            None => self
                .tams
                .resolve_source_id(&event.flow_id)
                .await?
                .ok_or_else(|| WorkerError::NoSource(event.flow_id.clone()))?,
        };
        let source_entity_id = source_entity_id(&self.cfg.tams.base_url, &source_id);
        let track_id = derive_track_id(&source_entity_id, KEY_MOMENT_ANALYSIS_TYPE);

        if event.segments.is_empty() {
            tracing::info!(flow_id = %event.flow_id, "no new segments; nothing to analyse");
            return Ok(AnalysisPlan {
                track_id,
                source_entity_id,
                events: Vec::new(),
            });
        }

        // Build the look-back window (context) around the new segments.
        let new_ids: HashSet<String> = event.segments.iter().map(|s| s.object_id.clone()).collect();
        let query = window::query_timerange(&event.segments, self.cfg.window.lookback_n);
        let fetched = self
            .tams
            .get_flow_segments(&event.flow_id, query.as_deref())
            .await?;
        let sorted = window::sort_segments(if fetched.is_empty() {
            event.segments.clone()
        } else {
            fetched
        });
        let win = window::select_window(sorted, &new_ids, self.cfg.window.lookback_n);

        // Assemble the clip, infer, and map to Events.
        let media_base64 = self.assemble_clip(&win).await?;
        let raw = self.model.analyze_key_moments(&media_base64).await?;
        let generated_at = chrono::Utc::now().to_rfc3339();
        let prov = ProvenanceCtx {
            model_id: &self.cfg.bedrock.model_id,
            generated_at: &generated_at,
            tams_host: &self.cfg.tams.base_url,
            flow_id: &event.flow_id,
        };
        let events = window::assign_events(&win, raw, track_id, &prov);
        Ok(AnalysisPlan {
            track_id,
            source_entity_id,
            events,
        })
    }

    /// Process one `flows/segments_added` message body: analyse, then (unless `dry_run`) ensure the
    /// Payload Schema + Track exist and POST the Events.
    #[tracing::instrument(skip_all)]
    pub async fn process_message(&self, body: &str) -> Result<(), WorkerError> {
        let plan = self.analyze(body).await?;
        if plan.events.is_empty() {
            return Ok(());
        }

        if self.cfg.dry_run {
            tracing::info!(
                track_id = %plan.track_id,
                schema_external_id = %self.cfg.key_moment.schema_external_id,
                source = %plan.source_entity_id,
                events = plan.events.len(),
                "dry-run: analysed only, NOT writing to TADS"
            );
            for ev in &plan.events {
                tracing::info!(
                    event_id = %ev.id,
                    timerange = %ev.timerange,
                    title = ev.payload.get("title").and_then(|v| v.as_str()).unwrap_or(""),
                    "dry-run key moment"
                );
            }
            return Ok(());
        }

        let outcomes = self.write(&plan).await?;
        let rejected = outcomes.iter().filter(|o| !o.is_success()).count();
        if rejected > 0 {
            return Err(WorkerError::Writeback(format!(
                "{rejected} event(s) rejected"
            )));
        }
        Ok(())
    }

    /// Perform the TADS writes for a plan: resolve the pre-seeded Payload Schema by external_id
    /// (never created here), ensure the (TAMS-compatible) Track references it, and POST the Events.
    /// Returns the per-event outcomes (the caller decides whether a rejection is fatal).
    pub async fn write(&self, plan: &AnalysisPlan) -> Result<Vec<EventOutcome>, WorkerError> {
        if plan.events.is_empty() {
            return Ok(Vec::new());
        }
        let external_id = &self.cfg.key_moment.schema_external_id;
        let schema_id = self
            .tads
            .resolve_schema_id(external_id)
            .await?
            .ok_or_else(|| WorkerError::SchemaNotFound(external_id.clone()))?;
        self.tads
            .ensure_track(&key_moment_track(
                plan.track_id,
                schema_id,
                &plan.source_entity_id,
            ))
            .await?;

        tracing::info!(track_id = %plan.track_id, events = plan.events.len(), "writing key-moment events");
        Ok(self.tads.post_events(plan.track_id, &plan.events).await?)
    }

    /// Explicit re-analysis: delete the given Event ids, wait for the async delete to complete, then
    /// recreate. Needed because `editable_events=false` forbids in-place upsert.
    pub async fn reanalyze(
        &self,
        track_id: TadsId,
        event_ids: &[TadsId],
        events: &[TrackEvent],
    ) -> Result<(), WorkerError> {
        if let Some(handle) = self.tads.delete_events_by_ids(track_id, event_ids).await? {
            self.tads.wait_delete_complete(&handle).await?;
        }
        self.write_events(track_id, events).await
    }

    async fn write_events(
        &self,
        track_id: TadsId,
        events: &[TrackEvent],
    ) -> Result<(), WorkerError> {
        if events.is_empty() {
            return Ok(());
        }
        let outcomes = self.tads.post_events(track_id, events).await?;
        // Created / Replaced / AlreadyPresent all count as success (the latter is the idempotent
        // redelivery case under editable_events=false). Only genuine rejections fail the message.
        let rejected = outcomes.iter().filter(|o| !o.is_success()).count();
        if rejected > 0 {
            return Err(WorkerError::Writeback(format!(
                "{rejected} event(s) rejected"
            )));
        }
        Ok(())
    }

    /// Fetch the window's segment media, assemble one clip, and base64-encode it. If the inline cap
    /// is exceeded, drop the oldest *context* segments (never the new ones) and retry.
    async fn assemble_clip(&self, win: &window::Window) -> Result<String, WorkerError> {
        let mut blobs: Vec<(bool, Vec<u8>)> = Vec::with_capacity(win.segments.len());
        for s in &win.segments {
            let url = s
                .media_url()
                .ok_or_else(|| WorkerError::NoMedia(s.object_id.clone()))?;
            let bytes = self.tams.fetch_media(url).await?;
            blobs.push((win.new_object_ids.contains(&s.object_id), bytes));
        }

        loop {
            let clips: Vec<Vec<u8>> = blobs.iter().map(|(_, b)| b.clone()).collect();
            let assembled = self
                .assembler
                .assemble(&clips)
                .await
                .map_err(WorkerError::Media)?;
            let b64 = base64::engine::general_purpose::STANDARD.encode(&assembled);
            if b64.len() <= MAX_BASE64_BYTES {
                return Ok(b64);
            }
            match blobs.iter().position(|(is_new, _)| !is_new) {
                Some(pos) => {
                    tracing::warn!(
                        bytes = b64.len(),
                        "clip over inline cap; dropping oldest context segment"
                    );
                    blobs.remove(pos);
                }
                None => return Err(WorkerError::ClipTooLarge(b64.len())),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use tads_domain::{PayloadSchema, Track};
    use vu_infer::{InferError, RawMoment};
    use vu_tads::{DeleteHandle, EventOutcome, TadsError};
    use vu_tams::{FlowSegment, GetUrl, TamsError};

    fn seg(object_id: &str, range: &str) -> FlowSegment {
        FlowSegment {
            object_id: object_id.into(),
            timerange: range.parse().unwrap(),
            get_urls: vec![GetUrl {
                url: format!("https://media/{object_id}"),
                label: None,
                presigned: None,
            }],
        }
    }

    struct FakeTams {
        segments: Vec<FlowSegment>,
        source_id: Option<String>,
    }
    impl TamsApi for FakeTams {
        async fn get_flow_segments(
            &self,
            _flow_id: &str,
            _timerange: Option<&str>,
        ) -> Result<Vec<FlowSegment>, TamsError> {
            Ok(self.segments.clone())
        }
        async fn resolve_source_id(&self, _flow_id: &str) -> Result<Option<String>, TamsError> {
            Ok(self.source_id.clone())
        }
        async fn fetch_media(&self, _url: &str) -> Result<Vec<u8>, TamsError> {
            Ok(b"fake-media".to_vec())
        }
    }

    struct FakeModel {
        moments: Vec<RawMoment>,
    }
    impl KeyMomentModel for FakeModel {
        async fn analyze_key_moments(&self, _b64: &str) -> Result<Vec<RawMoment>, InferError> {
            Ok(self.moments.clone())
        }
    }

    struct ConcatAssembler;
    impl MediaAssembler for ConcatAssembler {
        async fn assemble(&self, clips: &[Vec<u8>]) -> Result<Vec<u8>, String> {
            Ok(clips.concat())
        }
    }

    #[derive(Clone, Copy)]
    enum PostMode {
        Created,
        AlreadyPresent,
        RejectAll,
    }

    #[derive(Default)]
    struct Recorder {
        ensured_track: Option<Track>,
        posted: Vec<Vec<TrackEvent>>,
        deleted: Vec<Vec<TadsId>>,
        waited: usize,
    }

    struct FakeTads {
        rec: Arc<Mutex<Recorder>>,
        mode: PostMode,
    }
    impl TadsApi for FakeTads {
        async fn resolve_schema_id(&self, _external_id: &str) -> Result<Option<TadsId>, TadsError> {
            Ok(Some(TadsId::new_v4()))
        }
        async fn ensure_schema(&self, _schema: &PayloadSchema) -> Result<(), TadsError> {
            Ok(())
        }
        async fn ensure_track(&self, track: &Track) -> Result<(), TadsError> {
            self.rec.lock().unwrap().ensured_track = Some(track.clone());
            Ok(())
        }
        async fn post_events(
            &self,
            _track_id: TadsId,
            events: &[TrackEvent],
        ) -> Result<Vec<EventOutcome>, TadsError> {
            self.rec.lock().unwrap().posted.push(events.to_vec());
            let outcomes = events
                .iter()
                .map(|e| match self.mode {
                    PostMode::Created => EventOutcome::Created(e.id),
                    PostMode::AlreadyPresent => EventOutcome::AlreadyPresent(Some(e.id)),
                    PostMode::RejectAll => EventOutcome::Rejected {
                        id: Some(e.id),
                        status: 400,
                        summary: "overlap".into(),
                    },
                })
                .collect();
            Ok(outcomes)
        }
        async fn delete_events_by_ids(
            &self,
            _track_id: TadsId,
            ids: &[TadsId],
        ) -> Result<Option<DeleteHandle>, TadsError> {
            self.rec.lock().unwrap().deleted.push(ids.to_vec());
            Ok(Some(DeleteHandle {
                location: "/track-delete-requests/x".into(),
            }))
        }
        async fn wait_delete_complete(&self, _h: &DeleteHandle) -> Result<(), TadsError> {
            self.rec.lock().unwrap().waited += 1;
            Ok(())
        }
    }

    fn cfg() -> WorkerConfig {
        serde_json::from_value(serde_json::json!({
            "tams": { "base_url": "https://tams.example.com" },
            "tads": {
                "base_url": "https://tads.example.com",
                "oauth": { "token_url": "https://idp/token", "client_id": "id", "client_secret": "shh" }
            },
            "bedrock": { "model_id": "us.twelvelabs.pegasus-1-5-v1:0", "max_output_tokens": 4096 },
            "window": { "lookback_n": 2 },
            "key_moment": { "schema_external_id": "vu:key-moment:v1" }
        }))
        .unwrap()
    }

    fn raw(start: f64, end: f64, title: &str) -> RawMoment {
        RawMoment {
            start_sec: start,
            end_sec: end,
            title: title.into(),
            moment_type: Some("highlight".into()),
            confidence: Some(0.5),
            description: Some(format!("{title} description")),
        }
    }

    fn body() -> String {
        serde_json::json!({
            "flow_id": "f-1",
            "source_id": "s-1",
            "segments": [{ "object_id": "new", "timerange": "[10:0_14:0)", "get_urls": [{ "url": "https://media/new" }] }]
        })
        .to_string()
    }

    fn worker(
        mode: PostMode,
        moments: Vec<RawMoment>,
    ) -> (
        Worker<FakeTams, FakeModel, FakeTads, ConcatAssembler>,
        Arc<Mutex<Recorder>>,
    ) {
        let rec = Arc::new(Mutex::new(Recorder::default()));
        let tams = FakeTams {
            segments: vec![seg("ctx", "[6:0_10:0)"), seg("new", "[10:0_14:0)")],
            source_id: Some("s-1".into()),
        };
        let w = Worker::new(
            tams,
            FakeModel { moments },
            FakeTads {
                rec: rec.clone(),
                mode,
            },
            ConcatAssembler,
            cfg(),
        );
        (w, rec)
    }

    #[tokio::test]
    async fn happy_path_ensures_tams_compatible_track_and_posts_new_segment_events() {
        // window_start = 6s (ctx). rel 4..7 -> abs [10:0_13:0) in the new segment.
        let (w, rec) = worker(PostMode::Created, vec![raw(4.0, 7.0, "goal")]);
        w.process_message(&body()).await.unwrap();

        let r = rec.lock().unwrap();
        let track = r.ensured_track.as_ref().expect("track ensured");
        assert!(!track.allow_overlaps);
        assert!(!track.editable_events);
        assert_eq!(
            track.timeline_reference.entity_id,
            "tams://tams.example.com/sources/s-1"
        );
        assert_eq!(r.posted.len(), 1);
        assert_eq!(r.posted[0].len(), 1);
        assert_eq!(r.posted[0][0].timerange.to_string(), "[10:0_13:0)");
    }

    #[tokio::test]
    async fn dry_run_analyses_but_writes_nothing() {
        let rec = Arc::new(Mutex::new(Recorder::default()));
        let tams = FakeTams {
            segments: vec![seg("ctx", "[6:0_10:0)"), seg("new", "[10:0_14:0)")],
            source_id: Some("s-1".into()),
        };
        let mut c = cfg();
        c.dry_run = true;
        let w = Worker::new(
            tams,
            FakeModel {
                moments: vec![raw(4.0, 7.0, "goal")],
            },
            FakeTads {
                rec: rec.clone(),
                mode: PostMode::Created,
            },
            ConcatAssembler,
            c,
        );

        // analyze() still produces the Events...
        let plan = w.analyze(&body()).await.unwrap();
        assert_eq!(plan.events.len(), 1);
        // ...but process_message in dry-run must not touch TADS.
        w.process_message(&body()).await.unwrap();
        let r = rec.lock().unwrap();
        assert!(r.ensured_track.is_none(), "dry-run must not ensure a track");
        assert!(r.posted.is_empty(), "dry-run must not post events");
    }

    #[tokio::test]
    async fn redelivery_already_present_is_success() {
        let (w, _rec) = worker(PostMode::AlreadyPresent, vec![raw(4.0, 7.0, "goal")]);
        // 403-on-existing must not fail the message.
        assert!(w.process_message(&body()).await.is_ok());
    }

    #[tokio::test]
    async fn rejected_events_fail_the_message() {
        let (w, _rec) = worker(PostMode::RejectAll, vec![raw(4.0, 7.0, "goal")]);
        let err = w.process_message(&body()).await.unwrap_err();
        assert!(matches!(err, WorkerError::Writeback(_)));
    }

    #[tokio::test]
    async fn moments_only_in_context_produce_no_write() {
        // rel 0..2 -> abs [6:0_8:0) which is the context segment -> skipped; no post.
        let (w, rec) = worker(PostMode::Created, vec![raw(0.0, 2.0, "ctx-only")]);
        w.process_message(&body()).await.unwrap();
        assert!(rec.lock().unwrap().posted.is_empty(), "no events to post");
    }

    #[tokio::test]
    async fn reanalyze_deletes_waits_then_recreates() {
        let (w, rec) = worker(PostMode::Created, vec![]);
        let track = TadsId::new_v4();
        let ids = vec![TadsId::new_v4()];
        let events = {
            // Build one event to recreate.
            let m = raw(0.0, 1.0, "x");
            let payload = vu_domain::KeyMomentPayload {
                title: m.title,
                kind: vu_domain::MomentType::NotableEvent,
                confidence: 0.5,
                description: m.description,
                evidence: Vec::new(),
                provenance: None,
            };
            vec![payload.into_track_event(ids[0], "[0:0_1:0)".parse().unwrap())]
        };
        w.reanalyze(track, &ids, &events).await.unwrap();

        let r = rec.lock().unwrap();
        assert_eq!(r.deleted.len(), 1, "delete issued");
        assert_eq!(r.waited, 1, "waited for delete to complete");
        assert_eq!(r.posted.len(), 1, "recreated after delete");
    }
}
