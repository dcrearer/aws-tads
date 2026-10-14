//! Pure windowing + moment-to-Event mapping.
//!
//! The look-back window is **inference context**: we send N consecutive segments to Pegasus so it
//! sees moments that span boundaries, but we persist only the moments anchored in the message's
//! *newly-added* segment(s). Each segment owns a disjoint span, so stored Events never overlap
//! across messages (satisfying `allow_overlaps=false`); within a segment the moments are clipped to
//! non-overlapping. Event ids are keyed on the owning segment's immutable TAI anchor + ordinal, so
//! the mapping is deterministic.

use std::collections::HashSet;

use tads_domain::timerange::Timestamp;
use tads_domain::{TadsId, TimeRange, TrackEvent};
use vu_domain::{
    KeyMomentPayload, Moment, MomentType, Provenance, clip_non_overlapping, derive_event_id,
};
use vu_infer::RawMoment;
use vu_tams::FlowSegment;
use vu_time::relative_to_absolute;

const NANOS_PER_SEC: i128 = 1_000_000_000;

/// A selected look-back window: the context segments (chronological) and which of them are new.
#[derive(Debug, Clone)]
pub struct Window {
    pub segments: Vec<FlowSegment>,
    pub new_object_ids: HashSet<String>,
}

impl Window {
    /// Absolute TAI start of the clip (start of the first segment in the window).
    pub fn start(&self) -> Option<Timestamp> {
        self.segments.first().and_then(FlowSegment::segment_anchor)
    }

    /// Timerange covering the whole window `[first.start, last.end)`.
    pub fn timerange(&self) -> Option<TimeRange> {
        let first = self.segments.first()?;
        let last = self.segments.last()?;
        Some(TimeRange {
            start: first.timerange.start,
            end: last.timerange.end,
            empty: false,
        })
    }

    fn new_segments(&self) -> impl Iterator<Item = &FlowSegment> {
        self.segments
            .iter()
            .filter(|s| self.new_object_ids.contains(&s.object_id))
    }
}

/// Sort segments chronologically by their lower bound.
pub fn sort_segments(mut segments: Vec<FlowSegment>) -> Vec<FlowSegment> {
    segments.sort_by_key(|s| s.timerange.lower_nanos());
    segments
}

/// Build the TAMS query timerange that is sure to include the look-back context: from
/// `(lookback_n - 1)` segment-durations before the earliest new segment, to the end of the latest.
/// Returns `None` if there are no (bounded) new segments.
pub fn query_timerange(new_segments: &[FlowSegment], lookback_n: usize) -> Option<String> {
    let mut new_lo = i128::MAX;
    let mut new_hi = i128::MIN;
    let mut max_dur = 0i128;
    for s in new_segments {
        if s.timerange.empty || s.timerange.start.is_none() {
            continue;
        }
        let lo = s.timerange.lower_nanos();
        let hi = s.timerange.upper_nanos();
        new_lo = new_lo.min(lo);
        new_hi = new_hi.max(hi);
        max_dur = max_dur.max(hi - lo);
    }
    if new_lo == i128::MAX || new_hi == i128::MIN {
        return None;
    }
    let dur = max_dur.max(NANOS_PER_SEC); // at least 1s of look-back granularity
    let back = (lookback_n.saturating_sub(1)) as i128 * dur;
    let lo = (new_lo - back).max(0);
    Some(
        TimeRange {
            start: Some(bound(lo, true)),
            end: Some(bound(new_hi, false)),
            empty: false,
        }
        .to_string(),
    )
}

fn bound(nanos: i128, inclusive: bool) -> tads_domain::timerange::Bound {
    tads_domain::timerange::Bound {
        ts: Timestamp::new(
            nanos.div_euclid(NANOS_PER_SEC) as i64,
            nanos.rem_euclid(NANOS_PER_SEC) as u32,
        ),
        inclusive,
    }
}

/// Select the window = the (up to) `lookback_n` segments ending at the last new segment. If no new
/// segment is present in `sorted`, the whole list is used as the window.
pub fn select_window(
    sorted: Vec<FlowSegment>,
    new_object_ids: &HashSet<String>,
    lookback_n: usize,
) -> Window {
    let last_new = sorted
        .iter()
        .rposition(|s| new_object_ids.contains(&s.object_id));
    let segments = match last_new {
        Some(last) => {
            let start = (last + 1).saturating_sub(lookback_n.max(1));
            sorted[start..=last].to_vec()
        }
        None => sorted,
    };
    Window {
        segments,
        new_object_ids: new_object_ids.clone(),
    }
}

/// Default confidence when the model omits it (the field is required by the schema).
const DEFAULT_CONFIDENCE: f64 = 0.5;

/// Provenance context shared by every moment in one analysis run. `source` is built per-Event from
/// the owning segment so a moment traces to its exact media object.
#[derive(Debug, Clone, Copy)]
pub struct ProvenanceCtx<'a> {
    pub model_id: &'a str,
    pub generated_at: &'a str,
    pub tams_host: &'a str,
    pub flow_id: &'a str,
}

fn build_payload(
    raw: &RawMoment,
    prov: &ProvenanceCtx<'_>,
    owner_object_id: &str,
) -> KeyMomentPayload {
    let source = vu_tams::flow_media_object_uri(prov.tams_host, prov.flow_id, owner_object_id);
    KeyMomentPayload {
        title: raw.title.clone(),
        kind: raw
            .moment_type
            .as_deref()
            .map(MomentType::parse_lenient)
            .unwrap_or(MomentType::NotableEvent),
        confidence: raw.confidence.unwrap_or(DEFAULT_CONFIDENCE).clamp(0.0, 1.0),
        description: raw.description.clone(),
        evidence: Vec::new(),
        provenance: Some(Provenance {
            model: Some(prov.model_id.to_string()),
            model_version: None,
            source: Some(source),
            generated_at: Some(prov.generated_at.to_string()),
        }),
    }
}

/// Map raw Pegasus moments onto absolute, non-overlapping Track Events — keeping only those
/// anchored in a newly-added segment, clipped to that segment, with deterministic ids.
pub fn assign_events(
    window: &Window,
    raw: Vec<RawMoment>,
    track_id: TadsId,
    prov: &ProvenanceCtx<'_>,
) -> Vec<TrackEvent> {
    let Some(window_start) = window.start() else {
        return Vec::new();
    };

    // Group moments by the new segment that owns their absolute start.
    let new_segs: Vec<&FlowSegment> = window.new_segments().collect();
    let mut by_owner: Vec<(usize, Vec<Moment>)> =
        new_segs.iter().map(|_| (0usize, Vec::new())).collect();

    for rm in &raw {
        let abs = relative_to_absolute(window_start, rm.start_sec, rm.end_sec);
        let start_ns = abs.lower_nanos();
        let owner_idx = new_segs.iter().position(|s| {
            !s.timerange.empty
                && s.timerange.lower_nanos() <= start_ns
                && start_ns < s.timerange.upper_nanos()
        });
        let Some(idx) = owner_idx else {
            continue; // Moment starts in a context (non-new) segment — not persisted.
        };
        if new_segs[idx].segment_anchor().is_none() {
            continue;
        }
        by_owner[idx].1.push(Moment {
            timerange: abs,
            payload: build_payload(rm, prov, &new_segs[idx].object_id),
        });
    }

    let mut events = Vec::new();
    for (idx, (_, moments)) in by_owner.into_iter().enumerate() {
        let owner = new_segs[idx];
        let anchor = match owner.segment_anchor() {
            Some(a) => a,
            None => continue,
        };
        let clipped = clip_non_overlapping(moments, owner.timerange);
        for (ordinal, m) in clipped.into_iter().enumerate() {
            let id = derive_event_id(track_id, anchor, ordinal as u32);
            events.push(m.payload.into_track_event(id, m.timerange));
        }
    }
    events
}

#[cfg(test)]
mod tests {
    use super::*;
    use vu_tams::GetUrl;

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

    fn raw(start: f64, end: f64, title: &str) -> RawMoment {
        RawMoment {
            start_sec: start,
            end_sec: end,
            title: title.into(),
            moment_type: Some("highlight".into()),
            confidence: Some(0.8),
            description: Some(format!("{title} description")),
        }
    }

    const PROV: ProvenanceCtx<'static> = ProvenanceCtx {
        model_id: "model-x",
        generated_at: "2026-10-13T23:00:00Z",
        tams_host: "host",
        flow_id: "f-1",
    };

    #[test]
    fn query_timerange_extends_back_by_lookback() {
        let tr = query_timerange(&[seg("n", "[10:0_12:0)")], 3).unwrap();
        // seg_dur = 2s; back = (3-1)*2 = 4s; lo = 10-4 = 6s.
        assert_eq!(tr, "[6:0_12:0)");
    }

    #[test]
    fn select_window_takes_lookback_segments_ending_at_new() {
        let sorted = vec![
            seg("a", "[0:0_2:0)"),
            seg("b", "[2:0_4:0)"),
            seg("c", "[4:0_6:0)"),
            seg("d", "[6:0_8:0)"),
        ];
        let new: HashSet<String> = ["d".to_string()].into_iter().collect();
        let win = select_window(sorted, &new, 3);
        let ids: Vec<&str> = win.segments.iter().map(|s| s.object_id.as_str()).collect();
        assert_eq!(ids, vec!["b", "c", "d"]);
        assert_eq!(win.start(), Some(Timestamp::new(2, 0)));
        assert_eq!(
            win.timerange().map(|t| t.to_string()).as_deref(),
            Some("[2:0_8:0)")
        );
    }

    #[test]
    fn only_moments_in_new_segment_are_persisted_and_clipped() {
        // Window: context [6:0_10:0) then new [10:0_14:0). Clip assembled from 6s, so window_start=6s.
        let sorted = vec![seg("ctx", "[6:0_10:0)"), seg("new", "[10:0_14:0)")];
        let new: HashSet<String> = ["new".to_string()].into_iter().collect();
        let win = select_window(sorted, &new, 2);
        let track = TadsId::new_v4();

        let moments = vec![
            raw(1.0, 3.0, "in-context"), // abs [7:0_9:0) -> context segment -> skipped
            raw(4.0, 7.0, "spans-into-new"), // abs [10:0_13:0) -> new segment
            raw(6.0, 9.0, "overlaps-prev"), // abs [12:0_15:0) -> new; clipped to [13:0_14:0)
        ];
        let events = assign_events(&win, moments, track, &PROV);
        assert_eq!(events.len(), 2, "only new-segment moments persist");

        // Non-overlapping and within the owning segment.
        assert_eq!(events[0].timerange.to_string(), "[10:0_13:0)");
        assert_eq!(events[1].timerange.to_string(), "[13:0_14:0)");

        // Deterministic ids keyed on the new segment's anchor (10:0) + ordinal.
        let anchor = Timestamp::new(10, 0);
        assert_eq!(events[0].id, derive_event_id(track, anchor, 0));
        assert_eq!(events[1].id, derive_event_id(track, anchor, 1));

        // Payload conforms to the schema shape and records provenance.
        assert_eq!(events[0].payload["type"], serde_json::json!("highlight"));
        assert_eq!(events[0].payload["confidence"], serde_json::json!(0.8));
        assert_eq!(
            events[0].payload["provenance"]["model"],
            serde_json::json!("model-x")
        );
        // Per-Event provenance.source points at the owning segment's media object (object_id "new").
        assert_eq!(
            events[0].payload["provenance"]["source"],
            serde_json::json!("tams://host/flows/f-1/media-objects/new")
        );
    }

    #[test]
    fn reanalysis_of_same_window_yields_identical_ids() {
        let sorted = vec![seg("new", "[10:0_14:0)")];
        let new: HashSet<String> = ["new".to_string()].into_iter().collect();
        let win = select_window(sorted, &new, 1);
        let track = TadsId::new_v4();
        let a = assign_events(&win, vec![raw(0.0, 2.0, "m")], track, &PROV);
        let b = assign_events(&win, vec![raw(0.0, 2.0, "m")], track, &PROV);
        assert_eq!(a[0].id, b[0].id);
    }
}
