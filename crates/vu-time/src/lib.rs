//! Segment-relative → absolute TAI timerange mapping for video-understanding events.
//!
//! Inference models emit timings as *relative seconds* within the clip they were handed (e.g.
//! "3.2s into this segment"). TADS Events need an *absolute* TAI [`TimeRange`] on the media
//! timeline. The authoritative anchor is the TAMS segment's own start timestamp (from the
//! `flows/segments_added` segment `timerange`), so:
//!
//! ```text
//! absolute = segment_start + relative
//! ```
//!
//! This crate owns that mapping and nothing else — it is pure, AWS-free, and reuses the TAI types
//! from [`tads_domain`] rather than re-implementing them (both crates share one workspace). Getting
//! this wrong produces events that don't line up with the media, so it is tested first (TDD).

use tads_domain::timerange::Bound;
use tads_domain::{TimeRange, Timestamp};

/// Nanoseconds in one second — the TAI tick resolution used throughout.
pub const NANOS_PER_SEC: i128 = 1_000_000_000;

/// Errors from mapping model-relative seconds onto the absolute timeline.
#[derive(Debug, Clone, Copy, PartialEq, thiserror::Error)]
pub enum VuTimeError {
    /// A relative offset was not finite (NaN/∞) or was negative — model timings are measured
    /// forward from the start of the clip, so they must be finite and `>= 0`.
    #[error("relative seconds must be finite and non-negative (start={start}, end={end})")]
    NonFiniteOrNegative { start: f64, end: f64 },
    /// The relative end preceded the relative start — an inverted span.
    #[error("relative end {end} precedes relative start {start}")]
    EndBeforeStart { start: f64, end: f64 },
}

/// Convert a nanosecond count on the TAI line into a [`Timestamp`]. Euclidean division keeps the
/// nanosecond part in `0..NANOS_PER_SEC` even for negative instants.
pub fn timestamp_from_nanos(nanos: i128) -> Timestamp {
    let seconds = nanos.div_euclid(NANOS_PER_SEC) as i64;
    let nanoseconds = nanos.rem_euclid(NANOS_PER_SEC) as u32;
    Timestamp::new(seconds, nanoseconds)
}

/// Convert relative seconds (as a model emits them) into nanoseconds, rounded to the nearest tick.
///
/// Returns `None` for non-finite or negative input (callers map that to [`VuTimeError`]).
fn relative_nanos(seconds: f64) -> Option<i128> {
    if !seconds.is_finite() || seconds < 0.0 {
        return None;
    }
    Some((seconds * NANOS_PER_SEC as f64).round() as i128)
}

/// The absolute instant `segment_start + relative_seconds`.
fn absolute_instant(segment_start: Timestamp, relative_seconds: f64) -> Option<Timestamp> {
    let rel = relative_nanos(relative_seconds)?;
    Some(timestamp_from_nanos(segment_start.as_nanos() + rel))
}

/// Map a model-relative span `[rel_start, rel_end)` (seconds, measured from the start of the
/// segment's clip) onto an absolute half-open TAI [`TimeRange`] `[abs_start_abs_end)`.
///
/// `rel_start == rel_end` collapses to an instantaneous `[t]` range (a zero-length "span" is
/// treated as a point rather than the empty range). Spans and transcript utterances use this;
/// a true point event (a scene cut) should use [`instant`] directly.
pub fn relative_to_absolute(
    segment_start: Timestamp,
    rel_start: f64,
    rel_end: f64,
) -> Result<TimeRange, VuTimeError> {
    let start =
        absolute_instant(segment_start, rel_start).ok_or(VuTimeError::NonFiniteOrNegative {
            start: rel_start,
            end: rel_end,
        })?;
    let end = absolute_instant(segment_start, rel_end).ok_or(VuTimeError::NonFiniteOrNegative {
        start: rel_start,
        end: rel_end,
    })?;
    if end.as_nanos() < start.as_nanos() {
        return Err(VuTimeError::EndBeforeStart {
            start: rel_start,
            end: rel_end,
        });
    }
    if end.as_nanos() == start.as_nanos() {
        return Ok(TimeRange::instant(start));
    }
    Ok(TimeRange {
        start: Some(Bound {
            ts: start,
            inclusive: true,
        }),
        end: Some(Bound {
            ts: end,
            inclusive: false,
        }),
        empty: false,
    })
}

/// Map a single relative offset (seconds) onto an absolute instantaneous `[t]` [`TimeRange`] —
/// the form a point event such as a scene cut uses.
pub fn instant(segment_start: Timestamp, rel_seconds: f64) -> Result<TimeRange, VuTimeError> {
    let ts =
        absolute_instant(segment_start, rel_seconds).ok_or(VuTimeError::NonFiniteOrNegative {
            start: rel_seconds,
            end: rel_seconds,
        })?;
    Ok(TimeRange::instant(ts))
}

#[cfg(test)]
mod tests {
    use super::*;

    // A plausible TAI segment start (2023-09-11T10:46:50Z, as in the spec examples).
    const SEG: Timestamp = Timestamp {
        seconds: 1_694_429_247,
        nanoseconds: 0,
    };

    fn b(ts: Timestamp, inclusive: bool) -> Bound {
        Bound { ts, inclusive }
    }

    #[test]
    fn maps_relative_span_to_absolute_half_open() {
        let r = relative_to_absolute(SEG, 3.0, 7.5).unwrap();
        assert_eq!(r.start.unwrap(), b(Timestamp::new(1_694_429_250, 0), true));
        assert_eq!(
            r.end.unwrap(),
            b(Timestamp::new(1_694_429_254, 500_000_000), false)
        );
    }

    #[test]
    fn sub_second_offsets_round_to_nearest_nanosecond() {
        // 0.25s -> 250_000_000 ns.
        let r = relative_to_absolute(SEG, 0.25, 1.75).unwrap();
        assert_eq!(
            r.start.unwrap().ts,
            Timestamp::new(1_694_429_247, 250_000_000)
        );
        assert_eq!(
            r.end.unwrap().ts,
            Timestamp::new(1_694_429_248, 750_000_000)
        );
    }

    #[test]
    fn equal_bounds_collapse_to_instant() {
        let r = relative_to_absolute(SEG, 2.0, 2.0).unwrap();
        assert!(r.is_instant());
        assert_eq!(r.start.unwrap().ts, Timestamp::new(1_694_429_249, 0));
    }

    #[test]
    fn instant_point_event() {
        let r = instant(SEG, 4.0).unwrap();
        assert!(r.is_instant());
        assert_eq!(r.start.unwrap().ts, Timestamp::new(1_694_429_251, 0));
    }

    #[test]
    fn rejects_inverted_span() {
        assert_eq!(
            relative_to_absolute(SEG, 5.0, 2.0),
            Err(VuTimeError::EndBeforeStart {
                start: 5.0,
                end: 2.0
            })
        );
    }

    #[test]
    fn rejects_negative_and_non_finite() {
        assert!(matches!(
            relative_to_absolute(SEG, -1.0, 2.0),
            Err(VuTimeError::NonFiniteOrNegative { .. })
        ));
        assert!(matches!(
            relative_to_absolute(SEG, 0.0, f64::NAN),
            Err(VuTimeError::NonFiniteOrNegative { .. })
        ));
        assert!(matches!(
            instant(SEG, f64::INFINITY),
            Err(VuTimeError::NonFiniteOrNegative { .. })
        ));
    }

    #[test]
    fn output_round_trips_through_timerange_string() {
        // The mapped range must survive serialization to the TADS wire form and back unchanged.
        let r = relative_to_absolute(SEG, 1.5, 10.25).unwrap();
        let printed = r.to_string();
        let reparsed: TimeRange = printed.parse().unwrap();
        assert_eq!(r, reparsed, "round-trip failed for {printed}");
    }

    #[test]
    fn timestamp_from_nanos_handles_negatives() {
        // -1 ns before the epoch = -1s + 999_999_999ns, not -0s/-1ns.
        let ts = timestamp_from_nanos(-1);
        assert_eq!(ts, Timestamp::new(-1, 999_999_999));
        assert_eq!(ts.as_nanos(), -1);
    }
}
