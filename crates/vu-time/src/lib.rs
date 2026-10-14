//! Map model-relative clip offsets onto the absolute TAI timeline.
//!
//! Pegasus (and video models generally) emit timings in **seconds relative to the start of the
//! clip** they were given. The clip we send is a look-back *window* assembled from TAMS segments,
//! so its first frame sits at a known absolute TAI instant (the window-start). This crate turns a
//! relative `[start_sec, end_sec)` span into an absolute [`TimeRange`] on the flow timeline:
//!
//! ```text
//! absolute = window_start + relative
//! ```
//!
//! Misalignment here means stored Events don't match the media, so the arithmetic is done in
//! integer nanoseconds (via [`Timestamp::as_nanos`]) with no floating-point accumulation beyond the
//! single seconds→nanoseconds conversion of each offset. Pure and I/O-free; reuses the
//! [`tads_domain`] TAI types rather than redefining them.

use tads_domain::TimeRange;
use tads_domain::timerange::{Bound, Timestamp};

const NANOS_PER_SEC: i128 = 1_000_000_000;

/// Convert a wide nanosecond count back into a [`Timestamp`], normalising the fraction to
/// `0..=999_999_999` even for negative instants (Euclidean division).
fn timestamp_from_nanos(nanos: i128) -> Timestamp {
    let seconds = nanos.div_euclid(NANOS_PER_SEC) as i64;
    let fraction = nanos.rem_euclid(NANOS_PER_SEC) as u32;
    Timestamp::new(seconds, fraction)
}

/// Seconds (as emitted by the model) → nanoseconds, rounded to the nearest nanosecond. Negative
/// offsets are clamped to zero: a model timing can never precede the clip it was given.
fn seconds_to_nanos(seconds: f64) -> i128 {
    if !seconds.is_finite() || seconds <= 0.0 {
        return 0;
    }
    (seconds * NANOS_PER_SEC as f64).round() as i128
}

/// Absolute instant for a clip-relative offset in seconds.
pub fn absolute_instant(window_start: Timestamp, relative_sec: f64) -> Timestamp {
    timestamp_from_nanos(window_start.as_nanos() + seconds_to_nanos(relative_sec))
}

/// Map a clip-relative `[start_sec, end_sec)` span onto an absolute, half-open [`TimeRange`]
/// (start inclusive, end exclusive) on the flow timeline.
///
/// If the resolved end is not strictly after the start (a zero- or negative-length model span), the
/// result collapses to an instantaneous range `[start]` so it is still a valid, non-empty Event
/// timerange.
pub fn relative_to_absolute(window_start: Timestamp, start_sec: f64, end_sec: f64) -> TimeRange {
    let start_ns = window_start.as_nanos() + seconds_to_nanos(start_sec);
    let end_ns = window_start.as_nanos() + seconds_to_nanos(end_sec);

    let start = timestamp_from_nanos(start_ns);
    if end_ns <= start_ns {
        return TimeRange::instant(start);
    }
    TimeRange {
        start: Some(Bound {
            ts: start,
            inclusive: true,
        }),
        end: Some(Bound {
            ts: timestamp_from_nanos(end_ns),
            inclusive: false,
        }),
        empty: false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const WINDOW: Timestamp = Timestamp {
        seconds: 1_700_000_000,
        nanoseconds: 0,
    };

    #[test]
    fn zero_offset_maps_to_window_start() {
        let tr = relative_to_absolute(WINDOW, 0.0, 2.0);
        assert_eq!(tr.start.unwrap().ts, WINDOW);
        assert!(tr.start.unwrap().inclusive);
        assert_eq!(tr.end.unwrap().ts, Timestamp::new(1_700_000_002, 0));
        assert!(!tr.end.unwrap().inclusive);
    }

    #[test]
    fn fractional_seconds_become_nanoseconds() {
        // 1.5s offset -> +1_500_000_000 ns; 2.25s -> +2_250_000_000 ns.
        let tr = relative_to_absolute(WINDOW, 1.5, 2.25);
        assert_eq!(
            tr.start.unwrap().ts,
            Timestamp::new(1_700_000_001, 500_000_000)
        );
        assert_eq!(
            tr.end.unwrap().ts,
            Timestamp::new(1_700_000_002, 250_000_000)
        );
    }

    #[test]
    fn offset_carries_into_seconds() {
        // 0.75s + 0.75s worth of nanoseconds must carry correctly into whole seconds.
        let anchor = Timestamp::new(10, 800_000_000);
        let tr = relative_to_absolute(anchor, 0.3, 0.5);
        assert_eq!(tr.start.unwrap().ts, Timestamp::new(11, 100_000_000));
        assert_eq!(tr.end.unwrap().ts, Timestamp::new(11, 300_000_000));
    }

    #[test]
    fn window_spanning_offsets_round_trip_via_timerange_string() {
        let tr = relative_to_absolute(WINDOW, 3.0, 9.0);
        let s = tr.to_string();
        assert_eq!(s, "[1700000003:0_1700000009:0)");
    }

    #[test]
    fn zero_length_span_collapses_to_instant() {
        let tr = relative_to_absolute(WINDOW, 5.0, 5.0);
        assert!(tr.is_instant());
        assert_eq!(tr.start.unwrap().ts, Timestamp::new(1_700_000_005, 0));
    }

    #[test]
    fn negative_or_inverted_offsets_are_clamped() {
        // Negative start clamps to the window start; inverted end collapses to an instant.
        let tr = relative_to_absolute(WINDOW, -4.0, 2.0);
        assert_eq!(tr.start.unwrap().ts, WINDOW);
        let inverted = relative_to_absolute(WINDOW, 8.0, 1.0);
        assert!(inverted.is_instant());
    }

    #[test]
    fn absolute_instant_helper() {
        assert_eq!(
            absolute_instant(WINDOW, 2.5),
            Timestamp::new(1_700_000_002, 500_000_000)
        );
    }
}
