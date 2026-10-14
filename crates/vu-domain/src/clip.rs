//! Clip a set of moments to a non-overlapping, in-bounds sequence.
//!
//! VU Tracks are created with `allow_overlaps=false`, so the Events we POST for one segment must not
//! overlap each other, and each must sit within the segment's own `[start, end)` span (the rest of
//! the inference window was context only). This is the pure transform that enforces both: it works
//! entirely in half-open nanosecond space and rebuilds canonical `[start_end)` ranges.

use tads_domain::TimeRange;
use tads_domain::timerange::{Bound, Timestamp};

use crate::payloads::Moment;

const NANOS_PER_SEC: i128 = 1_000_000_000;

fn timestamp_from_nanos(nanos: i128) -> Timestamp {
    Timestamp::new(
        nanos.div_euclid(NANOS_PER_SEC) as i64,
        nanos.rem_euclid(NANOS_PER_SEC) as u32,
    )
}

/// Build a half-open `[lo, hi)` range from nanosecond bounds (`hi` strictly greater than `lo`).
fn half_open(lo: i128, hi: i128) -> TimeRange {
    TimeRange {
        start: Some(Bound {
            ts: timestamp_from_nanos(lo),
            inclusive: true,
        }),
        end: Some(Bound {
            ts: timestamp_from_nanos(hi),
            inclusive: false,
        }),
        empty: false,
    }
}

/// Clip `moments` so the result is sorted, pairwise non-overlapping, and contained within
/// `segment_bounds`. Moments are first intersected with the bounds (those falling entirely outside
/// are dropped); then, in start order, each moment's start is pushed to the end of the previous one
/// so no two overlap. A moment wholly swallowed by its predecessor is dropped. Input order is not
/// significant; output is sorted by start.
pub fn clip_non_overlapping(moments: Vec<Moment>, segment_bounds: TimeRange) -> Vec<Moment> {
    let bound_lo = segment_bounds.lower_nanos();
    let bound_hi = segment_bounds.upper_nanos();

    // Intersect each moment with the segment bounds, keeping only non-empty results.
    let mut clipped: Vec<(i128, i128, Moment)> = moments
        .into_iter()
        .filter_map(|m| {
            let lo = m.timerange.lower_nanos().max(bound_lo);
            let hi = m.timerange.upper_nanos().min(bound_hi);
            (lo < hi).then_some((lo, hi, m))
        })
        .collect();

    // Sort by start, then by end so a fully-contained moment follows its container deterministically.
    clipped.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));

    let mut out = Vec::with_capacity(clipped.len());
    let mut last_end = bound_lo;
    for (lo, hi, moment) in clipped {
        let start = lo.max(last_end);
        if start >= hi {
            continue; // Wholly covered by an earlier moment.
        }
        out.push(Moment {
            timerange: half_open(start, hi),
            payload: moment.payload,
        });
        last_end = hi;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::payloads::{KeyMomentPayload, MomentType};

    fn moment(range: &str, title: &str) -> Moment {
        Moment {
            timerange: range.parse().unwrap(),
            payload: KeyMomentPayload {
                title: title.into(),
                kind: MomentType::NotableEvent,
                confidence: 0.5,
                description: None,
                evidence: Vec::new(),
                provenance: None,
            },
        }
    }

    fn ranges(ms: &[Moment]) -> Vec<String> {
        ms.iter().map(|m| m.timerange.to_string()).collect()
    }

    #[test]
    fn disjoint_in_bounds_are_untouched() {
        let out = clip_non_overlapping(
            vec![moment("[0:0_5:0)", "a"), moment("[5:0_10:0)", "b")],
            "[0:0_10:0)".parse().unwrap(),
        );
        assert_eq!(ranges(&out), vec!["[0:0_5:0)", "[5:0_10:0)"]);
    }

    #[test]
    fn overlapping_moments_are_clipped_to_non_overlapping() {
        let out = clip_non_overlapping(
            vec![moment("[0:0_6:0)", "a"), moment("[4:0_10:0)", "b")],
            "[0:0_10:0)".parse().unwrap(),
        );
        // b's start is pushed to a's end.
        assert_eq!(ranges(&out), vec!["[0:0_6:0)", "[6:0_10:0)"]);
    }

    #[test]
    fn boundary_spanning_moment_is_clipped_to_owning_segment() {
        // A moment that runs past the segment end is truncated to the segment bounds.
        let out = clip_non_overlapping(
            vec![moment("[8:0_15:0)", "spans")],
            "[0:0_10:0)".parse().unwrap(),
        );
        assert_eq!(ranges(&out), vec!["[8:0_10:0)"]);
    }

    #[test]
    fn moment_outside_bounds_is_dropped() {
        let out = clip_non_overlapping(
            vec![moment("[20:0_30:0)", "outside")],
            "[0:0_10:0)".parse().unwrap(),
        );
        assert!(out.is_empty());
    }

    #[test]
    fn wholly_covered_moment_is_dropped() {
        let out = clip_non_overlapping(
            vec![moment("[0:0_10:0)", "big"), moment("[2:0_5:0)", "inside")],
            "[0:0_10:0)".parse().unwrap(),
        );
        assert_eq!(ranges(&out), vec!["[0:0_10:0)"]);
    }

    #[test]
    fn output_is_globally_non_overlapping() {
        let out = clip_non_overlapping(
            vec![
                moment("[0:0_4:0)", "a"),
                moment("[3:0_7:0)", "b"),
                moment("[6:0_9:0)", "c"),
            ],
            "[0:0_10:0)".parse().unwrap(),
        );
        for pair in out.windows(2) {
            assert!(!pair[0].timerange.overlaps(&pair[1].timerange));
        }
    }
}
