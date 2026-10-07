//! TimeRange ↔ Postgres `int8range` / `bigint` mapping.
//!
//! TAI nanoseconds are stored as `bigint` (`seconds*1e9 + nanos`), matching the reference
//! implementation. The domain [`TimeRange`] exposes `i128` bounds with `i128::MIN`/`i128::MAX`
//! sentinels for unboundedness (eternity); those map to open `int8range` bounds here.

use std::ops::Bound;

use sqlx::postgres::types::PgRange;
use tads_domain::TimeRange;

/// Clamp an `i128` nanosecond bound to `i64`. Finite TAI values fit comfortably; the sentinels
/// saturate (and are treated as unbounded by [`pg_range`]).
pub fn ns_to_i64(n: i128) -> i64 {
    n.clamp(i64::MIN as i128, i64::MAX as i128) as i64
}

/// The inclusive-lower scalar used for ordering / the `start_ns` column.
pub fn start_ns(tr: &TimeRange) -> i64 {
    ns_to_i64(tr.lower_nanos())
}

/// The exclusive-upper scalar used for the `end_ns` column.
pub fn end_ns(tr: &TimeRange) -> i64 {
    ns_to_i64(tr.upper_nanos())
}

/// Build the half-open `[lower, upper)` `int8range` for a TimeRange, using open bounds for the
/// eternity sentinels so GiST `&&` overlap behaves correctly at the extremes.
pub fn pg_range(tr: &TimeRange) -> PgRange<i64> {
    let lower = tr.lower_nanos();
    let upper = tr.upper_nanos();
    let start = if lower == i128::MIN {
        Bound::Unbounded
    } else {
        Bound::Included(ns_to_i64(lower))
    };
    let end = if upper == i128::MAX {
        Bound::Unbounded
    } else {
        Bound::Excluded(ns_to_i64(upper))
    };
    PgRange { start, end }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finite_range_is_half_open() {
        let tr: TimeRange = "[0:0_10:0)".parse().unwrap();
        let r = pg_range(&tr);
        assert_eq!(r.start, Bound::Included(0));
        assert_eq!(r.end, Bound::Excluded(10_000_000_000));
        assert_eq!(start_ns(&tr), 0);
        assert_eq!(end_ns(&tr), 10_000_000_000);
    }

    #[test]
    fn eternity_is_unbounded_both_ends() {
        let tr = TimeRange::eternity();
        let r = pg_range(&tr);
        assert_eq!(r.start, Bound::Unbounded);
        assert_eq!(r.end, Bound::Unbounded);
    }

    #[test]
    fn open_start_is_unbounded_lower() {
        let tr: TimeRange = "(5:0_".parse().unwrap();
        let r = pg_range(&tr);
        // exclusive start 5:0 is nudged to 5:0+1ns by lower_nanos()
        assert_eq!(r.start, Bound::Included(5_000_000_001));
        assert_eq!(r.end, Bound::Unbounded);
    }
}
