//! TAI `TimeRange` grammar (`timerange.json`).
//!
//! A TimeRange is a string of up to two TAI timestamps (`seconds:nanoseconds`) with
//! inclusive `[` `]` / exclusive `(` `)` bound markers and a `_` separator:
//!
//! | input                   | meaning                                   |
//! |-------------------------|-------------------------------------------|
//! | `_`                     | eternity (whole timeline)                 |
//! | `()`                    | never (empty)                             |
//! | `[t]` or `t`            | instantaneous at `t` (inclusive only)     |
//! | `[a_b)`                 | from `a` inclusive to `b` exclusive       |
//! | `(5:0_`                 | after `5:0` to eternity                   |
//!
//! Defaults when a marker is omitted: start inclusive, end exclusive. Instantaneous ranges may
//! not use exclusive markers. This type is the single source of truth for the store's sort-key
//! encoding, via [`TimeRange::lower_nanos`] / [`TimeRange::upper_nanos`].

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::error::DomainError;

const NANOS_PER_SEC: i128 = 1_000_000_000;

/// A TAI timestamp: whole seconds (may be negative) plus a nanosecond fraction (0..=999_999_999).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Timestamp {
    pub seconds: i64,
    pub nanoseconds: u32,
}

impl Timestamp {
    pub fn new(seconds: i64, nanoseconds: u32) -> Self {
        Self {
            seconds,
            nanoseconds,
        }
    }

    /// Total nanoseconds since the TAI epoch, as a wide integer that can't overflow.
    pub fn as_nanos(&self) -> i128 {
        self.seconds as i128 * NANOS_PER_SEC + self.nanoseconds as i128
    }

    fn parse(s: &str) -> Result<Self, String> {
        let (sec, nsec) = s
            .split_once(':')
            .ok_or_else(|| format!("timestamp `{s}` missing `:` separator"))?;
        let seconds: i64 = sec
            .parse()
            .map_err(|_| format!("invalid seconds `{sec}`"))?;
        if nsec.len() > 9 {
            return Err(format!("nanoseconds `{nsec}` has more than 9 digits"));
        }
        let nanoseconds: u32 = nsec
            .parse()
            .map_err(|_| format!("invalid nanoseconds `{nsec}`"))?;
        Ok(Self {
            seconds,
            nanoseconds,
        })
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.seconds, self.nanoseconds)
    }
}

/// One end of a range: a timestamp plus whether the bound is inclusive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Bound {
    pub ts: Timestamp,
    pub inclusive: bool,
}

/// A parsed TimeRange. `start`/`end` of `None` mean unbounded on that side (eternity).
/// `empty` marks the "never" range `()`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeRange {
    pub start: Option<Bound>,
    pub end: Option<Bound>,
    pub empty: bool,
}

impl TimeRange {
    /// The whole timeline (`_`).
    pub fn eternity() -> Self {
        Self {
            start: None,
            end: None,
            empty: false,
        }
    }

    /// The empty range (`()`).
    pub fn never() -> Self {
        Self {
            start: None,
            end: None,
            empty: true,
        }
    }

    /// An instantaneous range at `ts` (`[ts]`).
    pub fn instant(ts: Timestamp) -> Self {
        let b = Bound {
            ts,
            inclusive: true,
        };
        Self {
            start: Some(b),
            end: Some(b),
            empty: false,
        }
    }

    pub fn is_eternity(&self) -> bool {
        !self.empty && self.start.is_none() && self.end.is_none()
    }

    pub fn is_instant(&self) -> bool {
        match (self.start, self.end) {
            (Some(s), Some(e)) => s.inclusive && e.inclusive && s.ts == e.ts,
            _ => false,
        }
    }

    /// Effective lower bound in nanoseconds for sort-key encoding. Unbounded start → [`i128::MIN`];
    /// an exclusive start is nudged up by one nanosecond so comparisons stay half-open.
    pub fn lower_nanos(&self) -> i128 {
        match self.start {
            None => i128::MIN,
            Some(b) if b.inclusive => b.ts.as_nanos(),
            Some(b) => b.ts.as_nanos().saturating_add(1),
        }
    }

    /// Effective upper bound in nanoseconds (exclusive) for sort-key encoding. Unbounded end →
    /// [`i128::MAX`]; an inclusive end is nudged up by one nanosecond so the range is half-open.
    pub fn upper_nanos(&self) -> i128 {
        match self.end {
            None => i128::MAX,
            Some(b) if b.inclusive => b.ts.as_nanos().saturating_add(1),
            Some(b) => b.ts.as_nanos(),
        }
    }

    /// Do two ranges share any instant? Uses the half-open `[lower, upper)` projection, so
    /// inclusivity is already accounted for. The "never" range overlaps nothing.
    pub fn overlaps(&self, other: &TimeRange) -> bool {
        if self.empty || other.empty {
            return false;
        }
        self.lower_nanos() < other.upper_nanos() && other.lower_nanos() < self.upper_nanos()
    }

    /// Does this range wholly contain `other`? Backs the delete semantics ("Events that wholly
    /// overlap with this timerange are deleted").
    pub fn contains_range(&self, other: &TimeRange) -> bool {
        if other.empty {
            return true;
        }
        if self.empty {
            return false;
        }
        self.lower_nanos() <= other.lower_nanos() && other.upper_nanos() <= self.upper_nanos()
    }
}

impl FromStr for TimeRange {
    type Err = DomainError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let err = |reason: String| DomainError::InvalidTimeRange {
            input: input.to_string(),
            reason,
        };

        let s = input.trim();
        // Empty string is treated as eternity (the spec default for `timerange` query params).
        if s.is_empty() || s == "_" {
            return Ok(TimeRange::eternity());
        }

        // Peel optional leading/trailing bracket markers.
        let mut body = s;
        let start_marker = match body.chars().next() {
            Some('[') => {
                body = &body[1..];
                Some(true)
            }
            Some('(') => {
                body = &body[1..];
                Some(false)
            }
            _ => None,
        };
        let end_marker = match body.chars().last() {
            Some(']') => {
                body = &body[..body.len() - 1];
                Some(true)
            }
            Some(')') => {
                body = &body[..body.len() - 1];
                Some(false)
            }
            _ => None,
        };

        // `()` (or any empty-body bracketed form) is the "never" range.
        if body.is_empty() {
            return Ok(TimeRange::never());
        }

        match body.split_once('_') {
            None => {
                // Single timestamp → instantaneous. Exclusive markers are not allowed.
                if start_marker == Some(false) || end_marker == Some(false) {
                    return Err(err(
                        "instantaneous ranges cannot use exclusive markers".into()
                    ));
                }
                let ts = Timestamp::parse(body).map_err(err)?;
                Ok(TimeRange::instant(ts))
            }
            Some((left, right)) => {
                let start = if left.is_empty() {
                    None
                } else {
                    Some(Bound {
                        ts: Timestamp::parse(left).map_err(err)?,
                        inclusive: start_marker.unwrap_or(true),
                    })
                };
                let end = if right.is_empty() {
                    None
                } else {
                    Some(Bound {
                        ts: Timestamp::parse(right).map_err(err)?,
                        inclusive: end_marker.unwrap_or(false),
                    })
                };
                Ok(TimeRange {
                    start,
                    end,
                    empty: false,
                })
            }
        }
    }
}

impl fmt::Display for TimeRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.empty {
            return write!(f, "()");
        }
        if self.is_eternity() {
            return write!(f, "_");
        }
        if self.is_instant() {
            // Prefer the short instantaneous form `[t]`.
            return write!(f, "[{}]", self.start.unwrap().ts);
        }
        match self.start {
            Some(b) if b.inclusive => write!(f, "[{}", b.ts)?,
            Some(b) => write!(f, "({}", b.ts)?,
            None => write!(f, "(")?,
        }
        write!(f, "_")?;
        match self.end {
            Some(b) if b.inclusive => write!(f, "{}]", b.ts)?,
            Some(b) => write!(f, "{})", b.ts)?,
            None => write!(f, ")")?,
        }
        Ok(())
    }
}

impl Serialize for TimeRange {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for TimeRange {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tr(s: &str) -> TimeRange {
        s.parse().unwrap()
    }

    #[test]
    fn parses_eternity_and_never() {
        assert!(tr("_").is_eternity());
        assert!(tr("").is_eternity());
        assert!(tr("()").empty);
    }

    #[test]
    fn parses_instantaneous_short_and_bracketed() {
        let a = tr("1694429247:0");
        let b = tr("[1694429247:0]");
        assert_eq!(a, b);
        assert!(a.is_instant());
        assert_eq!(a.start.unwrap().ts, Timestamp::new(1694429247, 0));
    }

    #[test]
    fn rejects_exclusive_instant() {
        assert!("(1:0)".parse::<TimeRange>().is_err() || tr("(1:0)").empty);
        // A single timestamp with an exclusive marker is invalid.
        assert!("[1:0)".parse::<TimeRange>().is_err());
    }

    #[test]
    fn parses_bounded_range_default_markers() {
        let r = tr("[0:0_10:0)");
        assert_eq!(r.start.unwrap().ts, Timestamp::new(0, 0));
        assert!(r.start.unwrap().inclusive);
        assert_eq!(r.end.unwrap().ts, Timestamp::new(10, 0));
        assert!(!r.end.unwrap().inclusive);
    }

    #[test]
    fn parses_half_bounded() {
        let after = tr("(5:0_");
        assert!(!after.start.unwrap().inclusive);
        assert_eq!(after.start.unwrap().ts, Timestamp::new(5, 0));
        assert!(after.end.is_none());
    }

    #[test]
    fn nanoseconds_bounds() {
        assert_eq!(Timestamp::new(1, 500_000_000).as_nanos(), 1_500_000_000);
        // More than 9 nanosecond digits is rejected.
        assert!(Timestamp::parse("1:1000000000").is_err());
        assert!(Timestamp::parse("1:999999999").is_ok());
    }

    #[test]
    fn round_trips_canonical_forms() {
        for s in ["_", "()", "[1694429247:0]", "[0:0_10:0)", "(5:0_", "_10:0)"] {
            let parsed = tr(s);
            let printed = parsed.to_string();
            let reparsed = tr(&printed);
            assert_eq!(parsed, reparsed, "round-trip failed for {s} -> {printed}");
        }
    }

    #[test]
    fn overlap_semantics() {
        // Contiguous half-open ranges touch at the boundary but do not overlap.
        assert!(!tr("[0:0_10:0)").overlaps(&tr("[10:0_20:0)")));
        assert!(tr("[0:0_10:0)").overlaps(&tr("[5:0_15:0)")));
        // Eternity overlaps any non-empty range.
        assert!(tr("_").overlaps(&tr("[5:0_15:0)")));
        // Never overlaps nothing.
        assert!(!tr("()").overlaps(&tr("_")));
        // An instant on an inclusive boundary overlaps.
        assert!(tr("[10:0]").overlaps(&tr("[0:0_20:0)")));
    }

    #[test]
    fn contains_range_for_delete() {
        let window = tr("[0:0_100:0)");
        assert!(window.contains_range(&tr("[10:0_20:0)")));
        assert!(!window.contains_range(&tr("[50:0_150:0)")));
    }

    #[test]
    fn lower_upper_nanos_for_keys() {
        let r = tr("[0:0_10:0)");
        assert_eq!(r.lower_nanos(), 0);
        assert_eq!(r.upper_nanos(), 10 * NANOS_PER_SEC);
        assert_eq!(tr("_").lower_nanos(), i128::MIN);
        assert_eq!(tr("_").upper_nanos(), i128::MAX);
    }
}
