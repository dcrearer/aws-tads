//! Sort-key encoding for the events table.
//!
//! DynamoDB sorts range keys lexicographically, so a signed nanosecond value must be mapped to
//! an order-preserving fixed-width string. We flip the sign bit of the `i128` and format it as
//! 32 lower-case hex digits; the composite SK appends the event id to disambiguate events that
//! share a start instant (overlaps are allowed).

/// Order-preserving 32-hex-digit encoding of a signed nanosecond value.
pub fn encode_sortable(nanos: i128) -> String {
    let biased = (nanos as u128) ^ (1u128 << 127);
    format!("{biased:032x}")
}

/// Composite event sort key: `<sortable(start_ns)>#<event_id>`.
pub fn event_sort_key(start_nanos: i128, event_id: &str) -> String {
    format!("{}#{}", encode_sortable(start_nanos), event_id)
}

/// Upper-bound sentinel for a start-time range query: the encoding of `b` with no id suffix,
/// which sorts strictly below any `encode_sortable(b)#id`, giving `start_ns < b` semantics.
pub fn start_upper_bound(exclusive_upper_nanos: i128) -> String {
    encode_sortable(exclusive_upper_nanos)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encoding_is_monotonic_including_negatives() {
        let samples = [
            i128::MIN,
            -1_000_000_000,
            -1,
            0,
            1,
            1_000_000_000,
            i128::MAX,
        ];
        let mut encoded: Vec<String> = samples.iter().map(|n| encode_sortable(*n)).collect();
        let sorted = {
            let mut e = encoded.clone();
            e.sort();
            e
        };
        assert_eq!(encoded, sorted, "hex encoding must preserve numeric order");
        encoded.dedup();
        assert_eq!(encoded.len(), samples.len(), "encodings must be unique");
    }

    #[test]
    fn composite_key_orders_by_start_then_id() {
        let a = event_sort_key(10, "aaaa");
        let b = event_sort_key(10, "bbbb");
        let c = event_sort_key(20, "aaaa");
        assert!(a < b);
        assert!(b < c);
    }

    #[test]
    fn upper_bound_excludes_equal_start() {
        // An event starting exactly at the exclusive upper bound must sort above the sentinel.
        let sentinel = start_upper_bound(20);
        let at_bound = event_sort_key(20, "aaaa");
        assert!(at_bound > sentinel);
        let below = event_sort_key(19, "zzzz");
        assert!(below < sentinel);
    }
}
