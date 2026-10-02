use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// A single tag value: either a string or an array of strings (`tags.json`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum TagValue {
    One(String),
    Many(Vec<String>),
}

/// Freeform key/value tags. Keys are strings; values are a string or array of strings.
///
/// Per the spec a PUT **replaces** the whole dictionary (it does not merge). Backed by a
/// `BTreeMap` so serialization is deterministic (stable keys aid caching and test assertions).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Tags(pub BTreeMap<String, TagValue>);

impl Tags {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// `true` if a tag with `name` exists (regardless of value) — backs `tag_exists.{name}`.
    pub fn contains_key(&self, name: &str) -> bool {
        self.0.contains_key(name)
    }

    /// `true` if the tag `name` holds (or contains) the value `want` — backs `tag.{name}`.
    pub fn matches(&self, name: &str, want: &str) -> bool {
        match self.0.get(name) {
            Some(TagValue::One(v)) => v == want,
            Some(TagValue::Many(vs)) => vs.iter().any(|v| v == want),
            None => false,
        }
    }
}
