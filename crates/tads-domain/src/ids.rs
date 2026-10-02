use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use uuid::Uuid;

use crate::error::DomainError;

/// A TADS resource identifier. Wraps a [`Uuid`] and (de)serializes as a plain string, matching
/// the spec's `uuid.json` (canonical hyphenated form).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TadsId(Uuid);

impl TadsId {
    /// Generate a fresh random (v4) identifier — used when a client POSTs an Event without an id.
    pub fn new_v4() -> Self {
        Self(Uuid::new_v4())
    }

    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl fmt::Display for TadsId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Canonical hyphenated, lower-case — matches the spec pattern.
        write!(f, "{}", self.0.as_hyphenated())
    }
}

impl FromStr for TadsId {
    type Err = DomainError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(s)
            .map(TadsId)
            .map_err(|_| DomainError::InvalidUuid(s.to_string()))
    }
}

impl From<Uuid> for TadsId {
    fn from(u: Uuid) -> Self {
        Self(u)
    }
}

impl Serialize for TadsId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for TadsId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_through_string() {
        let id = TadsId::new_v4();
        let s = id.to_string();
        assert_eq!(id, s.parse::<TadsId>().unwrap());
    }

    #[test]
    fn rejects_non_uuid() {
        assert!("not-a-uuid".parse::<TadsId>().is_err());
    }

    #[test]
    fn serde_is_a_bare_string() {
        let id: TadsId = "2d1f2b3c-4a5b-4c6d-8e9f-0a1b2c3d4e5f".parse().unwrap();
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, "\"2d1f2b3c-4a5b-4c6d-8e9f-0a1b2c3d4e5f\"");
        let back: TadsId = serde_json::from_str(&json).unwrap();
        assert_eq!(id, back);
    }
}
