//! Constant-time verification of the inbound TAMS webhook HMAC signature.
//!
//! The signature is HMAC-SHA256 over the raw request body, keyed by a shared secret, presented as
//! a hex string (optionally prefixed `sha256=`). Comparison is constant-time ([`subtle`]) so a
//! timing side-channel can't be used to forge a signature byte-by-byte.

use hmac::{Hmac, Mac};
use sha2::Sha256;
use subtle::ConstantTimeEq;

type HmacSha256 = Hmac<Sha256>;

fn decode_hex(s: &str) -> Option<Vec<u8>> {
    let s = s.trim();
    if s.is_empty() || !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
        .collect()
}

/// Verify that `header_value` is a valid HMAC-SHA256 signature of `body` under `secret`.
///
/// Accepts an optional `sha256=` prefix (case-insensitive). Returns `false` for a malformed header
/// or any mismatch. The HMAC compare is constant-time; a length mismatch yields `false` without a
/// data-dependent branch on the secret bytes.
pub fn verify_signature(secret: &[u8], body: &[u8], header_value: &str) -> bool {
    let hex = header_value
        .strip_prefix("sha256=")
        .or_else(|| header_value.strip_prefix("SHA256="))
        .unwrap_or(header_value);
    let Some(provided) = decode_hex(hex) else {
        return false;
    };

    let mut mac =
        <HmacSha256 as Mac>::new_from_slice(secret).expect("HMAC accepts a key of any length");
    mac.update(body);
    let expected = mac.finalize().into_bytes();

    expected.as_slice().ct_eq(provided.as_slice()).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &[u8] = b"super-secret-shared-key";
    const BODY: &[u8] = b"{\"flow_id\":\"f-1\",\"segments\":[]}";

    /// Reference signature computed the same way the sender would.
    fn sign(secret: &[u8], body: &[u8]) -> String {
        let mut mac = <HmacSha256 as Mac>::new_from_slice(secret).unwrap();
        mac.update(body);
        let tag = mac.finalize().into_bytes();
        tag.iter().map(|b| format!("{b:02x}")).collect()
    }

    #[test]
    fn accepts_valid_signature() {
        let sig = sign(SECRET, BODY);
        assert!(verify_signature(SECRET, BODY, &sig));
    }

    #[test]
    fn accepts_sha256_prefixed_signature() {
        let sig = format!("sha256={}", sign(SECRET, BODY));
        assert!(verify_signature(SECRET, BODY, &sig));
    }

    #[test]
    fn rejects_tampered_body() {
        let sig = sign(SECRET, BODY);
        assert!(!verify_signature(SECRET, b"different body", &sig));
    }

    #[test]
    fn rejects_wrong_secret() {
        let sig = sign(b"other-key", BODY);
        assert!(!verify_signature(SECRET, BODY, &sig));
    }

    #[test]
    fn rejects_malformed_header() {
        assert!(!verify_signature(SECRET, BODY, "not-hex"));
        assert!(!verify_signature(SECRET, BODY, "sha256=zzzz"));
        assert!(!verify_signature(SECRET, BODY, ""));
    }
}
