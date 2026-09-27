//! Protected secret material. The raw value never leaves these types via
//! `Debug`, `Display`, or serialization (none are implemented that reveal it),
//! access is only through the explicit `expose_*` accessors (easy to audit by
//! grep), and buffers are best-effort zeroized on drop via the `zeroize` crate.
//!
//! Size limits are enforced **inside the constructors** - not merely advised - so
//! the type invariant "no material exceeds the configured limit" holds on every
//! construction path. A provider preflight (file size / `Content-Length`) is
//! still useful to avoid reading a huge body, but the constructor re-checks after
//! reading because that metadata can be absent, wrong, or race with content.
//!
//! Zeroization is **best-effort** and its limits are documented honestly: it
//! overwrites the owned buffer before the allocation is freed, but it cannot
//! scrub copies made elsewhere nor pages the allocator later reuses. Bytes
//! rejected during UTF-8 validation are explicitly zeroized before being dropped.

use zeroize::{Zeroize, Zeroizing};

use crate::error::SecretError;
use crate::reference::SecretReference;

/// Default maximum resolved-secret size.
pub const DEFAULT_MAX_SECRET_BYTES: usize = 1 << 20; // 1 MiB

/// Reject an over-size secret **before** allocating/decoding it. Providers call
/// this with the advertised length prior to reading the body; the protected-type
/// constructors enforce the same limit again after reading.
pub fn check_size(
    len: usize,
    limit: usize,
    reference: &SecretReference,
) -> Result<(), SecretError> {
    if len > limit {
        return Err(SecretError::SizeExceeded {
            reference: reference.safe(),
            limit,
        });
    }
    Ok(())
}

/// Protected UTF-8 secret material. Zeroized on drop; size-bounded on construction.
pub struct SecretString(Zeroizing<String>);

impl SecretString {
    /// Wrap an already-valid UTF-8 secret, enforcing the size limit. An over-size
    /// value is zeroized and rejected.
    pub fn new(
        value: String,
        limit: usize,
        reference: &SecretReference,
    ) -> Result<Self, SecretError> {
        if value.len() > limit {
            let mut v = value;
            v.zeroize();
            return Err(SecretError::SizeExceeded {
                reference: reference.safe(),
                limit,
            });
        }
        Ok(Self(Zeroizing::new(value)))
    }

    /// Build from bytes, enforcing the size limit and validating UTF-8. Over-size
    /// or non-UTF-8 input is zeroized and rejected (timeline M8/M9).
    pub fn from_utf8(
        bytes: Vec<u8>,
        limit: usize,
        reference: &SecretReference,
    ) -> Result<Self, SecretError> {
        if bytes.len() > limit {
            let mut b = bytes;
            b.zeroize();
            return Err(SecretError::SizeExceeded {
                reference: reference.safe(),
                limit,
            });
        }
        match String::from_utf8(bytes) {
            Ok(s) => Ok(Self(Zeroizing::new(s))),
            Err(e) => {
                // Zeroize the rejected bytes rather than letting them drop as-is.
                let mut recovered = e.into_bytes();
                recovered.zeroize();
                Err(SecretError::NotUtf8(reference.safe()))
            }
        }
    }

    /// The only way to read the value. Named for auditability.
    pub fn expose_secret(&self) -> &str {
        self.0.as_str()
    }

    /// Length in bytes (non-secret metadata).
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl std::fmt::Debug for SecretString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SecretString(REDACTED)")
    }
}

/// Protected binary secret material (PKCS#12, DER, key stores, raw keys).
/// Zeroized on drop; size-bounded on construction.
pub struct SecretBytes(Zeroizing<Vec<u8>>);

impl SecretBytes {
    /// Wrap binary material, enforcing the size limit. Over-size input is
    /// zeroized and rejected.
    pub fn new(
        value: Vec<u8>,
        limit: usize,
        reference: &SecretReference,
    ) -> Result<Self, SecretError> {
        if value.len() > limit {
            let mut v = value;
            v.zeroize();
            return Err(SecretError::SizeExceeded {
                reference: reference.safe(),
                limit,
            });
        }
        Ok(Self(Zeroizing::new(value)))
    }

    /// The only way to read the value. Named for auditability.
    pub fn expose_bytes(&self) -> &[u8] {
        self.0.as_slice()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl std::fmt::Debug for SecretBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SecretBytes(REDACTED)")
    }
}

// Deliberately NOT implemented on either type:
// - `Display` / `Serialize`: the value must never render or serialize.
// - `Clone`: avoid copying material.
// - `PartialEq`/`Eq`: no timing-sensitive value comparison.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reference::{SecretProvider, SecretReference};

    fn any_ref() -> SecretReference {
        SecretReference::new(SecretProvider::Env, "X")
    }

    const LIM: usize = DEFAULT_MAX_SECRET_BYTES;

    #[test]
    fn secret_string_debug_is_redacted() {
        let s =
            SecretString::new("hunter2".to_string(), LIM, &any_ref()).unwrap();
        assert_eq!(format!("{s:?}"), "SecretString(REDACTED)");
        assert!(!format!("{s:?}").contains("hunter2"));
        assert_eq!(s.expose_secret(), "hunter2");
    }

    #[test]
    fn secret_bytes_debug_is_redacted_and_holds_arbitrary_bytes() {
        let raw = vec![0x00, 0xff, 0x10, 0x80]; // not valid UTF-8
        let b = SecretBytes::new(raw.clone(), LIM, &any_ref()).unwrap();
        assert_eq!(format!("{b:?}"), "SecretBytes(REDACTED)");
        assert_eq!(b.expose_bytes(), raw.as_slice());
    }

    #[test]
    fn from_utf8_accepts_valid_and_rejects_invalid() {
        let ok =
            SecretString::from_utf8(b"pass".to_vec(), LIM, &any_ref()).unwrap();
        assert_eq!(ok.expose_secret(), "pass");

        // Invalid UTF-8 is rejected (rejected bytes are zeroized before drop).
        let err = SecretString::from_utf8(vec![0xff, 0xfe], LIM, &any_ref())
            .unwrap_err();
        assert!(matches!(err, SecretError::NotUtf8(_)));
    }

    #[test]
    fn oversized_material_cannot_be_constructed_via_any_path() {
        let big = vec![b'a'; 33];
        let big_str = "a".repeat(33);
        let r = any_ref();
        assert!(matches!(
            SecretString::new(big_str.clone(), 32, &r),
            Err(SecretError::SizeExceeded { limit: 32, .. })
        ));
        assert!(matches!(
            SecretString::from_utf8(big.clone(), 32, &r),
            Err(SecretError::SizeExceeded { limit: 32, .. })
        ));
        assert!(matches!(
            SecretBytes::new(big, 32, &r),
            Err(SecretError::SizeExceeded { limit: 32, .. })
        ));
        // Exactly at the limit is allowed.
        assert!(SecretBytes::new(vec![0u8; 32], 32, &r).is_ok());
    }

    #[test]
    fn check_size_enforces_limit() {
        assert!(check_size(16, 16, &any_ref()).is_ok());
        assert!(matches!(
            check_size(17, 16, &any_ref()),
            Err(SecretError::SizeExceeded { limit: 16, .. })
        ));
    }

    #[test]
    fn zeroization_mechanism_zeros_owned_buffer() {
        // Best-effort zeroization via the `zeroize` crate; verify on an owned
        // buffer (post-drop memory cannot be asserted).
        let mut buf = vec![1u8, 2, 3, 4, 5];
        buf.zeroize();
        assert!(buf.iter().all(|&b| b == 0));
    }
}
