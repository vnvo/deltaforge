//! Cross-cutting leak-prevention tests: prove that references serialize but
//! resolved material never renders or serializes its value, across the public
//! surface (Deliverable 5.5 §8; review gate "no resolved secret can be
//! serialized or exposed").

use secrets::{
    CredentialSet, DEFAULT_MAX_SECRET_BYTES, ResolvedSecret, SecretMaterial,
    SecretProvider, SecretReference, SecretString,
};

const SENTINEL: &str = "S3NT1NEL-do-not-leak";

fn sentinel_string() -> SecretString {
    let r = SecretReference::new(SecretProvider::Env, "SENTINEL");
    SecretString::new(SENTINEL.into(), DEFAULT_MAX_SECRET_BYTES, &r).unwrap()
}

fn resolved_with_sentinel() -> ResolvedSecret {
    // Provenance/metadata builders are sealed to crate-internal resolution code,
    // so an external caller can only construct a bare resolved secret. That is
    // the point: connectors receive resolved secrets, they do not fabricate them.
    ResolvedSecret::new(SecretMaterial::Utf8(sentinel_string()))
}

#[test]
fn reference_serializes_but_has_no_secret() {
    let r = SecretReference::new(SecretProvider::Vault, "secret/data/orders")
        .with_selector("password")
        .with_purpose("source-password");
    let json = serde_json::to_string(&r).unwrap();
    // Reference metadata is present; there is no secret value to leak.
    assert!(json.contains("secret/data/orders"));
    assert!(!json.contains(SENTINEL));
}

#[test]
fn material_debug_never_reveals_value() {
    let s = sentinel_string();
    assert!(!format!("{s:?}").contains(SENTINEL));

    let m = SecretMaterial::Utf8(sentinel_string());
    assert!(!format!("{m:?}").contains(SENTINEL));

    let r = resolved_with_sentinel();
    let shown = format!("{r:?}");
    assert!(!shown.contains(SENTINEL));
}

#[test]
fn credential_set_debug_never_reveals_value() {
    let mut cs = CredentialSet::new();
    cs.insert("password", resolved_with_sentinel()).unwrap();
    let shown = format!("{cs:?}");
    assert!(!shown.contains(SENTINEL));
    assert!(shown.contains("password")); // field name is not secret
}

#[test]
fn error_display_and_debug_are_safe() {
    // Construct the error kinds that reference a location and confirm none carry
    // a secret value.
    let r = SecretReference::new(SecretProvider::File, "/etc/df/pg.key")
        .with_purpose("tls-private-key");
    let not_found = secrets::SecretError::NotFound(r.safe());
    assert!(format!("{not_found}").contains("/etc/df/pg.key"));
    assert!(!format!("{not_found}").contains(SENTINEL));
    assert!(!format!("{not_found:?}").contains(SENTINEL));
}
