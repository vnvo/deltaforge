//! Elasticsearch destination ownership and write fencing.
//!
//! An index is created by the sink in one request with its immutable marker
//! (`mappings._meta.deltaforge`) and the internal write alias
//! `df-w-<instance>-<fence>`. Every bulk action targets that alias with
//! `require_alias=true`, so an index, create or update action through a
//! removed alias fails at the server instead of auto-creating an index. A
//! delete action is not covered by `require_alias`; it fails only when the
//! cluster's `action.auto_create_index` forbids `df-w-*`, which every claim
//! proves first (`fencing_unenforced` otherwise). A fence rotates by one
//! atomic `_aliases` request that removes the old alias with `must_exist`
//! and adds the new one: a compare-and-swap. Rotations never write
//! `_meta`, and user aliases and other `_meta` keys are left as they are.

use async_trait::async_trait;
use deltaforge_core::ownership::{
    ClaimFailure, FencedDestination, Marker, Observation, OwnershipClass,
    RemoteOutcome, is_token,
};
use serde_json::{Value, json};

use super::client::EsTransport;

/// The prefix of the sink's internal write aliases.
pub const ALIAS_PREFIX: &str = "df-w-";

/// The write alias of `instance` at `fence`.
pub fn write_alias(instance: &str, fence: &str) -> String {
    format!("{ALIAS_PREFIX}{instance}-{fence}")
}

/// The `(instance, fence)` an internal alias names, if it is one.
fn parse_alias(alias: &str) -> Option<(&str, &str)> {
    let rest = alias.strip_prefix(ALIAS_PREFIX)?;
    let (instance, fence) = rest.split_once('-')?;
    (is_token(instance) && is_token(fence)).then_some((instance, fence))
}

/// The cluster settings read that proves deletes are fenced.
/// Nested settings (`filter_path` reads dots as object paths, so a flat
/// key would never match).
pub(crate) const AUTO_CREATE_SETTINGS: &str = "_cluster/settings?\
     include_defaults=true&filter_path=*.action.auto_create_index";

/// Whether the effective `action.auto_create_index` (transient over
/// persistent over default) forbids auto-creating every index named
/// `df-w-*`. `Err` explains why it is not proven.
pub(crate) fn auto_create_forbids_internal(
    settings: &Value,
) -> Result<(), String> {
    let value = ["transient", "persistent", "defaults"]
        .iter()
        .find_map(|scope| {
            settings[scope]["action"]["auto_create_index"].as_str()
        })
        .ok_or("action.auto_create_index could not be read")?;
    rules_forbid_prefix(value, ALIAS_PREFIX)
}

/// Whether the ordered rule list `rules` (Elasticsearch's
/// `action.auto_create_index`: `true`, `false`, or comma-separated
/// patterns, each optionally prefixed `+` or `-`, the first match deciding,
/// and an unmatched name not auto-created) forbids every name starting with
/// `prefix`. Conservative: a rule that may allow any such name makes it
/// unproven.
pub fn rules_forbid_prefix(rules: &str, prefix: &str) -> Result<(), String> {
    match rules.trim() {
        "false" => return Ok(()),
        "true" => {
            return Err(format!(
                "action.auto_create_index is true: {prefix}* may be auto-created"
            ));
        }
        _ => {}
    }
    for entry in rules.split(',').map(str::trim) {
        if entry.is_empty() {
            return Err("action.auto_create_index has an empty pattern".into());
        }
        let (allow, pattern) = match entry.as_bytes()[0] {
            b'+' => (true, &entry[1..]),
            b'-' => (false, &entry[1..]),
            _ => (true, entry),
        };
        if pattern.is_empty() {
            return Err(format!(
                "action.auto_create_index has an empty pattern ({entry})"
            ));
        }
        let literal = pattern.split('*').next().unwrap_or("");
        let wildcard = pattern.contains('*');
        // The pattern matches every name starting with `prefix`.
        let covers = wildcard
            && prefix.starts_with(literal)
            && pattern[literal.len()..].bytes().all(|b| b == b'*');
        // The pattern may match some name starting with `prefix`.
        let may = if wildcard {
            literal.starts_with(prefix) || prefix.starts_with(literal)
        } else {
            literal.starts_with(prefix)
        };
        if covers {
            return if allow {
                Err(format!(
                    "action.auto_create_index rule {entry} allows {prefix}*"
                ))
            } else {
                Ok(())
            };
        }
        if may && allow {
            return Err(format!(
                "action.auto_create_index rule {entry} may allow a {prefix}* name"
            ));
        }
    }
    // No rule matches: such a name is not auto-created.
    Ok(())
}

/// One index the sink writes, for a claim.
pub(crate) struct EsDestination<'a> {
    pub transport: &'a dyn EsTransport,
    pub index: &'a str,
    /// The generated mapping (`{"properties": ...}`) it is created with.
    pub mapping: Value,
}

fn unavailable(what: &str, e: impl std::fmt::Display) -> ClaimFailure {
    ClaimFailure::Unavailable(format!("elasticsearch {what}: {e}"))
}

fn error_type(body: &Value) -> &str {
    body["error"]["type"].as_str().unwrap_or("")
}

#[async_trait]
impl FencedDestination for EsDestination<'_> {
    async fn read(&self) -> Result<Observation, ClaimFailure> {
        let (code, settings) = self
            .transport
            .send_json(reqwest::Method::GET, AUTO_CREATE_SETTINGS, None)
            .await
            .map_err(|e| unavailable("cluster settings", e))?;
        if code != 200 {
            tracing::error!(
                status = code,
                "elasticsearch: cluster settings unreadable; deletes cannot \
                 be proven fenced (needs the monitor cluster privilege)"
            );
            return Err(ClaimFailure::refused(
                OwnershipClass::FencingUnenforced,
            ));
        }
        if let Err(why) = auto_create_forbids_internal(&settings) {
            tracing::error!(reason = %why, "elasticsearch: deletes are not fenced");
            return Err(ClaimFailure::refused(
                OwnershipClass::FencingUnenforced,
            ));
        }
        let path = format!(
            "{}?filter_path=*.aliases,*.mappings._meta,*.settings.index.uuid",
            self.index
        );
        let (code, body) = self
            .transport
            .send_json(reqwest::Method::GET, &path, None)
            .await
            .map_err(|e| unavailable("index read", e))?;
        if code == 404 {
            return Ok(Observation::Absent { server: None });
        }
        if code != 200 {
            return Err(unavailable("index read", format!("status {code}")));
        }
        // Exactly the concrete index of that name: an alias or a data
        // stream answers under other names.
        let obj = body.as_object().cloned().unwrap_or_default();
        if obj.len() != 1 || !obj.contains_key(self.index) {
            return Err(ClaimFailure::refused(
                OwnershipClass::AliasNotSupported,
            ));
        }
        let index = &obj[self.index];
        let marker = index["mappings"]["_meta"].get("deltaforge").cloned();
        let instance = marker
            .as_ref()
            .and_then(|m| m["instance"].as_str().map(str::to_string));
        let internal: Vec<&String> = index["aliases"]
            .as_object()
            .map(|a| a.keys().filter(|k| k.starts_with(ALIAS_PREFIX)).collect())
            .unwrap_or_default();
        let fence = match (internal.as_slice(), instance.as_deref()) {
            ([one], Some(inst)) => match parse_alias(one) {
                Some((i, f)) if i == inst => Ok(f.to_string()),
                _ => {
                    Err(format!("internal alias {one} is not of this instance"))
                }
            },
            (all, _) => Err(format!("{} internal write aliases", all.len())),
        };
        Ok(Observation::Present {
            marker,
            fence,
            server: None,
        })
    }

    async fn create(
        &self,
        marker: &Marker,
        fence: &str,
    ) -> Result<RemoteOutcome, ClaimFailure> {
        let mut mappings = self.mapping.clone();
        mappings["_meta"] = json!({ "deltaforge": marker });
        let body = json!({
            "mappings": mappings,
            "aliases": {
                write_alias(&marker.instance, fence): { "is_write_index": true }
            }
        });
        let Ok((code, resp)) = self
            .transport
            .send_json(reqwest::Method::PUT, self.index, Some(&body))
            .await
        else {
            return Ok(RemoteOutcome::Unknown);
        };
        match code {
            200 => Ok(RemoteOutcome::Applied),
            400 if error_type(&resp) == "resource_already_exists_exception" => {
                Ok(RemoteOutcome::Refused)
            }
            c if c >= 500 => Ok(RemoteOutcome::Unknown),
            c => Err(unavailable(
                "index creation",
                format!("status {c}: {}", error_type(&resp)),
            )),
        }
    }

    async fn swap_fence(
        &self,
        instance: &str,
        from: &str,
        to: &str,
    ) -> Result<RemoteOutcome, ClaimFailure> {
        let body = json!({ "actions": [
            { "remove": {
                "index": self.index,
                "alias": write_alias(instance, from),
                "must_exist": true
            }},
            { "add": {
                "index": self.index,
                "alias": write_alias(instance, to),
                "is_write_index": true
            }}
        ]});
        let Ok((code, resp)) = self
            .transport
            .send_json(reqwest::Method::POST, "_aliases", Some(&body))
            .await
        else {
            return Ok(RemoteOutcome::Unknown);
        };
        Ok(match code {
            200 if resp["acknowledged"] == true => RemoteOutcome::Applied,
            404 if error_type(&resp) == "aliases_not_found_exception" => {
                RemoteOutcome::Refused
            }
            // Not acknowledged, or any other answer: resolved by a reread.
            _ => RemoteOutcome::Unknown,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn forbids(rules: &str) -> bool {
        rules_forbid_prefix(rules, ALIAS_PREFIX).is_ok()
    }

    #[test]
    fn ordered_auto_create_rules_are_evaluated_first_match_first() {
        assert!(forbids("false"));
        assert!(!forbids("true"));
        assert!(forbids("-df-w-*,+*"));
        assert!(forbids("-df-*,+*"));
        assert!(forbids("-*"));
        // The allow rule comes first: it wins.
        assert!(!forbids("+*,-df-w-*"));
        assert!(!forbids("+df-*,-df-w-*"));
        // An allow rule that may match some df-w- name, before the forbid.
        assert!(!forbids("+df-w-1*,-df-w-*"));
        assert!(!forbids("+df-w-abc,-df-w-*"));
        // Rules that cannot match df-w- names, then an unmatched default.
        assert!(forbids("+logs-*,+metrics-*"));
        assert!(forbids("+logs-*,-df-w-*,+*"));
        // A partial forbid, then a broad allow.
        assert!(!forbids("-df-w-a*,+*"));
        // Malformed.
        assert!(!forbids("+logs-*,,+x"));
        assert!(!forbids("-"));
    }

    #[test]
    fn the_effective_setting_is_transient_then_persistent_then_default() {
        let s = json!({
            "persistent": {"action": {"auto_create_index": "-df-w-*,+*"}},
            "defaults": {"action": {"auto_create_index": "true"}}
        });
        assert!(auto_create_forbids_internal(&s).is_ok());
        let s = json!({
            "transient": {"action": {"auto_create_index": "true"}},
            "persistent": {"action": {"auto_create_index": "-df-w-*,+*"}}
        });
        assert!(auto_create_forbids_internal(&s).is_err());
        assert!(auto_create_forbids_internal(&json!({})).is_err());
    }

    #[test]
    fn internal_aliases_parse_only_with_two_tokens() {
        let i = "0123456789abcdef0123456789abcdef";
        let f = "fedcba9876543210fedcba9876543210";
        assert_eq!(parse_alias(&write_alias(i, f)), Some((i, f)));
        assert_eq!(parse_alias("df-w-short-x"), None);
        assert_eq!(parse_alias("orders-read"), None);
    }
}
