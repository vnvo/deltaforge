use serde::{Deserialize, Serialize};

/// How to handle empty objects (e.g. `"meta": {}`).
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum EmptyObjectPolicy {
    /// Keep the field as-is: `"meta": {}` (default — safe, no surprises).
    #[default]
    Preserve,
    /// Remove the key entirely.
    Drop,
    /// Convert to null: `"meta": null`.
    Null,
}

/// How to handle list values (e.g. `"tags": [1, 2, 3]`).
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ListPolicy {
    /// Keep the list as-is: `"tags": [1,2,3]` (default).
    #[default]
    Preserve,
    /// Expand to indexed keys: `"tags__0": 1, "tags__1": 2`.
    Index,
}

/// How to handle empty lists (e.g. `"tags": []`).
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum EmptyListPolicy {
    /// Keep the field as-is: `"tags": []` (default).
    #[default]
    Preserve,
    /// Remove the key entirely.
    Drop,
    /// Convert to null: `"tags": null`.
    Null,
}

/// How to handle key collisions when flattening produces duplicate keys.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum CollisionPolicy {
    /// Last writer wins (default).
    #[default]
    Last,
    /// First writer wins.
    First,
    /// Fail the batch on collision.
    Error,
}

/// Flatten processor config.
///
/// Flattens nested JSON objects in event payloads into dot-separated (or
/// custom separator) keys. Works on any object-valued field present on the
/// event - no assumptions about CDC structure. CDC `before`/`after` fields
/// are flattened automatically; outbox `payload` or any other custom field
/// is handled equally.
///
/// ```yaml
/// processors:
///   - type: flatten
///     id: flat
///     separator: "__"
///     max_depth: 3
///     on_collision: last
///     empty_object: preserve
///     lists: preserve
///     empty_list: drop
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct FlattenProcessorCfg {
    /// Processor identifier.
    #[serde(default = "default_flatten_id")]
    pub id: String,

    /// Key separator for nested paths (default: `"__"`).
    pub separator: String,

    /// Maximum recursion depth. Objects at this depth are treated as leaves
    /// and kept as-is (subject to `empty_object` policy). `None` = unlimited.
    pub max_depth: Option<usize>,

    /// How to handle collisions when two paths produce the same flattened key.
    pub on_collision: CollisionPolicy,

    /// How to handle empty object values.
    pub empty_object: EmptyObjectPolicy,

    /// How to handle list values.
    pub lists: ListPolicy,

    /// How to handle empty list values.
    pub empty_list: EmptyListPolicy,
}

fn default_flatten_id() -> String {
    "flatten".into()
}

impl Default for FlattenProcessorCfg {
    fn default() -> Self {
        Self {
            id: default_flatten_id(),
            separator: "__".into(),
            max_depth: None,
            on_collision: CollisionPolicy::default(),
            empty_object: EmptyObjectPolicy::default(),
            lists: ListPolicy::default(),
            empty_list: EmptyListPolicy::default(),
        }
    }
}
