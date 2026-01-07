//! Pattern matching utilities for filtering tables, topics, and streams.
//!
//! This module provides flexible allow-lists that can match database tables,
//! Kafka topics, Redis streams, and other hierarchical names using patterns.
//!
//! # Pattern Syntax
//!
//! - `qualifier.name` - Exact match (e.g., `public.users`, `mydb.orders`)
//! - `name` - Match any qualifier (e.g., `orders` matches `public.orders`, `myschema.orders`)
//! - `qualifier.*` - All names in qualifier (e.g., `public.*`)
//! - `qualifier.prefix%` - LIKE-style prefix (e.g., `public.order_%`)
//! - `*.name` or `%.name` - Name in any qualifier
//!
//! # Semantics
//!
//! The "qualifier" can represent different concepts:
//! - **MySQL**: Database name
//! - **PostgreSQL**: Schema name
//! - **Kafka**: Topic prefix or namespace
//! - **Redis**: Key prefix
//!
//! # Examples
//!
//! ```
//! use common::AllowList;
//!
//! let list = AllowList::new(&[
//!     "public.users".to_string(),
//!     "orders".to_string(),      // matches any qualifier
//!     "audit.*".to_string(),     // all tables in audit
//! ]);
//!
//! assert!(list.matches("public", "users"));
//! assert!(list.matches("any_schema", "orders"));
//! assert!(list.matches("audit", "logs"));
//! assert!(!list.matches("public", "products"));
//! ```

/// Flexible allow-list for pattern matching.
///
/// Supports patterns like:
/// - `db.table` / `schema.table` - exact match with qualifier
/// - `table` - match any qualifier
/// - `db.*` / `schema.*` - match all in qualifier
/// - `db.prefix%` - LIKE-style prefix matching
#[derive(Clone, Debug, Default)]
pub struct AllowList {
    items: Vec<(Option<String>, String)>,
}

impl AllowList {
    /// Create a new AllowList from a list of patterns.
    ///
    /// # Examples
    ///
    /// ```
    /// use common::AllowList;
    ///
    /// let list = AllowList::new(&[
    ///     "public.users".to_string(),
    ///     "orders".to_string(),
    /// ]);
    /// ```
    pub fn new(patterns: &[String]) -> Self {
        let items = patterns
            .iter()
            .map(|s| {
                if let Some((qualifier, name)) = s.split_once('.') {
                    (Some(qualifier.to_string()), name.to_string())
                } else {
                    // No qualifier specified, match any
                    (None, s.to_string())
                }
            })
            .collect();
        Self { items }
    }

    /// Create an AllowList from string slices.
    pub fn from_strs(patterns: &[&str]) -> Self {
        Self::new(&patterns.iter().map(|s| s.to_string()).collect::<Vec<_>>())
    }

    /// Create an empty AllowList (matches everything).
    pub fn allow_all() -> Self {
        Self { items: Vec::new() }
    }

    /// Check if a qualifier.name combination matches the allow list.
    ///
    /// An empty allow list matches everything.
    ///
    /// # Arguments
    ///
    /// - `qualifier`: The database/schema/namespace part
    /// - `name`: The table/topic/stream name part
    ///
    /// # Examples
    ///
    /// ```
    /// use common::AllowList;
    ///
    /// let list = AllowList::new(&["public.users".to_string()]);
    /// assert!(list.matches("public", "users"));
    /// assert!(!list.matches("other", "users"));
    /// ```
    pub fn matches(&self, qualifier: &str, name: &str) -> bool {
        if self.items.is_empty() {
            return true;
        }
        self.items.iter().any(|(q_opt, pattern)| {
            let qualifier_matches = q_opt
                .as_ref()
                .map(|q| Self::pattern_matches_single(q, qualifier))
                .unwrap_or(true);
            let name_matches = Self::pattern_matches_single(pattern, name);
            qualifier_matches && name_matches
        })
    }

    /// Check if a full name (without qualifier) matches.
    ///
    /// This is useful when the source doesn't have hierarchical naming.
    pub fn matches_name(&self, name: &str) -> bool {
        if self.items.is_empty() {
            return true;
        }
        self.items
            .iter()
            .any(|(_, pattern)| Self::pattern_matches_single(pattern, name))
    }

    /// Check if this allow list is empty (matches everything).
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Get the number of patterns in this allow list.
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Get the patterns as (qualifier, name) pairs.
    pub fn patterns(&self) -> impl Iterator<Item = (Option<&str>, &str)> {
        self.items.iter().map(|(q, n)| (q.as_deref(), n.as_str()))
    }

    /// Check if a pattern matches a single string.
    fn pattern_matches_single(pattern: &str, value: &str) -> bool {
        match pattern {
            "*" | "%" => true,
            p if p.ends_with('%') => {
                let prefix = &p[..p.len() - 1];
                value.starts_with(prefix)
            }
            p if p.ends_with('*') => {
                let prefix = &p[..p.len() - 1];
                value.starts_with(prefix)
            }
            p => p == value,
        }
    }
}

// =============================================================================
// Display Implementation
// =============================================================================

impl std::fmt::Display for AllowList {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.items.is_empty() {
            write!(f, "[*]")
        } else {
            let patterns: Vec<String> = self
                .items
                .iter()
                .map(|(q, n)| {
                    q.as_ref()
                        .map(|q| format!("{}.{}", q, n))
                        .unwrap_or_else(|| n.clone())
                })
                .collect();
            write!(f, "[{}]", patterns.join(", "))
        }
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_list_matches_everything() {
        let list = AllowList::new(&[]);
        assert!(list.matches("any", "table"));
        assert!(list.is_empty());
    }

    #[test]
    fn exact_qualified_match() {
        let list = AllowList::new(&["public.users".to_string()]);
        assert!(list.matches("public", "users"));
        assert!(!list.matches("public", "orders"));
        assert!(!list.matches("other", "users"));
    }

    #[test]
    fn unqualified_pattern_matches_any_qualifier() {
        let list = AllowList::new(&["orders".to_string()]);
        assert!(list.matches("public", "orders"));
        assert!(list.matches("myschema", "orders"));
        assert!(list.matches("", "orders"));
        assert!(!list.matches("public", "users"));
    }

    #[test]
    fn wildcard_matches_all_names() {
        // Both * and % work as wildcards
        let list = AllowList::new(&["public.*".to_string()]);
        assert!(list.matches("public", "users"));
        assert!(list.matches("public", "orders"));
        assert!(!list.matches("other", "users"));
    }

    #[test]
    fn prefix_pattern_with_percent() {
        let list = AllowList::new(&["public.order_%".to_string()]);
        assert!(list.matches("public", "order_items"));
        assert!(list.matches("public", "order_history"));
        assert!(list.matches("public", "order_")); // prefix itself matches
        assert!(!list.matches("public", "orders")); // doesn't start with order_
        assert!(!list.matches("public", "user_orders"));
    }

    #[test]
    fn prefix_pattern_with_star() {
        let list = AllowList::new(&["public.user_*".to_string()]);
        assert!(list.matches("public", "user_profiles"));
        assert!(list.matches("public", "user_settings"));
        assert!(!list.matches("public", "users"));
    }

    #[test]
    fn multiple_patterns_any_can_match() {
        let list = AllowList::new(&[
            "public.users".to_string(),
            "orders".to_string(),
            "audit.*".to_string(),
        ]);
        assert!(list.matches("public", "users"));
        assert!(list.matches("any", "orders"));
        assert!(list.matches("audit", "logs"));
        assert!(!list.matches("public", "products"));
    }

    #[test]
    fn qualifier_wildcard() {
        let list = AllowList::new(&["*.users".to_string()]);
        assert!(list.matches("public", "users"));
        assert!(list.matches("any", "users"));
        assert!(!list.matches("public", "orders"));
    }

    #[test]
    fn matches_name_ignores_qualifier() {
        let list =
            AllowList::new(&["public.orders".to_string(), "users".to_string()]);
        // matches_name only looks at the name part
        assert!(list.matches_name("orders"));
        assert!(list.matches_name("users"));
        assert!(!list.matches_name("products"));
    }

    #[test]
    fn display_formatting() {
        let empty = AllowList::new(&[]);
        assert_eq!(format!("{}", empty), "[*]");

        let list =
            AllowList::new(&["public.users".to_string(), "orders".to_string()]);
        assert_eq!(format!("{}", list), "[public.users, orders]");
    }

    #[test]
    fn patterns_iterator_returns_parsed_components() {
        let list =
            AllowList::new(&["public.users".to_string(), "orders".to_string()]);
        let patterns: Vec<_> = list.patterns().collect();

        assert_eq!(patterns.len(), 2);
        assert_eq!(patterns[0], (Some("public"), "users"));
        assert_eq!(patterns[1], (None, "orders"));
    }
}
