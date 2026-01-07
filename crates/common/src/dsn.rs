//! DSN (Data Source Name) parsing and credential redaction utilities.
//!
//! This module provides tools for working with database and service
//! connection strings across different formats:
//!
//! - **URL format**: `postgres://user:pass@host:port/db`
//! - **Key-value format**: `host=localhost user=postgres password=secret`
//! - **Query parameter auth**: `libsql://db.turso.io?authToken=secret`
//!
//! # Security
//!
//! The redaction functions are designed to make connection strings safe
//! for logging without exposing credentials. They handle edge cases like:
//! - Missing passwords (no-op)
//! - URL-encoded special characters
//! - Multiple credential locations
//!
//! # Examples
//!
//! ```
//! use common::dsn::{redact_dsn, DsnComponents};
//!
//! // Auto-detect format and redact
//! let safe = redact_dsn("postgres://user:secret@localhost/db");
//! assert!(!safe.contains("secret"));
//!
//! // Parse URL-style DSN
//! let comp = DsnComponents::from_url("mysql://root:pass@127.0.0.1:3306/mydb", 3306).unwrap();
//! assert_eq!(comp.host, "127.0.0.1");
//! ```

use url::Url;

// =============================================================================
// DSN Components
// =============================================================================

/// Common components extracted from a database DSN/connection string.
///
/// This struct provides a unified representation of connection parameters
/// across different database types (MySQL, PostgreSQL, Redis, etc.).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DsnComponents {
    /// Hostname or IP address
    pub host: String,
    /// Port number
    pub port: u16,
    /// Username for authentication
    pub user: String,
    /// Password for authentication
    pub password: String,
    /// Database/keyspace name
    pub database: String,
}

impl DsnComponents {
    /// Parse a URL-style DSN into components.
    ///
    /// Supports standard database URL schemes:
    /// - `mysql://`, `postgres://`, `postgresql://`
    /// - `redis://`, `rediss://` (TLS)
    /// - `libsql://`, `http://`, `https://`
    ///
    /// # Arguments
    ///
    /// * `dsn` - The connection string URL
    /// * `default_port` - Port to use if not specified in URL
    ///
    /// # Examples
    ///
    /// ```
    /// use common::dsn::DsnComponents;
    ///
    /// let comp = DsnComponents::from_url("postgres://user:pass@localhost:5432/mydb", 5432).unwrap();
    /// assert_eq!(comp.host, "localhost");
    /// assert_eq!(comp.port, 5432);
    /// assert_eq!(comp.user, "user");
    /// assert_eq!(comp.database, "mydb");
    /// ```
    pub fn from_url(dsn: &str, default_port: u16) -> Result<Self, String> {
        let url = Url::parse(dsn).map_err(|e| e.to_string())?;

        Ok(Self {
            host: url.host_str().unwrap_or("localhost").to_string(),
            port: url.port().unwrap_or(default_port),
            user: url.username().to_string(),
            password: url.password().unwrap_or("").to_string(),
            database: url.path().trim_start_matches('/').to_string(),
        })
    }

    /// Parse a key=value style DSN (PostgreSQL libpq format).
    ///
    /// Common keys: `host`, `port`, `user`, `password`, `dbname`/`database`
    ///
    /// # Arguments
    ///
    /// * `dsn` - Space-separated key=value pairs
    /// * `default_port` - Port to use if not specified
    /// * `default_user` - Username if not specified
    /// * `default_database` - Database if not specified
    ///
    /// # Examples
    ///
    /// ```
    /// use common::dsn::DsnComponents;
    ///
    /// let comp = DsnComponents::from_keyvalue(
    ///     "host=localhost port=5432 user=postgres password=secret dbname=mydb",
    ///     5432, "postgres", "postgres"
    /// );
    /// assert_eq!(comp.host, "localhost");
    /// assert_eq!(comp.password, "secret");
    /// ```
    pub fn from_keyvalue(
        dsn: &str,
        default_port: u16,
        default_user: &str,
        default_database: &str,
    ) -> Self {
        let mut components = Self {
            host: "localhost".to_string(),
            port: default_port,
            user: default_user.to_string(),
            password: String::new(),
            database: default_database.to_string(),
        };

        for part in dsn.split_whitespace() {
            if let Some((key, value)) = part.split_once('=') {
                match key.to_lowercase().as_str() {
                    "host" => components.host = value.to_string(),
                    "port" => {
                        components.port = value.parse().unwrap_or(default_port)
                    }
                    "user" => components.user = value.to_string(),
                    "password" => components.password = value.to_string(),
                    "dbname" | "database" => {
                        components.database = value.to_string()
                    }
                    _ => {} // Ignore unknown keys
                }
            }
        }

        components
    }

    /// Check if this DSN has authentication credentials.
    pub fn has_credentials(&self) -> bool {
        !self.user.is_empty() || !self.password.is_empty()
    }
}

// =============================================================================
// Password Redaction
// =============================================================================

/// Redact password from a URL-style DSN for safe logging.
///
/// Works with any URL-style connection string. If the URL has no password
/// or cannot be parsed, returns the original string unchanged.
///
/// # Examples
///
/// ```
/// use common::dsn::redact_url_password;
///
/// let dsn = "postgres://user:secret@localhost/db";
/// let safe = redact_url_password(dsn);
/// assert!(!safe.contains("secret"));
/// assert!(safe.contains("***"));
/// ```
pub fn redact_url_password(dsn: &str) -> String {
    if let Ok(mut url) = Url::parse(dsn) {
        if url.password().is_some() {
            let _ = url.set_password(Some("***"));
        }
        url.to_string()
    } else {
        dsn.to_string()
    }
}

/// Redact password from a key=value style DSN for safe logging.
///
/// # Examples
///
/// ```
/// use common::dsn::redact_keyvalue_password;
///
/// let dsn = "host=localhost password=secret user=test";
/// let safe = redact_keyvalue_password(dsn);
/// assert!(!safe.contains("secret"));
/// assert!(safe.contains("password=***"));
/// ```
pub fn redact_keyvalue_password(dsn: &str) -> String {
    dsn.split_whitespace()
        .map(|p| {
            if p.to_lowercase().starts_with("password=") {
                "password=***"
            } else {
                p
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// Redact sensitive data from any DSN format (auto-detects format).
///
/// This is a convenience function that handles both URL and key=value formats.
/// Use this when you don't know the DSN format in advance.
///
/// # Examples
///
/// ```
/// use common::dsn::redact_dsn;
///
/// // URL style
/// assert!(!redact_dsn("mysql://root:secret@localhost/db").contains("secret"));
///
/// // Key-value style
/// assert!(!redact_dsn("host=localhost password=secret").contains("secret"));
/// ```
pub fn redact_dsn(dsn: &str) -> String {
    if dsn.contains("://") {
        redact_url_password(dsn)
    } else {
        redact_keyvalue_password(dsn)
    }
}

/// Redact auth token from a URL query parameter for safe logging.
///
/// Used primarily for services that use query parameter authentication,
/// such as Turso/libSQL (`authToken=`) or signed URLs.
///
/// # Examples
///
/// ```
/// use common::dsn::redact_auth_token;
///
/// let url = "libsql://db.turso.io?authToken=secret123";
/// let safe = redact_auth_token(url);
/// assert!(!safe.contains("secret123"));
/// assert!(safe.contains("authToken=***"));
/// ```
pub fn redact_auth_token(url: &str) -> String {
    if let Some(idx) = url.find("authToken=") {
        let end = url[idx..].find('&').unwrap_or(url.len() - idx);
        format!("{}authToken=***{}", &url[..idx], &url[idx + end..])
    } else {
        url.to_string()
    }
}

// =============================================================================
// Host Extraction
// =============================================================================

/// Extract host from a URL for metadata purposes.
///
/// Handles various URL formats and extracts just the host portion,
/// stripping credentials, ports, paths, and query strings.
///
/// # Examples
///
/// ```
/// use common::dsn::extract_host_from_url;
///
/// assert_eq!(extract_host_from_url("postgres://user:pass@db.example.com:5432/mydb"), "db.example.com");
/// assert_eq!(extract_host_from_url("libsql://mydb.turso.io"), "mydb.turso.io");
/// ```
pub fn extract_host_from_url(url: &str) -> String {
    // Try proper URL parsing first
    if let Ok(parsed) = Url::parse(url) {
        if let Some(host) = parsed.host_str() {
            return host.to_string();
        }
    }

    // Fallback: manual extraction
    url.split("://")
        .nth(1)
        .and_then(|s| s.split('/').next())
        .and_then(|s| s.split('?').next())
        .and_then(|s| s.split('@').next_back())
        .and_then(|s| s.split(':').next()) // Remove port
        .unwrap_or("unknown")
        .to_string()
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    mod dsn_components {
        use super::*;

        #[test]
        fn from_url_parses_all_components() {
            let comp = DsnComponents::from_url(
                "postgres://user:pass@localhost:5433/mydb",
                5432,
            )
            .unwrap();
            assert_eq!(comp.host, "localhost");
            assert_eq!(comp.port, 5433);
            assert_eq!(comp.user, "user");
            assert_eq!(comp.password, "pass");
            assert_eq!(comp.database, "mydb");
        }

        #[test]
        fn from_url_uses_default_port_when_missing() {
            let comp = DsnComponents::from_url(
                "postgres://user:pass@localhost/mydb",
                5432,
            )
            .unwrap();
            assert_eq!(comp.port, 5432);
        }

        #[test]
        fn from_url_handles_missing_password() {
            let comp =
                DsnComponents::from_url("postgres://user@localhost/db", 5432)
                    .unwrap();
            assert_eq!(comp.user, "user");
            assert_eq!(comp.password, "");
        }

        #[test]
        fn has_credentials_checks_user_or_password() {
            let with_both = DsnComponents::from_url(
                "postgres://user:pass@localhost/db",
                5432,
            )
            .unwrap();
            assert!(with_both.has_credentials());

            let user_only =
                DsnComponents::from_url("postgres://user@localhost/db", 5432)
                    .unwrap();
            assert!(user_only.has_credentials()); // user alone counts

            let neither =
                DsnComponents::from_url("postgres://localhost/db", 5432)
                    .unwrap();
            assert!(!neither.has_credentials());
        }

        #[test]
        fn from_url_returns_error_for_invalid() {
            assert!(DsnComponents::from_url("not a url", 5432).is_err());
        }

        #[test]
        fn from_keyvalue_parses_all_components() {
            let comp = DsnComponents::from_keyvalue(
                "host=127.0.0.1 port=5433 user=postgres password=secret dbname=test",
                5432,
                "default_user",
                "default_db",
            );
            assert_eq!(comp.host, "127.0.0.1");
            assert_eq!(comp.port, 5433);
            assert_eq!(comp.user, "postgres");
            assert_eq!(comp.password, "secret");
            assert_eq!(comp.database, "test");
        }

        #[test]
        fn from_keyvalue_uses_defaults_for_missing() {
            let comp = DsnComponents::from_keyvalue(
                "host=myhost",
                5432,
                "default",
                "defaultdb",
            );
            assert_eq!(comp.host, "myhost");
            assert_eq!(comp.port, 5432);
            assert_eq!(comp.user, "default");
            assert_eq!(comp.database, "defaultdb");
        }
    }

    mod redaction {
        use super::*;

        #[test]
        fn redact_url_password_replaces_with_stars() {
            let dsn = "postgres://user:secret@localhost/db";
            let redacted = redact_url_password(dsn);
            assert!(
                !redacted.contains("secret"),
                "password should be redacted"
            );
            assert!(redacted.contains("***"), "should show redaction marker");
            assert!(redacted.contains("user"), "username should remain");
        }

        #[test]
        fn redact_url_password_preserves_url_without_password() {
            let dsn = "postgres://user@localhost/db";
            assert_eq!(redact_url_password(dsn), dsn);
        }

        #[test]
        fn redact_url_password_returns_invalid_urls_unchanged() {
            let dsn = "not a valid url";
            assert_eq!(redact_url_password(dsn), dsn);
        }

        #[test]
        fn redact_keyvalue_password_replaces_with_stars() {
            let dsn = "host=localhost password=secret user=test";
            let redacted = redact_keyvalue_password(dsn);
            assert!(!redacted.contains("secret"));
            assert!(redacted.contains("password=***"));
        }

        #[test]
        fn redact_auth_token_in_query_string() {
            let url = "libsql://db.turso.io?authToken=secret123";
            let redacted = redact_auth_token(url);
            assert!(!redacted.contains("secret123"));
            assert!(redacted.contains("authToken=***"));
        }

        #[test]
        fn redact_auth_token_preserves_other_params() {
            let url = "libsql://db.turso.io?foo=bar&authToken=secret&baz=qux";
            let redacted = redact_auth_token(url);
            assert!(!redacted.contains("secret"));
            assert!(redacted.contains("foo=bar"));
            assert!(redacted.contains("baz=qux"));
        }
    }

    mod host_extraction {
        use super::*;

        #[test]
        fn extract_host_strips_credentials_port_and_path() {
            assert_eq!(
                extract_host_from_url(
                    "postgres://user:pass@db.example.com:5432/mydb"
                ),
                "db.example.com"
            );
        }

        #[test]
        fn extract_host_from_simple_url() {
            assert_eq!(
                extract_host_from_url("libsql://mydb.turso.io"),
                "mydb.turso.io"
            );
        }
    }
}
