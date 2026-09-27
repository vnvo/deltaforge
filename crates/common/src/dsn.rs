//! DSN (Data Source Name) parsing and credential redaction utilities.
//!
//! This module provides tools for working with database and service
//! connection strings across different formats:
//!
//! - **URL format**: `postgres://user:pass@host:port/db`
//! - **Key-value format**: `host=localhost user=postgres password=secret`
//! - **Query parameter auth**: `https://api.example.com/db?authToken=secret`
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
    /// - `http://`, `https://`
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

        // URL userinfo is percent-encoded per the URL spec; decode it so callers
        // (e.g. the PostgreSQL replication config) receive the raw credential,
        // matching what `tokio_postgres`/`mysql` clients do when they parse the URL
        // themselves. Non-UTF-8 or malformed escapes fall back to the raw text.
        Ok(Self {
            host: url.host_str().unwrap_or("localhost").to_string(),
            port: url.port().unwrap_or(default_port),
            user: percent_decode(url.username()),
            password: percent_decode(url.password().unwrap_or("")),
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

        // Best-effort for the general parser: a malformed DSN yields no pairs
        // (defaults are kept); strict callers use `tokenize_libpq` directly.
        for (key, value) in tokenize_libpq(dsn).unwrap_or_default() {
            match key.to_lowercase().as_str() {
                "host" => components.host = value,
                "port" => {
                    components.port = value.parse().unwrap_or(default_port)
                }
                "user" => components.user = value,
                "password" => components.password = value,
                "dbname" | "database" => components.database = value,
                _ => {} // Ignore unknown keys
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
// libpq key=value parsing and DSN reconstruction
// =============================================================================

/// Tokenize a libpq-style `key=value` connection string into ordered pairs.
///
/// Implements the libpq quoting rules precisely so reconstruction and redaction
/// can both rely on it:
/// - single-quoted values may contain whitespace; `\` escapes the next char
///   (used for `\'` and `\\`) inside quotes,
/// - unquoted values also honor `\` escapes (e.g. `secret\ value` is one value),
/// - after a closing quote the next character must be whitespace (or end),
/// - an unterminated quote or a trailing `\` with no following char is malformed.
///
/// Fails closed (`Err`) on any malformed fragment rather than silently
/// reinterpreting it - callers that must not leak (redaction) or must not corrupt
/// options (credential injection) depend on that.
fn tokenize_libpq(dsn: &str) -> Result<Vec<(String, String)>, ()> {
    let chars: Vec<char> = dsn.chars().collect();
    let n = chars.len();
    let mut i = 0;
    let mut pairs = Vec::new();
    while i < n {
        // Skip whitespace between pairs.
        while i < n && chars[i].is_whitespace() {
            i += 1;
        }
        if i >= n {
            break;
        }
        // Keyword up to '=' or whitespace.
        let key_start = i;
        while i < n && chars[i] != '=' && !chars[i].is_whitespace() {
            i += 1;
        }
        let key: String = chars[key_start..i].iter().collect();
        if key.is_empty() {
            return Err(()); // stray '=' or separator with no keyword
        }
        while i < n && chars[i].is_whitespace() {
            i += 1;
        }
        if i >= n || chars[i] != '=' {
            return Err(()); // keyword with no '=' value
        }
        i += 1; // consume '='
        while i < n && chars[i].is_whitespace() {
            i += 1;
        }
        // Value: single-quoted or bare, both escape-aware.
        let mut value = String::new();
        if i < n && chars[i] == '\'' {
            i += 1; // opening quote
            let mut closed = false;
            while i < n {
                let c = chars[i];
                if c == '\\' {
                    if i + 1 >= n {
                        return Err(()); // dangling escape
                    }
                    value.push(chars[i + 1]);
                    i += 2;
                } else if c == '\'' {
                    i += 1; // closing quote
                    closed = true;
                    break;
                } else {
                    value.push(c);
                    i += 1;
                }
            }
            if !closed {
                return Err(()); // unterminated quote
            }
            // A closing quote must be followed by a separator or end of input.
            if i < n && !chars[i].is_whitespace() {
                return Err(()); // junk immediately after closing quote
            }
        } else {
            while i < n && !chars[i].is_whitespace() {
                let c = chars[i];
                if c == '\\' {
                    if i + 1 >= n {
                        return Err(()); // dangling escape
                    }
                    value.push(chars[i + 1]);
                    i += 2;
                } else {
                    value.push(c);
                    i += 1;
                }
            }
        }
        pairs.push((key, value));
    }
    Ok(pairs)
}

/// Escape a value for inclusion in a libpq `key=value` DSN by single-quoting it
/// and backslash-escaping `\` and `'`. Always quoting is valid libpq and keeps
/// values with spaces or special characters intact through both `tokio_postgres`
/// and [`DsnComponents::from_keyvalue`].
fn quote_libpq_value(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('\'');
    for c in value.chars() {
        if c == '\\' || c == '\'' {
            out.push('\\');
        }
        out.push(c);
    }
    out.push('\'');
    out
}

/// Quote a libpq value only when it needs it (empty, or containing whitespace,
/// a quote, or a backslash), so ordinary options round-trip textually unchanged.
fn quote_libpq_if_needed(value: &str) -> String {
    let needs = value.is_empty()
        || value
            .chars()
            .any(|c| c.is_whitespace() || c == '\'' || c == '\\');
    if needs {
        quote_libpq_value(value)
    } else {
        value.to_string()
    }
}

/// Replace (or append) **only** the `user` and `password` in a libpq `key=value`
/// DSN, re-emitting every other key/value pair in its original order and form.
/// libpq does not percent-decode, so the injected credentials are literal. Any
/// pre-existing `user`/`password` keys are normalized to the single injected pair;
/// all other options (sslmode, connect_timeout, application_name, options, host,
/// hostaddr, target_session_attrs, keepalives, unix socket, unknown/future keys)
/// round-trip unchanged. A malformed fragment (a token with no `=`) fails closed.
pub fn replace_libpq_credentials(
    dsn: &str,
    user: &str,
    password: &str,
) -> Result<String, String> {
    let pairs = tokenize_libpq(dsn)
        .map_err(|_| "malformed libpq key=value DSN".to_string())?;
    let mut out: Vec<String> = Vec::with_capacity(pairs.len() + 2);
    let mut user_done = false;
    let mut password_done = false;
    for (key, value) in pairs {
        let lower = key.to_lowercase();
        if lower == "user" {
            if !user_done {
                out.push(format!("user={}", quote_libpq_if_needed(user)));
                user_done = true;
            }
            // drop duplicate user keys
        } else if lower == "password" {
            if !password_done {
                out.push(format!(
                    "password={}",
                    quote_libpq_if_needed(password)
                ));
                password_done = true;
            }
            // drop duplicate password keys
        } else {
            out.push(format!("{key}={}", quote_libpq_if_needed(&value)));
        }
    }
    if !user_done {
        out.push(format!("user={}", quote_libpq_if_needed(user)));
    }
    if !password_done {
        out.push(format!("password={}", quote_libpq_if_needed(password)));
    }
    Ok(out.join(" "))
}

/// Inject a username/password into a URL-style base DSN, percent-encoding them via
/// the `url` crate (never manual concatenation) and preserving host, path, and all
/// query parameters. An empty password is omitted. Used for both MySQL and
/// URL-form PostgreSQL DSNs (their clients, and [`DsnComponents::from_url`],
/// percent-decode the userinfo back to the raw value).
pub fn inject_url_credentials(
    base_url: &str,
    user: &str,
    password: &str,
) -> Result<String, String> {
    let mut url = Url::parse(base_url).map_err(|e| e.to_string())?;
    url.set_username(user)
        .map_err(|_| "cannot set username on this URL".to_string())?;
    let pw = if password.is_empty() {
        None
    } else {
        Some(password)
    };
    url.set_password(pw)
        .map_err(|_| "cannot set password on this URL".to_string())?;
    Ok(url.to_string())
}

/// Decode `%XX` percent-escapes in a URL component, falling back to the original
/// text if an escape is malformed or the result is not UTF-8.
fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).ok();
            if let Some(h) = hex.and_then(|h| u8::from_str_radix(h, 16).ok()) {
                out.push(h);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8(out).unwrap_or_else(|_| s.to_string())
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
    // Use the quote-aware tokenizer and replace the whole password value. A naive
    // whitespace split would leak part of a quoted password such as
    // `password='secret value'`. If the DSN cannot be parsed, fail safe: return a
    // fully redacted placeholder rather than any original fragment.
    match tokenize_libpq(dsn) {
        Ok(pairs) => pairs
            .iter()
            .map(|(key, value)| {
                if key.eq_ignore_ascii_case("password") {
                    "password=***".to_string()
                } else {
                    format!("{key}={}", quote_libpq_if_needed(value))
                }
            })
            .collect::<Vec<_>>()
            .join(" "),
        Err(()) => REDACTED_DSN.to_string(),
    }
}

/// Placeholder returned when a DSN cannot be parsed for redaction, so no original
/// (possibly sensitive) fragment is ever surfaced publicly.
const REDACTED_DSN: &str = "<redacted>";

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
        // URL form: redact the userinfo password, but if the URL does not parse,
        // fail safe rather than returning the original (possibly sensitive) string.
        // This is the redactor used at the public status boundary.
        match Url::parse(dsn) {
            Ok(mut url) => {
                if url.password().is_some() {
                    let _ = url.set_password(Some("***"));
                }
                url.to_string()
            }
            Err(_) => REDACTED_DSN.to_string(),
        }
    } else {
        redact_keyvalue_password(dsn)
    }
}

/// Redact auth token from a URL query parameter for safe logging.
///
/// Used primarily for services that use query parameter authentication,
/// such as services using token/signed-URL auth (`authToken=`).
///
/// # Examples
///
/// ```
/// use common::dsn::redact_auth_token;
///
/// let url = "https://api.example.com/db?authToken=secret123";
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
/// assert_eq!(extract_host_from_url("https://mydb.example.com"), "mydb.example.com");
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

    #[test]
    fn redact_dsn_hides_password_but_keeps_the_rest() {
        // URL style: password gone, host retained. The positive assertions
        // pin `redact_dsn` against being replaced by "" / a constant.
        let url = redact_dsn("mysql://root:secret@localhost/db");
        assert!(!url.contains("secret"), "password must be redacted: {url}");
        assert!(url.contains("localhost"), "host must survive: {url}");

        // Key-value style.
        let kv = redact_dsn("host=localhost password=secret");
        assert!(!kv.contains("secret"), "password must be redacted: {kv}");
        assert!(kv.contains("host=localhost"), "host must survive: {kv}");
    }

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

        #[test]
        fn from_keyvalue_parses_quoted_values_with_spaces_and_escapes() {
            // A quoted password with a space and an escaped quote/backslash.
            let comp = DsnComponents::from_keyvalue(
                r"host=localhost user='u' password='p\'a ss\\x' dbname=db",
                5432,
                "d",
                "d",
            );
            assert_eq!(comp.user, "u");
            assert_eq!(comp.password, "p'a ss\\x");
            assert_eq!(comp.database, "db");
        }
    }

    mod reconstruction {
        use super::*;

        #[test]
        fn libpq_replace_preserves_options_and_injects_credentials() {
            // Base DSN with SSL, timeout, application_name, an options string with
            // spaces, an unknown/future key, and a pre-existing (to be replaced)
            // user/password.
            let base = "host=db.internal port=5432 dbname=orders \
                        user=old password=old sslmode=require \
                        connect_timeout=10 application_name='df worker' \
                        options='-c statement_timeout=1000' \
                        target_session_attrs=read-write \
                        future_opt=keepme";
            let dsn =
                replace_libpq_credentials(base, "svc", "p@ss:w0rd/x!").unwrap();
            let comp = DsnComponents::from_keyvalue(&dsn, 5432, "d", "d");
            // Credentials replaced with the exact literal values.
            assert_eq!(comp.user, "svc");
            assert_eq!(comp.password, "p@ss:w0rd/x!");
            // Non-credential options preserved.
            assert!(dsn.contains("sslmode=require"), "{dsn}");
            assert!(dsn.contains("connect_timeout=10"), "{dsn}");
            assert!(dsn.contains("application_name='df worker'"), "{dsn}");
            assert!(
                dsn.contains("options='-c statement_timeout=1000'"),
                "{dsn}"
            );
            assert!(dsn.contains("target_session_attrs=read-write"), "{dsn}");
            assert!(dsn.contains("future_opt=keepme"), "{dsn}");
            // Old credentials gone; exactly one user and one password.
            assert!(!dsn.contains("=old"));
            assert_eq!(dsn.matches("user=").count(), 1);
            assert_eq!(dsn.matches("password=").count(), 1);
        }

        #[test]
        fn libpq_replace_appends_when_absent_and_handles_unix_socket() {
            let base = "host=/var/run/postgresql dbname=orders";
            let dsn =
                replace_libpq_credentials(base, "svc", r"a b'c\d").unwrap();
            let comp = DsnComponents::from_keyvalue(&dsn, 5432, "d", "d");
            assert_eq!(comp.host, "/var/run/postgresql");
            assert_eq!(comp.user, "svc");
            assert_eq!(comp.password, r"a b'c\d");
        }

        #[test]
        fn libpq_replace_fails_closed_on_malformed() {
            // A bare token with no '=' is malformed and must not be skipped.
            let err =
                replace_libpq_credentials("host=db bogusfragment", "u", "p");
            assert!(err.is_err());
        }

        #[test]
        fn url_injection_preserves_query_and_percent_encodes() {
            // Base URL with SSL and other query params, and a path.
            let base = "postgres://db.internal:5432/orders?sslmode=require&application_name=df";
            let dsn = inject_url_credentials(base, "svc_user", "p@ss:w0rd/x")
                .unwrap();
            let parsed = Url::parse(&dsn).unwrap();
            assert_eq!(parsed.host_str(), Some("db.internal"));
            assert_eq!(parsed.path(), "/orders");
            // Query parameters preserved.
            let query = parsed.query().unwrap_or("");
            assert!(query.contains("sslmode=require"), "{query}");
            assert!(query.contains("application_name=df"), "{query}");
            // Userinfo percent-encoded on the wire, decodes back to raw.
            assert!(!dsn.contains("p@ss:w0rd/x"));
            assert_eq!(percent_decode(parsed.username()), "svc_user");
            assert_eq!(
                percent_decode(parsed.password().unwrap()),
                "p@ss:w0rd/x"
            );
        }

        #[test]
        fn from_url_decodes_percent_encoded_userinfo() {
            // A URL with percent-encoded credentials decodes to the raw values,
            // matching what real clients do.
            let comp = DsnComponents::from_url(
                "postgres://svc%20user:p%40ss%3Aw0rd@db/orders",
                5432,
            )
            .unwrap();
            assert_eq!(comp.user, "svc user");
            assert_eq!(comp.password, "p@ss:w0rd");
        }

        #[test]
        fn unquoted_backslash_escape_roundtrips_semantically() {
            // `secret\ value` is a single value "secret value".
            let comp = DsnComponents::from_keyvalue(
                r"host=db password=secret\ value",
                5432,
                "",
                "",
            );
            assert_eq!(comp.password, "secret value");

            // A non-credential option with an escaped space is preserved (value
            // unchanged) through credential injection.
            let dsn = replace_libpq_credentials(
                r"host=db application_name=my\ app",
                "u",
                "p",
            )
            .unwrap();
            assert!(dsn.contains("application_name='my app'"), "{dsn}");
        }

        #[test]
        fn junk_after_closing_quote_fails_closed() {
            assert!(
                replace_libpq_credentials("host=db password='x'y", "u", "p")
                    .is_err()
            );
        }

        #[test]
        fn unterminated_quote_and_dangling_escape_fail_closed() {
            assert!(
                replace_libpq_credentials("host=db password='x", "u", "p")
                    .is_err()
            );
            assert!(
                replace_libpq_credentials("host=db password=x\\", "u", "p")
                    .is_err()
            );
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
        fn redact_quoted_password_with_whitespace_never_leaks() {
            // A naive whitespace split would leak "value'"; the quote-aware
            // redactor replaces the entire value.
            let dsn = "host=db password='secret value' sslmode=require";
            let r = redact_dsn(dsn);
            assert!(!r.contains("secret"), "{r}");
            assert!(!r.contains("value"), "{r}");
            assert!(r.contains("password=***"), "{r}");
            assert!(r.contains("sslmode=require"), "{r}");
        }

        #[test]
        fn redact_quoted_password_with_quotes_and_backslashes_never_leaks() {
            let dsn = r"host=db password='a\'b c\\d' x=y";
            let r = redact_dsn(dsn);
            assert!(!r.contains("a'b"), "{r}");
            assert!(!r.contains(r"c\d"), "{r}");
            assert!(r.contains("password=***"), "{r}");
        }

        #[test]
        fn redaction_parse_failure_returns_no_fragments() {
            // Unterminated quote containing a sentinel: nothing of the original
            // may appear in the public output.
            let dsn = "host=db password='S3NT1NEL-secret";
            let r = redact_dsn(dsn);
            assert!(!r.contains("S3NT1NEL-secret"), "{r}");
            assert_eq!(r, "<redacted>");

            // Junk after a closing quote also fails safe.
            let dsn2 = "host=db password='x'S3NT1NEL";
            let r2 = redact_dsn(dsn2);
            assert!(!r2.contains("S3NT1NEL"), "{r2}");
            assert_eq!(r2, "<redacted>");
        }

        #[test]
        fn redact_dsn_url_parse_failure_fails_safe() {
            // Contains "://" but is not a valid URL: must not echo the original.
            let dsn = "postgres://:sec ret@ bad url/db";
            let r = redact_dsn(dsn);
            assert!(!r.contains("sec ret"), "{r}");
        }

        #[test]
        fn redact_auth_token_in_query_string() {
            let url = "https://api.example.com/db?authToken=secret123";
            let redacted = redact_auth_token(url);
            assert!(!redacted.contains("secret123"));
            assert!(redacted.contains("authToken=***"));
        }

        #[test]
        fn redact_auth_token_preserves_other_params() {
            let url =
                "https://api.example.com/db?foo=bar&authToken=secret&baz=qux";
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
                extract_host_from_url("https://mydb.example.com"),
                "mydb.example.com"
            );
        }
    }
}
