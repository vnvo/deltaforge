//! The immutable-publication contract: a publication DeltaForge streams
//! from is registered, owned by a NOLOGIN role, and cannot change while any
//! registration exists in its database. Enforcement is two event triggers
//! that work while DeltaForge is offline:
//! - `ddl_command_start` refuses every ALTER and DROP PUBLICATION, for every
//!   role including superusers, before PostgreSQL starts the command (so
//!   none reaches its per-relation command collection);
//! - `sql_drop` refuses any drop that removes a registered publication or a
//!   member of one (DROP TABLE, DROP SCHEMA ... CASCADE, DROP OWNED, ...).
//!
//! There is no `ddl_command_end` trigger. Changing a publication is a
//! database-wide maintenance procedure: every source stopped, every
//! registration removed, enforcement uninstalled, the change applied, the
//! publications registered again (new digests) and the sources restarted.
//!
//! A superuser can disable or drop the triggers: that is outside the
//! guarantee; sources detect it at startup and on every reconnect.

use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};
use sha2::{Digest, Sha256};
use tokio_postgres::{Client, GenericClient};

/// Owner of the schema, the registration table and the guard functions.
pub const OWNER: &str = "deltaforge_owner";
/// Owner of every registered publication.
pub const PUB_OWNER: &str = "deltaforge_publication_owner";
/// The `ddl_command_start` trigger.
pub const DDL_GUARD: &str = "deltaforge_publication_guard";
/// The `sql_drop` trigger.
pub const DROP_GUARD: &str = "deltaforge_drop_guard";

/// The installed objects (idempotent; run by a superuser).
pub const INSTALL: &str = r##"
DO $do$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'deltaforge_owner') THEN
    CREATE ROLE deltaforge_owner NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'deltaforge_publication_owner') THEN
    CREATE ROLE deltaforge_publication_owner NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
  END IF;
END $do$;

CREATE SCHEMA IF NOT EXISTS deltaforge AUTHORIZATION deltaforge_owner;
REVOKE ALL ON SCHEMA deltaforge FROM PUBLIC;
GRANT USAGE ON SCHEMA deltaforge TO PUBLIC;

CREATE TABLE IF NOT EXISTS deltaforge.registration (
  pub_oid oid PRIMARY KEY,
  pubname text NOT NULL UNIQUE,
  digest text NOT NULL,
  previous_owner oid NOT NULL,
  registered_lsn pg_lsn NOT NULL,
  registered_at timestamptz NOT NULL DEFAULT pg_catalog.now()
);
ALTER TABLE deltaforge.registration OWNER TO deltaforge_owner;
REVOKE ALL ON deltaforge.registration FROM PUBLIC;
GRANT SELECT ON deltaforge.registration TO PUBLIC;

-- ddl_command_start, tags ALTER and DROP PUBLICATION: refused for every
-- role while any registration exists.
CREATE OR REPLACE FUNCTION deltaforge.guard_publication_ddl() RETURNS event_trigger
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, pg_temp AS $f$
DECLARE names text;
BEGIN
  SELECT pg_catalog.string_agg(r.pubname, ', ' ORDER BY r.pubname) INTO names
  FROM deltaforge.registration r;
  IF names IS NOT NULL THEN
    RAISE EXCEPTION 'deltaforge: % is refused while publications are registered in this database (%)', tg_tag, names
      USING HINT = 'Publications are immutable while DeltaForge sources use them: stop every source of this database and run the publication maintenance procedure.';
  END IF;
END $f$;
ALTER FUNCTION deltaforge.guard_publication_ddl() OWNER TO deltaforge_owner;
REVOKE ALL ON FUNCTION deltaforge.guard_publication_ddl() FROM PUBLIC;

-- sql_drop: refuses removing a registered publication or any member of one.
CREATE OR REPLACE FUNCTION deltaforge.guard_drop() RETURNS event_trigger
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, pg_temp AS $f$
DECLARE hit text;
BEGIN
  SELECT pg_catalog.string_agg(DISTINCT d.object_identity, ', ') INTO hit
  FROM pg_catalog.pg_event_trigger_dropped_objects() d
  WHERE (d.object_type = 'publication relation'
         AND d.address_args[1] IN (SELECT r.pubname FROM deltaforge.registration r))
     OR (d.object_type = 'publication'
         AND d.objid IN (SELECT r.pub_oid FROM deltaforge.registration r));
  IF hit IS NOT NULL THEN
    RAISE EXCEPTION 'deltaforge: % would remove registered publication membership (%)', tg_tag, hit
      USING HINT = 'Publications are immutable while DeltaForge sources use them: stop every source of this database and run the publication maintenance procedure.';
  END IF;
END $f$;
ALTER FUNCTION deltaforge.guard_drop() OWNER TO deltaforge_owner;
REVOKE ALL ON FUNCTION deltaforge.guard_drop() FROM PUBLIC;
"##;

/// The event triggers (created by the installing superuser).
pub const TRIGGERS: &str = r##"
CREATE EVENT TRIGGER deltaforge_publication_guard ON ddl_command_start
  WHEN TAG IN ('ALTER PUBLICATION', 'DROP PUBLICATION')
  EXECUTE FUNCTION deltaforge.guard_publication_ddl();
ALTER EVENT TRIGGER deltaforge_publication_guard ENABLE ALWAYS;
CREATE EVENT TRIGGER deltaforge_drop_guard ON sql_drop
  EXECUTE FUNCTION deltaforge.guard_drop();
ALTER EVENT TRIGGER deltaforge_drop_guard ENABLE ALWAYS;
"##;

/// One registration row.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Registration {
    pub pub_oid: u32,
    pub pubname: String,
    pub digest: String,
    pub previous_owner: u32,
    pub registered_lsn: String,
}

/// A publication as the catalogs show it now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveState {
    pub pub_oid: u32,
    pub owner: String,
    pub digest: String,
    /// Why the publication cannot be registered (empty: it can).
    pub unsupported: Vec<String>,
    /// Explicit members (relid, qualified name), by relid.
    pub members: Vec<(u32, String)>,
}

/// The canonical state of `pubname`, read in one snapshot. `None`: absent.
///
/// The digest covers every publication fact rows depend on, by OID (names of
/// tables play no part): its own name, OID and owner, `puballtables`, the
/// publish flags, `pubviaroot`, `pubgencols`, the schema entries, and each
/// member's relid, relkind, row filter and column list.
pub async fn live_state(
    client: &Client,
    pubname: &str,
) -> Result<Option<LiveState>> {
    in_tx(client, READ, async |c| read_state(c, pubname).await).await
}

async fn read_state(
    c: &impl GenericClient,
    pubname: &str,
) -> Result<Option<LiveState>> {
    let Some(p) = c
        .query_opt(
            "SELECT p.oid, pg_catalog.pg_get_userbyid(p.pubowner)::text, \
             pg_catalog.json_build_array('p', p.pubname, p.oid::int8, p.pubowner::int8, \
               p.puballtables, p.pubinsert, p.pubupdate, p.pubdelete, p.pubtruncate, \
               p.pubviaroot, pg_catalog.to_jsonb(p) ->> 'pubgencols')::text, \
             p.puballtables, p.pubinsert AND p.pubupdate AND p.pubdelete AND p.pubtruncate, \
             p.pubviaroot, coalesce(pg_catalog.to_jsonb(p) ->> 'pubgencols', 'n') \
             FROM pg_catalog.pg_publication p WHERE p.pubname = $1",
            &[&pubname],
        )
        .await?
    else {
        return Ok(None);
    };
    let pub_oid: u32 = p.get(0);
    let mut lines = vec![p.get::<_, String>(2)];
    let mut unsupported = Vec::new();
    if p.get::<_, bool>(3) {
        unsupported.push(
            "FOR ALL TABLES (membership changes with CREATE TABLE)".into(),
        );
    }
    if !p.get::<_, bool>(4) {
        unsupported.push(
            "publish must include insert, update, delete and truncate".into(),
        );
    }
    if p.get::<_, bool>(5) {
        unsupported.push("publish_via_partition_root is on".into());
    }
    if p.get::<_, String>(6) != "n" {
        unsupported.push("publish_generated_columns is on".into());
    }
    let has_schemas: bool = c
        .query_one(
            "SELECT pg_catalog.to_regclass('pg_catalog.pg_publication_namespace') IS NOT NULL",
            &[],
        )
        .await?
        .get(0);
    if has_schemas {
        for r in c
            .query(
                "SELECT pn.pnnspid::int8 FROM pg_catalog.pg_publication_namespace pn \
                 WHERE pn.pnpubid = $1 ORDER BY 1",
                &[&pub_oid],
            )
            .await?
        {
            lines.push(format!("[\"s\", {}]", r.get::<_, i64>(0)));
            unsupported.push("TABLES IN SCHEMA (membership changes with CREATE TABLE)".into());
        }
    }
    let mut members = Vec::new();
    for r in c
        .query(
            "SELECT pr.prrelid::int8, c.relkind::text, \
             pg_catalog.to_jsonb(pr) ->> 'prqual', pg_catalog.to_jsonb(pr) ->> 'prattrs', \
             n.nspname::text || '.' || c.relname::text \
             FROM pg_catalog.pg_publication_rel pr \
             JOIN pg_catalog.pg_class c ON c.oid = pr.prrelid \
             JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
             WHERE pr.prpubid = $1 ORDER BY 1",
            &[&pub_oid],
        )
        .await?
    {
        let relid = r.get::<_, i64>(0) as u32;
        let relkind: String = r.get(1);
        let qual: Option<String> = r.get(2);
        let attrs: Option<String> = r.get(3);
        let name: String = r.get(4);
        lines.push(
            serde_json::json!([
                "r",
                relid,
                relkind,
                qual.as_deref().map(sha_hex),
                attrs.as_deref().map(sha_hex)
            ])
            .to_string(),
        );
        if relkind == "p" {
            unsupported.push(format!("{name} is partitioned (membership changes with ATTACH)"));
        }
        if qual.is_some() {
            unsupported.push(format!("{name} has a row filter"));
        }
        if attrs.is_some() {
            unsupported.push(format!("{name} has a column list"));
        }
        members.push((relid, name));
    }
    let mut h = Sha256::new();
    h.update(b"deltaforge-publication-v1\0");
    for l in &lines {
        h.update(l.as_bytes());
        h.update(b"\n");
    }
    Ok(Some(LiveState {
        pub_oid,
        owner: p.get(1),
        digest: hex::encode(h.finalize()),
        unsupported,
        members,
    }))
}

/// Run `f` in a transaction on `c` (committed on success, rolled back on
/// any error).
async fn in_tx<T>(
    c: &Client,
    begin: &str,
    f: impl AsyncFnOnce(&Client) -> Result<T>,
) -> Result<T> {
    c.batch_execute(begin).await?;
    match f(c).await {
        Ok(v) => {
            c.batch_execute("COMMIT").await?;
            Ok(v)
        }
        Err(e) => {
            let _ = c.batch_execute("ROLLBACK").await;
            Err(e)
        }
    }
}

const READ: &str =
    "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY";

fn sha_hex(s: &str) -> String {
    hex::encode(Sha256::digest(s.as_bytes()))
}

/// What registering changes database-wide, for the operator to confirm.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Impact {
    /// Every publication of the database with its owner.
    pub publications: Vec<(String, String)>,
    /// Publications already registered.
    pub registered: Vec<String>,
}

impl Impact {
    /// The text an operator confirms before registering.
    pub fn describe(&self, database: &str) -> String {
        let mut s = format!(
            "Registering makes publication DDL governance database-wide in \
             database {database}: while any publication is registered, every \
             ALTER PUBLICATION and DROP PUBLICATION is refused for every role, \
             superusers included, and no table of a registered publication can \
             be dropped. Changing any publication then requires stopping every \
             DeltaForge source of this database and the maintenance procedure.\n\
             Publications in this database:\n"
        );
        for (p, o) in &self.publications {
            let reg = if self.registered.contains(p) {
                " (registered)"
            } else {
                ""
            };
            s.push_str(&format!("  {p} (owner {o}){reg}\n"));
        }
        s
    }
}

pub async fn impact(client: &Client) -> Result<Impact> {
    let publications = client
        .query(
            "SELECT p.pubname::text, pg_catalog.pg_get_userbyid(p.pubowner)::text \
             FROM pg_catalog.pg_publication p ORDER BY 1",
            &[],
        )
        .await?
        .into_iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();
    Ok(Impact {
        publications,
        registered: registrations(client)
            .await?
            .into_iter()
            .map(|r| r.pubname)
            .collect(),
    })
}

/// Every registration (none if not installed).
pub async fn registrations(
    c: &impl GenericClient,
) -> Result<Vec<Registration>> {
    let installed: bool = c
        .query_one(
            "SELECT pg_catalog.to_regclass('deltaforge.registration') IS NOT NULL",
            &[],
        )
        .await?
        .get(0);
    if !installed {
        return Ok(Vec::new());
    }
    Ok(c.query(
        "SELECT pub_oid, pubname, digest, previous_owner, registered_lsn::text \
             FROM deltaforge.registration ORDER BY pubname",
        &[],
    )
    .await?
    .into_iter()
    .map(|r| Registration {
        pub_oid: r.get(0),
        pubname: r.get(1),
        digest: r.get(2),
        previous_owner: r.get(3),
        registered_lsn: r.get(4),
    })
    .collect())
}

async fn require_superuser(c: &impl GenericClient) -> Result<()> {
    let su: bool = c
        .query_one(
            "SELECT rolsuper FROM pg_catalog.pg_roles WHERE rolname = current_user",
            &[],
        )
        .await?
        .get(0);
    if !su {
        bail!("the publication contract is administered by a superuser");
    }
    Ok(())
}

/// Run `f` with the DDL guard off inside the caller's transaction (it is
/// back on before the transaction can commit; a crash or rollback restores
/// it with everything else).
async fn guard_off(tx: &Client, off: bool) -> Result<()> {
    let installed: bool = tx
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_event_trigger WHERE evtname = $1)",
            &[&DDL_GUARD],
        )
        .await?
        .get(0);
    if installed {
        tx.batch_execute(&format!(
            "ALTER EVENT TRIGGER {DDL_GUARD} {}",
            if off { "DISABLE" } else { "ENABLE ALWAYS" }
        ))
        .await?;
    }
    Ok(())
}

/// Register `pubs` in one transaction: install enforcement if absent, check
/// each publication's shape, transfer it to the publication owner and record
/// its digest and the registration position. Refused (nothing changes) if
/// any publication cannot be registered.
pub async fn register(
    client: &Client,
    pubs: &[String],
) -> Result<Vec<Registration>> {
    let out =
        in_tx(client, "BEGIN", async |tx| register_tx(tx, pubs).await).await?;
    // A publication DDL that passed the start guard before a first install
    // committed can still apply after it (it waited on the publication's
    // lock): re-read, and undo the registration on any difference.
    let mut changed = Vec::new();
    for r in &out {
        let now = live_state(client, &r.pubname).await?;
        if now.as_ref().map(|l| &l.digest) != Some(&r.digest) {
            changed.push(r.pubname.clone());
        }
    }
    if !changed.is_empty() {
        let names: Vec<String> =
            out.iter().map(|r| r.pubname.clone()).collect();
        unregister(client, &names).await?;
        bail!(
            "publication(s) {} changed while being registered; the \
             registration was undone, retry",
            changed.join(", ")
        );
    }
    Ok(out)
}

async fn register_tx(
    tx: &Client,
    pubs: &[String],
) -> Result<Vec<Registration>> {
    require_superuser(tx).await?;
    tx.batch_execute(INSTALL)
        .await
        .context("install enforcement")?;
    tx.batch_execute(
        "LOCK TABLE deltaforge.registration IN SHARE ROW EXCLUSIVE MODE",
    )
    .await?;
    let have: i64 = tx
        .query_one(
            "SELECT count(*) FROM pg_catalog.pg_event_trigger WHERE evtname IN ($1, $2)",
            &[&DDL_GUARD, &DROP_GUARD],
        )
        .await?
        .get(0);
    if have == 0 {
        tx.batch_execute(TRIGGERS)
            .await
            .context("create the guards")?;
    } else if have != 2 {
        bail!("enforcement is partially installed; uninstall it first");
    }
    guard_off(tx, true).await?;
    let mut out = Vec::new();
    for name in pubs {
        let state = read_state(tx, name)
            .await?
            .with_context(|| format!("publication {name} does not exist"))?;
        if !state.unsupported.is_empty() {
            bail!(
                "publication {name} cannot be registered: {}",
                state.unsupported.join("; ")
            );
        }
        let previous: u32 = tx
            .query_one(
                "SELECT pubowner FROM pg_catalog.pg_publication WHERE oid = $1",
                &[&state.pub_oid],
            )
            .await?
            .get(0);
        tx.batch_execute(&format!(
            "ALTER PUBLICATION {} OWNER TO {PUB_OWNER}",
            quote_ident(name)
        ))
        .await?;
        let state = read_state(tx, name).await?.expect("still present");
        let lsn: String = tx
            .query_one(
                "INSERT INTO deltaforge.registration \
                 (pub_oid, pubname, digest, previous_owner, registered_lsn) \
                 VALUES ($1, $2, $3, $4, pg_catalog.pg_current_wal_lsn()) \
                 RETURNING registered_lsn::text",
                &[&state.pub_oid, name, &state.digest, &previous],
            )
            .await
            .with_context(|| {
                format!("publication {name} is already registered")
            })?
            .get(0);
        out.push(Registration {
            pub_oid: state.pub_oid,
            pubname: name.clone(),
            digest: state.digest,
            previous_owner: previous,
            registered_lsn: lsn,
        });
    }
    guard_off(tx, false).await?;
    Ok(out)
}

/// Remove the registrations of `pubs` and give each publication back to its
/// previous owner, in one transaction. Enforcement stays installed.
pub async fn unregister(client: &Client, pubs: &[String]) -> Result<()> {
    in_tx(client, "BEGIN", async |tx| unregister_tx(tx, pubs).await).await
}

async fn unregister_tx(tx: &Client, pubs: &[String]) -> Result<()> {
    require_superuser(tx).await?;
    tx.batch_execute(
        "LOCK TABLE deltaforge.registration IN SHARE ROW EXCLUSIVE MODE",
    )
    .await?;
    guard_off(tx, true).await?;
    for name in pubs {
        let row = tx
            .query_opt(
                "DELETE FROM deltaforge.registration WHERE pubname = $1 \
                 RETURNING pg_catalog.pg_get_userbyid(previous_owner)::text",
                &[name],
            )
            .await?
            .with_context(|| format!("publication {name} is not registered"))?;
        let previous: String = row.get(0);
        tx.batch_execute(&format!(
            "ALTER PUBLICATION {} OWNER TO {}",
            quote_ident(name),
            quote_ident(&previous)
        ))
        .await?;
    }
    guard_off(tx, false).await?;
    Ok(())
}

/// Remove the guards, in one transaction; refused while any publication is
/// registered (naming them), so no registered publication is ever left
/// without enforcement.
pub async fn uninstall(client: &Client) -> Result<()> {
    in_tx(client, "BEGIN", async |tx| uninstall_tx(tx).await).await
}

async fn uninstall_tx(tx: &Client) -> Result<()> {
    require_superuser(tx).await?;
    if tx
        .query_one(
            "SELECT pg_catalog.to_regclass('deltaforge.registration') IS NOT NULL",
            &[],
        )
        .await?
        .get::<_, bool>(0)
    {
        tx.batch_execute(
            "LOCK TABLE deltaforge.registration IN ACCESS EXCLUSIVE MODE",
        )
        .await?;
        let left = registrations(tx).await?;
        if !left.is_empty() {
            bail!(
                "enforcement cannot be uninstalled while publications are \
                 registered: {}",
                left.iter()
                    .map(|r| r.pubname.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
    }
    tx.batch_execute(&format!(
        "DROP EVENT TRIGGER IF EXISTS {DDL_GUARD}; DROP EVENT TRIGGER IF EXISTS {DROP_GUARD};"
    ))
    .await?;
    Ok(())
}

fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

/// `name -> sha256(body)` of every installed function body.
fn expected_bodies() -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    let mut rest = INSTALL;
    while let Some(i) = rest.find("CREATE OR REPLACE FUNCTION deltaforge.") {
        rest = &rest[i + "CREATE OR REPLACE FUNCTION deltaforge.".len()..];
        let name = rest[..rest.find('(').unwrap_or(0)].to_string();
        let Some(b) = rest.find("$f$") else { break };
        let start = b + 3;
        let Some(e) = rest[start..].find("$f$") else {
            break;
        };
        out.insert(name, sha_hex(&rest[start..start + e]));
        rest = &rest[start + e + 3..];
    }
    out
}

/// The privilege letters an `aclitem[]` text grants to PUBLIC.
fn public_privileges(acl: &str) -> String {
    acl.trim_matches(|c| c == '{' || c == '}')
        .split(',')
        .filter_map(|e| e.strip_prefix('='))
        .map(|e| e.split('/').next().unwrap_or(""))
        .collect()
}

/// What makes enforcement untrustworthy (empty: intact): the owner roles
/// (NOLOGIN, no attributes, no members), the guard functions (owner, body,
/// SECURITY DEFINER, search path, no PUBLIC execute), the two triggers
/// (enabled always, events, tags, functions), no DeltaForge
/// `ddl_command_end` trigger, the registration table (owner, no PUBLIC
/// write) and, before PostgreSQL 16, no non-superuser CREATEROLE.
pub async fn enforcement_violations(
    c: &impl GenericClient,
) -> Result<Vec<String>> {
    let mut bad = Vec::new();
    for role in [OWNER, PUB_OWNER] {
        match c
            .query_opt(
                "SELECT rolsuper OR rolcanlogin OR rolcreatedb OR rolcreaterole OR \
                 rolreplication OR rolbypassrls, \
                 (SELECT count(*) FROM pg_catalog.pg_auth_members m WHERE m.roleid = r.oid) \
                 FROM pg_catalog.pg_roles r WHERE rolname = $1",
                &[&role],
            )
            .await?
        {
            None => bad.push(format!("role {role} is missing")),
            Some(r) => {
                if r.get::<_, bool>(0) {
                    bad.push(format!("role {role} has extra attributes"));
                }
                if r.get::<_, i64>(1) != 0 {
                    bad.push(format!("a role is a member of {role}"));
                }
            }
        }
    }
    let creators: Vec<String> = c
        .query(
            "SELECT rolname::text FROM pg_catalog.pg_roles WHERE rolcreaterole AND NOT rolsuper \
             AND pg_catalog.current_setting('server_version_num')::int < 160000 ORDER BY 1",
            &[],
        )
        .await?
        .into_iter()
        .map(|r| r.get(0))
        .collect();
    if !creators.is_empty() {
        bad.push(format!(
            "non-superuser CREATEROLE roles can join {PUB_OWNER} before PostgreSQL 16: {}",
            creators.join(", ")
        ));
    }
    let expected = expected_bodies();
    let mut seen = BTreeMap::new();
    for r in c
        .query(
            "SELECT p.proname::text, pg_catalog.pg_get_userbyid(p.proowner)::text, \
             pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(p.prosrc, 'UTF8')), 'hex'), \
             p.prosecdef, coalesce(p.proconfig::text, ''), coalesce(p.proacl::text, '') \
             FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace \
             WHERE n.nspname = 'deltaforge'",
            &[],
        )
        .await?
    {
        let name: String = r.get(0);
        let acl: String = r.get(5);
        if r.get::<_, String>(1) != OWNER {
            bad.push(format!("function {name} has another owner"));
        }
        if !r.get::<_, bool>(3)
            || !r.get::<_, String>(4).contains("search_path=pg_catalog, pg_temp")
        {
            bad.push(format!("function {name} lost SECURITY DEFINER or its search path"));
        }
        if acl.is_empty() || public_privileges(&acl).contains('X') {
            bad.push(format!("function {name} is executable by PUBLIC"));
        }
        seen.insert(name, r.get::<_, String>(2));
    }
    for (name, digest) in &expected {
        match seen.get(name) {
            Some(d) if d == digest => {}
            Some(_) => bad.push(format!("function {name} has another body")),
            None => bad.push(format!("function {name} is missing")),
        }
    }
    let triggers = c
        .query(
            "SELECT e.evtname::text, e.evtevent::text, e.evtenabled::text, \
             coalesce(e.evttags::text, ''), p.proname::text, \
             pg_catalog.pg_get_userbyid(p.proowner)::text, n.nspname::text \
             FROM pg_catalog.pg_event_trigger e JOIN pg_catalog.pg_proc p ON p.oid = e.evtfoid \
             JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace",
            &[],
        )
        .await?;
    let found = |name: &str, event: &str, tags: &[&str], function: &str| {
        triggers.iter().any(|r| {
            let t: String = r.get(3);
            r.get::<_, String>(0) == name
                && r.get::<_, String>(1) == event
                && r.get::<_, String>(2) == "A"
                && r.get::<_, String>(4) == function
                && r.get::<_, String>(6) == "deltaforge"
                && if tags.is_empty() {
                    t.is_empty()
                } else {
                    tags.iter().all(|x| t.contains(x))
                        && t.matches(',').count() + 1 == tags.len()
                }
        })
    };
    if !found(
        DDL_GUARD,
        "ddl_command_start",
        &["ALTER PUBLICATION", "DROP PUBLICATION"],
        "guard_publication_ddl",
    ) {
        bad.push(format!(
            "event trigger {DDL_GUARD} is missing, disabled or altered"
        ));
    }
    if !found(DROP_GUARD, "sql_drop", &[], "guard_drop") {
        bad.push(format!(
            "event trigger {DROP_GUARD} is missing, disabled or altered"
        ));
    }
    if triggers.iter().any(|r| {
        r.get::<_, String>(1) == "ddl_command_end"
            && r.get::<_, String>(6) == "deltaforge"
    }) {
        bad.push("a DeltaForge ddl_command_end trigger exists".into());
    }
    match c
        .query_opt(
            "SELECT pg_catalog.pg_get_userbyid(relowner)::text, coalesce(relacl::text, '') \
             FROM pg_catalog.pg_class WHERE oid = pg_catalog.to_regclass('deltaforge.registration')",
            &[],
        )
        .await?
    {
        Some(r) if r.get::<_, String>(0) == OWNER => {
            let public = public_privileges(&r.get::<_, String>(1));
            if public.chars().any(|p| p != 'r') {
                bad.push("PUBLIC may write deltaforge.registration".into());
            }
        }
        _ => bad.push("deltaforge.registration is missing or has another owner".into()),
    }
    Ok(bad)
}

/// Why a source cannot rely on `pubname` (empty: registered, enforced,
/// unchanged since registration).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Verified {
    pub registration: Registration,
    pub live: LiveState,
}

/// What a verification found wrong.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VerifyError {
    /// Enforcement is missing or tampered with.
    Enforcement(Vec<String>),
    /// The publication is not registered (or does not exist).
    Unregistered,
    /// The publication changed since registration.
    Changed {
        registration: Registration,
        live_digest: Option<String>,
        live_owner: Option<String>,
    },
}

/// Verify enforcement, the registration of `pubname` and its live state.
pub async fn verify(
    client: &Client,
    pubname: &str,
) -> Result<std::result::Result<Verified, VerifyError>> {
    in_tx(client, READ, async |tx| verify_tx(tx, pubname).await).await
}

async fn verify_tx(
    tx: &Client,
    pubname: &str,
) -> Result<std::result::Result<Verified, VerifyError>> {
    let installed: bool = tx
        .query_one(
            "SELECT pg_catalog.to_regclass('deltaforge.registration') IS NOT NULL",
            &[],
        )
        .await?
        .get(0);
    if !installed {
        return Ok(Err(VerifyError::Unregistered));
    }
    let bad = enforcement_violations(tx).await?;
    if !bad.is_empty() {
        return Ok(Err(VerifyError::Enforcement(bad)));
    }
    let Some(registration) = registrations(tx)
        .await?
        .into_iter()
        .find(|r| r.pubname == pubname)
    else {
        return Ok(Err(VerifyError::Unregistered));
    };
    match read_state(tx, pubname).await? {
        Some(live)
            if live.digest == registration.digest
                && live.pub_oid == registration.pub_oid
                && live.owner == PUB_OWNER =>
        {
            Ok(Ok(Verified { registration, live }))
        }
        live => Ok(Err(VerifyError::Changed {
            live_digest: live.as_ref().map(|l| l.digest.clone()),
            live_owner: live.map(|l| l.owner),
            registration,
        })),
    }
}

// ---------------------------------------------------------------------------
// The source side: the registration a source accepted, maintenance
// decisions, and verification at startup and on every reconnect.
// ---------------------------------------------------------------------------

/// The registration each source accepted (latest wins).
pub const ACCEPTED_NS: &str = "schemas.v1.pg.publication_registration";
/// Maintenance decisions (latest wins).
pub const DECISION_NS: &str = "schemas.v1.pg.publication_maintenance";

/// How a source continues after its publication was re-registered.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Decision {
    /// `resnapshot` (with the `resnapshot` recovery operation) or `abandon`
    /// (audited: the backlog before the new registration is skipped).
    pub mode: String,
    /// The accepted registration the decision replaces.
    pub for_digest: String,
    pub for_lsn: String,
}

/// The durable key of a source's publication records. (A source id reused
/// on another database is refused by the lineage check before this is read;
/// a different registration fails closed regardless.)
pub fn record_key(tenant: &str, source_id: &str, publication: &str) -> String {
    format!("{tenant}/{source_id}/{publication}")
}

pub async fn accepted(
    backend: &storage::ArcStorageBackend,
    key: &str,
) -> Result<Option<Registration>> {
    match backend.log_latest(ACCEPTED_NS, key).await? {
        None => Ok(None),
        Some((_, b)) => Ok(Some(serde_json::from_slice(&b)?)),
    }
}

async fn accept(
    backend: &storage::ArcStorageBackend,
    key: &str,
    r: &Registration,
) -> Result<()> {
    backend
        .log_append_if_absent(
            ACCEPTED_NS,
            key,
            &format!("{}:{}", r.digest, r.registered_lsn),
            &serde_json::to_vec(r)?,
        )
        .await?;
    Ok(())
}

pub async fn decision(
    backend: &storage::ArcStorageBackend,
    key: &str,
) -> Result<Option<Decision>> {
    match backend.log_latest(DECISION_NS, key).await? {
        None => Ok(None),
        Some((_, b)) => Ok(Some(serde_json::from_slice(&b)?)),
    }
}

pub async fn decide(
    backend: &storage::ArcStorageBackend,
    key: &str,
    d: &Decision,
) -> Result<()> {
    backend
        .log_append(DECISION_NS, key, &serde_json::to_vec(d)?)
        .await?;
    Ok(())
}

/// Where the source is about to start.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Start {
    /// Streaming from this LSN (a checkpoint, a completed snapshot anchor or
    /// an existing slot).
    Stream(u64),
    /// A new snapshot generation (its anchor is taken after this check).
    Generation,
}

/// What the startup verification admitted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Admitted {
    pub registration: Registration,
    /// Under an `abandon` decision: stream from here instead.
    pub abandon_to: Option<u64>,
}

fn other(e: impl Into<anyhow::Error>) -> deltaforge_core::SourceError {
    deltaforge_core::SourceError::Other(e.into())
}

/// Verify the publication and its database's enforcement before the source
/// snapshots, decodes, emits or advances anything; on success the
/// registration is the source's accepted one.
pub async fn verify_for_start(
    dsn: &str,
    source_id: &str,
    publication: &str,
    backend: &storage::ArcStorageBackend,
    key: &str,
    start: Start,
) -> deltaforge_core::SourceResult<Admitted> {
    use crate::incident_drafts::publication_changed;
    let registration = verify_live(dsn, source_id, publication).await?;
    let reg_lsn = parse_lsn(&registration.registered_lsn).ok_or_else(|| {
        other(anyhow::anyhow!(
            "registration position {}",
            registration.registered_lsn
        ))
    })?;
    let previous = accepted(backend, key).await.map_err(other)?;
    let mut abandon_to = None;
    match previous {
        Some(p) if p == registration => {}
        None => {}
        Some(p) => {
            let d = decision(backend, key).await.map_err(other)?;
            match d {
                Some(d)
                    if d.for_digest == p.digest
                        && d.for_lsn == p.registered_lsn =>
                {
                    match (d.mode.as_str(), start) {
                        ("resnapshot", Start::Generation) => {}
                        ("resnapshot", Start::Stream(_)) => {
                            return Err(publication_changed(
                                source_id,
                                publication,
                                "resnapshot_pending",
                                format!(
                                    "publication {publication} was re-registered under a \
                                     resnapshot decision: run the resnapshot recovery \
                                     operation before restarting"
                                ),
                            ));
                        }
                        ("abandon", Start::Stream(at)) if at < reg_lsn => {
                            tracing::warn!(
                                source_id, publication,
                                from = %format_lsn(at), to = %registration.registered_lsn,
                                "publication maintenance: abandoning the backlog before \
                                 the new registration (operator decision)"
                            );
                            abandon_to = Some(reg_lsn);
                        }
                        ("abandon", _) => {}
                        (mode, _) => {
                            return Err(other(anyhow::anyhow!(
                                "unknown maintenance decision {mode}"
                            )));
                        }
                    }
                }
                _ => {
                    return Err(publication_changed(
                        source_id,
                        publication,
                        "reregistered",
                        format!(
                            "publication {publication} was re-registered (digest {}) \
                             since this source accepted digest {}; record a maintenance \
                             decision (pg-publication-maintenance)",
                            registration.digest, p.digest
                        ),
                    ));
                }
            }
        }
    }
    // Retained WAL from before the registration may hold rows published
    // under another publication state.
    if let (Start::Stream(at), None) = (start, abandon_to)
        && at < reg_lsn
    {
        return Err(publication_changed(
            source_id,
            publication,
            "retained_wal",
            format!(
                "the stream would start at {} before the registration of \
                 {publication} at {}",
                format_lsn(at),
                registration.registered_lsn
            ),
        ));
    }
    accept(backend, key, &registration).await.map_err(other)?;
    Ok(Admitted {
        registration,
        abandon_to,
    })
}

/// Verify again on a reconnect: the same registration, unchanged, enforced.
pub async fn verify_for_reconnect(
    dsn: &str,
    source_id: &str,
    publication: &str,
    accepted: &Registration,
) -> deltaforge_core::SourceResult<()> {
    let live = verify_live(dsn, source_id, publication).await?;
    if &live != accepted {
        return Err(crate::incident_drafts::publication_changed(
            source_id,
            publication,
            "reregistered",
            format!(
                "publication {publication} was re-registered while the source ran"
            ),
        ));
    }
    Ok(())
}

async fn verify_live(
    dsn: &str,
    source_id: &str,
    publication: &str,
) -> deltaforge_core::SourceResult<Registration> {
    use crate::incident_drafts::{
        publication_changed, publication_enforcement,
    };
    let (client, conn) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
        .await
        .map_err(|e| deltaforge_core::SourceError::Connect {
            details: format!("publication verification: {e}").into(),
        })?;
    let task = tokio::spawn(async move {
        let _ = conn.await;
    });
    let out = verify(&client, publication).await.map_err(other);
    drop(client);
    task.abort();
    match out? {
        Ok(v) => Ok(v.registration),
        Err(VerifyError::Enforcement(bad)) => {
            Err(publication_enforcement(source_id, publication, &bad))
        }
        Err(VerifyError::Unregistered) => Err(publication_changed(
            source_id,
            publication,
            "unregistered",
            format!(
                "publication {publication} is not registered \
                 (deltaforge pg-publication register)"
            ),
        )),
        Err(VerifyError::Changed {
            registration,
            live_digest,
            live_owner,
        }) => Err(publication_changed(
            source_id,
            publication,
            "changed",
            format!(
                "publication {publication} changed since its registration: \
                 digest {} (registered {}), owner {}",
                live_digest.as_deref().unwrap_or("absent"),
                registration.digest,
                live_owner.as_deref().unwrap_or("absent"),
            ),
        )),
    }
}

/// `X/Y` hex LSN text as a number.
pub fn parse_lsn(s: &str) -> Option<u64> {
    let (hi, lo) = s.split_once('/')?;
    Some(
        (u64::from_str_radix(hi, 16).ok()? << 32)
            | u64::from_str_radix(lo, 16).ok()?,
    )
}

fn format_lsn(v: u64) -> String {
    format!("{:X}/{:X}", v >> 32, v as u32)
}

#[cfg(test)]
#[path = "postgres_publication_live.rs"]
mod live;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn both_guard_functions_have_expected_bodies() {
        let b = expected_bodies();
        assert_eq!(
            b.keys().collect::<Vec<_>>(),
            ["guard_drop", "guard_publication_ddl"]
        );
    }

    #[test]
    fn no_ddl_command_end_trigger_is_installed() {
        assert!(!TRIGGERS.contains("ddl_command_end"));
        assert!(!INSTALL.contains("ddl_command_end"));
    }

    #[test]
    fn public_entries_are_read_from_acls() {
        assert_eq!(public_privileges("{a=arwd/a,=r/a}"), "r");
        assert_eq!(public_privileges("{a=arwd/a}"), "");
    }

    #[test]
    fn lsns_round_trip() {
        assert_eq!(parse_lsn("1/A0"), Some((1 << 32) | 0xA0));
        assert_eq!(
            format_lsn(parse_lsn("16/B374D848").unwrap()),
            "16/B374D848"
        );
        assert_eq!(parse_lsn("garbage"), None);
    }
}

/// Fixtures and tooling: recreate publications under the contract.
pub mod fixtures {
    use super::*;

    /// Remove every registration (each publication back to its previous
    /// owner) and the enforcement, so publications can change.
    pub async fn release_all(client: &Client) -> Result<()> {
        let names: Vec<String> = registrations(client)
            .await?
            .into_iter()
            .map(|r| r.pubname)
            .collect();
        if !names.is_empty() {
            unregister(client, &names).await?;
        }
        uninstall(client).await
    }

    /// Record a maintenance decision for `source_id` against the
    /// registration it accepted last (tests; operators use the recovery
    /// operation `pg-publication-maintenance`).
    pub async fn decide_for(
        backend: &storage::ArcStorageBackend,
        tenant: &str,
        source_id: &str,
        publication: &str,
        mode: &str,
    ) -> Result<()> {
        let key = record_key(tenant, source_id, publication);
        let acc = accepted(backend, &key)
            .await?
            .context("the source accepted no registration")?;
        decide(
            backend,
            &key,
            &Decision {
                mode: mode.to_string(),
                for_digest: acc.digest,
                for_lsn: acc.registered_lsn,
            },
        )
        .await
    }

    /// Recreate `pubname` FOR TABLE `tables` (every user table when empty)
    /// and register it, as a superuser, in one transaction (the guard is off
    /// only inside it). Other registrations are untouched.
    pub async fn recreate_registered(
        client: &Client,
        pubname: &str,
        tables: &[&str],
    ) -> Result<Registration> {
        let list = if tables.is_empty() {
            client
                .query_one(
                    "SELECT coalesce(pg_catalog.string_agg(pg_catalog.format('%I.%I', \
                     n.nspname, c.relname), ', ' ORDER BY c.oid), '') \
                     FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n \
                     ON n.oid = c.relnamespace WHERE c.relkind = 'r' \
                     AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'deltaforge') \
                     AND n.nspname NOT LIKE 'pg_toast%'",
                    &[],
                )
                .await?
                .get::<_, String>(0)
        } else {
            tables.join(", ")
        };
        let names: Vec<&str> = if list.is_empty() {
            Vec::new()
        } else {
            list.split(", ").collect()
        };
        if names.len() > 500 {
            // One statement over thousands of tables would hold a lock per
            // table: build the publication in committed batches first (no
            // registration may exist meanwhile: ALTER is refused then).
            let others = registrations(client).await?;
            anyhow::ensure!(
                others.iter().all(|r| r.pubname == pubname),
                "a large publication is built while no other publication is registered"
            );
            release_all(client).await?;
            client
                .batch_execute(&format!(
                    "DROP PUBLICATION IF EXISTS {q}; CREATE PUBLICATION {q}",
                    q = quote_ident(pubname)
                ))
                .await?;
            for batch in names.chunks(500) {
                client
                    .batch_execute(&format!(
                        "ALTER PUBLICATION {} ADD TABLE {}",
                        quote_ident(pubname),
                        batch.join(", ")
                    ))
                    .await?;
            }
            let regs = register(client, &[pubname.to_string()]).await?;
            return Ok(regs.into_iter().next().expect("registered"));
        }
        let target = if list.is_empty() {
            String::new()
        } else {
            format!(" FOR TABLE {list}")
        };
        let regs = in_tx(client, "BEGIN", async |tx| {
            require_superuser(tx).await?;
            tx.batch_execute(INSTALL).await?;
            let installed: bool = tx
                .query_one(
                    "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_event_trigger WHERE evtname = $1)",
                    &[&DDL_GUARD],
                )
                .await?
                .get(0);
            if installed {
                guard_off(tx, true).await?;
            }
            tx.execute("DELETE FROM deltaforge.registration WHERE pubname = $1", &[&pubname])
                .await?;
            tx.batch_execute(&format!(
                "DROP PUBLICATION IF EXISTS {q}; CREATE PUBLICATION {q}{target}",
                q = quote_ident(pubname)
            ))
            .await?;
            register_tx(tx, &[pubname.to_string()]).await
        })
        .await?;
        Ok(regs.into_iter().next().expect("registered"))
    }
}

/// A connection for the administration commands (driven in the background).
pub async fn connect(dsn: &str) -> Result<Client> {
    let (client, conn) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
        .await
        .context("connect to the database")?;
    tokio::spawn(async move {
        let _ = conn.await;
    });
    Ok(client)
}
