//! What PostgreSQL itself sends for an UPDATE that leaves out-of-line
//! (TOASTed) values unchanged, on every major DeltaForge is expected to run
//! on. Unchanged-TOAST reconstruction relies on it: under REPLICA IDENTITY
//! FULL the old row carries each such value in full, exactly as stored, while
//! the new row marks it unchanged ('u'); under DEFAULT there is no old row.
//!
//! The pgoutput messages are read with pg_logical_slot_get_binary_changes and
//! parsed here, independently of DeltaForge's decoder.

use anyhow::{Context, Result, bail, ensure};
use gate_ownership::GateOwned;
use testcontainers::core::WaitFor;
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};
use tokio_postgres::NoTls;

const MAJORS: [&str; 5] = ["14", "15", "16", "17", "18"];

/// One column of a pgoutput TupleData.
#[derive(Debug, PartialEq)]
enum Col {
    Null,
    Unchanged,
    Text(Vec<u8>),
}

/// A pgoutput Update ('U'): the old tuple ('O' under FULL, 'K' for a key
/// change), if sent, and the new tuple.
#[derive(Debug)]
struct Update {
    old: Option<(u8, Vec<Col>)>,
    new: Vec<Col>,
}

fn tuple(b: &[u8], at: &mut usize) -> Result<Vec<Col>> {
    let n = u16::from_be_bytes(b[*at..*at + 2].try_into()?) as usize;
    *at += 2;
    let mut cols = Vec::with_capacity(n);
    for _ in 0..n {
        let kind = b[*at];
        *at += 1;
        cols.push(match kind {
            b'n' => Col::Null,
            b'u' => Col::Unchanged,
            b't' => {
                let len =
                    u32::from_be_bytes(b[*at..*at + 4].try_into()?) as usize;
                *at += 4;
                let v = b[*at..*at + len].to_vec();
                *at += len;
                Col::Text(v)
            }
            other => bail!("unexpected tuple column kind {:?}", other as char),
        });
    }
    Ok(cols)
}

fn parse_update(b: &[u8]) -> Result<Update> {
    ensure!(b[0] == b'U', "not an update");
    let mut at = 5; // 'U' + relation id
    let old = match b[at] {
        k @ (b'O' | b'K') => {
            at += 1;
            Some((k, tuple(b, &mut at)?))
        }
        _ => None,
    };
    ensure!(b[at] == b'N', "no new tuple");
    at += 1;
    Ok(Update {
        old,
        new: tuple(b, &mut at)?,
    })
}

/// The large values, as PostgreSQL renders them in text, read back from the
/// table (so the comparison is with exactly what is stored).
async fn stored(c: &tokio_postgres::Client) -> Result<Vec<Vec<u8>>> {
    let row = c
        .query_one(
            "SELECT body::text, doc::text, blob::text, nums::text, note::text \
             FROM toasty WHERE id = 1",
            &[],
        )
        .await?;
    Ok((0..5)
        .map(|i| row.get::<_, String>(i).into_bytes())
        .collect())
}

async fn updates(c: &tokio_postgres::Client) -> Result<Vec<Update>> {
    let rows = c
        .query(
            "SELECT data FROM pg_logical_slot_get_binary_changes('toast_slot', NULL, NULL, \
             'proto_version', '1', 'publication_names', 'toast_pub')",
            &[],
        )
        .await?;
    rows.iter()
        .map(|r| r.get::<_, Vec<u8>>(0))
        .filter(|b| b.first() == Some(&b'U'))
        .map(|b| parse_update(&b))
        .collect()
}

async fn prove(major: &str) -> Result<()> {
    let container = GenericImage::new("postgres", major)
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_PASSWORD", "pw")
        .with_cmd(vec!["postgres", "-c", "wal_level=logical"])
        .gate_owned()
        .start()
        .await
        .with_context(|| format!("start postgres:{major}"))?;
    let port = container.get_host_port_ipv4(5432).await?;
    let dsn = format!(
        "host=127.0.0.1 port={port} user=postgres password=pw dbname=postgres"
    );
    let (c, conn) = loop {
        match tokio_postgres::connect(&dsn, NoTls).await {
            Ok(x) => break x,
            Err(_) => {
                tokio::time::sleep(std::time::Duration::from_millis(200)).await
            }
        }
    };
    tokio::spawn(conn);

    // Every large column stored out of line (EXTERNAL: no inline
    // compression), a domain over text among them; `small` is what changes.
    c.batch_execute(
        "CREATE DOMAIN note_text AS text;
         CREATE TABLE toasty (
             id int PRIMARY KEY, small int, body text, doc jsonb, blob bytea,
             nums int[], note note_text, gone text);
         ALTER TABLE toasty ALTER body SET STORAGE EXTERNAL,
             ALTER doc SET STORAGE EXTERNAL, ALTER blob SET STORAGE EXTERNAL,
             ALTER nums SET STORAGE EXTERNAL, ALTER note SET STORAGE EXTERNAL;
         ALTER TABLE toasty REPLICA IDENTITY FULL;
         CREATE PUBLICATION toast_pub FOR TABLE toasty;",
    )
    .await?;
    // A slot cannot be created in a transaction that has written.
    c.batch_execute(
        "SELECT pg_create_logical_replication_slot('toast_slot', 'pgoutput')",
    )
    .await?;
    c.batch_execute(
        "INSERT INTO toasty SELECT 1, 0,
             (SELECT string_agg(md5(g::text), '') FROM generate_series(1, 4000) g),
             jsonb_build_object('k', (SELECT string_agg(md5(g::text), '') FROM generate_series(1, 4000) g)),
             (SELECT decode(string_agg(md5(g::text), ''), 'hex') FROM generate_series(1, 4000) g),
             (SELECT array_agg(g) FROM generate_series(1, 20000) g),
             (SELECT string_agg(md5((g + 7)::text), '') FROM generate_series(1, 4000) g),
             NULL;",
    )
    .await?;
    let toast_bytes: i64 = c
        .query_one(
            "SELECT pg_relation_size(reltoastrelid) FROM pg_class \
             WHERE relname = 'toasty'",
            &[],
        )
        .await?
        .get(0);
    ensure!(
        toast_bytes > 500_000,
        "{major}: the values are not stored out of line ({toast_bytes} bytes)"
    );
    let values = stored(&c).await?;

    // FULL: only `small` changes.
    c.batch_execute("UPDATE toasty SET small = 1 WHERE id = 1")
        .await?;
    let full = updates(&c).await?;
    ensure!(full.len() == 1, "{major}: {} updates", full.len());
    let u = &full[0];
    let Some((b'O', old)) = &u.old else {
        bail!(
            "{major}: no full old tuple under FULL: {:?}",
            u.old.as_ref().map(|o| o.0)
        );
    };
    // Columns: id, small, body, doc, blob, nums, note, gone.
    for (i, v) in values.iter().enumerate() {
        ensure!(
            old[2 + i] == Col::Text(v.clone()),
            "{major}: old column {} is not the stored value ({} bytes)",
            2 + i,
            v.len()
        );
        ensure!(
            u.new[2 + i] == Col::Unchanged,
            "{major}: new column {} is {:?}, not unchanged",
            2 + i,
            match &u.new[2 + i] {
                Col::Text(t) => format!("text of {} bytes", t.len()),
                other => format!("{other:?}"),
            }
        );
    }
    ensure!(
        old[7] == Col::Null && u.new[7] == Col::Null,
        "{major}: SQL NULL"
    );
    ensure!(
        u.new[1] == Col::Text(b"1".to_vec()),
        "{major}: the changed column"
    );

    // A changed large value is sent in full in the new tuple.
    c.batch_execute("UPDATE toasty SET body = body || 'x' WHERE id = 1")
        .await?;
    let changed = updates(&c).await?;
    let new_body = stored(&c).await?.remove(0);
    ensure!(
        changed[0].new[2] == Col::Text(new_body),
        "{major}: a changed value is sent"
    );

    // DEFAULT: no old tuple, so nothing to reconstruct from.
    c.batch_execute(
        "ALTER TABLE toasty REPLICA IDENTITY DEFAULT;
         UPDATE toasty SET small = 2 WHERE id = 1",
    )
    .await?;
    let default = updates(&c).await?;
    ensure!(
        default.len() == 1 && default[0].old.is_none(),
        "{major}: an old tuple under DEFAULT"
    );
    ensure!(
        default[0].new[2] == Col::Unchanged,
        "{major}: unchanged under DEFAULT"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn full_identity_sends_complete_old_toasted_values_on_every_major()
-> Result<()> {
    let mut failed = Vec::new();
    for major in MAJORS {
        match prove(major).await {
            Ok(()) => println!("postgres:{major}: proven"),
            Err(e) => failed.push(format!("postgres:{major}: {e:#}")),
        }
    }
    ensure!(failed.is_empty(), "{failed:#?}");
    Ok(())
}
