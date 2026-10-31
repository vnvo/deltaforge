#!/usr/bin/env python3
"""Apply the perf-phases instrumentation to a checkout of a revision that
predates it (f0171b4e or 3ff3773c), at the same points as the change: the
same `Phase` guard, the same phase names, the same features. Run from the
root of a clean checkout of exactly that revision; then `git diff` is the
revision's patch (kept in patches/<rev>-perf-phases.patch).

Phases that exist only on one path: the change records `plan`, `commit` and
`fresh_check`; these revisions record `register` and `bind` instead. Both
record `register_bind` around the whole registration and binding step.
"""
import pathlib
import subprocess
import sys

def edit(path, pairs):
    p = pathlib.Path(path)
    s = p.read_text()
    for a, b in pairs:
        if s.count(a) != 1:
            sys.exit(f"{path}: anchor found {s.count(a)} times: {a[:80]!r}")
        s = s.replace(a, b)
    p.write_text(s)


def main():
    rev = subprocess.run(
        ["git", "rev-parse", "--short=8", "HEAD"],
        capture_output=True, text=True, check=True,
    ).stdout.strip()
    if rev not in ("f0171b4e", "3ff3773c"):
        sys.exit(f"unsupported revision {rev}")
    dirty = subprocess.run(
        ["git", "status", "--porcelain"], capture_output=True, text=True,
        check=True,
    ).stdout
    if dirty:
        sys.exit("the checkout is not clean")
    if len(sys.argv) != 2:
        sys.exit("usage: backport_phases.py <the change's crates/sources/src/perf_phases.rs>")
    phases = pathlib.Path(sys.argv[1]).read_text()
    pathlib.Path("crates/sources/src/perf_phases.rs").write_text(phases)
    storage = phases.replace(
        "//! Phase timings of the MySQL DDL path, for measurement builds only (the\n"
        "//! `perf-phases` feature; `docs/design/mysql-ddl-proof-transactions.md`,\n"
        "//! section 6). A [`Phase`] records its elapsed time when dropped, into the\n"
        "//! histogram `deltaforge_perf_phase_latency_seconds{phase}`: a distribution\n"
        "//! per phase over a run, not a trace of any one DDL. Without the feature it\n"
        "//! is empty and records nothing.\n",
        "//! Phase timings of the store, for measurement builds only (the\n"
        "//! `perf-phases` feature; `docs/design/mysql-ddl-proof-transactions.md`,\n"
        "//! section 6): see the sources crate's `perf_phases`. Without the feature a\n"
        "//! [`Phase`] is empty and records nothing.\n",
    )
    pathlib.Path("crates/storage/src/perf_phases.rs").write_text(storage)
    edit("crates/sources/src/lib.rs", [("pub mod mysql;\n", "pub mod mysql;\npub(crate) mod perf_phases;\n")])
    edit("crates/storage/src/lib.rs", [("pub mod memory;\n", "pub mod memory;\npub(crate) mod perf_phases;\n")])
    edit("crates/sources/Cargo.toml", [(
        "[features]\ndefault = []\n",
        "[features]\ndefault = []\n# Phase timings of the MySQL DDL path, for measurement builds only.\n"
        "perf-phases = [\"storage/perf-phases\"]\n",
    )])
    edit("crates/storage/Cargo.toml", [(
        "[features]\ndefault = [\"sqlite\", \"postgres\"]\n",
        "[features]\ndefault = [\"sqlite\", \"postgres\"]\n# Phase timings of the store, for measurement builds only.\n"
        "perf-phases = []\n",
    )])
    edit("crates/runner/Cargo.toml", [(
        "[features]\ndefault = [\"jemalloc\"]\n",
        "[features]\ndefault = [\"jemalloc\"]\n# Phase timings of the MySQL DDL path and the store, for measurement builds\n"
        "# only (`docs/design/mysql-ddl-proof-transactions.md`, section 6).\n"
        "perf-phases = [\"sources/perf-phases\", \"storage/perf-phases\"]\n",
    )])
    edit("crates/storage/src/postgres.rs", [(
        "macro_rules! client {\n    ($self:expr) => {\n        $self\n            .pool\n"
        "            .get()\n            .await\n            .map_err(|e| anyhow!(\"pool get: {e}\"))?\n    };\n}",
        "macro_rules! client {\n    ($self:expr) => {{\n"
        "        let _wait = crate::perf_phases::Phase::start(\"store_pool_wait\");\n"
        "        $self\n            .pool\n"
        "            .get()\n            .await\n            .map_err(|e| anyhow!(\"pool get: {e}\"))?\n    }};\n}",
    )])
    edit("crates/sources/src/mysql/mysql_event.rs", [
        ("        DdlEffect::Barrier(BarrierScopeOf::Database(db)) => {\n"
         "            (Vec::new(), Some(BarrierScope::Database { db: name(&db) }))\n"
         "        }\n    };\n",
         "        DdlEffect::Barrier(BarrierScopeOf::Database(db)) => {\n"
         "            (Vec::new(), Some(BarrierScope::Database { db: name(&db) }))\n"
         "        }\n    };\n"
         "    let _phase = crate::perf_phases::Phase::start(\"ddl_records\");\n"),
        ("        let ddl_id =\n"
         "            record_ddl(backend, registry, &key, &event, position.clone())\n"
         "                .await\n"
         "                .map_err(|e| fail(\"ddl record\", e))?;",
         "        let recorded = {\n"
         "            let _phase = crate::perf_phases::Phase::start(\"pending_record\");\n"
         "            record_ddl(backend, registry, &key, &event, position.clone())\n"
         "                .await\n"
         "        };\n"
         "        let ddl_id = recorded.map_err(|e| fail(\"ddl record\", e))?;"),
        ("        payload.insert(\"sql\".into(), q.query.clone().into());\n"
         "        emit_ddl_event(ctx, header, &db, payload.into()).await?;",
         "        payload.insert(\"sql\".into(), q.query.clone().into());\n"
         "        let _phase = crate::perf_phases::Phase::start(\"emit\");\n"
         "        emit_ddl_event(ctx, header, &db, payload.into()).await?;"),
    ])
    fp = "crates/sources/src/mysql/mysql_forward_proof.rs"
    edit(fp, [
        ("    // Replay: a binding already recorded for the pending DDL is the proof.\n"
         "    let mut open = Vec::new();\n",
         "    // Replay: a binding already recorded for the pending DDL is the proof.\n"
         "    let replay_phase = crate::perf_phases::Phase::start(\"replay_check\");\n"
         "    let mut open = Vec::new();\n"),
        ("    if open.is_empty() {\n        return Ok(0);\n    }\n",
         "    drop(replay_phase);\n    if open.is_empty() {\n        return Ok(0);\n    }\n"),
        ("    // 1. Captures, bound to their intervals by step 3.\n    let mut captured = Vec::new();\n",
         "    // 1. Captures, bound to their intervals by step 3.\n"
         "    let capture_phase = crate::perf_phases::Phase::start(\"capture\");\n"
         "    let mut captured = Vec::new();\n"),
        ("    let Some(s_cp) = captured.last().map(|(_, cap)| cap.position.clone())",
         "    drop(capture_phase);\n    let Some(s_cp) = captured.last().map(|(_, cap)| cap.position.clone())"),
        ("    // 3. One complete scan of (D, S].\n    let report = match",
         "    // 3. One complete scan of (D, S].\n"
         "    let scan_phase = crate::perf_phases::Phase::start(\"scan\");\n    let report = match"),
        ("    // 4. Register, then bind.\n",
         "    drop(scan_phase);\n    // 4. Register, then bind.\n"
         "    let register_phase = crate::perf_phases::Phase::start(\"register_bind\");\n"),
        ("        let (version, schema_hash) = ctx\n"
         "            .schema\n"
         "            .register_captured(&p.db, &p.table, &cap.schema, &checkpoint)\n"
         "            .await?;\n",
         "        let register = crate::perf_phases::Phase::start(\"register\");\n"
         "        let (version, schema_hash) = ctx\n"
         "            .schema\n"
         "            .register_captured(&p.db, &p.table, &cap.schema, &checkpoint)\n"
         "            .await?;\n"
         "        drop(register);\n"
         "        let bind = crate::perf_phases::Phase::start(\"bind\");\n"),
        ("            SourceError::Other(e.context(\"persist a forward-proof binding\"))\n        })?;\n",
         "            SourceError::Other(e.context(\"persist a forward-proof binding\"))\n        })?;\n"
         "        drop(bind);\n"),
        ("    info!(source_id = %ctx.source_id, bindings = written, \"forward proof after DDL\");\n",
         "    drop(register_phase);\n"
         "    info!(source_id = %ctx.source_id, bindings = written, \"forward proof after DDL\");\n"),
    ])
    sel = "crates/sources/src/mysql/mysql_selection.rs"
    if rev == "3ff3773c":
        edit(sel, [(
            "    let mine = install(ctx, &keys, |k| k == key).await?;\n",
            "    let _phase = crate::perf_phases::Phase::start(\"history_load\");\n"
            "    let mine = install(ctx, &keys, |k| k == key).await?;\n",
        )])
    else:
        edit(sel, [(
            "    let registry = ctx.schema.registry();\n"
            "    let (records, table_head) =\n"
            "        mysql_activation::read_table_head(&ctx.registry_backend, registry, key)\n",
            "    let _phase = crate::perf_phases::Phase::start(\"history_load\");\n"
            "    let registry = ctx.schema.registry();\n"
            "    let (records, table_head) =\n"
            "        mysql_activation::read_table_head(&ctx.registry_backend, registry, key)\n",
        )])


if __name__ == "__main__":
    main()
