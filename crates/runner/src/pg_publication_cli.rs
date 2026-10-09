//! `deltaforge pg-publication`: administer the immutable-publication
//! contract of a PostgreSQL database (as a superuser).

use anyhow::{Result, bail};
use sources::postgres::postgres_publication as contract;

pub enum Action {
    Status,
    Register {
        publications: Vec<String>,
        confirm: bool,
    },
    Unregister {
        publications: Vec<String>,
    },
    Uninstall,
}

pub async fn run(dsn: &str, action: Action) -> Result<()> {
    let client = contract::connect(dsn).await?;
    let database: String = client
        .query_one("SELECT current_database()::text", &[])
        .await?
        .get(0);
    match action {
        Action::Status => {
            let impact = contract::impact(&client).await?;
            println!("database {database}");
            for (p, o) in &impact.publications {
                let reg = if impact.registered.contains(p) {
                    " registered"
                } else {
                    ""
                };
                println!("  publication {p} (owner {o}){reg}");
            }
            for r in contract::registrations(&client).await? {
                println!(
                    "  registration {} digest {} at {}",
                    r.pubname, r.digest, r.registered_lsn
                );
            }
            if !impact.registered.is_empty() {
                let bad = contract::enforcement_violations(&client).await?;
                if bad.is_empty() {
                    println!("enforcement: intact");
                } else {
                    for b in &bad {
                        println!("enforcement VIOLATION: {b}");
                    }
                    bail!("enforcement is not intact");
                }
            }
            Ok(())
        }
        Action::Register {
            publications,
            confirm,
        } => {
            let impact = contract::impact(&client).await?;
            println!("{}", impact.describe(&database));
            if !confirm {
                bail!(
                    "nothing changed: review the impact above and repeat with --confirm"
                );
            }
            for r in contract::register(&client, &publications).await? {
                println!(
                    "registered {} digest {} at {}",
                    r.pubname, r.digest, r.registered_lsn
                );
            }
            Ok(())
        }
        Action::Unregister { publications } => {
            contract::unregister(&client, &publications).await?;
            println!(
                "unregistered {}; enforcement stays installed until every \
                 registration of {database} is removed and `uninstall` runs",
                publications.join(", ")
            );
            Ok(())
        }
        Action::Uninstall => {
            contract::uninstall(&client).await?;
            println!("enforcement removed from {database}");
            Ok(())
        }
    }
}
