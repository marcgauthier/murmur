//! Direct Rust Database peer used by the Go interoperability integration test.
use serde_json::Value;
use sqlite_session::{Database, Migration, OpenMode, Options, replication};
use std::io::{BufRead, Write};
fn field<T: serde::de::DeserializeOwned>(v: &Value, name: &str) -> T {
    serde_json::from_value(v[name].clone()).unwrap()
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let v: Value = serde_json::from_slice(&std::fs::read(
        std::env::args().nth(1).expect("configuration path"),
    )?)?;
    let c = &v["config"];
    let creds = &c["credentials"];
    let mut config = replication::Config::new(
        field(c, "bind"),
        replication::Credentials {
            chain: field(creds, "chain"),
            private_key: zeroize::Zeroizing::new(field(creds, "private_key")),
            roots: field(creds, "roots"),
        },
        field(c, "signing_key"),
    );
    for trust in c["trusted_origins"].as_array().unwrap() {
        config
            .trusted_origins
            .insert(field(trust, "node"), field(trust, "key"));
    }
    config.allowed_peers = field(c, "allowed_peers");
    config.trusted_snapshot_sources = field(c, "trusted_snapshot_sources");
    config.incoming_interval = std::time::Duration::from_millis(10);
    config.anti_entropy_interval = std::time::Duration::from_millis(30);
    let db = Database::open(
        field::<String>(&v, "path"),
        field::<Vec<u8>>(&v, "key"),
        Options {
            mode: OpenMode::CreateNew,
            database_id: Some(field(&v, "database")),
            node_id: Some(field(&v, "node")),
            replication: Some(config),
            ..Options::default()
        },
    )?;
    db.migrate(Migration::new(
        "CREATE TABLE docs(id INTEGER PRIMARY KEY,title TEXT)",
    ))?;
    db.transaction(|sql| {
        sql.execute("INSERT INTO docs VALUES(900,'Written by Rust')", [])?;
        Ok(())
    })?;
    db.flush()?;
    println!("{}", db.replication().unwrap().status().address.unwrap());
    std::io::stdout().flush()?;
    for line in std::io::stdin().lock().lines() {
        match line?.as_str() {
            "count" => {
                let n: i64 = db.read(|sql| {
                    Ok(sql.query_row("SELECT COUNT(*) FROM docs", [], |r| r.get(0))?)
                })?;
                println!("{n}");
                std::io::stdout().flush()?;
            }
            "stop" => break,
            _ => return Err("invalid command".into()),
        }
    }
    db.close()?;
    Ok(())
}
