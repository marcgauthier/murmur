use crate::*;
use std::{
    io::{Read, Write},
    time::Duration,
};
pub struct Reader(pub Foreign);
impl Read for Reader {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        let n = out.len().min(64 << 10);
        if n == 0 {
            return Ok(0);
        }
        let data = self
            .0
            .bytes(3, 0, &(n as u64).to_le_bytes())
            .map_err(std::io::Error::other)?;
        if data.len() > n {
            return Err(std::io::Error::other("Go reader exceeded buffer"));
        }
        out[..data.len()].copy_from_slice(&data);
        Ok(data.len())
    }
}
pub struct Writer(pub Foreign);
impl Write for Writer {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        let n = data.len().min(64 << 10);
        let out = self
            .0
            .bytes(4, 0, &data[..n])
            .map_err(std::io::Error::other)?;
        if out.len() != 8 {
            return Err(std::io::Error::other("invalid Go writer result"));
        }
        let written = u64::from_le_bytes(out.try_into().unwrap()) as usize;
        if written > n {
            return Err(std::io::Error::other("invalid Go write count"));
        }
        Ok(written)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
pub fn foreign(v: &Json, callback: Option<Callback>) -> db::Result<Foreign> {
    Ok(Foreign::new(
        field(v, "callback")?,
        callback.ok_or_else(|| invalid("callback missing"))?,
    ))
}
pub fn call(id: u64, v: Json, callback: Option<Callback>) -> db::Result<Json> {
    let op: String = field(&v, "op")?;
    match op.as_str() {
        "defaults" => return Ok(config::defaults()),
        "open" => {
            let options = config::options(&v["options"], callback)?;
            let database = db::Database::open(
                field::<String>(&v, "path")?,
                field::<Vec<u8>>(&v, "key")?,
                options,
            )?;
            return Ok(json!(put(Entry::Database(database))));
        }
        "raw_open" => return Ok(json!(put(Entry::Raw(sql::Raw::open()?)))),
        "restore" => {
            let options = db::RestoreOptions {
                new_database_id: decode(v["new_database_id"].clone())?,
                new_node_id: decode(v["new_node_id"].clone())?,
            };
            db::Database::restore_with_options(
                field::<String>(&v, "backup")?,
                field::<String>(&v, "destination")?,
                field::<Vec<u8>>(&v, "key")?,
                options,
            )?;
            return Ok(Json::Null);
        }
        "verify_backup" => {
            db::Database::verify_backup(
                field::<String>(&v, "path")?,
                field::<Vec<u8>>(&v, "key")?,
            )?;
            return Ok(Json::Null);
        }
        "scheduler_new" => {
            return Ok(json!(put(Entry::Scheduler(Arc::new(
                db::WriterScheduler::new(config::scheduler(&v["config"])?)
            )))));
        }
        _ => {}
    }
    if op.starts_with("helper_")
        || op.starts_with("codec_")
        || op.starts_with("backend_")
        || op == "replication_start"
        || op == "objects_open"
        || op == "bridge_open"
        || op == "transport_endpoint"
    {
        return advanced::call(id, &v, callback);
    }
    let entry = get(id)?;
    if sql::in_callback() {
        return Err(invalid(
            "native object calls cannot reenter a SQL callback; use the supplied Sql",
        ));
    }
    match entry.as_ref() {
        Entry::Database(database) => match op.as_str() {
            "clone" => Ok(json!(put(Entry::Database(database.clone())))),
            "read" | "transaction" => {
                let f = foreign(&v, callback)?;
                let invoke =
                    move |s: &db::Sql<'_>| sql::managed(s, |scope| f.json(1, scope, Json::Null));
                if op == "read" {
                    database.read(invoke)
                } else {
                    database.transaction(invoke)
                }
            }
            "migrate" => {
                let mut m = db::Migration::new(field::<String>(&v, "sql")?);
                if let Some(maps) = v.get("mappings") {
                    for mapping in maps.as_array().ok_or_else(|| invalid("mappings"))? {
                        m.mappings.push(db::TableMapping {
                            old_name: field(mapping, "old_name")?,
                            new_name: field(mapping, "new_name")?,
                            columns: field(mapping, "columns")?,
                        });
                    }
                }
                database.migrate(m)?;
                Ok(Json::Null)
            }
            "flush" => {
                database.flush()?;
                Ok(Json::Null)
            }
            "close" => {
                database.close()?;
                remove(id);
                Ok(Json::Null)
            }
            "release" => {
                remove(id);
                Ok(Json::Null)
            }
            "backup" => {
                database.backup(field::<String>(&v, "destination")?)?;
                Ok(Json::Null)
            }
            "refresh_indexes" => {
                database.refresh_indexes()?;
                Ok(Json::Null)
            }
            "set_search_indexes" => {
                database.set_search_indexes(config::indexes(&v["indexes"])?)?;
                Ok(Json::Null)
            }
            "set_replication" => {
                database.set_replication(&field::<String>(&v, "table")?, field(&v, "enabled")?)?;
                Ok(Json::Null)
            }
            "replication_enabled" => Ok(json!(
                database.replication_enabled(&field::<String>(&v, "table")?)?
            )),
            "rotate_data_key" => {
                database.rotate_data_key()?;
                Ok(Json::Null)
            }
            "rotate_master_key" => {
                database.rotate_master_key(field(&v, "key")?)?;
                Ok(Json::Null)
            }
            "retry_storage" => {
                database.retry_storage()?;
                Ok(Json::Null)
            }
            "export_state" => encoded(database.export_state()?),
            "merge_state" => {
                database.merge_state(field(&v, "state")?)?;
                Ok(Json::Null)
            }
            "open_progress" => encoded(database.open_progress()),
            "status" => encoded(database.status()),
            "sqlite_metrics" => encoded(database.sqlite_metrics()?),
            "spool_metrics" => encoded(database.spool_metrics()),
            "database_id" => encoded(database.database_id()),
            "node_id" => encoded(database.node_id()),
            "key_inventory" => inventory(database.key_inventory()),
            "schema" => {
                let s = database.schema()?;
                let mut j = encoded(&s)?;
                j["table_replication"] = json!(s.table_replication);
                Ok(j)
            }
            "scheduler" => Ok(json!(put(Entry::Scheduler(database.scheduler())))),
            "replication" => Ok(match database.replication() {
                Some(h) => json!(put(Entry::Replication(h))),
                None => Json::Null,
            }),
            "retire_peer" => {
                database.retire_peer(field(&v, "node")?)?;
                Ok(Json::Null)
            }
            "retention_status" => retention(database.retention_status()?),
            "collect_retention" => encoded(database.collect_retention()?),
            "subscribe" => {
                let f = foreign(&v, callback)?;
                let options: db::SubscriptionOptions = super_options(&v["options"])?;
                let subscription = database.subscribe(
                    move |s| {
                        sql::managed(s, |scope| {
                            decode::<Vec<ForeignRow>>(f.json(5, scope, Json::Null)?)
                        })
                        .map_err(|e| rusqlite::Error::InvalidParameterName(e.to_string()))
                    },
                    |r: &ForeignRow| r.key.clone(),
                    options,
                )?;
                Ok(json!(put(Entry::Subscription(Mutex::new(subscription)))))
            }
            "upload_file" => encoded(database.upload_file(
                &field::<String>(&v, "name")?,
                Reader(foreign(&v, callback)?),
            )?),
            "get_file" => encoded(database.get_file(
                &field::<String>(&v, "name")?,
                Writer(foreign(&v, callback)?),
            )?),
            "list_files" => encoded(database.list_files()?),
            "search_files" => encoded(database.search_files(&field::<String>(&v, "query")?)?),
            "file_metadata" => encoded(database.file_metadata(&field::<String>(&v, "name")?)?),
            "file_availability" => {
                encoded(database.file_availability(&field::<String>(&v, "name")?)?)
            }
            "file_available" => encoded(database.file_available(&field::<String>(&v, "name")?)?),
            "file_key_rotation_status" => encoded(database.file_key_rotation_status()?),
            "delete_file" => {
                database.delete_file(&field::<String>(&v, "name")?)?;
                Ok(Json::Null)
            }
            _ => Err(invalid(format!("database operation {op}"))),
        },
        Entry::Scheduler(s) => match op.as_str() {
            "snapshot" => encoded(s.snapshot()),
            "active" => encoded(s.active()),
            "close" => {
                s.close();
                remove(id);
                Ok(Json::Null)
            }
            "release" => {
                remove(id);
                Ok(Json::Null)
            }
            "admit" | "admit_timeout" => {
                let class = decode(v["class"].clone())?;
                let ticket = if op == "admit" {
                    s.admit(class)?
                } else {
                    s.admit_timeout(class, Duration::from_nanos(field(&v, "timeout")?))?
                };
                Ok(json!(put(Entry::Ticket(Mutex::new(Some(ticket))))))
            }
            _ => Err(invalid("scheduler operation")),
        },
        Entry::Ticket(ticket) => {
            let mut guard = ticket.lock().unwrap();
            let t = guard.as_mut().ok_or(db::Error::Closed)?;
            match op.as_str() {
                "class" => encoded(t.class()),
                "dual" => encoded(t.dual()),
                "granted_elapsed" => encoded(t.granted_at().elapsed().as_nanos() as u64),
                "release" | "close" => {
                    t.release();
                    guard.take();
                    remove(id);
                    Ok(Json::Null)
                }
                _ => Err(invalid("ticket operation")),
            }
        }
        Entry::Subscription(subscription) => {
            let mut s = subscription.lock().unwrap();
            match op.as_str() {
                "recv" => encoded(s.recv_timeout(Duration::from_nanos(field(&v, "timeout")?))?),
                "try_recv" => encoded(s.try_recv()?),
                "close" => {
                    s.close();
                    remove(id);
                    Ok(Json::Null)
                }
                _ => Err(invalid("subscription operation")),
            }
        }
        Entry::Replication(h) => match op.as_str() {
            "status" => encoded(h.status()),
            "add_peer" => {
                h.add_peer(field(&v, "address")?).map_err(network)?;
                Ok(Json::Null)
            }
            "remove_peer" => {
                h.remove_peer(field(&v, "node")?).map_err(network)?;
                Ok(Json::Null)
            }
            "force_sync" => {
                h.force_sync().map_err(network)?;
                Ok(Json::Null)
            }
            "retire_peer" => {
                h.retire_peer(field(&v, "node")?).map_err(network)?;
                Ok(Json::Null)
            }
            "retention_status" => retention(h.retention_status().map_err(network)?),
            "collect_retention" => encoded(h.collect_retention().map_err(network)?),
            "files" => Ok(h
                .files()
                .map(|f| json!(put(Entry::Objects(f))))
                .unwrap_or(Json::Null)),
            "bridge" => Ok(h
                .bridge()
                .map(|b| json!(put(Entry::Bridge(b))))
                .unwrap_or(Json::Null)),
            "close" => {
                h.close().map_err(network)?;
                remove(id);
                Ok(Json::Null)
            }
            "release" => {
                remove(id);
                Ok(Json::Null)
            }
            _ => Err(invalid("replication operation")),
        },
        Entry::Raw(raw) => match op.as_str() {
            "read" | "transaction" => raw.run(foreign(&v, callback)?),
            "close" => {
                remove(id);
                Ok(Json::Null)
            }
            _ if op.starts_with("session_") => raw.session(&v),
            _ => Err(invalid("raw operation")),
        },
        _ => advanced::entry(id, entry.as_ref(), &v, callback),
    }
}
fn super_options(v: &Json) -> db::Result<db::SubscriptionOptions> {
    let mut o = db::SubscriptionOptions::default();
    if let Some(x) = v.get("buffer_size") {
        o.buffer_size = decode(x.clone())?;
    }
    if let Some(x) = v.get("emit_unchanged") {
        o.emit_unchanged = decode(x.clone())?;
    }
    if let Some(x) = v.get("resume_from") {
        o.resume_from = decode(x.clone())?;
    }
    if let Some(x) = v.get("tables") {
        o.tables = decode(x.clone())?;
    }
    Ok(o)
}

fn retention(s: net::retention::RetentionStatus) -> db::Result<Json> {
    Ok(
        json!({"peers":s.peers.into_iter().map(|p|json!({"node":p.node,"last_seen_millis":p.last_seen_millis,"retired":p.retired,"acknowledged":p.acknowledged.into_iter().collect::<Vec<_>>()})).collect::<Vec<_>>(),"collected_transactions":s.collected_transactions,"last_collection_millis":s.last_collection_millis}),
    )
}

fn inventory(k: db::KeyInventory) -> db::Result<Json> {
    Ok(
        json!({"encrypted":k.encrypted,"wrapping_key_id":k.wrapping_key_id,"active_data_key_id":k.active_data_key_id,"keyring_seq":k.keyring_seq,"manifest_generation":k.manifest_generation,"maintenance_phase":k.maintenance_phase,"maintenance_done":k.maintenance_done,"maintenance_total":k.maintenance_total,"data_keys":k.data_keys.into_iter().map(|k|json!({"id":k.id,"created_at":k.created_at.duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_nanos() as u64,"status":format!("{:?}",k.status)})).collect::<Vec<_>>()}),
    )
}
