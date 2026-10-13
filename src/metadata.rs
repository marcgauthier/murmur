//! Conflict versions live in Spool. These types are transient record payloads,
//! not an independently maintained in-memory metadata database.
use crate::{Error, Result, model::*};
use rusqlite::{
    fallible_streaming_iterator::FallibleStreamingIterator, hooks::Action, session::ChangesetIter,
};
use serde::{Deserialize, Serialize};
use spool::{Mutation, Store};
#[derive(Serialize, Deserialize)]
pub(crate) struct RowMeta {
    pub key: Vec<u8>,
    pub generation: Version,
    pub cells: Vec<Cell>,
}
pub(crate) fn row_key(table: &str, key: &[u8]) -> Vec<u8> {
    format!("04/meta/row/{table}/{}", blake3::hash(key).to_hex()).into_bytes()
}
fn tomb_key(t: &Tombstone) -> Result<Vec<u8>> {
    Ok(format!(
        "05/meta/tomb/{}/{}",
        t.table,
        blake3::hash(&encode(&(t.key.clone(), t.generation))?).to_hex()
    )
    .into_bytes())
}
pub(crate) fn read(store: &Store, table: &str, key: &[u8]) -> Result<Option<RowMeta>> {
    let value = store
        .get(&row_key(table, key))?
        .map(|b| decode::<RowMeta>(&b))
        .transpose()?;
    if value.as_ref().is_some_and(|v| v.key != key) {
        return Err(Error::Corrupt("metadata key collision".into()));
    }
    Ok(value)
}
pub(crate) fn put(table: &str, meta: &RowMeta) -> Result<Mutation> {
    Ok(Mutation::put(row_key(table, &meta.key), encode(meta)?))
}
fn tomb(table: &str, key: Vec<u8>, generation: Version, version: Version) -> Result<Mutation> {
    let t = Tombstone {
        table: table.into(),
        key,
        generation,
        version,
    };
    Ok(Mutation::put(tomb_key(&t)?, encode(&t)?))
}
pub(crate) fn tables_in_changeset(bytes: &[u8]) -> Vec<String> {
    if bytes.is_empty() {
        return Vec::new();
    }
    let mut cursor = std::io::Cursor::new(bytes);
    let reader: &mut dyn std::io::Read = &mut cursor;
    let mut iter = match ChangesetIter::start_strm(&reader) {
        Ok(it) => it,
        Err(_) => return Vec::new(),
    };
    let mut set = std::collections::HashSet::new();
    while let Ok(Some(item)) = iter.next() {
        if let Ok(op) = item.op() {
            set.insert(op.table_name().to_string());
        }
    }
    set.into_iter().collect()
}
pub(crate) fn delta(
    store: &Store,
    schema: &Schema,
    bytes: &[u8],
    version: Version,
) -> Result<Vec<Mutation>> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    let mut cursor = std::io::Cursor::new(bytes);
    let reader: &mut dyn std::io::Read = &mut cursor;
    let mut iter = ChangesetIter::start_strm(&reader)?;
    let mut changes = Vec::new();
    while let Some(item) = iter.next()? {
        let op = item.op()?;
        let table = schema
            .tables
            .iter()
            .find(|t| t.name == op.table_name())
            .ok_or_else(|| Error::Corrupt("unknown changeset table".into()))?;
        if op.number_of_columns() as usize != table.columns.len()
            || item
                .pk()?
                .iter()
                .zip(&table.columns)
                .any(|(pk, c)| (*pk != 0) != (c.primary_key > 0))
        {
            return Err(Error::SchemaMismatch);
        }
        let key = crate::schema::pk_columns(table)
            .iter()
            .map(|col| {
                let i = table.columns.iter().position(|c| c.id == col.id).unwrap();
                Value::read(if op.code() == Action::SQLITE_INSERT {
                    item.new_value(i)?
                } else {
                    item.old_value(i)?
                })
            })
            .collect::<rusqlite::Result<Vec<_>>>()?;
        if key.iter().any(|v| matches!(v, Value::Null)) {
            return Err(Error::Corrupt("null changeset primary key".into()));
        }
        let key = encode(&key)?;
        let old = read(store, &table.id, &key)?;
        match op.code() {
            Action::SQLITE_INSERT => {
                if let Some(old) = old {
                    changes.push(tomb(&table.id, key.clone(), old.generation, version)?);
                }
                changes.push(put(
                    &table.id,
                    &RowMeta {
                        key,
                        generation: version,
                        cells: table
                            .columns
                            .iter()
                            .enumerate()
                            .map(|(i, c)| Ok(Cell { column: c.id.clone(), value: Value::read(item.new_value(i)?)?, version }))
                            .collect::<Result<Vec<_>>>()?,
                    },
                )?);
            }
            Action::SQLITE_UPDATE => {
                let mut meta =
                    old.ok_or_else(|| Error::Corrupt("update has no row metadata".into()))?;
                for (i, c) in table.columns.iter().enumerate() {
                    match item.new_value(i) {
                        Ok(value) => {
                            let cell = meta
                                .cells
                                .iter_mut()
                                .find(|cell| cell.column == c.id)
                                .ok_or(Error::SchemaMismatch)?;
                            cell.version = version;
                            cell.value = Value::read(value)?;
                        }
                        Err(rusqlite::Error::InvalidColumnIndex(_)) => {}
                        Err(e) => return Err(e.into()),
                    }
                }
                changes.push(put(&table.id, &meta)?);
            }
            Action::SQLITE_DELETE => {
                let meta =
                    old.ok_or_else(|| Error::Corrupt("delete has no row metadata".into()))?;
                changes.push(tomb(&table.id, key.clone(), meta.generation, version)?);
                changes.push(Mutation::delete(row_key(&table.id, &key)));
            }
            _ => return Err(Error::Corrupt("unsupported changeset action".into())),
        }
    }
    Ok(changes)
}
pub(crate) fn migration(
    store: &Store,
    before: &[RowState],
    after: &[RowState],
    version: Version,
) -> Result<Vec<Mutation>> {
    let mut changes = Vec::new();
    for row in before {
        if !after
            .iter()
            .any(|r| r.table == row.table && r.key == row.key)
        {
            let key = encode(&row.key)?;
            let old = read(store, &row.table, &key)?
                .ok_or_else(|| Error::Corrupt("migration row metadata missing".into()))?;
            changes.push(tomb(&row.table, key.clone(), old.generation, version)?);
            changes.push(Mutation::delete(row_key(&row.table, &key)));
        }
    }
    for row in after {
        let key = encode(&row.key)?;
        let old = read(store, &row.table, &key)?;
        let prior = before
            .iter()
            .find(|r| r.table == row.table && r.key == row.key);
        let cells = row
            .cells
            .iter()
            .map(|cell| {
                let unchanged = prior
                    .and_then(|r| r.cells.iter().find(|c| c.column == cell.column))
                    .is_some_and(|c| c.value == cell.value);
                let v = if unchanged {
                    old.as_ref()
                        .and_then(|m| m.cells.iter().find(|c| c.column == cell.column))
                        .map(|c| c.version)
                        .unwrap_or(version)
                } else {
                    version
                };
                Cell { column: cell.column.clone(), value: cell.value.clone(), version: v }
            })
            .collect();
        changes.push(put(
            &row.table,
            &RowMeta {
                key,
                generation: old.map(|o| o.generation).unwrap_or(version),
                cells,
            },
        )?);
    }
    Ok(changes)
}
pub(crate) fn replica(store: &Store, state: &ReplicaState) -> Result<Vec<Mutation>> {
    let mut mutations = store
        .keys_with_prefix(b"04/meta/row/")
        .into_iter()
        .map(Mutation::delete)
        .collect::<Vec<_>>();
    // State is the validated join, including all previously known tombstones.
    for row in &state.rows {
        mutations.push(put(
            &row.table,
            &RowMeta {
                key: encode(&row.key)?,
                generation: row.generation,
                cells: row.cells.clone(),
            },
        )?);
    }
    for t in &state.tombstones {
        mutations.push(Mutation::put(tomb_key(t)?, encode(t)?));
    }
    Ok(mutations)
}
pub(crate) fn export(
    store: &Store,
    schema: Schema,
    database_id: [u8; 16],
    mut rows: Vec<RowState>,
    clock: Version,
) -> Result<ReplicaState> {
    for row in &mut rows {
        let meta = read(store, &row.table, &encode(&row.key)?)?
            .ok_or_else(|| Error::Corrupt("row metadata missing".into()))?;
        row.generation = meta.generation;
        for cell in &mut row.cells {
            cell.version = meta
                .cells
                .iter()
                .find(|c| c.column == cell.column)
                .map(|c| c.version)
                .ok_or(Error::SchemaMismatch)?;
        }
    }
    let mut tombstones = Vec::new();
    for key in store.keys_with_prefix(b"05/meta/tomb/") {
        let b = store
            .get(&key)?
            .ok_or_else(|| Error::Corrupt("tombstone missing".into()))?;
        tombstones.push(decode::<Tombstone>(&b)?);
    }
    tombstones
        .sort_by(|a, b| (&a.table, &a.key, a.generation).cmp(&(&b.table, &b.key, b.generation)));
    Ok(ReplicaState {
        database_id,
        schema,
        rows,
        tombstones,
        clock,
    })
}

/// Read a transient logical state from the authoritative encrypted Spool.
/// The checkpoint worker serializes this read with all metadata commits.
pub(crate) fn state(store: &Store, schema: Schema, database_id: [u8;16], clock: Version) -> Result<ReplicaState> {
    let mut rows = Vec::new();
    for table in &schema.tables {
        for key in store.keys_with_prefix(format!("04/meta/row/{}/", table.id).as_bytes()) {
            let bytes = store.get(&key)?.ok_or_else(|| Error::Corrupt("missing row record".into()))?;
            let meta: RowMeta = decode(&bytes)?;
            rows.push(RowState { table: table.id.clone(), key: decode(&meta.key)?, generation: meta.generation, cells: meta.cells });
        }
    }
    export(store, schema, database_id, rows, clock)
}

/// Only the rows/tombstones changed by one committed local capture.
pub(crate) fn changed_state(records: &[Mutation], schema: Schema, database_id: [u8;16], clock: Version) -> Result<ReplicaState> {
    let mut rows = Vec::new(); let mut tombstones = Vec::new();
    for record in records {
        if record.deleted {continue;}
        if record.key.starts_with(b"04/meta/row/") {
            let meta: RowMeta = decode(&record.value)?;
            let key = std::str::from_utf8(&record.key).map_err(|_|Error::Corrupt("row key".into()))?;
            let table = key.split('/').nth(3).ok_or(Error::SchemaMismatch)?.to_string();
            rows.push(RowState {table, key:decode(&meta.key)?,generation:meta.generation,cells:meta.cells});
        } else if record.key.starts_with(b"05/meta/tomb/") {tombstones.push(decode(&record.value)?);}
    }
    Ok(ReplicaState {database_id,schema,rows,tombstones,clock})
}

pub(crate) fn patch(store:&Store,rows:&[RowState],deleted:&[RowState],tombstones:&[Tombstone])->Result<Vec<Mutation>> {
    let mut changes=Vec::new();
    for row in deleted {changes.push(Mutation::delete(row_key(&row.table,&encode(&row.key)?)));}
    for row in rows {changes.push(put(&row.table,&RowMeta{key:encode(&row.key)?,generation:row.generation,cells:row.cells.clone()})?);}
    for t in tombstones {let key=tomb_key(t)?;let mut t=t.clone();if let Some(bytes)=store.get(&key)? {let old:Tombstone=decode(&bytes)?;t.version=t.version.max(old.version);}changes.push(Mutation::put(key,encode(&t)?));}
    Ok(changes)
}
