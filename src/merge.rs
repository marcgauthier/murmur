use crate::{Error, Result, model::*};
use std::collections::BTreeMap;
/// Pure state join. No SQLite handles or local clock allocation.
pub(crate) fn join(a: &ReplicaState, b: &ReplicaState) -> Result<ReplicaState> {
    if a.database_id != b.database_id || a.schema != b.schema {
        return Err(Error::SchemaMismatch);
    }
    let mut tombs: BTreeMap<(String, Vec<u8>, Version), Tombstone> = BTreeMap::new();
    for t in a.tombstones.iter().chain(&b.tombstones) {
        let key = (t.table.clone(), t.key.clone(), t.generation);
        let e = tombs.entry(key).or_insert_with(|| t.clone());
        e.version = e.version.max(t.version);
    }
    let mut rows: BTreeMap<(String, Vec<u8>), RowState> = BTreeMap::new();
    for row in a.rows.iter().chain(&b.rows) {
        let table = a
            .schema
            .tables
            .iter()
            .find(|t| t.id == row.table)
            .ok_or_else(|| Error::Corrupt("remote table".into()))?;
        if row.key.len() != crate::schema::pk_columns(table).len()
            || row.key.iter().any(|k| matches!(k, Value::Null))
            || row.cells.len() != table.columns.len()
        {
            return Err(Error::Corrupt("remote row shape".into()));
        }
        for col in &table.columns {
            if row.cells.iter().filter(|c| c.column == col.id).count() != 1 {
                return Err(Error::Corrupt("remote columns".into()));
            }
        }
        for (col, key) in crate::schema::pk_columns(table).iter().zip(&row.key) {
            if &row.cells.iter().find(|c| c.column == col.id).unwrap().value != key {
                return Err(Error::Corrupt("remote key mismatch".into()));
            }
        }
        let key = (row.table.clone(), encode(&row.key)?);
        if let Some(old) = rows.get_mut(&key) {
            if row.generation > old.generation {
                *old = row.clone();
            } else if row.generation == old.generation {
                for cell in &row.cells {
                    let target = old
                        .cells
                        .iter_mut()
                        .find(|c| c.column == cell.column)
                        .unwrap();
                    let policy=table.columns.iter().find(|c|c.id==cell.column).ok_or(Error::SchemaMismatch)?.policy;
                    #[cfg(feature="replication")]
                    if matches!(policy,MergePolicy::Min|MergePolicy::Max) {
                        let order=crate::policy::compare(&cell.value,&target.value)?;
                        if policy==MergePolicy::Min && order.is_lt() || policy==MergePolicy::Max && order.is_gt() {*target=cell.clone();}
                        continue;
                    }
                    if cell.version > target.version {
                        *target = cell.clone();
                    } else if cell.version == target.version && cell.value != target.value && !matches!(policy,MergePolicy::Counter|MergePolicy::Set) {
                        return Err(Error::Corrupt(
                            "different values for identical version".into(),
                        ));
                    }
                }
            }
        } else {
            rows.insert(key, row.clone());
        }
    }
    let mut max_deleted: BTreeMap<(String, Vec<u8>), Version> = BTreeMap::new();
    for (table, key, generation) in tombs.keys() {
        max_deleted
            .entry((table.clone(), key.clone()))
            .and_modify(|g| *g = (*g).max(*generation))
            .or_insert(*generation);
    }
    rows.retain(|key, row| {
        max_deleted
            .get(key)
            .is_none_or(|generation| *generation < row.generation)
    });
    let mut clock = a.clock.max(b.clock);
    for row in rows.values() {
        clock = clock.max(row.generation);
        for cell in &row.cells {
            clock = clock.max(cell.version);
        }
    }
    for t in tombs.values() {
        clock = clock.max(t.version).max(t.generation);
    }
    Ok(ReplicaState {
        database_id: a.database_id,
        schema: a.schema.clone(),
        rows: rows.into_values().collect(),
        tombstones: tombs.into_values().collect(),
        clock,
    })
}
#[derive(Debug)]
pub(crate) struct Patch {
    pub rows: Vec<RowState>,
    pub deleted: Vec<RowState>,
    pub tombstones: Vec<Tombstone>,
    pub cursor: u64,
    pub state: ReplicaState,
}
pub(crate) fn prepare(a: &ReplicaState, b: &ReplicaState, cursor: u64) -> Result<Patch> {
    prepare_joined(a, join(a,b)?, cursor)
}
pub(crate) fn prepare_joined(a:&ReplicaState,joined:ReplicaState,cursor:u64)->Result<Patch> {
    let rows = joined
        .rows
        .iter()
        .filter(|row| !a.rows.contains(row))
        .cloned()
        .collect();
    let deleted = a
        .rows
        .iter()
        .filter(|old| {
            !joined
                .rows
                .iter()
                .any(|new| new.table == old.table && new.key == old.key)
        })
        .cloned()
        .collect();
    let tombstones = joined
        .tombstones
        .iter()
        .filter(|t| !a.tombstones.contains(t))
        .cloned()
        .collect();
    Ok(Patch {
        rows,
        deleted,
        tombstones,
        cursor,
        state: joined,
    })
}
