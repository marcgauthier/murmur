use crate::{Error, Result, model::*};
use rusqlite::Connection;
pub(crate) fn capture(c: &Connection, s: &Schema) -> Result<crate::capture::Capture> {
    crate::capture::Capture::new(c, s.tables.iter().map(|t| t.name.clone()))
}
pub(crate) fn inspect(c: &Connection, previous: &Schema, m: &Migration) -> Result<Schema> {
    let mut tables = Vec::new();
    let mut stmt=c.prepare("SELECT name,sql FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' AND lower(name) NOT GLOB '_crdt_*' ORDER BY name")?;
    for entry in stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))? {
        let (name, sql) = entry?;
        let upper = keywords(&sql);
        if [
            "VIRTUAL",
            "CHECK",
            "REFERENCES",
            "GENERATED",
            "AUTOINCREMENT",
        ]
        .iter()
        .any(|w| upper.iter().any(|part| part == *w))
        {
            return Err(Error::Invalid(format!("unsupported schema for {name}")));
        }
        let mapping = m.mappings.iter().find(|map| map.new_name == name);
        let old = previous
            .tables
            .iter()
            .find(|t| t.name == mapping.map(|x| x.old_name.as_str()).unwrap_or(&name));
        let table_id = old
            .map(|t| t.id.clone())
            .unwrap_or_else(|| id(&[b"sqlite-table/v4/".as_slice(), name.as_bytes()].concat()));
        let mut pragma = c.prepare(&format!("PRAGMA table_xinfo({})", quote(&name)))?;
        let mut columns = Vec::new();
        for row in pragma.query_map([], |r| {
            Ok((
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, bool>(3)?,
                r.get::<_, u32>(5)? as usize,
                r.get::<_, i32>(6)?,
            ))
        })? {
            let (col, declared_type, not_null, primary_key, hidden) = row?;
            if hidden != 0 {
                return Err(Error::Invalid(
                    "generated/hidden columns unsupported".into(),
                ));
            }
            let source = mapping
                .and_then(|map| map.columns.iter().find(|(_, new)| new == &col))
                .map(|(old, _)| old.as_str())
                .unwrap_or(&col);
            let oldcol = old.and_then(|t| t.columns.iter().find(|c| c.name == source));
            let column_id = oldcol.map(|c| c.id.clone()).unwrap_or_else(|| {
                id(&[
                    table_id.as_bytes(),
                    col.as_bytes(),
                ]
                .concat())
            });
            columns.push(Column {
                policy: oldcol.map(|c|c.policy).unwrap_or_default(),
                id: column_id,
                name: col,
                primary_key,
                declared_type,
                not_null,
            });
        }
        let pk: Vec<_> = columns.iter().filter(|c| c.primary_key > 0).collect();
        if pk.is_empty()
            || pk.iter().any(|c| {
                !c.not_null && !(pk.len() == 1 && c.declared_type.eq_ignore_ascii_case("INTEGER"))
            })
        {
            return Err(Error::Invalid(format!(
                "{name} requires a non-null primary key"
            )));
        }
        if let Some(old) = old {
            let mut a: Vec<_> = old
                .columns
                .iter()
                .filter(|c| c.primary_key > 0)
                .map(|c| {
                    (
                        c.primary_key,
                        c.id.clone(),
                        c.declared_type.to_ascii_uppercase(),
                    )
                })
                .collect();
            let mut b: Vec<_> = columns
                .iter()
                .filter(|c| c.primary_key > 0)
                .map(|c| {
                    (
                        c.primary_key,
                        c.id.clone(),
                        c.declared_type.to_ascii_uppercase(),
                    )
                })
                .collect();
            a.sort();
            b.sort();
            if a != b {
                return Err(Error::Invalid(
                    "migration changes primary-key identity".into(),
                ));
            }
        }
        let mut indexes = c.prepare(&format!("PRAGMA index_list({})", quote(&name)))?;
        for index in
            indexes.query_map([], |r| Ok((r.get::<_, bool>(2)?, r.get::<_, String>(3)?)))?
        {
            let (unique, origin) = index?;
            if unique && origin != "pk" {
                return Err(Error::Invalid(
                    "non-primary-key unique constraints unsupported".into(),
                ));
            }
        }
        tables.push(Table {
            id: table_id,
            name,
            sql,
            columns,
        });
    }
    let forbidden:i64=c.query_row("SELECT count(*) FROM sqlite_schema WHERE type IN ('trigger','view') AND lower(name) NOT GLOB '_crdt_*'",[],|r|r.get(0))?;
    if forbidden > 0 {
        return Err(Error::Invalid(
            "application triggers and views unsupported".into(),
        ));
    }
    let mut q = c.prepare(
        "SELECT sql FROM sqlite_schema WHERE type='index' AND sql IS NOT NULL ORDER BY name",
    )?;
    let indexes = q
        .query_map([], |r| r.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    // SQL and explicit mappings are part of schema history; all replicas must
    // deploy the same migration, rather than infer identity from local clocks.
    let table_ids: std::collections::BTreeSet<_> = tables.iter().map(|t| &t.id).collect();
    if table_ids.len() != tables.len()
        || tables.iter().any(|t| {
            t.columns
                .iter()
                .map(|c| &c.id)
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                != t.columns.len()
        })
    {
        return Err(Error::Invalid(
            "ambiguous migration identity mappings".into(),
        ));
    }
    for map in &m.mappings {
        if !previous.tables.iter().any(|t| t.name == map.old_name)
            || !tables.iter().any(|t| t.name == map.new_name)
        {
            return Err(Error::Invalid(
                "migration mapping references a missing table".into(),
            ));
        }
        let old = previous
            .tables
            .iter()
            .find(|t| t.name == map.old_name)
            .unwrap();
        let new = tables.iter().find(|t| t.name == map.new_name).unwrap();
        if map.columns.iter().any(|(a, b)| {
            !old.columns.iter().any(|c| &c.name == a) || !new.columns.iter().any(|c| &c.name == b)
        }) {
            return Err(Error::Invalid(
                "migration mapping references a missing column".into(),
            ));
        }
    }
    let history = *blake3::hash(
        &[
            previous.history.as_slice(),
            m.sql.as_bytes(),
            format!("{:?}", m.mappings).as_bytes(),
        ]
        .concat(),
    )
    .as_bytes();
    let table_replication = previous.table_replication.iter()
        .filter(|(id, _)| tables.iter().any(|table| &table.id == *id))
        .map(|(id, enabled)| (id.clone(), *enabled)).collect();
    Ok(Schema {
        table_replication,
        tables,
        indexes,
        history,
    })
}
pub(crate) fn pk_columns(t: &Table) -> Vec<&Column> {
    let mut pk: Vec<_> = t.columns.iter().filter(|c| c.primary_key > 0).collect();
    pk.sort_by_key(|c| c.primary_key);
    pk
}
pub(crate) fn read_table_rows(c: &Connection, t: &Table) -> Result<Vec<RowState>> {
    let mut rows = Vec::new();
    let mut q = c.prepare(&format!(
        "SELECT {} FROM {} ORDER BY {}",
        t.columns
            .iter()
            .map(|c| quote(&c.name))
            .collect::<Vec<_>>()
            .join(","),
        quote(&t.name),
        pk_columns(t)
            .iter()
            .map(|c| quote(&c.name))
            .collect::<Vec<_>>()
            .join(",")
    ))?;
    let mut cursor = q.query([])?;
    while let Some(row) = cursor.next()? {
        let values = (0..t.columns.len())
            .map(|i| Value::read(row.get_ref(i)?))
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let key = pk_columns(t)
            .iter()
            .map(|col| values[t.columns.iter().position(|c| c.id == col.id).unwrap()].clone())
            .collect::<Vec<_>>();
        if key.iter().any(|v| matches!(v, Value::Null)) {
            return Err(Error::Invalid("null primary key".into()));
        }
        rows.push(RowState {
            table: t.id.clone(),
            key,
            generation: Version::default(),
            cells: t
                .columns
                .iter()
                .zip(values)
                .map(|(c, value)| Cell {
                    column: c.id.clone(),
                    value,
                    version: Version::default(),
                })
                .collect(),
        });
    }
    Ok(rows)
}
// Used only for explicit export and schema migration, never normal commits.
pub(crate) fn read_rows(c: &Connection, s: &Schema) -> Result<Vec<RowState>> {
    let mut rows = Vec::new();
    for t in &s.tables {
        rows.extend(read_table_rows(c, t)?);
    }
    Ok(rows)
}

fn row_approx_size(r: &RowState) -> usize {
    64 + r.key.len() * 16
        + r.cells
            .iter()
            .map(|c| {
                c.column.len()
                    + match &c.value {
                        Value::Text(s) => s.len(),
                        Value::Blob(b) => b.len(),
                        _ => 16,
                    }
            })
            .sum::<usize>()
}

pub(crate) fn chunk_migration_rows(
    before: Vec<RowState>,
    after: Vec<RowState>,
    chunk_rows: usize,
    max_chunk_bytes: usize,
) -> Vec<(Vec<RowState>, Vec<RowState>)> {
    if before.is_empty() && after.is_empty() {
        return Vec::new();
    }
    let chunk_rows = chunk_rows.max(1);
    let max_chunk_bytes = max_chunk_bytes.max(1024);

    let mut table_order = Vec::new();
    for r in &before {
        if !table_order.contains(&r.table) {
            table_order.push(r.table.clone());
        }
    }
    for r in &after {
        if !table_order.contains(&r.table) {
            table_order.push(r.table.clone());
        }
    }

    let mut before_by_table: std::collections::BTreeMap<String, Vec<RowState>> =
        std::collections::BTreeMap::new();
    for r in before {
        before_by_table.entry(r.table.clone()).or_default().push(r);
    }
    let mut after_by_table: std::collections::BTreeMap<String, Vec<RowState>> =
        std::collections::BTreeMap::new();
    for r in after {
        after_by_table.entry(r.table.clone()).or_default().push(r);
    }

    let mut chunks = Vec::new();

    for table_id in table_order {
        let b_rows = before_by_table.remove(&table_id).unwrap_or_default();
        let a_rows = after_by_table.remove(&table_id).unwrap_or_default();

        if b_rows.is_empty() && a_rows.is_empty() {
            continue;
        }

        let mut b_iter = b_rows.into_iter().peekable();
        let mut a_iter = a_rows.into_iter().peekable();

        while b_iter.peek().is_some() || a_iter.peek().is_some() {
            let mut chunk_b = Vec::new();
            let mut chunk_a = Vec::new();
            let mut count = 0;
            let mut chunk_bytes = 0;

            while (count < chunk_rows && chunk_bytes < max_chunk_bytes)
                && (b_iter.peek().is_some() || a_iter.peek().is_some())
            {
                match (b_iter.peek(), a_iter.peek()) {
                    (Some(b), Some(a)) => match cmp_values(&b.key, &a.key) {
                        std::cmp::Ordering::Less => {
                            let item = b_iter.next().unwrap();
                            chunk_bytes += row_approx_size(&item);
                            chunk_b.push(item);
                        }
                        std::cmp::Ordering::Greater => {
                            let item = a_iter.next().unwrap();
                            chunk_bytes += row_approx_size(&item);
                            chunk_a.push(item);
                        }
                        std::cmp::Ordering::Equal => {
                            let item_b = b_iter.next().unwrap();
                            let item_a = a_iter.next().unwrap();
                            chunk_bytes += row_approx_size(&item_b) + row_approx_size(&item_a);
                            chunk_b.push(item_b);
                            chunk_a.push(item_a);
                        }
                    },
                    (Some(_), None) => {
                        let item = b_iter.next().unwrap();
                        chunk_bytes += row_approx_size(&item);
                        chunk_b.push(item);
                    }
                    (None, Some(_)) => {
                        let item = a_iter.next().unwrap();
                        chunk_bytes += row_approx_size(&item);
                        chunk_a.push(item);
                    }
                    (None, None) => break,
                }
                count += 1;
            }

            if !chunk_b.is_empty() || !chunk_a.is_empty() {
                chunks.push((chunk_b, chunk_a));
            }
        }
    }

    chunks
}

fn cmp_values(a: &[Value], b: &[Value]) -> std::cmp::Ordering {
    for (va, vb) in a.iter().zip(b.iter()) {
        let ord = cmp_value(va, vb);
        if ord != std::cmp::Ordering::Equal {
            return ord;
        }
    }
    a.len().cmp(&b.len())
}

fn cmp_value(a: &Value, b: &Value) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Less,
        (_, Value::Null) => Ordering::Greater,
        (Value::Integer(x), Value::Integer(y)) => x.cmp(y),
        (Value::Real(x), Value::Real(y)) => x.total_cmp(y),
        (Value::Integer(x), Value::Real(y)) => (*x as f64).total_cmp(y),
        (Value::Real(x), Value::Integer(y)) => x.total_cmp(&(*y as f64)),
        (Value::Integer(_) | Value::Real(_), _) => Ordering::Less,
        (_, Value::Integer(_) | Value::Real(_)) => Ordering::Greater,
        (Value::Text(x), Value::Text(y)) => x.cmp(y),
        (Value::Text(_), _) => Ordering::Less,
        (_, Value::Text(_)) => Ordering::Greater,
        (Value::Blob(x), Value::Blob(y)) => x.cmp(y),
    }
}
fn row_values<'a>(table:&Table,row:&'a RowState)->Result<Vec<&'a Value>> {
    cell_values(table, &row.cells)
}
pub(crate) fn cell_values<'a>(table:&Table,cells:&'a [Cell])->Result<Vec<&'a Value>> {
    table.columns.iter().map(|column|cells.iter().find(|cell|cell.column==column.id).map(|cell|&cell.value).ok_or_else(||Error::Corrupt("missing cell".into()))).collect()
}
pub(crate) fn row_insert_sql(table:&Table)->String {
    format!("INSERT OR REPLACE INTO {}({}) VALUES({})",quote(&table.name),table.columns.iter().map(|column|quote(&column.name)).collect::<Vec<_>>().join(","),vec!["?";table.columns.len()].join(","))
}
pub(crate) fn put_user_row(c:&Connection,table:&Table,row:&RowState)->Result<()> {
    let values=row_values(table,row)?;
    c.prepare_cached(&row_insert_sql(table))?.execute(rusqlite::params_from_iter(&values))?;Ok(())
}
/// Preserve row order while reusing a statement for adjacent rows in a table.
#[cfg(feature="replication")]
pub(crate) fn put_user_rows(c:&Connection,schema:&Schema,rows:&[RowState])->Result<()> {
    for group in rows.chunk_by(|a,b|a.table==b.table) {
        let table=schema.tables.iter().find(|table|table.id==group[0].table).ok_or(Error::SchemaMismatch)?;
        let mut statement=c.prepare_cached(&row_insert_sql(table))?;
        for row in group {let values=row_values(table,row)?;statement.execute(rusqlite::params_from_iter(&values))?;}
    }Ok(())
}
#[cfg(feature="replication")]
pub(crate) fn delete_user_rows(c:&Connection,schema:&Schema,rows:&[RowState])->Result<()> {
    for group in rows.chunk_by(|a,b|a.table==b.table) {
        let table=schema.tables.iter().find(|table|table.id==group[0].table).ok_or(Error::SchemaMismatch)?;
        let predicate=pk_columns(table).iter().map(|column|format!("{} IS ?",quote(&column.name))).collect::<Vec<_>>().join(" AND ");
        let mut statement=c.prepare_cached(&format!("DELETE FROM {} WHERE {predicate}",quote(&table.name)))?;
        for row in group {statement.execute(rusqlite::params_from_iter(&row.key))?;}
    }Ok(())
}

/// Table named by a plain `CREATE [UNIQUE] INDEX [IF NOT EXISTS] <name> ON
/// [<schema>.]<table> (...)` statement. Quoted identifiers and comments are
/// honored. Returns None when the shape is unrecognized; callers keep such
/// indexes and let SQLite validate them on adopt.
pub(crate) fn index_table(sql: &str) -> Option<String> {
    enum T {
        Bare(String),
        Name(String),
        Dot,
    }
    let b = sql.as_bytes();
    let mut toks: Vec<T> = Vec::new();
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        if c == b'\'' {
            i += 1;
            while i < b.len() {
                if b[i] == b'\'' {
                    i += 1;
                    if i < b.len() && b[i] == b'\'' {
                        i += 1;
                    } else {
                        break;
                    }
                } else {
                    i += 1;
                }
            }
        } else if c == b'"' || c == b'`' {
            i += 1;
            let mut raw: Vec<u8> = Vec::new();
            while i < b.len() {
                if b[i] == c {
                    if i + 1 < b.len() && b[i + 1] == c {
                        raw.push(c);
                        i += 2;
                    } else {
                        i += 1;
                        break;
                    }
                } else {
                    raw.push(b[i]);
                    i += 1;
                }
            }
            toks.push(T::Name(String::from_utf8_lossy(&raw).into_owned()));
        } else if c == b'[' {
            i += 1;
            let start = i;
            while i < b.len() && b[i] != b']' {
                i += 1;
            }
            toks.push(T::Name(
                String::from_utf8_lossy(&b[start..i]).into_owned(),
            ));
            i = (i + 1).min(b.len());
        } else if c == b'-' && b[i..].starts_with(b"--") {
            while i < b.len() && b[i] != b'\n' {
                i += 1;
            }
        } else if c == b'/' && i + 1 < b.len() && b[i + 1] == b'*' {
            i += 2;
            while i + 1 < b.len() && !(b[i] == b'*' && b[i + 1] == b'/') {
                i += 1;
            }
            i = (i + 2).min(b.len());
        } else if c.is_ascii_alphabetic() || c == b'_' {
            let start = i;
            while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_' || b[i] == b'$') {
                i += 1;
            }
            toks.push(T::Bare(sql[start..i].to_ascii_uppercase()));
        } else if c == b'.' {
            toks.push(T::Dot);
            i += 1;
        } else {
            i += 1;
        }
    }
    let mut k = 0;
    let eat = |toks: &[T], k: &mut usize, word: &str| -> bool {
        if matches!(&toks.get(*k), Some(T::Bare(w)) if w == word) {
            *k += 1;
            true
        } else {
            false
        }
    };
    if !eat(&toks, &mut k, "CREATE") {
        return None;
    }
    eat(&toks, &mut k, "UNIQUE");
    if !eat(&toks, &mut k, "INDEX") {
        return None;
    }
    if eat(&toks, &mut k, "IF") {
        if !eat(&toks, &mut k, "NOT") || !eat(&toks, &mut k, "EXISTS") {
            return None;
        }
    }
    match toks.get(k) {
        Some(T::Bare(_)) | Some(T::Name(_)) => k += 1,
        _ => return None,
    }
    if !eat(&toks, &mut k, "ON") {
        return None;
    }
    let first = match toks.get(k) {
        Some(T::Bare(n)) | Some(T::Name(n)) => n.clone(),
        _ => return None,
    };
    k += 1;
    if matches!(toks.get(k), Some(T::Dot)) {
        k += 1;
        return match toks.get(k) {
            Some(T::Bare(n)) | Some(T::Name(n)) => Some(n.clone()),
            _ => None,
        };
    }
    // Bare tokens were uppercased for keyword matching; table names compare
    // case-insensitively at use sites.
    Some(first)
}

// Ignore quoted values/identifiers and comments when detecting unsupported
// schema features. SQLite remains the SQL parser and validates syntax.
pub(crate) fn keywords(sql: &str) -> Vec<String> {
    let b = sql.as_bytes();
    let mut i = 0;
    let mut out = Vec::new();
    while i < b.len() {
        if b[i] == b'\'' || b[i] == b'"' || b[i] == b'`' || b[i] == b'[' {
            let end = if b[i] == b'[' { b']' } else { b[i] };
            i += 1;
            while i < b.len() {
                if b[i] == end {
                    i += 1;
                    if i < b.len() && b[i] == end {
                        i += 1;
                    } else {
                        break;
                    }
                } else {
                    i += 1;
                }
            }
        } else if b[i..].starts_with(b"--") {
            while i < b.len() && b[i] != b'\n' {
                i += 1;
            }
        } else if b[i..].starts_with(b"/*") {
            i += 2;
            while i + 1 < b.len() && !b[i..].starts_with(b"*/") {
                i += 1;
            }
            i = (i + 2).min(b.len());
        } else if b[i].is_ascii_alphabetic() {
            let start = i;
            while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_') {
                i += 1;
            }
            out.push(sql[start..i].to_ascii_uppercase());
        } else {
            i += 1;
        }
    }
    out
}

#[cfg(test)]
mod index_table_tests {
    use super::index_table;

    #[test]
    fn names_index_tables_through_quoting_and_comments() {
        assert_eq!(
            index_table("CREATE INDEX items_n ON items(n)").as_deref(),
            Some("ITEMS")
        );
        assert_eq!(
            index_table("create index \"my idx\" on \"my table\" (a)").as_deref(),
            Some("my table")
        );
        assert_eq!(
            index_table("CREATE INDEX IF NOT EXISTS i ON main.items(a)").as_deref(),
            Some("ITEMS")
        );
        assert_eq!(
            index_table("CREATE UNIQUE INDEX [i] ON [items](a)").as_deref(),
            Some("items")
        );
        assert_eq!(
            index_table("-- leading comment\nCREATE INDEX i ON items(a) /* tail */").as_deref(),
            Some("ITEMS")
        );
        // Quoted keywords are identifiers, not syntax.
        assert_eq!(
            index_table("CREATE INDEX \"on\" ON \"where\"(a)").as_deref(),
            Some("where")
        );
    }

    #[test]
    fn rejects_non_index_shapes() {
        assert_eq!(index_table("CREATE TABLE t(a)"), None);
        assert_eq!(index_table("DROP INDEX i"), None);
        assert_eq!(index_table("CREATE INDEX"), None);
        assert_eq!(index_table("CREATE INDEX i"), None);
        assert_eq!(index_table("not sql at all"), None);
        assert_eq!(index_table(""), None);
    }
}
