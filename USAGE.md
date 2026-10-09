# Murmur query usage

How to read data from Murmur with the managed typed-record Go API.
There is no SQL: you define Go structs, open the database with
`Config.Tables`, get a handle with `TableOf`, and query through `Get`,
`Where`, compiled queries, and joins.

Every example below was run against a live embedded node (see
[examples/basic](examples/basic/main.go) for a runnable starter). All
query results are detached deep copies: mutating a returned record never
affects the database.

## 0. Setup: define tables, open, get handles

```go
import (
    "context"

    "github.com/marcgauthier/murmur"
    "github.com/marcgauthier/murmur/ids"
    "github.com/marcgauthier/murmur/rime"
)

type Author struct {
    ID   ids.RowID `rime:"primary"`
    Name string    `rime:"index"`
    Age  int       `rime:"ordered"`
}

type Book struct {
    ID     ids.RowID `rime:"primary"`
    Title  string    `rime:"prefix"`
    Author string    `rime:"index"` // author name; the join key below
    Pages  int       `rime:"ordered"`
}

authorsDef, err := murmur.Define[Author]("authors", 101, murmur.RecordOptions{
    PrimaryField: "ID",
    FieldIDs:     map[string]uint32{"ID": 1, "Name": 2, "Age": 3},
})
if err != nil { return err }
booksDef, err := murmur.Define[Book]("books", 102, murmur.RecordOptions{
    PrimaryField: "ID",
    FieldIDs:     map[string]uint32{"ID": 1, "Title": 2, "Author": 3, "Pages": 4},
})
if err != nil { return err }

db, err := murmur.Open(ctx, murmur.Config{
    Path:          dir,
    NodeID:        nodeID,
    Schema:        murmur.SchemaConfig{Version: 1},
    Tables:        []murmur.TableDefinition{authorsDef, booksDef},
    Spool:         murmur.DefaultSpoolConfig(),
    Encryption:    murmur.EncryptionConfig{Key: key32, KeyID: "app-key-1"},
    OriginSigning: murmur.OriginSigningConfig{PrivateKey: priv, TrustedKeys: registry},
})
if err != nil { return err }
defer db.Close()

authors, err := murmur.TableOf[Author](db, "authors")
if err != nil { return err }
books, err := murmur.TableOf[Book](db, "books")
if err != nil { return err }
```

Rules for definitions: the table name and nonzero table ID are stable
durable identities (never reuse an ID for another table), the primary
field must carry `rime:"primary"` and be a 16-byte `ids.RowID`, and
every persisted field needs a stable ID in `FieldIDs`. Writes go
through `db.WriteTxContext` (see [examples/transactions](examples/transactions/main.go));
everything below is the read side.

## 1. Get one record by ID

```go
author, err := authors.Get(id)
if errors.Is(err, rime.ErrNotFound) {
    // no such row
}
```

`Get` reads the latest published snapshot and returns `rime.ErrNotFound`
when the key is absent.

## 2. Get all items

```go
all, err := authors.Where().Find() // no predicates: every row
```

`Where()` with no expressions matches all rows; `Find` returns the slice.

## 3. Filter with `Where`

Build a typed field handle once, then use its predicate methods:

```go
name := murmur.FieldOf[Author, string](authors, "Name")
age  := murmur.NumericFieldOf[Author, int](authors, "Age")

// Equality.
anns, err := authors.Where(name.Eq("ann")).Find()
others, err := authors.Where(name.Ne("ann")).Find()
some, err := authors.Where(name.In("ann", "bob")).Find()
rest, err := authors.Where(name.NotIn("ann")).Find()

// Ordered range comparisons (numeric, string, and other ordered fields).
adults, err := authors.Where(age.Ge(18)).Find()         // >, >=, <, <=
mid, err := authors.Where(age.Between(30, 45)).Find()   // inclusive range

// Repeated Where calls AND their predicates together.
annsOver30, err := authors.Where(name.Eq("ann")).Where(age.Ge(30)).Find()
```

String fields add match operators (no tokenized full-text search):

```go
title := murmur.StringFieldOf[Book](books, "Title")

prefix, err := books.Where(title.StartsWith("replication")).Find()
suffix, err := books.Where(title.EndsWith("storage")).Find()
substr, err := books.Where(title.Contains("repli")).Find()
wild, err := books.Where(title.Like("replication %")).Find() // % and _ wildcards
```

Compose predicates explicitly with `rime.And`, `rime.Or`, and `rime.Not`:

```go
either, err := authors.Where(rime.Or(name.Eq("ann"), name.Eq("bob"))).Find()
both, err := authors.Where(rime.And(name.Eq("ann"), age.Ge(30))).Find()
notAnn, err := authors.Where(rime.Not(name.Eq("ann"))).Find()
```

Field handles panic on unknown field names or Go type mismatches, so
create them once (they are cheap to reuse, not per query).

Index tags guide performance, not availability: `rime:"index"` for
equality, `rime:"ordered"` for ranges/ordering/extrema, `rime:"prefix"`
for `StartsWith` (and trailing-`%` `Like`). Untagged fields still work
via scans.

## 4. Order and paginate

```go
pages := murmur.NumericFieldOf[Book, int](books, "Pages")

asc, err := books.Where().OrderByAsc(pages).Find()
desc, err := books.Where().OrderByDesc(pages).Find()

page2, err := books.Where().OrderByAsc(pages).Limit(20).Offset(20).Find()
```

`OrderByAsc`/`OrderByDesc` take any field handle (`FieldOf`,
`NumericFieldOf`, `StringFieldOf` all support ordering).

## 5. Other result shapes: `First`, `Count`, `Exists`, `Each`

```go
first, err := books.Where().OrderByAsc(pages).First() // rime.ErrNotFound if empty

n, err := books.Where(title.StartsWith("replication")).Count()
ok, err := books.Where(title.Eq("encrypted storage")).Exists()

// Visit rows without building a result slice.
err = books.Where().Each(func(b *Book) error {
    fmt.Println(b.Title)
    return nil
})
```

## 6. Aggregates

```go
vals, err := books.Where().Aggregate(
    rime.Count[Book](),
    rime.SumOf(pages),
    rime.AvgOf(pages),
    rime.MinOf(pages),
    rime.MaxOf(pages),
)
// vals[0] is int (COUNT); SUM/AVG are float64; empty MIN/MAX is nil.
```

Aggregates are read-only and return scalar values in descriptor order.
Use `murmur.NumericFieldOf` to build the numeric field handle.

## 7. Group by

```go
bookAuthor := murmur.FieldOf[Book, string](books, "Author")

groups, err := books.Where().GroupBy(bookAuthor).Aggregate(rime.Count[Book]())
for _, g := range groups {
    fmt.Printf("%v -> %v\n", g.Keys, g.Values) // g.Names holds field names
}
```

`GroupBy` accepts any field handles; each `GroupRow` carries `Names`,
`Keys`, and per-group aggregate `Values`.

## 8. Reusable compiled queries

`Compile` builds a parameterized query once; bind values per execution.
Supported parameter types: `string`, `int`, `int64`, `uint`, `uint64`,
`float64`.

```go
byName := authors.Compile(name.Eq(rime.Param[string]()))

n, err := byName.Count("ann")
rows, err := byName.Find("bob")
```

Parameter count and types are validated at execution. Express
parameterized ranges with `Ge`/`Le`, not `Between`. See
[examples/prepared-statements](examples/prepared-statements/main.go).

## 9. Joins

Joins run on one pinned read snapshot so both sides see a consistent
view. Open it with `db.ReadTxContext`, join with key handles from
`FieldOf` (both keys must share the same comparable Go type), and close
the snapshot promptly so old versions can be reclaimed.

```go
rtx, err := db.ReadTxContext(ctx)
if err != nil { return err }
defer rtx.Close()

bookAuthor := murmur.FieldOf[Book, string](books, "Author")

pairs, err := murmur.InnerJoinReadTx(rtx, authors, name, books, bookAuthor)
for _, row := range pairs {
    fmt.Printf("%s wrote %s\n", row.Left.Name, row.Right.Title)
}

outer, err := murmur.LeftJoinReadTx(rtx, authors, name, books, bookAuthor)
for _, row := range outer {
    if row.Right == nil {
        fmt.Printf("%s wrote nothing\n", row.Left.Name)
        continue
    }
    fmt.Printf("%s wrote %s\n", row.Left.Name, row.Right.Title)
}
```

Results are `RecordJoinRow[A, B]{Left, Right}` with detached records.
`LeftJoinReadTx` keeps left rows with no match and sets `Right` to nil.
Right/full joins are not provided.

## 10. Pinned snapshot reads

Use the same `RecordReadTx` for repeatable reads: every call inside it
sees the same commit, unaffected by concurrent writers.

```go
rtx, err := db.ReadTxContext(ctx)
if err != nil { return err }
defer rtx.Close()

pinned, err := authors.GetRead(rtx, id)

q, err := authors.WhereReadTx(rtx, name.Eq("ann"))
rows, err := q.OrderByAsc(age).Find()
```

Snapshot IDs are local to one node and one materializer generation, not
cluster-wide timestamps.

## 11. Read inside a write transaction

`GetTx` and `WhereTx` see the transaction's snapshot plus its own staged
writes (read-your-writes):

```go
err := db.WriteTxContext(ctx, func(tx *murmur.Tx) error {
    staged, err := authors.GetTx(tx, id)
    if err != nil { return err }
    staged.Age++
    if err := authors.Save(tx, staged); err != nil { return err }

    q, err := authors.WhereTx(tx, age.Ge(40))
    if err != nil { return err }
    n, err := q.Count() // includes the staged update above
    fmt.Println("authors age>=40:", n)
    return err
})
```

## Notes and limits

- Reads return independent deep copies; writes never happen through a
  query object (`RecordQuery` has no mutation methods).
- Missing single-row reads report `rime.ErrNotFound`; check with
  `errors.Is`.
- There is no SQL layer and no full-text ranking: prefix/suffix/
  substring/`LIKE` matching is literal string matching only.
- Reactive live queries use `RecordTable.Subscribe`, which streams an
  initial snapshot plus row diffs; see
  [examples/subscriptions](examples/subscriptions/main.go) and
  [query and search](architecture/query-and-search.md#reactive-query-subscriptions).

## Further reading

- [RIME usage and syntax](rime/USAGE.md): full predicate, aggregate,
  view, and constraint reference for the underlying engine.
- [API and configuration](architecture/api-and-configuration.md#native-typed-record-api-status):
  typed facade status, scopes, and durability/commit semantics.
- [Examples index](examples/README.md): runnable programs from single
  node to 3-node mesh.
