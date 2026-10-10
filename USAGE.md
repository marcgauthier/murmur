# Murmur application usage

Register Go models once, then use the same CRUD functions for every collection.
Murmur stores native records through RIME and commits to encrypted Spool before
publishing them. Replication uses the existing QUIC/TLS pipeline.

Runnable starters: [basic](examples/basic/main.go) and
[transactions](examples/transactions/main.go).

## Register models at startup

```go
import (
    "context"
    "errors"
    "time"

    "github.com/marcgauthier/murmur"
    "github.com/marcgauthier/murmur/ids"
    "github.com/marcgauthier/murmur/q"
    "github.com/marcgauthier/murmur/rime"
)

type Device struct {
    ID       ids.RowID `rime:"ID"`
    Name     string    `rime:"primary"`
    Hostname string    `rime:"index,prefix"`
    Site     string    `rime:"index"`
    Status   int       `rime:"ordered"`
    Online   bool
    LastSeen time.Time `rime:"index,ordered"`
}

db, err := murmur.Open(ctx, murmur.Config{
    Path:          dir,
    NodeID:        nodeID,
    Models:        []any{Device{}},
    Schema:        murmur.SchemaConfig{Version: 1},
    Spool:         murmur.DefaultSpoolConfig(),
    Encryption:    murmur.EncryptionConfig{Key: key32, KeyID: "app-key-1"},
    OriginSigning: signing,
})
if err != nil { return err }
defer db.Close()
```

The application supplies its directory, persistent node identity, 32-byte
storage key and origin signing configuration. Origin signing is mandatory even
for an offline database; see [origin signatures](architecture/origin-signatures.md).
Replication configuration is optional for a single local node.

Each model must be a named struct with an exported `ID ids.RowID` carrying
`rime:"ID"`. Registration fails if identity is missing or invalid. The legacy
`ID ids.RowID` tagged `rime:"primary"` is also accepted and has no business key.
Index tags are optional: `index` accelerates equality, `ordered` accelerates
ranges and ordering, and `prefix` accelerates string-prefix queries. Exported fields persist by default; `rime:"-"` ignores a
field. `rime:"name=Column"` pins a logical column name across Go field renames.

Table names default to struct names. Durable table and field IDs are generated
from names, independently of declaration and model order. Top-level IDs use the
logical column name; nested IDs use `nested:` plus the logical field name in
their descriptor scope. Renaming a durable identity requires explicit overrides.
Registration rejects collisions, reserved IDs and unsupported field types.

## Create and automatic IDs

```go
device := Device{
    Name:     "router-01",
    Hostname: "router-01.example.net",
    Site:     "OTT",
    Status:   1,
}
err = db.InsertItem(ctx, &device)
// device.ID now contains its generated UUIDv5.
```

A separate field tagged `primary` is the business key. At most one is allowed;
it may be a string, integer or 16-byte UUID, including named equivalents. When
ID is unset, Murmur derives UUIDv5 from the stable model name and the canonical
encoded business-key value. An unset business key fails generation. The
business key is indexed automatically and immutable in local updates and
replacements. Keep model identity rules consistent across replicas.

Without a separate business key, insertion generates UUIDv4:

```go
type Event struct {
    ID      ids.RowID `rime:"primary"` // legacy identity form
    Message string
}
// Include Event{} in Config.Models before using it.
event := Event{Message: "device started"}
err = db.InsertItem(ctx, &event)
```

A supplied ID is authoritative, including one from `murmur.NewRowID()`. Different
explicit IDs may share a business key; it is not a distributed uniqueness
constraint. A duplicate row ID returns `rime.ErrAlreadyExists`. Generated IDs
are assigned before staging and remain on caller records after rollback or an
uncertain commit; use the existing transaction receipt API to resolve uncertain
commit outcomes.

## Read, update and delete

```go
// Either an ID or a nonzero business key identifies the row.
loaded := Device{Name: "router-01"}
err = db.GetItem(ctx, &loaded)
if errors.Is(err, rime.ErrNotFound) {
    // Handle the missing device.
}

err = db.Update(ctx, &loaded,
    murmur.Set("Status", 2),
    murmur.Set("Online", false),
)

// Use a map when the application builds changes dynamically.
err = db.UpdateFields(ctx, &loaded, map[string]any{"Hostname": "router-01"})

// Replace every stored field; fail if the row is absent.
loaded.Hostname = "new-router.example.net"
err = db.UpdateItem(ctx, &loaded)

// Insert or replace a complete record.
err = db.SaveItem(ctx, &loaded)

err = db.DeleteItem(ctx, &loaded)
```

Reads, updates and deletes derive an unset ID from the supplied business key;
they never generate a random ID. Use a non-nil struct pointer for reads and
for automatic ID generation. With an explicit ID, insertion and saving also
accept struct values. Reads return detached records.

`Update` accepts explicit `murmur.Set(field, value)` assignments and validates
all assignments before staging. Duplicate field names and zero-value
`Assignment` objects fail. With no assignments it follows the empty
`UpdateFields` no-op behavior. For a non-empty update, the row must exist.

`UpdateFields` takes Go field names. Map presence selects fields, including
`0`, `false`, empty strings and nil. Numeric widths convert with range checks.
Unknown fields, identity/business-key changes and direct assignments to
counter/set/extrema fields fail. Use the counter, set, and extrema operations
for those fields. After a successful partial update, the supplied pointer reflects
the requested values; transaction rollback does not undo changes to caller
objects.

## Counters, sets, and extrema

Fields with merge policies converge across replicas instead of last-write-wins.
Declare them with `Model` options, then mutate them with the matching operation;
the item supplies the row identity and is never updated in place:

```go
definition, err := murmur.Model[Stats](murmur.ModelOptions{
    RecordOptions: murmur.RecordOptions{MergePolicies: map[string]murmur.RecordMergePolicy{
        "Count": murmur.RecordMergeCounter,
        "Tags":  murmur.RecordMergeORSet,
        "High":  murmur.RecordMergeMax,
    }},
})
// Include definition in Config.Models before using it.

err = db.CounterAdd(ctx, &Stats{ID: id}, "Count", 5)
err = db.SetAdd(ctx, &Stats{ID: id}, "Tags", "edge")
err = db.SetRemove(ctx, &Stats{ID: id}, "Tags", "stale")
err = db.Max(ctx, &Stats{ID: id}, "High", 99)
```

`Tx` offers the same five operations (`CounterAdd`, `SetAdd`, `SetRemove`,
`Max`, `Min`) without context arguments. Counters accept any delta with
`int64` overflow checks; a zero delta is a no-op. Set operations take strings;
re-adding a present value and removing an absent one are no-ops. Extrema
values coerce to the field type with the usual range checks. A missing row
reports `rime.ErrNotFound`. Re-read with `GetItem` for the converged value.

## Query into application slices

Infer the collection from the destination:

```go
var results []Device
err = db.Find(ctx, &results, q.Eq("Site", "OTT"), q.Gte("Status", 2))

var first Device
err = db.FindOne(ctx, &first, q.Eq("Name", "router-01"))
count, err := db.Count(ctx, Device{}, q.Eq("Site", "OTT"))
exists, err := db.Exists(ctx, Device{}, q.Eq("Site", "OTT"))
```

`Find` accepts `*[]T` and `*[]*T`; `FindOne` accepts `*T` and returns
`rime.ErrNotFound` when empty. Results are detached and destinations are changed
only on success. `Count` and `Exists` require a model exemplar because their
scalar results cannot identify a collection. Invalid destinations and ambiguous
or unregistered model types return errors.

Use the query builder for ordering, pagination, and `Explain`:

```go
err = db.Query(Device{},
    q.Eq("Site", "OTT"),
    q.Gte("Status", 2),
).WithContext(ctx).OrderBy("Hostname").Limit(100).FindInto(&results)

err = db.Query(Device{}, q.Eq("Site", "OTT")).FirstInto(&first)
plan, err := db.Query(Device{}, q.Eq("Site", "OTT")).Explain()
```

Match multiple filters with AND. Compose `q.And`, `q.Or`, and `q.Not` explicitly;
leaf comparisons are `Eq`, `Ne`, `Gt`, `Gte`, `Lt`, `Lte`, and `In`.
`NotIn` negates membership; an empty list matches every row. `Between` combines
inclusive lower and upper bounds; reversed bounds match no rows.

```go
err = db.Find(ctx, &results,
    q.StartsWith("Hostname", "router-"),
    q.EndsWith("Hostname", ".net"),
    q.Contains("Hostname", "01"),
    q.Like("Hostname", "router-0_.net"),
    q.Between("Status", 2, 5),
    q.NotIn("Site", "LAB", "DEV"),
    q.Gte("LastSeen", time.Now().Add(-time.Hour)),
)
```

String matchers accept strings and named string types. `Like` uses RIME's
byte-oriented `%` (any run) and `_` (one byte) wildcards; it is not regex.
Prefix matchers and literal-prefix `Like` patterns can use `prefix` indexes.
Suffix and substring matchers use filtered scans.

`OrderByDescending`, `Limit` and `Offset` support pagination. An empty query
matches all rows. `FindInto` accepts `*[]T` and `*[]*T`, replaces the destination
on success, and returns detached records. `FirstInto` returns
`rime.ErrNotFound` when empty. Invalid fields, comparisons or destination types
return errors. Query builders return independent values.

Match field names from the Go struct. Equality and range predicates retain RIME
index planning; unindexed fields use scans. The simple matchers cover built-in
string/bool/numeric fields, their named equivalents, 16-byte identities such
as `ids.RowID` and `rime.UUID`, and exact `time.Time` fields. Bool and identity
fields support equality/membership and sorting, with no range predicates.
Numeric conversions retain range checks. Timestamp comparisons use wall-clock
instants: zones and monotonic clock components do not affect equality. Timestamp
hash and ordered indexes use the same normalization as predicates, including
after reopen. Records and their existing timestamp encoding are unchanged.
Nested paths, optional-value predicates, slice membership, and regex are not
supported by `q`. `Explain` reports the selected plan without executing the
query.

## Aggregates and group-by

Run read-only aggregates over query matches with `q` descriptors:

```go
values, err := db.Query(Device{}, q.Eq("Site", "OTT")).Aggregate(
    q.Count(), q.Sum("Status"), q.Avg("Status"), q.Min("Hostname"), q.Max("Hostname"),
)

rows, err := db.Query(Device{}).GroupBy("Site").Aggregate(q.Count(), q.Sum("Status"))
for _, row := range rows {
    fmt.Println(row.Names, row.Keys, row.Values)
}
```

`Sum` and `Avg` return `float64` and need built-in numeric fields; `Count`
returns `int`; `Min` and `Max` return native values (`nil` when nothing
matches) and need string or built-in numeric fields. Aggregates respect the
query's filters, ordering, and pagination. `GroupBy` keys must be comparable
built-in scalar, identity, or timestamp fields; each `ItemGroupRow` carries
the key names, key values, and one value per aggregation.

## Joins

Joins run on one pinned read snapshot so both sides see a consistent view.
Both keys must share the same comparable Go type:

```go
rtx, err := db.ReadTxContext(ctx)
if err != nil { return err }
defer rtx.Close()

pairs, err := rtx.InnerJoin(Author{}, "Name", Book{}, "Author")
for _, row := range pairs {
    author := row.Left.(*Author)
    book := row.Right.(*Book)
    fmt.Printf("%s wrote %s\n", author.Name, book.Title)
}

outer, err := rtx.LeftJoin(Author{}, "Name", Book{}, "Author")
for _, row := range outer {
    if row.Right == nil {
        fmt.Printf("%s wrote nothing\n", row.Left.(*Author).Name)
    }
}
```

`Left` and `Right` hold detached `*T` pointers for their models; `Right` is
`nil` for unmatched left rows. Duplicate keys fan out into one row per pair.
Close the snapshot promptly so old versions can be reclaimed.

## Compiled item queries

Prepare a query once and bind values per execution with `q.Param()`
placeholders:

```go
bySite := db.Query(Device{},
    q.Eq("Site", q.Param()),
    q.Gte("Status", q.Param()),
).OrderBy("Hostname").Limit(100).Compile()

var results []Device
err = bySite.FindInto(&results, "OTT", 2)
count, err := bySite.Count("MTL", 1)
```

Bind arguments positionally in matcher order (depth-first, left to right).
Reusing one placeholder value in several positions binds a single argument
to every position. Each bind coerces arguments through the same conversions
and range checks as one-shot queries; a wrong argument count or a value the
field cannot hold fails before touching the database. Placeholders work in
comparisons, `In` values, and `Between` bounds; string matchers keep concrete
patterns. Ordering, limit, and offset are fixed at compile time, while `In`,
`InRead`, and `WithContext` return bound copies for transactions, snapshots,
and cancellation. Compiling without placeholders yields a reusable fixed
query. Running a placeholder query without `Compile` reports an unbound
parameter instead of executing.

## Subscriptions

Subscribe to a filtered query for an initial snapshot plus coalesced updates
after durable local or remote changes:

```go
sub, err := db.Subscribe(ctx, Device{}, murmur.ItemSubscriptionOptions{},
    q.Eq("Site", "OTT"))
if err != nil { return err }
defer sub.Close()

for event := range sub.Events() {
    if event.Err != nil {
        return event.Err // reset: resubscribe for a fresh snapshot
    }
    for _, row := range event.Rows {
        device := row.(*Device)
        _ = device
    }
    for _, change := range event.Changes {
        // change.Type is added, updated, or removed; change.Key is the row ID.
        _ = change
    }
}
```

`Rows` holds detached `*T` records; `Changes` carries the canonical
primary-key diff since the previous event (`Row` is nil for removals).
`EventInitial` starts the stream, `EventUpdate` carries changes, and
`EventReset` ends it when the subscription overflows, the generation turns
over, or the database closes. `ResumeCursor` replays the latest snapshot in a
new subscription while the same open epoch and generation remain active;
stale tokens report `ErrSubscriptionExpired`. Placeholders are not accepted
in subscription filters.

## Transactions and batches

```go
err = db.Transaction(ctx, func(tx *murmur.Tx) error {
    if err := tx.InsertItem(&Device{Name: "router-02", Site: "OTT"}); err != nil {
        return err
    }
    return tx.Update(&Device{Name: "router-01"}, murmur.Set("Status", 3))
})

devices := []Device{
    {Name: "router-03", Site: "OTT"},
    {Name: "router-04", Site: "MTL"},
}
err = db.InsertMany(ctx, &devices)
// Every element now has its ID.
devices[0].Status = 2
err = db.UpdateMany(ctx, &devices) // all rows must exist
err = db.SaveMany(ctx, &devices)   // upsert
err = db.DeleteMany(ctx, &devices)
```

A callback error rolls back the transaction. `Tx` has the same item and batch
methods without context arguments. Use `.In(tx)` on a query to see staged
writes. Pinned read snapshots use `db.ReadTxContext`, `snapshot.GetItem`, and
query `.InRead(snapshot)`; close snapshots after use. `Tx` and `RecordReadTx`
also offer `Find`, `FindOne`, `Count`, and `Exists` without context arguments.
Transaction shortcuts see staged writes. Snapshot shortcuts use their captured
model bindings and context. `tx.Update(item, murmur.Set(...))` stages assignments
with the same rules as `db.Update`. Pinned snapshots retain
their original model types across migration.

Batches accept homogeneous `[]T` or `[]*T`, directly or through a slice pointer.
Nil elements fail. Each batch is atomic; failure returns a `*rime.BatchError`
with its operation and input index and preserves preceding successful writes
inside an explicit transaction. Empty batches are no-ops. Transactions are
single-goroutine-use.

## Model options and explicit migrations

Use `Model[T]` when you need typed handles or name/identity overrides:

```go
definition, err := murmur.Model[Device](murmur.ModelOptions{
    Name: "devices",
})
if err != nil { return err }
// Register definition in Config.Models instead of Device{}.
// Models: []any{definition}

// After opening with that definition:
devicesTable, err := murmur.TableOf[Device](db, "devices")
```

Plain exemplars provide the simple API. Definitions from `Model[T]` also provide
`TableOf[T]`; `Define[T]` remains the explicit advanced registration API.
`ModelOptions` embeds `RecordOptions`: `PrimaryField` selects a tagged legacy
identity field, and `FieldIDs`, `MergePolicies`, `Scope`, `Codecs`, and `MaxDepth`
retain their existing meanings. `TableID` and `FieldIDs` preserve an existing
collection's durable identities. `Config.Models` and `Config.Tables` can be
combined; the complete registration set must have unique names and IDs.

First open creates the schema. Later opens retain compatibility checks and do
not implicitly publish additions. Open with the current model set, then
explicitly migrate to the complete new set:

```go
type DeviceV2 struct {
    ID       ids.RowID `rime:"ID"`
    Name     string    `rime:"primary"`
    Hostname string    `rime:"index"`
    Site     string    `rime:"index"`
    Status   int       `rime:"ordered"`
    Online   bool
    Owner    *string // additive field
}
next, err := murmur.Model[DeviceV2](murmur.ModelOptions{Name: "Device"})
if err != nil { return err }
err = db.MigrateModels(ctx, []any{next})
```

Retain every collection in the migration list, including any explicit
`TableDefinition` values. Migrations use the existing manifest publication,
compatibility checks and materializer rebuild. Drops and incompatible changes
are rejected; older bindings preserve unknown durable fields. Old queries
become stale after a rebuild; build fresh queries for the new model set.

## Advanced typed API

The following examples retain explicit schema control and typed RIME queries.
Their collection definitions are independent of the Device examples above.

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
