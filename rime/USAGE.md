# RIME usage and syntax reference

RIME stores typed Go records in memory and exposes query and mutation APIs
directly in Go. This document describes the current public API. RIME has no
built-in persistence or replication; those belong to the embedding application.

```go
type Device struct {
	ID       string `rime:"primary"`
	Hostname string `rime:"unique,prefix"`
	Site     string `rime:"index"`
	Status   int    `rime:"ordered"`
	Active   bool
}

db := rime.New()
defer db.Close()
devices, err := rime.Register[Device](db)
if err != nil { return err }
if err := devices.Insert(&Device{ID:"d1", Site:"OTT"}); err != nil { return err }
```

## Schema and registration

`Register[T](db, options...)` creates a table and returns `*Table[T]`. `T` must
be a struct with exactly one supported primary key. Its name is the default
table name; `WithTableName` overrides it. Exported fields participate in schema
discovery. The primary key is immutable after insertion. Common tags are:

| Tag | Meaning |
| --- | --- |
| `primary` | Primary key (one required) |
| `uuid5` | Generate UUIDv5 when the primary value is zero |
| `unique` | Enforce uniqueness |
| `index` | Equality index |
| `ordered` | Range, ordering and extrema index |
| `prefix` | Prefix index for strings |
| `nullable` | Legacy scalar zero-value NULL behavior |
| `notnull` | Require a pointer or `Optional[T]` field to be present |
| `default=value` | Fill an absent pointer or `Optional[T]` on insert/save |
| `fk=Table.Field` | Foreign key declaration |
| `name=Column` | Logical field name override |
| `-` | Ignore the field |

Options: `WithTableName`, `WithTableShards`, `WithCompound`, `WithCheck`,
`WithForeignKey`, `WithTableForeignKeys`, and `WithCloner`. Database options
include `WithShardCount`, `WithMaxResults`, `WithMaxScan`, `WithMaxMutations`,
`WithMaxTxAge`, `WithGCInterval`, `WithEventQueueSize`,
`WithEventDropOldest`, and `WithForeignKeys`. Zero limit values mean unlimited.
Foreign-key enforcement is disabled by default. RIME recursively copies
exported record fields, including slices, maps, arrays and pointers, when it
stores an input and when it creates an updated version. Implement
`Clone() *T` or use `WithCloner` when a record needs specialized copying,
including mutable state held in unexported fields. Published values returned by
reads are immutable and must not be modified by the caller. `CloneRecord[T]`
returns an independent mutable copy using the same recursive ownership rules;
it returns nil for nil input and honors `Clone() *T` when implemented.

Use `rime.Optional[T]`, `rime.Some(value)`, and `rime.None[T]()` when a field
must distinguish absence from a legitimate zero value. Ordinary scalar fields
are always present, so `0`, `false`, and `""` are data and are not replaced by
defaults. Pointer fields also express absence; defaults apply only when the
pointer is nil. The `nullable` tag retains its older zero-as-NULL behavior for
existing RIME schemas.

## Table handles, transactions, and contexts

The simplest calls create a fresh automatic write transaction or read the
latest snapshot:

```go
device, err := devices.Get("d1")
err = devices.Upsert(&Device{ID:"d1", Site:"MTL"})
err = devices.Insert(&Device{ID:"d2"}) // ErrAlreadyExists if key is present
err = devices.Update("d1", func(d *Device) error { d.Active = true; return nil })
err = devices.Delete("d1")
device, found, err := devices.Lookup("d1") // absence is found=false, err=nil
```

`Upsert` inserts or replaces, `Insert` fails for an existing key, and `Update`
edits a private copy of an existing record. `Get` returns `ErrNotFound` when
absent; `Lookup` is the non-error absence form. `InsertMany`, `UpsertMany`, and
`DeleteMany` apply an atomic batch. A failing batch returns `*BatchError` with
the failing operation and input index; its writes are discarded without
removing earlier changes in an explicit transaction.

Bind a handle to a transaction with `table.In(tx)` and to cancellation/deadline
with `table.WithContext(ctx)`. Both return a `BoundTable[T]`; bind both as a
chain when needed. Bound handles are values and do not mutate the base table.
The same binding methods are available on queries, compiled queries, and grouped
queries. `nil` transaction means automatic transaction behavior.

```go
err := db.WriteTx(func(tx *rime.Tx) error {
    if err := devices.In(tx).Insert(&Device{ID:"d3"}); err != nil { return err }
    return devices.In(tx).Update("d1", func(d *Device) error {
        d.Site = "YOW"
        return nil
    })
})

snapshot := db.ReadTx()
defer snapshot.Close()
d, err := devices.In(snapshot).Get("d1")
```

`ReadTx`, `ReadTxContext`, and `ReadAt(commitID)` open pinned snapshots;
`WriteTx` and `WriteTxContext` run an atomic callback. Callback errors abort.
Close read transactions so history can be reclaimed. A transaction is
single-goroutine-use. Concurrent writes to the same key may return
`ErrConflict`; retry the full transaction when appropriate. `Tx.Snapshot`,
`Tx.Age`, `Tx.Context`, and `Tx.Grow` expose snapshot metadata/capacity.
For host-managed lifetimes, `BeginTx(ctx)` returns an explicit write
transaction. Call `Commit()` to publish it or `Rollback()` to discard it; both
close the transaction. A failed commit also closes it, and its error remains
authoritative.
Persistence adapters can call `PrepareCommit()` on a `BeginTx` transaction.
The token exposes `Changes()` and holds commit order until `Publish()` or
`Abort()`. Materialize `Changes()` and finish durable encoding before
committing the external store. Writes through the transaction fail with
`ErrTxPrepared` while the token is active. Publish only after durable
acceptance; abort only when the adapter knows no durable commit occurred. Use
`defer token.Abort()` to release the reservation on pre-durable errors. A
panic during locked publication returns `*UncertainCommitError` (matching
`ErrDBFaulted`), disables new reads and writes on that RIME DB, and includes
the commit generation and panic cause. Its memory may be partially installed,
so reopen from the durable source of truth. Publication still allocates while
installing changes; the migration preallocation gate remains open.
`Tx.PreparedChanges()` returns a detached view of final staged changes,
coalesced to one entry per table/key with the original base commit ID. Its
`Old` and `New` values are typed `*T` pointers stored as `any`; changes can be
inspected or encoded without changing the transaction. This is a capture view,
not a conflict check or publication reservation, so `WriteTx` can still fail
with `ErrConflict` after capture.

## Queries and fields

`Where(exprs...)` starts a query; no expressions means all rows. Each builder
method returns an independent query value, so a shared base can safely branch:

```go
site := rime.StringFieldOf[Device](devices, "Site")
status := rime.OrderedFieldOf[int](devices, "Status")
active := rime.BoolFieldOf[Device](devices, "Active")

base := devices.Where(site.Eq("OTT"))
firstPage := base.OrderByAsc(status).Limit(20)
inactive := base.Where(active.Eq(false))
rows, err := firstPage.Find()
n, err := inactive.Count()
first, err := base.First() // ErrNotFound if no match
exists, err := base.Exists()
```

Field constructors include `F[T,V]`/`FieldOf[T,V]`, `OF[T,V]`/
`OrderedFieldOf[V]`, `SF[T]`/`StringFieldOf[T]`, and `BF[T]`/
`BoolFieldOf[T]`. The `*From` constructors (`FieldFrom`, `OrderedFieldFrom`,
`StringFieldFrom`, `NumericFieldFrom`, `BoolFieldFrom`) accept direct Go field
accessors and avoid name-based access. Field names must match registered schema
names; invalid names or type mismatches panic when constructing a handle.

`Field` supports `Eq`, `Ne`, `In`, `NotIn`, `IsNull`, and `IsNotNull`.
`OrderedField` also supports `Gt`, `Ge`, `Lt`, `Le`, and `Between`.
`StringField` adds `StartsWith`, `EndsWith`, `Contains`, and `Like` (`%` and
`_` wildcards). `BoolField` uses the comparable operators. Compose predicates
with `And`, `Or`, `Not`, `True[T]`, and `False[T]`. Repeated `Where` calls are
ANDed. `OrderByAsc`/`OrderBy` and `OrderByDesc`/`OrderByDescending` set sort
keys; `Limit` and `Offset` control the result page. `Find`, `First`, `Count`,
and `Exists` execute the query. Query `Update(fn)` and `Delete()` mutate all
matching rows atomically and return affected row counts. `Explain()` gives the
chosen plan description. `Each(func(*T) error)` visits rows without building a
result slice when the plan can provide the requested order directly; callback
errors stop iteration. In-memory sorting and transaction-local write overlays
use the materialized path. Context and transaction bindings are available through
`WithContext` and `In`.

`All()` is shorthand for `Where()` and `table.Count()` / `table.Exists()` are
shortcuts for an unfiltered query. `TableStats`, `DB.Stats`,
`DB.OpenTransactions`, and `DB.DetectLeaks` expose operational counts.

## Compiled queries

`Compile(exprs...)` builds a reusable positional-parameter query. `Param[V]()`
is a typed placeholder; supported types are `string`, `int`, `int64`, `uint`,
`uint64`, and `float64`.

```go
query := devices.Compile(site.Eq(rime.Param[string]()), status.Ge(rime.Param[int]()))
rows, err := query.Find("OTT", 2)
count, err := query.Count("MTL", 1)
plan := query.Explain("OTT", 2)
```

Parameter count and types are checked at execution. Bool parameters and
parameters inside `Between` are unsupported; express parameterized ranges with
`Ge` and `Le`. Compiled query values support `In`, `WithContext`, `Limit`,
`Offset`, `OrderByAsc`, and `OrderByDesc`.

## Aggregates, groups, joins, and projection

`Count[T]`, `SumOf(field)`, `AvgOf(field)`, `MinOf(field)`, and `MaxOf(field)`
create aggregate descriptors. `Query.Aggregate(...)` returns values in the same
order; COUNT is `int`, SUM/AVG are `float64`, and an empty MIN/MAX is `nil`.
`Query.Sum` and `Query.Avg` are scalar conveniences over numeric descriptors.
`GroupBy(fields...).Aggregate(...)` returns `GroupRow` values with `Names`,
`Keys`, and `Values`.

```go
latency := rime.NumericFieldOf[int](devices, "Latency")
values, err := devices.Where(site.Eq("OTT")).Aggregate(
    rime.Count[Device](), rime.AvgOf(latency), rime.MaxOf(latency))
groups, err := devices.Where().GroupBy(site).Aggregate(rime.Count[Device]())
```

`InnerJoin`, `InnerJoinOn`, and `LeftJoinOn` take table handles bound with
`In(tx)` or `WithContext(ctx)`. The two handles must belong to the same
database and cannot carry different non-nil transactions. Join results are
`JoinRow[A,B]{Left,Right}`; `Right` is nil for unmatched left-join rows.
`InnerJoin` takes key functions; `*JoinOn` takes typed `KeyField`s. Optional
filters run after key matches. `JoinPairs` joins already materialized slices.
Right/full joins are not provided.

`Project(query, fn)` maps matching records to a result type; `ProjectContext`
accepts an explicit context. `Map(rows, fn)` maps a slice already in memory.

## Maintained query views

A view saves a Go query and keeps its results ready to read. It lasts until
`view.Close()` or `db.Close()`; recreate definitions when starting an application.
RIME does not persist definitions or results.

```go
activeView, err := rime.NewView("active_devices", devices.Where(active.Eq(true)))
if err != nil { return err }
defer activeView.Close()
snapshot, err := activeView.Snapshot()
if err != nil { return err }
for _, device := range snapshot.Rows {
    // Read immutable device records; the source query is not executed here.
    _ = device
}
_ = snapshot.Commit // Last evaluation's commit; unrelated writes do not advance it.
```

`NewView[T](name, query)` returns `*View[*T]`. It freezes the query builder and
rejects transaction-bound queries. The query context controls initial creation
only. Filters without ordering, limit or positive offset maintain membership
from changed rows; ordered/paginated queries reevaluate once per relevant commit.
Unordered results have unspecified order. Snapshot retrieval copies the result
slice and costs O(result count); it performs no source query or deep record copy.
Treat returned records and all nested values as immutable. An earlier snapshot
remains valid after subsequent writes or closing the view.

`NewComputedView[R](db, name, sources, build)` supports complex Go queries:

```go
// left and right are registered *Table handles belonging to db.
pairs, err := rime.NewComputedView(db, "device_pairs",
    []rime.ViewSource{left, right},
    func(tx *rime.Tx) ([]rime.JoinRow[Device, Device], error) {
        return rime.InnerJoin(left.In(tx), right.In(tx),
            func(d *Device) string { return d.Site },
            func(d *Device) string { return d.Site })
    })
if err != nil { return err }
defer pairs.Close()
```

Callbacks can also use groups, aggregates, `Each`, point reads, and projections.
Bind **every source read** to the supplied read-only transaction. During commit
it sees the latest committed state plus the final pending changes, including
changes made by other writers since the source writer's original snapshot.
Declare all source tables in `sources`; undeclared reads through this transaction
fail with `ErrViewDependency`, including when the callback ignores the error.
Writes through it fail with `ErrTxReadOnly` and invalidate the evaluation.

Callbacks must be pure, return promptly, and honor `tx.Context()`. Do not start
other transactions, call GC, close the database, operate on views, retain `tx`,
mutate returned records, or perform external side effects. Evaluation holds the
commit lock; reentering commit/GC/view lifecycle methods can deadlock. RIME
cannot detect reads through captured unbound table handles or external state;
these do not form valid maintained queries. Results containing nested mutable
data remain subject to the same immutability contract as ordinary reads.

Registration evaluates initial results before returning. Invalid definitions
return `ErrBadView`; duplicate names return `ErrViewExists`; cross-database
sources return `ErrTxDatabase`. Initial evaluation failures return `*ViewError`
and register nothing. View names occupy a separate database-local namespace.

On each relevant commit, successful results or failure states publish atomically
with source records. Unrelated tables do not trigger maintenance. A refresh
error, panic, or result limit makes only that view unavailable; the otherwise
valid source write still commits. `Snapshot` then returns no rows and a
`*ViewError` containing `Name`, attempted `Commit`, and the underlying `Err`.
Use `errors.Is`/`errors.As` to inspect it. Other views keep updating. Source
transaction cancellation still aborts the write and publishes no view changes.

Failed views rebuild on the next relevant commit. `Refresh(ctx)` explicitly
rebuilds at the current commit, including when external callback configuration
has changed; a failed explicit refresh also marks the view unavailable.
Existing query scan, result, and transaction-age limits apply to evaluation,
and custom computed results respect `WithMaxResults`. Incremental filter
maintenance applies `WithMaxScan` to final changed rows; join inputs use the
scan limit and join outputs use the result limit.
`Close()` is idempotent and unregisters maintenance. Later reads/refreshes
return `ErrViewClosed`, or `ErrDBClosed` after database shutdown.

Views expose complete latest results only: there is no historical view API,
additional filter builder, nested view dependency, asynchronous refresh or
incremental join/aggregate maintenance. Query reevaluation adds write latency;
incremental filters avoid rescanning unchanged source records.

## Constraints, hooks, events, and lifecycle

Schema tags and `WithCheck`/`AddCheck` configure checks. Foreign keys use
`WithForeignKey`/`AddForeignKey`, with enforcement controlled by
`WithForeignKeys`, `WithTableForeignKeys`, or `SetForeignKeys`; `ForeignKeys`
lists declarations. Errors such as `ErrUnique`, `ErrNotNull`, `ErrCheck`, and
`ErrForeignKey` identify constraint failures.

Tables support synchronous `BeforeInsert`, `AfterInsert`, `BeforeUpdate`,
`AfterUpdate`, `BeforeDelete`, `AfterDelete`, `BeforeCommit`, `AfterCommit`, and
`AfterSave` hooks. `OnInserted`, `OnUpdated`, `OnDeleted`, and `OnCommitted`
subscribe to asynchronous events. Event queue capacity and drop-oldest behavior
are configured on the DB. Callbacks should be short; blocking event backpressure
can slow writers. Do not mutate records passed to hooks/events or returned by
reads. The database `Close` stops event workers; call `GC`/`GCContext` to reclaim
unneeded versions, or enable periodic collection with `WithGCInterval`.

`UUID`, `ParseUUID`, `NewUUIDv5`, and `TableNamespace` provide UUID helpers.
Common errors include `ErrConflict`, `ErrNotFound`, `ErrAlreadyExists`,
`ErrTxClosed`, `ErrTxReadOnly`, `ErrTxPrepared`, `ErrManagedCommitRequired`,
`ErrSnapshotUnavailable`, `ErrLimitExceeded`, `ErrBadSchema`, `ErrDBClosed`,
and `ErrDBFaulted`; see [errors.go](errors.go).

## Optional code generation

The standard-library-only `rimegen` command generates a typed wrapper and
direct-access field handles for a struct with a `rime:"primary"` field. Run it
from the package containing that struct:

```go
//go:generate go run github.com/marcgauthier/murmur/rime/cmd/rimegen -type Device
```

Then run `go generate ./...`. The generated file exposes `RegisterDevice`,
`DeviceRimeTable`, `Fields`, and typed key methods. Generated files should be
reviewed and committed with their schema source. The generator uses standard
library parsing and formatting only.

## Limits and guarantees

Shard counts partition storage and locking; they do not enable parallel commits.
Commits currently serialize under database-wide coordination. Queries retain a
snapshot for execution and may be constrained by `WithMaxResults` and
`WithMaxScan`; transactions may expire under `WithMaxTxAge`. RIME returns
published record pointers, which callers must treat as immutable, including
nested data. For the storage model and current concurrency guarantees, read
[ARCHITECTURE.md](ARCHITECTURE.md). Qualification and performance material is
indexed in [architecture/README.md](../architecture/README.md).
