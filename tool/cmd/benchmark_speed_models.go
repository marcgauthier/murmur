package cmd

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/q"
)

// 10 distinct, structured record models representing real-world table workloads.

type SpeedUser struct {
	ID        ids.RowID `rime:"primary"`
	TenantID  int64     `rime:"index"`
	Email     string
	Score     int64
	Bio       string
	CreatedAt int64
}

type SpeedDevice struct {
	ID       ids.RowID `rime:"primary"`
	SiteID   int64     `rime:"index"`
	MAC      string
	Status   int32
	IP       string
	LastSeen int64
}

type SpeedOrder struct {
	ID         ids.RowID `rime:"primary"`
	CustomerID int64     `rime:"index"`
	Status     string
	Total      int64
	Notes      string
	CreatedAt  int64
}

type SpeedProduct struct {
	ID          ids.RowID `rime:"primary"`
	CategoryID  int64     `rime:"index"`
	SKU         string
	Price       int64
	Description string
	Stock       int64
}

type SpeedInvoice struct {
	ID        ids.RowID `rime:"primary"`
	AccountID int64     `rime:"index"`
	Number    string
	Amount    int64
	Paid      bool
	DueDate   int64
}

type SpeedPayment struct {
	ID        ids.RowID `rime:"primary"`
	InvoiceID int64     `rime:"index"`
	Method    string
	Amount    int64
	Ref       string
	PaidAt    int64
}

type SpeedLog struct {
	ID        ids.RowID `rime:"primary"`
	Service   string    `rime:"index"`
	Level     int32
	Message   string
	TraceID   string
	Timestamp int64
}

type SpeedEvent struct {
	ID        ids.RowID `rime:"primary"`
	Source    string    `rime:"index"`
	Type      string
	Payload   string
	Priority  int32
	Timestamp int64
}

type SpeedMetric struct {
	ID         ids.RowID `rime:"primary"`
	MetricName string    `rime:"index"`
	Tag        string
	Val        int64
	Step       int32
	Timestamp  int64
}

type SpeedSession struct {
	ID        ids.RowID `rime:"primary"`
	UserID    int64     `rime:"index"`
	Token     string
	IP        string
	UserAgent string
	ExpiresAt int64
}

func makeRowID(tableID uint32, seq int) ids.RowID {
	var id ids.RowID
	binary.BigEndian.PutUint64(id[:8], uint64(tableID))
	binary.BigEndian.PutUint64(id[8:], uint64(seq))
	return id
}

type speedTableRunner interface {
	Name() string
	TableID() uint32
	TableDefinition() (murmur.TableDefinition, error)
	GenerateBatch(startSeq, count int) any
	ExtractIDs(batch any) []ids.RowID
	MutateBatch(batch any)
	SingleItem(seq int) (any, ids.RowID)
	MutateSingle(item any)
	PointQuery(ctx context.Context, db *murmur.DB, id ids.RowID) error
	FilterQuery(ctx context.Context, db *murmur.DB, targetSeq int) (int, error)
	ScanQuery(ctx context.Context, db *murmur.DB, limit int) (int, error)
	Count(ctx context.Context, db *murmur.DB) (int, error)
}

type typedSpeedTable[T any] struct {
	name         string
	tableID      uint32
	makeRow      func(seq int) *T
	mutateRow    func(row *T)
	extractID    func(row *T) ids.RowID
	setID        func(row *T, id ids.RowID)
	filterField  string
	filterVal    func(seq int) any
	emptyModel   T
}

func (t *typedSpeedTable[T]) Name() string     { return t.name }
func (t *typedSpeedTable[T]) TableID() uint32 { return t.tableID }

func (t *typedSpeedTable[T]) TableDefinition() (murmur.TableDefinition, error) {
	return murmur.Model[T](murmur.ModelOptions{
		Name:    t.name,
		TableID: t.tableID,
	})
}

func (t *typedSpeedTable[T]) GenerateBatch(startSeq, count int) any {
	rows := make([]*T, count)
	for i := 0; i < count; i++ {
		rows[i] = t.makeRow(startSeq + i)
	}
	return rows
}

func (t *typedSpeedTable[T]) ExtractIDs(batch any) []ids.RowID {
	rows := batch.([]*T)
	result := make([]ids.RowID, len(rows))
	for i, r := range rows {
		result[i] = t.extractID(r)
	}
	return result
}

func (t *typedSpeedTable[T]) MutateBatch(batch any) {
	rows := batch.([]*T)
	for _, r := range rows {
		t.mutateRow(r)
	}
}

func (t *typedSpeedTable[T]) SingleItem(seq int) (any, ids.RowID) {
	row := t.makeRow(seq)
	return row, t.extractID(row)
}

func (t *typedSpeedTable[T]) MutateSingle(item any) {
	t.mutateRow(item.(*T))
}

func (t *typedSpeedTable[T]) PointQuery(ctx context.Context, db *murmur.DB, id ids.RowID) error {
	probe := new(T)
	t.setID(probe, id)
	return db.GetItem(ctx, probe)
}

func (t *typedSpeedTable[T]) FilterQuery(ctx context.Context, db *murmur.DB, targetSeq int) (int, error) {
	var results []T
	val := t.filterVal(targetSeq)
	err := db.Find(ctx, &results, q.Eq(t.filterField, val))
	return len(results), err
}

func (t *typedSpeedTable[T]) ScanQuery(ctx context.Context, db *murmur.DB, limit int) (int, error) {
	var results []T
	err := db.Query(t.emptyModel).Limit(limit).FindInto(&results)
	return len(results), err
}

func (t *typedSpeedTable[T]) Count(ctx context.Context, db *murmur.DB) (int, error) {
	return db.Count(ctx, t.emptyModel)
}

func getSpeedTableRunners() []speedTableRunner {
	return []speedTableRunner{
		&typedSpeedTable[SpeedUser]{
			name:    "table01_users",
			tableID: 101,
			makeRow: func(seq int) *SpeedUser {
				return &SpeedUser{
					ID:        makeRowID(101, seq),
					TenantID:  int64((seq % 500) + 1),
					Email:     fmt.Sprintf("user_%07d@bench.corp", seq),
					Score:     int64(seq * 3),
					Bio:       "Engineer at enterprise platform operations",
					CreatedAt: 1700000000 + int64(seq),
				}
			},
			mutateRow: func(r *SpeedUser) {
				r.Score += 100
				r.Bio = "Senior Engineer at enterprise platform operations (updated)"
			},
			extractID:   func(r *SpeedUser) ids.RowID { return r.ID },
			setID:       func(r *SpeedUser, id ids.RowID) { r.ID = id },
			filterField: "TenantID",
			filterVal:   func(seq int) any { return int64((seq % 500) + 1) },
		},
		&typedSpeedTable[SpeedDevice]{
			name:    "table02_devices",
			tableID: 102,
			makeRow: func(seq int) *SpeedDevice {
				return &SpeedDevice{
					ID:       makeRowID(102, seq),
					SiteID:   int64((seq % 100) + 1),
					MAC:      fmt.Sprintf("52:54:00:%02x:%02x:%02x", (seq>>16)&0xff, (seq>>8)&0xff, seq&0xff),
					Status:   1,
					IP:       fmt.Sprintf("10.0.%d.%d", (seq>>8)&0xff, seq&0xff),
					LastSeen: 1700000000 + int64(seq),
				}
			},
			mutateRow: func(r *SpeedDevice) {
				r.Status = 2
				r.LastSeen += 60
			},
			extractID:   func(r *SpeedDevice) ids.RowID { return r.ID },
			setID:       func(r *SpeedDevice, id ids.RowID) { r.ID = id },
			filterField: "SiteID",
			filterVal:   func(seq int) any { return int64((seq % 100) + 1) },
		},
		&typedSpeedTable[SpeedOrder]{
			name:    "table03_orders",
			tableID: 103,
			makeRow: func(seq int) *SpeedOrder {
				return &SpeedOrder{
					ID:         makeRowID(103, seq),
					CustomerID: int64((seq % 1000) + 1),
					Status:     "pending",
					Total:      int64((seq % 5000) + 10),
					Notes:      "Express delivery requested via air shipment",
					CreatedAt:  1700000000 + int64(seq),
				}
			},
			mutateRow: func(r *SpeedOrder) {
				r.Status = "completed"
				r.Total += 5
			},
			extractID:   func(r *SpeedOrder) ids.RowID { return r.ID },
			setID:       func(r *SpeedOrder, id ids.RowID) { r.ID = id },
			filterField: "CustomerID",
			filterVal:   func(seq int) any { return int64((seq % 1000) + 1) },
		},
		&typedSpeedTable[SpeedProduct]{
			name:    "table04_products",
			tableID: 104,
			makeRow: func(seq int) *SpeedProduct {
				return &SpeedProduct{
					ID:          makeRowID(104, seq),
					CategoryID:  int64((seq % 50) + 1),
					SKU:         fmt.Sprintf("SKU-%08d", seq),
					Price:       int64((seq % 20000) + 100),
					Description: "High-grade optical network interface transceiver",
					Stock:       int64((seq % 500) + 5),
				}
			},
			mutateRow: func(r *SpeedProduct) {
				r.Stock -= 1
				r.Price += 10
			},
			extractID:   func(r *SpeedProduct) ids.RowID { return r.ID },
			setID:       func(r *SpeedProduct, id ids.RowID) { r.ID = id },
			filterField: "CategoryID",
			filterVal:   func(seq int) any { return int64((seq % 50) + 1) },
		},
		&typedSpeedTable[SpeedInvoice]{
			name:    "table05_invoices",
			tableID: 105,
			makeRow: func(seq int) *SpeedInvoice {
				return &SpeedInvoice{
					ID:        makeRowID(105, seq),
					AccountID: int64((seq % 1000) + 1),
					Number:    fmt.Sprintf("INV-2026-%07d", seq),
					Amount:    int64((seq % 100000) + 500),
					Paid:      seq%2 == 0,
					DueDate:   1700000000 + int64(seq*86400),
				}
			},
			mutateRow: func(r *SpeedInvoice) {
				r.Paid = true
			},
			extractID:   func(r *SpeedInvoice) ids.RowID { return r.ID },
			setID:       func(r *SpeedInvoice, id ids.RowID) { r.ID = id },
			filterField: "AccountID",
			filterVal:   func(seq int) any { return int64((seq % 1000) + 1) },
		},
		&typedSpeedTable[SpeedPayment]{
			name:    "table06_payments",
			tableID: 106,
			makeRow: func(seq int) *SpeedPayment {
				return &SpeedPayment{
					ID:        makeRowID(106, seq),
					InvoiceID: int64((seq % 2000) + 1),
					Method:    "ach",
					Amount:    int64((seq % 100000) + 500),
					Ref:       fmt.Sprintf("REF-WIRE-%09d", seq),
					PaidAt:    1700000000 + int64(seq),
				}
			},
			mutateRow: func(r *SpeedPayment) {
				r.Method = "cleared_ach"
			},
			extractID:   func(r *SpeedPayment) ids.RowID { return r.ID },
			setID:       func(r *SpeedPayment, id ids.RowID) { r.ID = id },
			filterField: "InvoiceID",
			filterVal:   func(seq int) any { return int64((seq % 2000) + 1) },
		},
		&typedSpeedTable[SpeedLog]{
			name:    "table07_logs",
			tableID: 107,
			makeRow: func(seq int) *SpeedLog {
				services := []string{"auth-svc", "billing-svc", "gateway", "storage-node"}
				return &SpeedLog{
					ID:        makeRowID(107, seq),
					Service:   services[seq%len(services)],
					Level:     int32((seq % 4) + 1),
					Message:   fmt.Sprintf("Processed transaction block %d successfully", seq),
					TraceID:   fmt.Sprintf("tr-%016x", uint64(seq)*0x9e3779b97f4a7c15),
					Timestamp: 1700000000 + int64(seq),
				}
			},
			mutateRow: func(r *SpeedLog) {
				r.Level = 0
				r.Message = r.Message + " [archived]"
			},
			extractID:   func(r *SpeedLog) ids.RowID { return r.ID },
			setID:       func(r *SpeedLog, id ids.RowID) { r.ID = id },
			filterField: "Service",
			filterVal: func(seq int) any {
				services := []string{"auth-svc", "billing-svc", "gateway", "storage-node"}
				return services[seq%len(services)]
			},
		},
		&typedSpeedTable[SpeedEvent]{
			name:    "table08_events",
			tableID: 108,
			makeRow: func(seq int) *SpeedEvent {
				sources := []string{"sensor-a", "sensor-b", "stream-processor", "ingress-router"}
				return &SpeedEvent{
					ID:        makeRowID(108, seq),
					Source:    sources[seq%len(sources)],
					Type:      "telemetry_tick",
					Payload:   "{\"status\":\"healthy\",\"temp\":36.4,\"load\":0.42}",
					Priority:  int32((seq % 3) + 1),
					Timestamp: 1700000000 + int64(seq),
				}
			},
			mutateRow: func(r *SpeedEvent) {
				r.Priority = 0
			},
			extractID:   func(r *SpeedEvent) ids.RowID { return r.ID },
			setID:       func(r *SpeedEvent, id ids.RowID) { r.ID = id },
			filterField: "Source",
			filterVal: func(seq int) any {
				sources := []string{"sensor-a", "sensor-b", "stream-processor", "ingress-router"}
				return sources[seq%len(sources)]
			},
		},
		&typedSpeedTable[SpeedMetric]{
			name:    "table09_metrics",
			tableID: 109,
			makeRow: func(seq int) *SpeedMetric {
				metrics := []string{"cpu_utilization", "mem_rss_bytes", "network_rx_bytes", "disk_io_iops"}
				return &SpeedMetric{
					ID:         makeRowID(109, seq),
					MetricName: metrics[seq%len(metrics)],
					Tag:        fmt.Sprintf("node-%02d", (seq%32)+1),
					Val:        int64((seq % 1000) * 10),
					Step:       60,
					Timestamp:  1700000000 + int64(seq*60),
				}
			},
			mutateRow: func(r *SpeedMetric) {
				r.Val += 1
			},
			extractID:   func(r *SpeedMetric) ids.RowID { return r.ID },
			setID:       func(r *SpeedMetric, id ids.RowID) { r.ID = id },
			filterField: "MetricName",
			filterVal: func(seq int) any {
				metrics := []string{"cpu_utilization", "mem_rss_bytes", "network_rx_bytes", "disk_io_iops"}
				return metrics[seq%len(metrics)]
			},
		},
		&typedSpeedTable[SpeedSession]{
			name:    "table10_sessions",
			tableID: 110,
			makeRow: func(seq int) *SpeedSession {
				return &SpeedSession{
					ID:        makeRowID(110, seq),
					UserID:    int64((seq % 2000) + 1),
					Token:     fmt.Sprintf("sess-tok-%016x%016x", uint64(seq), uint64(seq*7919)),
					IP:        fmt.Sprintf("192.168.%d.%d", (seq>>8)&0xff, seq&0xff),
					UserAgent: "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
					ExpiresAt: 1700000000 + int64(seq+86400),
				}
			},
			mutateRow: func(r *SpeedSession) {
				r.ExpiresAt += 3600
			},
			extractID:   func(r *SpeedSession) ids.RowID { return r.ID },
			setID:       func(r *SpeedSession, id ids.RowID) { r.ID = id },
			filterField: "UserID",
			filterVal:   func(seq int) any { return int64((seq % 2000) + 1) },
		},
	}
}
