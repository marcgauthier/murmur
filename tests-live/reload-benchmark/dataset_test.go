package reloadbenchmark_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/google/uuid"
	db "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/schema"
)

// Increment when generation, schema, or benchmark configuration changes.
const datasetVersion = 2

type logTable struct {
	name  string
	extra []schema.ColumnSchema
}

func column(name string, typ schema.ColumnType) schema.ColumnSchema {
	return schema.ColumnSchema{Name: name, Type: typ}
}

var logTables = []logTable{
	{"application_logs", []schema.ColumnSchema{column("component", schema.ColText), column("error_code", schema.ColInteger)}},
	{"http_access_logs", []schema.ColumnSchema{column("method", schema.ColText), column("url", schema.ColText), column("status", schema.ColInteger), column("user_agent", schema.ColText)}},
	{"authentication_logs", []schema.ColumnSchema{column("username", schema.ColText), column("source_ip", schema.ColText), column("success", schema.ColInteger)}},
	{"audit_logs", []schema.ColumnSchema{column("actor", schema.ColText), column("action", schema.ColText), column("resource", schema.ColText)}},
	{"database_logs", []schema.ColumnSchema{column("statement", schema.ColText), column("affected_rows", schema.ColInteger)}},
	{"network_logs", []schema.ColumnSchema{column("source_ip", schema.ColText), column("destination_ip", schema.ColText), column("port", schema.ColInteger), column("bytes_sent", schema.ColInteger)}},
	{"job_logs", []schema.ColumnSchema{column("job_name", schema.ColText), column("attempt", schema.ColInteger), column("queue", schema.ColText)}},
	{"payment_logs", []schema.ColumnSchema{column("customer", schema.ColText), column("amount", schema.ColReal), column("currency", schema.ColText)}},
	{"email_logs", []schema.ColumnSchema{column("recipient", schema.ColText), column("subject", schema.ColText), column("delivery_status", schema.ColText)}},
	{"security_logs", []schema.ColumnSchema{column("source_ip", schema.ColText), column("rule", schema.ColText), column("risk_score", schema.ColInteger)}},
}

func tables() []schema.TableSchema {
	out := make([]schema.TableSchema, len(logTables))
	for i, spec := range logTables {
		cols := []schema.ColumnSchema{
			column("id", schema.ColBlob), column("timestamp", schema.ColInteger),
			column("severity", schema.ColText), column("service", schema.ColText),
			column("hostname", schema.ColText), column("message", schema.ColText), column("context", schema.ColText),
			{Name: "latency_ms", Type: schema.ColReal, Nullable: true},
			{Name: "trace", Type: schema.ColBlob, Nullable: true},
		}
		out[i] = schema.TableSchema{Name: spec.name, Columns: append(cols, spec.extra...)}
	}
	return out
}

func localDDL() []string {
	var ddl []string
	for _, table := range logTables {
		ddl = append(ddl,
			fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_timestamp ON %s(timestamp)", table.name, table.name),
			fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_service_severity ON %s(service,severity)", table.name, table.name))
	}
	return ddl
}

// Test-only public key; never use this fixture configuration for real data.
var fixtureKey = bytes.Repeat([]byte{0x72}, 32)

func config(m manifest, dir string) db.Config {
	return db.Config{
		Path: dir, NodeID: m.NodeID, DBID: m.DBID,
		Schema:     db.SchemaConfig{Version: 1, Tables: tables(), LocalDDL: localDDL()},
		Pebble:     db.DefaultPebbleConfig(),
		Encryption: db.EncryptionConfig{Key: bytes.Clone(fixtureKey), KeyID: "reload-benchmark"},
	}
}

func insertSQL(table schema.TableSchema) string {
	names := make([]string, len(table.Columns))
	for i, c := range table.Columns {
		names[i] = c.Name
	}
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table.Name, strings.Join(names, ","), strings.TrimSuffix(strings.Repeat("?,", len(names)), ","))
}

type traceEvent struct {
	OffsetMS  int    `json:"offset_ms"`
	Component string `json:"component"`
	Operation string `json:"operation"`
	Detail    string `json:"detail"`
}

// Each context represents a variable-sized diagnostic event bundle. Values
// are generated per row, rather than stretching one repeated message.
func randomValues(f *gofakeit.Faker, table int, ordinal int64) []any {
	id := uuid.MustParse(f.UUID())
	timestamp := int64(1_760_000_000_000) + ordinal*17
	// UUIDv7 keeps timestamped logs in chronological key order. This avoids
	// random absent-key probes against old SSTables during SQL population;
	// the random UUID suffix and every other value remain seeded gofakeit data.
	for i := 0; i < 6; i++ {
		id[i] = byte(uint64(timestamp) >> uint(8*(5-i)))
	}
	id[6] = (id[6] & 0x0f) | 0x70
	services := []string{"api", "worker", "gateway", "billing", "identity", "mailer", "scheduler"}
	ops := []string{"connect", "authorize", "read", "validate", "dispatch", "retry", "complete"}
	service := services[f.IntRange(0, len(services)-1)]
	events := make([]traceEvent, f.IntRange(8, 96))
	for i := range events {
		events[i] = traceEvent{f.IntRange(0, 10000), services[f.IntRange(0, len(services)-1)], ops[f.IntRange(0, len(ops)-1)], f.Sentence()}
	}
	contextJSON, err := json.Marshal(struct {
		RequestID string       `json:"request_id"`
		Customer  string       `json:"customer"`
		Region    string       `json:"region"`
		Events    []traceEvent `json:"events"`
	}{f.UUID(), f.Company(), f.Country(), events})
	if err != nil {
		panic(err)
	} // only strings/integers are marshaled
	message := fmt.Sprintf("%s %s: %s", service, ops[f.IntRange(0, len(ops)-1)], f.Sentence())
	if ordinal%37 == 0 {
		message += " — café / 東京 / 🔎\nrequest diagnostic"
	}
	var latency, trace any
	if ordinal%7 != 0 {
		latency = f.Float64Range(0.01, 2000)
	}
	if ordinal%5 == 0 {
		payload := make([]byte, f.IntRange(128, 4096))
		for i := range payload {
			payload[i] = byte(f.Uint8())
		}
		trace = payload
	}
	values := []any{id[:], timestamp, f.LogLevel("general"), service,
		fmt.Sprintf("%s-%03d.%s", service, f.IntRange(1, 300), f.DomainName()), message, string(contextJSON), latency, trace}
	switch table {
	case 0:
		values = append(values, f.Word(), int64(f.IntRange(0, 999)))
	case 1:
		values = append(values, f.HTTPMethod(), f.URL(), int64(f.HTTPStatusCode()), f.UserAgent())
	case 2:
		values = append(values, f.Username(), f.IPv4Address(), int64(f.IntRange(0, 1)))
	case 3:
		values = append(values, f.Name(), ops[f.IntRange(0, len(ops)-1)], "/accounts/"+f.UUID())
	case 4:
		values = append(values, "SELECT id FROM orders WHERE customer_id = ?", int64(f.IntRange(0, 10000)))
	case 5:
		values = append(values, f.IPv4Address(), f.IPv4Address(), int64(f.IntRange(1, 65535)), int64(f.IntRange(0, 10_000_000)))
	case 6:
		values = append(values, f.JobTitle(), int64(f.IntRange(1, 5)), service+"-queue")
	case 7:
		values = append(values, f.Name(), f.Float64Range(0.01, 10000), []string{"CAD", "USD", "EUR"}[f.IntRange(0, 2)])
	case 8:
		values = append(values, f.Email(), f.Sentence(), []string{"delivered", "deferred", "bounced"}[f.IntRange(0, 2)])
	case 9:
		values = append(values, f.IPv4Address(), "rule-"+f.Word(), int64(f.IntRange(0, 100)))
	}
	return values
}

func payloadBytes(values []any) int64 {
	var n int64
	for _, v := range values {
		switch v := v.(type) {
		case string:
			n += int64(len(v))
		case []byte:
			n += int64(len(v))
		case int64, float64:
			n += 8
		}
	}
	return n
}
