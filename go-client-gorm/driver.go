package murmurgorm

import (
	"context"
	"database/sql"
	"database/sql/driver"

	murmur "github.com/marcgauthier/murmur/go-client"
)

// dsnDriver is a database/sql/driver.Driver placeholder. Murmur connections
// need an open *murmur.DB (key, options, lifecycle owned by the caller), which
// a DSN string cannot express, so this package connects via Connector and
// sql.OpenDB only. The driver is deliberately not registered under a name.
type dsnDriver struct{}

func (dsnDriver) Open(name string) (driver.Conn, error) {
	return nil, errNoDSN(name)
}

// Connector attaches a *murmur.DB to database/sql. The caller's handle is the
// admin handle: every Connect clones it for pool use, and closing pooled
// connections releases the clone without touching the caller's database.
type Connector struct {
	db *murmur.DB
}

// NewConnector returns a database/sql connector over an open Murmur database.
// Use sql.OpenDB(NewConnector(db)); there is no DSN form.
func NewConnector(db *murmur.DB) *Connector {
	return &Connector{db: db}
}

// OpenDB is shorthand for sql.OpenDB(NewConnector(db)).
func OpenDB(db *murmur.DB) *sql.DB {
	return sql.OpenDB(NewConnector(db))
}

func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	clone, err := c.db.Clone()
	if err != nil {
		return nil, err
	}
	return &conn{db: clone}, nil
}

func (c *Connector) Driver() driver.Driver { return dsnDriver{} }
