package main

import (
	"bytes"
	"context"
	murmur "github.com/marcgauthier/murmur/clients/go"
	"github.com/marcgauthier/murmur/clients/go/examples/internal/demo"
	"os"
	"path/filepath"
)

func run(ctx context.Context, e *demo.Env) error {
	c, err := demo.NewCluster(2)
	if err != nil {
		return err
	}
	highDomain, err := demo.ID()
	if err != nil {
		return err
	}
	secret, err := demo.Key()
	if err != nil {
		return err
	}
	signer, err := demo.Key()
	if err != nil {
		return err
	}
	recipient, err := murmur.RecipientPublic(secret)
	if err != nil {
		return err
	}
	public, err := murmur.PublicKey(signer)
	if err != nil {
		return err
	}
	lowCfg := murmur.ExporterConfig(c.Database, e.Path("outbox"), "east", signer, recipient)
	highCfg, err := murmur.ReceiverConfig(highDomain, e.Path("inbox"), "east", secret, []murmur.NodeID{c.Database}, []murmur.TrustEntry{{Node: c.Nodes[0], Key: public}}, c.Configs[0].TrustedOrigins)
	if err != nil {
		return err
	}
	c.Configs[0].Bridge = &lowCfg
	c.Configs[1].Bridge = &highCfg
	for i := range c.Configs {
		key, err := demo.Key()
		if err != nil {
			return err
		}
		files, err := murmur.NewFileConfig(e.Path([]string{"low-files", "high-files"}[i]), key, "127.0.0.1:0")
		if err != nil {
			return err
		}
		c.Configs[i].Files = &files
	}
	low, err := c.Open(e, "low", 0, nil)
	if err != nil {
		return err
	}
	opts := demo.Options()
	opts.DatabaseID = &highDomain
	opts.NodeID = &c.Nodes[1]
	opts.Replication = &c.Configs[1]
	high, err := e.Open("high", opts)
	if err != nil {
		return err
	}
	for _, db := range []*murmur.DB{low, high} {
		if err = db.Migrate(murmur.NewMigration("CREATE TABLE items(id INTEGER PRIMARY KEY,n INTEGER NOT NULL)")); err != nil {
			return err
		}
		if err = db.Flush(); err != nil {
			return err
		}
	}
	lr, err := low.Replication()
	if err != nil {
		return err
	}
	e.Defer(lr.Release)
	hr, err := high.Replication()
	if err != nil {
		return err
	}
	e.Defer(hr.Release)
	exporter, err := lr.Bridge()
	if err != nil {
		return err
	}
	e.Defer(exporter.Close)
	receiver, err := hr.Bridge()
	if err != nil {
		return err
	}
	e.Defer(receiver.Close)
	// The domains never establish a peer connection. Only sealed bundles cross
	// this boundary; each domain encrypts its own file objects with its own key.
	transfer := func() error {
		if _, err := exporter.Capture(); err != nil {
			return err
		}
		entries, err := os.ReadDir(lowCfg.Directory)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if filepath.Ext(entry.Name()) != ".bundle" {
				continue
			}
			path := filepath.Join(lowCfg.Directory, entry.Name())
			raw, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			if _, _, err = receiver.OpenBundle(raw); err != nil {
				return err
			}
			if _, err = receiver.Stage(path); err != nil {
				return err
			}
		}
		_, err = receiver.Process()
		return err
	}
	if _, err = low.Exec("INSERT INTO items VALUES(1,10)"); err != nil {
		return err
	}
	if err = low.Flush(); err != nil {
		return err
	}
	if err = transfer(); err != nil {
		return err
	}
	if err = demo.Wait(ctx, "Low to High row", func() (bool, error) {
		n, err := demo.Int(high, "SELECT n FROM items WHERE id=1")
		if err != nil {
			return false, nil
		}
		return n == 10, nil
	}); err != nil {
		return err
	}
	if _, err = high.Exec("UPDATE items SET n=99 WHERE id=1"); err != nil {
		return err
	}
	if err = high.Flush(); err != nil {
		return err
	}
	if _, err = low.Exec("UPDATE items SET n=20 WHERE id=1"); err != nil {
		return err
	}
	if err = low.Flush(); err != nil {
		return err
	}
	if err = transfer(); err != nil {
		return err
	}
	n, err := demo.Int(high, "SELECT n FROM items WHERE id=1")
	if err != nil {
		return err
	}
	if err = demo.Require(n == 99, "Low overwrote High-owned field"); err != nil {
		return err
	}
	key, err := murmur.Encode("Values", []murmur.Value{murmur.Integer(1)})
	if err != nil {
		return err
	}
	column := "n"
	changed, err := receiver.ReleaseOwnership("items", key, &column)
	if err != nil {
		return err
	}
	n, err = demo.Int(high, "SELECT n FROM items WHERE id=1")
	if err != nil {
		return err
	}
	if err = demo.Require(changed && n == 20, "ownership release did not expose Low shadow"); err != nil {
		return err
	}
	payload := bytes.Repeat([]byte("cross-domain file\n"), 1000)
	if _, err = low.UploadFile("bridge.txt", bytes.NewReader(payload)); err != nil {
		return err
	}
	if err = exporter.ExportFile("bridge.txt"); err != nil {
		return err
	}
	if err = transfer(); err != nil {
		return err
	}
	if err = demo.Wait(ctx, "sealed bridge file", func() (bool, error) { return high.FileAvailable("bridge.txt") }); err != nil {
		return err
	}
	var out bytes.Buffer
	if _, err = high.GetFile("bridge.txt", &out); err != nil {
		return err
	}
	if err = demo.Require(bytes.Equal(out.Bytes(), payload), "bridge file mismatch"); err != nil {
		return err
	}
	// A snapshot bundle includes supported application state and can be delivered
	// alongside journal bundles. Replaying existing bundles is idempotent.
	if _, err = exporter.ExportSnapshot(); err != nil {
		return err
	}
	if err = transfer(); err != nil {
		return err
	}
	if err = transfer(); err != nil {
		return err
	}
	e.Print("Low row/file and snapshot bundles delivered; High value 99 protected, release exposed 20; replay safe")
	return nil
}
func main() { demo.Main("bridge", run) }
