package main

import (
	"bytes"
	"context"
	"errors"
	murmur "github.com/marcgauthier/murmur/clients/go"
	"github.com/marcgauthier/murmur/clients/go/examples/internal/demo"
)

func run(ctx context.Context, e *demo.Env) error {
	c, err := demo.NewCluster(2)
	if err != nil {
		return err
	}
	donor, err := demo.NewBackend(c.Database, c.Nodes[0], c.Configs[0])
	if err != nil {
		return err
	}
	e.Defer(donor.Handle.Close)
	receiver, err := demo.NewBackend(c.Database, c.Nodes[1], c.Configs[1])
	if err != nil {
		return err
	}
	e.Defer(receiver.Handle.Close)
	if err = donor.Commit([]murmur.Record{murmur.PutRecord([]byte("demo/snapshot"), bytes.Repeat([]byte("resumable"), 50000))}); err != nil {
		return err
	}
	data, err := donor.Snapshot()
	if err != nil {
		return err
	}
	schema, err := donor.Schema()
	if err != nil {
		return err
	}
	digest, err := murmur.SchemaDigest(schema)
	if err != nil {
		return err
	}
	now, err := murmur.NowMillis()
	if err != nil {
		return err
	}
	manifest, records, err := murmur.BuildSnapshot([2]murmur.NodeID{c.Database, c.Nodes[0]}, c.Nodes[1], data, []murmur.PeerProgress{{Origin: c.Nodes[0], Applied: 7, Observed: 7}}, digest, c.Configs[0], now)
	if err != nil {
		return err
	}
	if err = donor.Commit(records); err != nil {
		return err
	}
	outgoing, err := murmur.OutgoingSnapshot(donor.Handle, c.Nodes[1], manifest.Transfer, now)
	if err != nil {
		return err
	}
	if err = demo.Require(outgoing != nil, "outgoing manifest missing"); err != nil {
		return err
	}
	floor, err := murmur.SnapshotPruneLimit(donor.Handle, c.Nodes[0], now)
	if err != nil {
		return err
	}
	if err = demo.Require(floor == 7, "snapshot pin boundary %d", floor); err != nil {
		return err
	}
	if err = manifest.Verify(receiver.Handle, c.Configs[1], c.Nodes[0], manifest.Expires); !errors.Is(err, murmur.ErrReplicationAuth) {
		return demo.Require(false, "expired snapshot accepted: %v", err)
	}
	if err = murmur.AcceptSnapshot(receiver.Handle, c.Configs[1], c.Nodes[0], manifest, now); err != nil {
		return err
	}
	complete, err := murmur.FinishSnapshot(receiver.Handle, c.Configs[1], c.Nodes[0], manifest.Transfer, now)
	if err != nil {
		return err
	}
	if err = demo.Require(!complete, "incomplete snapshot completed"); err != nil {
		return err
	}
	constants, err := murmur.Constants()
	if err != nil {
		return err
	}
	chunk := int(constants.ChunkBytes)
	if err = murmur.ReceiveSnapshot(receiver.Handle, c.Nodes[0], manifest.Transfer, 0, data[:min(chunk, len(data))], now); err != nil {
		return err
	}
	incoming, err := murmur.IncomingSnapshot(receiver.Handle)
	if err != nil {
		return err
	}
	missing, err := murmur.MissingSnapshot(receiver.Handle, *incoming)
	if err != nil {
		return err
	}
	for _, i := range missing {
		start := int(i) * chunk
		end := min(start+chunk, len(data))
		if err = murmur.ReceiveSnapshot(receiver.Handle, c.Nodes[0], manifest.Transfer, i, data[start:end], now); err != nil {
			return err
		}
	}
	complete, err = murmur.FinishSnapshot(receiver.Handle, c.Configs[1], c.Nodes[0], manifest.Transfer, now)
	if err != nil {
		return err
	}
	if err = demo.Require(complete, "snapshot did not complete"); err != nil {
		return err
	}
	value, ok, err := receiver.Get([]byte("demo/snapshot"))
	if err != nil {
		return err
	}
	if err = demo.Require(ok && len(value) == 450000, "snapshot application lost bytes"); err != nil {
		return err
	}
	again, records, err := murmur.BuildSnapshot([2]murmur.NodeID{c.Database, c.Nodes[0]}, c.Nodes[1], data, nil, digest, c.Configs[0], now)
	if err != nil {
		return err
	}
	if err = donor.Commit(records); err != nil {
		return err
	}
	if err = murmur.CancelSnapshot(donor.Handle, c.Nodes[1], again.Transfer); err != nil {
		return err
	}
	outgoing, err = murmur.OutgoingSnapshot(donor.Handle, c.Nodes[1], again.Transfer, now)
	if err != nil {
		return err
	}
	if err = demo.Require(outgoing == nil, "cancel left outgoing snapshot"); err != nil {
		return err
	}
	if err = murmur.MaintainSnapshots(donor.Handle, manifest.Expires+1); err != nil {
		return err
	}
	e.Print("%d chunks: received one, resumed %d missing chunks, verified and applied; cancellation, expiry and pins checked", len(manifest.Chunks), len(missing))
	return nil
}
func main() { demo.Main("snapshots", run) }
