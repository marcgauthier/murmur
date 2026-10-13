package main

import (
	"bytes"
	"context"
	murmur "github.com/marcgauthier/murmur/clients/go"
	"github.com/marcgauthier/murmur/clients/go/examples/internal/demo"
	"time"
)

func run(ctx context.Context, e *demo.Env) error {
	c, err := demo.NewCluster(2)
	if err != nil {
		return err
	}
	fileKey, err := demo.Key()
	if err != nil {
		return err
	}
	for i := range c.Configs {
		cfg, err := murmur.NewFileConfig(e.Path([]string{"files-a", "files-b"}[i]), fileKey, "127.0.0.1:0")
		if err != nil {
			return err
		}
		cfg.FetchInterval = 20 * time.Millisecond
		c.Configs[i].Files = &cfg
	}
	a, err := c.Open(e, "a", 0, nil)
	if err != nil {
		return err
	}
	b, err := c.Open(e, "b", 1, nil)
	if err != nil {
		return err
	}
	if _, _, err = demo.Connect(e, a, b); err != nil {
		return err
	}
	payload := bytes.Repeat([]byte("streamed report\n"), 10000)
	meta, err := a.UploadFile("report.txt", bytes.NewReader(payload))
	if err != nil {
		return err
	}
	if err = demo.Wait(ctx, "remote encrypted object fetch", func() (bool, error) { return b.FileAvailable("report.txt") }); err != nil {
		return err
	}
	var out bytes.Buffer
	if _, err = b.GetFile("report.txt", &out); err != nil {
		return err
	}
	if err = demo.Require(bytes.Equal(out.Bytes(), payload), "remote file bytes differ"); err != nil {
		return err
	}
	availability, err := b.FileAvailability("report.txt")
	if err != nil {
		return err
	}
	list, err := b.ListFiles()
	if err != nil {
		return err
	}
	search, err := b.SearchFiles("report")
	if err != nil {
		return err
	}
	metadata, err := b.FileMetadata("report.txt")
	if err != nil {
		return err
	}
	rotation, err := b.FileKeyRotationStatus()
	if err != nil {
		return err
	}
	e.Print("Fetched %d bytes, availability=%s, listed=%d, found=%d, generation=%d", meta.Bytes, availability.Kind, len(list), len(search), rotation.CurrentGeneration)
	if err = demo.Require(metadata != nil && metadata.Digest == meta.Digest && len(search) == 1, "file metadata/search mismatch"); err != nil {
		return err
	}
	if err = a.DeleteFile("report.txt"); err != nil {
		return err
	}
	if err = demo.Wait(ctx, "replicated file deletion", func() (bool, error) { m, err := b.FileMetadata("report.txt"); return m == nil || m.Deleted, err }); err != nil {
		return err
	}
	// Standalone Objects exposes verification, fetched publication, chunk offsets,
	// and explicit rotation/collection beyond the managed convenience methods.
	backend, err := demo.NewBackend(c.Database, c.Nodes[0], c.Configs[0])
	if err != nil {
		return err
	}
	e.Defer(backend.Handle.Close)
	cfg, err := murmur.NewFileConfig(e.Path("standalone-objects"), fileKey, "127.0.0.1:0")
	if err != nil {
		return err
	}
	objects, err := murmur.OpenObjects(cfg, backend.Handle)
	if err != nil {
		return err
	}
	e.Defer(objects.Close)
	meta, err = objects.Upload("chunks", bytes.NewReader(payload))
	if err != nil {
		return err
	}
	path, err := objects.Path(meta.Digest, meta.Generation)
	if err != nil {
		return err
	}
	out.Reset()
	if err = objects.Verify(path, meta, &out); err != nil {
		return err
	}
	if err = objects.PublishFetched(path, meta); err != nil {
		return err
	}
	offset := uint64(4096)
	seen := uint64(0)
	_, total, err := objects.StreamPlainChunks("chunks", offset, 4096, func(start uint64, data []byte) error {
		if err := demo.Require(start == offset+seen, "chunk offset mismatch"); err != nil {
			return err
		}
		seen += uint64(len(data))
		return nil
	})
	if err != nil {
		return err
	}
	if err = demo.Require(total == uint64(len(payload)) && seen == total-offset, "resumed chunks mismatch"); err != nil {
		return err
	}
	if err = objects.Close(); err != nil {
		return err
	}
	next, err := demo.Key()
	if err != nil {
		return err
	}
	cfg.PreviousKey = &fileKey
	cfg.Key = next
	cfg.Generation++
	objects, err = murmur.OpenObjects(cfg, backend.Handle)
	if err != nil {
		return err
	}
	e.Defer(objects.Close)
	if _, err = objects.RotateKeys(10); err != nil {
		return err
	}
	rotated, err := objects.Metadata("chunks")
	if err != nil {
		return err
	}
	if err = demo.Require(rotated != nil && rotated.Generation == cfg.Generation, "file key rotation failed"); err != nil {
		return err
	}
	out.Reset()
	if _, err = objects.Read("chunks", &out); err != nil {
		return err
	}
	if err = demo.Require(bytes.Equal(out.Bytes(), payload), "rotation changed bytes"); err != nil {
		return err
	}
	if err = objects.Delete("chunks"); err != nil {
		return err
	}
	removed, err := objects.Collect(0)
	if err != nil {
		return err
	}
	e.Print("Standalone verification, resumed chunks, fetched publication, key rotation and collection (%d objects) verified", removed)
	return nil
}
func main() { demo.Main("files", run) }
