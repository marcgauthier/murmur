package murmur

import (
	"encoding/binary"
	"encoding/json"
	"io"
	"time"
)

type FileAvailability struct {
	Kind  string
	Peers []NodeID
}

func (a *FileAvailability) UnmarshalJSON(b []byte) error {
	var kind string
	if err := json.Unmarshal(b, &kind); err == nil {
		a.Kind = kind
		a.Peers = nil
		return nil
	}
	var m map[string][]NodeID
	if err := json.Unmarshal(b, &m); err != nil {
		return err
	}
	for k, v := range m {
		a.Kind = k
		a.Peers = v
	}
	return nil
}
func (a FileAvailability) MarshalJSON() ([]byte, error) {
	if a.Kind == "Remote" {
		return json.Marshal(map[string]any{"Remote": a.Peers})
	}
	return json.Marshal(a.Kind)
}

type Objects struct{ object }

func readerCallback(reader io.Reader) func(uint32, uint64, []byte) ([]byte, error) {
	var pending error
	return func(op uint32, _ uint64, b []byte) ([]byte, error) {
		if op != 3 || len(b) != 8 {
			return nil, invalidError("reader callback")
		}
		if pending != nil {
			err := pending
			pending = nil
			if err == io.EOF {
				return nil, nil
			}
			return nil, err
		}
		n := binary.LittleEndian.Uint64(b)
		if n > 64<<10 {
			return nil, ErrLimit
		}
		out := make([]byte, n)
		for attempt := 0; attempt < 100; attempt++ {
			read, err := reader.Read(out)
			if read < 0 || read > len(out) {
				return nil, invalidError("reader count")
			}
			if read > 0 {
				pending = err
				return out[:read], nil
			}
			if err == io.EOF {
				return nil, nil
			}
			if err != nil {
				return nil, err
			}
		}
		return nil, io.ErrNoProgress
	}
}
func writerCallback(writer io.Writer) func(uint32, uint64, []byte) ([]byte, error) {
	return func(op uint32, _ uint64, b []byte) ([]byte, error) {
		if op != 4 {
			return nil, invalidError("writer callback")
		}
		n, err := writer.Write(b)
		if n < 0 || n > len(b) {
			return nil, invalidError("writer count")
		}
		if err != nil {
			return nil, err
		}
		if n != len(b) {
			return nil, io.ErrShortWrite
		}
		return binary.LittleEndian.AppendUint64(nil, uint64(n)), nil
	}
}
func objectIO(o *object, op string, args map[string]any, reader io.Reader, writer io.Writer, out any) error {
	id, err := o.handle()
	if err != nil {
		return err
	}
	if reader != nil {
		return callbackInvoke(id, op, args, readerCallback(reader), out)
	}
	if writer != nil {
		return callbackInvoke(id, op, args, writerCallback(writer), out)
	}
	return invalidError("reader or writer is required")
}
func (d *DB) UploadFile(name string, reader io.Reader) (FileMetadata, error) {
	var m FileMetadata
	err := objectIO(&d.object, "upload_file", map[string]any{"name": name}, reader, nil, &m)
	return m, err
}
func (d *DB) GetFile(name string, writer io.Writer) (FileMetadata, error) {
	var m FileMetadata
	err := objectIO(&d.object, "get_file", map[string]any{"name": name}, nil, writer, &m)
	return m, err
}
func (d *DB) ListFiles() ([]FileMetadata, error) {
	var m []FileMetadata
	err := d.call("list_files", nil, &m)
	return m, err
}
func (d *DB) SearchFiles(query string) ([]FileMetadata, error) {
	var m []FileMetadata
	err := d.call("search_files", map[string]any{"query": query}, &m)
	return m, err
}
func (d *DB) FileMetadata(name string) (*FileMetadata, error) {
	var m *FileMetadata
	err := d.call("file_metadata", map[string]any{"name": name}, &m)
	return m, err
}
func (d *DB) FileAvailable(name string) (bool, error) {
	var b bool
	err := d.call("file_available", map[string]any{"name": name}, &b)
	return b, err
}
func (d *DB) FileAvailability(name string) (FileAvailability, error) {
	var v FileAvailability
	err := d.call("file_availability", map[string]any{"name": name}, &v)
	return v, err
}
func (d *DB) DeleteFile(name string) error {
	return d.call("delete_file", map[string]any{"name": name}, nil)
}
func (d *DB) FileKeyRotationStatus() (FileKeyRotationStatus, error) {
	var v FileKeyRotationStatus
	err := d.call("file_key_rotation_status", nil, &v)
	return v, err
}
func OpenObjects(config FileConfig, backend *BackendHandle) (*Objects, error) {
	id, err := backend.handle()
	if err != nil {
		return nil, err
	}
	var result uint64
	if err = invoke(0, "objects_open", map[string]any{"config": config, "backend": id}, &result); err != nil {
		return nil, err
	}
	o := &Objects{}
	o.set(result)
	return o, nil
}
func (o *Objects) Close() error { return o.object.close("release") }
func (o *Objects) Upload(name string, reader io.Reader) (FileMetadata, error) {
	var m FileMetadata
	err := objectIO(&o.object, "upload", map[string]any{"name": name}, reader, nil, &m)
	return m, err
}
func (o *Objects) Read(name string, writer io.Writer) (FileMetadata, error) {
	var m FileMetadata
	err := objectIO(&o.object, "read", map[string]any{"name": name}, nil, writer, &m)
	return m, err
}
func (o *Objects) Verify(path string, metadata FileMetadata, writer io.Writer) error {
	return objectIO(&o.object, "verify", map[string]any{"path": path, "metadata": metadata}, nil, writer, nil)
}
func (o *Objects) StreamPlainChunks(name string, skip uint64, chunkSize uint64, emit func(uint64, []byte) error) (FileMetadata, uint64, error) {
	id, err := o.handle()
	if err != nil {
		return FileMetadata{}, 0, err
	}
	var raw []json.RawMessage
	err = callbackInvoke(id, "stream_plain_chunks", map[string]any{"name": name, "skip": skip, "chunk_size": chunkSize}, func(op uint32, offset uint64, b []byte) ([]byte, error) {
		if op != 6 {
			return nil, invalidError("chunk callback")
		}
		return nil, emit(offset, b)
	}, &raw)
	if err != nil {
		return FileMetadata{}, 0, err
	}
	if len(raw) != 2 {
		return FileMetadata{}, 0, invalidError("chunk result")
	}
	var m FileMetadata
	var n uint64
	if err = json.Unmarshal(raw[0], &m); err != nil {
		return m, n, err
	}
	err = json.Unmarshal(raw[1], &n)
	return m, n, err
}
func (o *Objects) Metadata(name string) (*FileMetadata, error) {
	var m *FileMetadata
	err := o.call("metadata", map[string]any{"name": name}, &m)
	return m, err
}
func (o *Objects) List() ([]FileMetadata, error) {
	var m []FileMetadata
	err := o.call("list", nil, &m)
	return m, err
}
func (o *Objects) Available(name string) (bool, error) {
	var b bool
	err := o.call("available", map[string]any{"name": name}, &b)
	return b, err
}
func (o *Objects) Availability(name string) (FileAvailability, error) {
	var v FileAvailability
	err := o.call("availability", map[string]any{"name": name}, &v)
	return v, err
}
func (o *Objects) Delete(name string) error {
	return o.call("delete", map[string]any{"name": name}, nil)
}
func (o *Objects) LastFetchError() (*string, error) {
	var s *string
	err := o.call("last_fetch_error", nil, &s)
	return s, err
}
func (o *Objects) RotateKeys(limit uint64) (uint64, error) {
	var n uint64
	err := o.call("rotate_keys", map[string]any{"limit": limit}, &n)
	return n, err
}
func (o *Objects) KeyRotationStatus() (FileKeyRotationStatus, error) {
	var v FileKeyRotationStatus
	err := o.call("key_rotation_status", nil, &v)
	return v, err
}
func (o *Objects) Collect(grace time.Duration) (uint64, error) {
	var n uint64
	err := o.call("collect", map[string]any{"grace": grace}, &n)
	return n, err
}
func (o *Objects) Path(digest Key, generation uint64) (string, error) {
	var p string
	err := o.call("path", map[string]any{"digest": digest, "generation": generation}, &p)
	return p, err
}
func (o *Objects) PublishFetched(path string, metadata FileMetadata) error {
	return o.call("publish_fetched", map[string]any{"path": path, "metadata": metadata}, nil)
}
