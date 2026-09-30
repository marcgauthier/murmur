package filefetch

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/objectstore"
	"github.com/marcgauthier/murmur/transport"
)

// Server serves container byte ranges to cluster peers over a dedicated
// QUIC endpoint. It shares the cluster mTLS credentials: the QUIC handshake
// authenticates the peer NodeID, and each request additionally carries the
// DBID, which must match the local database.
type Server struct {
	objects *objectstore.Store
	dbid    ids.DBID
	ln      *transport.Listener

	maxConns   int
	reqTimeout time.Duration

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	conns    atomic.Int64
	served   atomic.Uint64
	bytesOut atomic.Uint64
	refused  atomic.Uint64
}

// ServerConfig configures a fetch server.
type ServerConfig struct {
	// Objects serves stored container bytes.
	Objects *objectstore.Store
	// Creds are the cluster mTLS credentials (shared with replication).
	Creds *transport.Credentials
	// DBID is the local database identity; mismatched requests are refused.
	DBID ids.DBID
	// Addr is the listen address.
	Addr string
	// MaxConns bounds concurrent serving connections. Zero selects 16.
	MaxConns int
	// RequestTimeout bounds one request stream when idle. Zero selects a minute.
	RequestTimeout time.Duration
}

// NewServer opens the fetch endpoint. Objects, Creds, and Addr are required.
func NewServer(cfg ServerConfig) (*Server, error) {
	if cfg.Objects == nil {
		return nil, fmt.Errorf("filefetch: objects store is required")
	}
	if cfg.Creds == nil {
		return nil, fmt.Errorf("filefetch: credentials are required")
	}
	if cfg.Addr == "" {
		return nil, fmt.Errorf("filefetch: listen address is required")
	}
	if cfg.MaxConns <= 0 {
		cfg.MaxConns = 16
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = time.Minute
	}
	ln, err := transport.Listen(cfg.Addr, cfg.Creds)
	if err != nil {
		return nil, fmt.Errorf("filefetch: listen: %w", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{
		objects:    cfg.Objects,
		dbid:       cfg.DBID,
		ln:         ln,
		maxConns:   cfg.MaxConns,
		reqTimeout: cfg.RequestTimeout,
		ctx:        ctx,
		cancel:     cancel,
	}
	s.wg.Add(1)
	go s.acceptLoop()
	return s, nil
}

// Addr is the bound listen address.
func (s *Server) Addr() string { return s.ln.Addr() }

// Stats reports serving counters.
type ServerStats struct {
	Conns    int64
	Served   uint64
	BytesOut uint64
	Refused  uint64
}

// Stats returns a serving snapshot.
func (s *Server) Stats() ServerStats {
	return ServerStats{
		Conns:    s.conns.Load(),
		Served:   s.served.Load(),
		BytesOut: s.bytesOut.Load(),
		Refused:  s.refused.Load(),
	}
}

// Close stops the endpoint and waits for in-flight requests.
func (s *Server) Close() error {
	s.cancel()
	_ = s.ln.Close()
	s.wg.Wait()
	return nil
}

func (s *Server) acceptLoop() {
	defer s.wg.Done()
	for {
		sess, err := s.ln.Accept(s.ctx)
		if err != nil {
			return
		}
		if s.conns.Load() >= int64(s.maxConns) {
			s.refused.Add(1)
			_ = sess.Close()
			continue
		}
		s.conns.Add(1)
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer s.conns.Add(-1)
			defer sess.Close()
			s.serveConn(sess)
		}()
	}
}

func (s *Server) serveConn(sess *transport.Session) {
	for {
		if s.ctx.Err() != nil {
			return
		}
		stream, err := sess.AcceptStream(s.ctx)
		if err != nil {
			return
		}
		s.serveStream(stream)
	}
}

func (s *Server) serveStream(stream interface {
	io.ReadWriteCloser
	SetDeadline(time.Time) error
}) {
	defer stream.Close()
	_ = stream.SetDeadline(time.Now().Add(s.reqTimeout))
	frame, err := ReadFrame(stream)
	if err != nil || frame.Type != MsgRequest {
		_ = WriteFrame(stream, MsgError, EncodeError(ErrCodeRefused, "request required"))
		s.refused.Add(1)
		return
	}
	req, err := DecodeRequest(frame.Payload)
	if err != nil {
		_ = WriteFrame(stream, MsgError, EncodeError(ErrCodeRefused, err.Error()))
		s.refused.Add(1)
		return
	}
	if req.DBID != s.dbid {
		_ = WriteFrame(stream, MsgError, EncodeError(ErrCodeRefused, "database mismatch"))
		s.refused.Add(1)
		return
	}
	obj, err := s.objects.Serve(req.Digest)
	if err != nil {
		if os.IsNotExist(err) {
			_ = WriteFrame(stream, MsgHeader, EncodeHeader(&Header{Status: StatusNotFound}))
			return
		}
		_ = WriteFrame(stream, MsgError, EncodeError(ErrCodeInternal, "serve failed"))
		return
	}
	defer obj.Close()
	if req.Offset > uint64(obj.Size()) {
		_ = WriteFrame(stream, MsgHeader, EncodeHeader(&Header{Status: StatusBadOffset, ContainerLen: uint64(obj.Size())}))
		return
	}
	if err := WriteFrame(stream, MsgHeader, EncodeHeader(&Header{Status: StatusOK, ContainerLen: uint64(obj.Size())})); err != nil {
		return
	}
	buf := make([]byte, ChunkBytes)
	off := int64(req.Offset)
	for off < obj.Size() {
		if s.ctx.Err() != nil {
			return
		}
		_ = stream.SetDeadline(time.Now().Add(s.reqTimeout))
		n, rerr := obj.ReadAt(buf, off)
		if n > 0 {
			if werr := WriteFrame(stream, MsgChunk, buf[:n]); werr != nil {
				return
			}
			s.bytesOut.Add(uint64(n))
			off += int64(n)
		}
		// A trailing EOF with bytes is the normal final partial chunk;
		// the loop condition exits. Empty reads always fail.
		if rerr != nil && (!errors.Is(rerr, io.EOF) || n == 0) {
			_ = WriteFrame(stream, MsgError, EncodeError(ErrCodeInternal, "read failed"))
			return
		}
		if n == 0 {
			_ = WriteFrame(stream, MsgError, EncodeError(ErrCodeInternal, "read failed"))
			return
		}
	}
	_ = stream.SetDeadline(time.Now().Add(s.reqTimeout))
	_ = WriteFrame(stream, MsgDone, nil)
	s.served.Add(1)
}
