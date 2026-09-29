package filefetch

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/marcgauthier/spedsql/ids"
	"github.com/marcgauthier/spedsql/objectstore"
	"github.com/marcgauthier/spedsql/transport"
)

// Client errors. ErrNotFound and ErrBadOffset select the next source;
// any other error selects the next source too but is worth logging.
var (
	// ErrNotFound indicates the peer has no such object.
	ErrNotFound = errors.New("filefetch: object not found on peer")
	// ErrBadOffset indicates the resume offset exceeds the container.
	ErrBadOffset = errors.New("filefetch: resume offset exceeds container")
	// ErrRefused indicates the peer refused the request (wrong database,
	// overloaded, or shutting down).
	ErrRefused = errors.New("filefetch: peer refused fetch")
)

// Client fetches container bytes from fetch servers.
type Client struct {
	creds *transport.Credentials
	// OpTimeout bounds each idle network operation. Zero selects a minute.
	OpTimeout time.Duration
}

// NewClient builds a fetcher sharing the cluster mTLS credentials.
func NewClient(creds *transport.Credentials) *Client {
	return &Client{creds: creds}
}

func (c *Client) opTimeout() time.Duration {
	if c.OpTimeout > 0 {
		return c.OpTimeout
	}
	return time.Minute
}

// FetchStream is one object fetch: Header describes the container, and the
// body streams exactly ContainerLen-Offset container bytes.
type FetchStream struct {
	Header Header
	stream interface {
		io.ReadWriteCloser
		SetDeadline(time.Time) error
	}
	sess    *transport.Session
	timeout time.Duration
	pending []byte
	done    bool
}

// Fetch opens a fetch stream for digest starting at offset. The caller must
// Close the stream. The expected peer NodeID authenticates the server
// certificate; dbid must match the server's database.
func (c *Client) Fetch(ctx context.Context, addr string, expect ids.NodeID, dbid ids.DBID, digest objectstore.Digest, offset uint64) (*FetchStream, error) {
	if c.creds == nil {
		return nil, fmt.Errorf("filefetch: credentials are required")
	}
	sess, err := transport.Dial(ctx, addr, c.creds, expect)
	if err != nil {
		return nil, err
	}
	closeSess := true
	defer func() {
		if closeSess {
			_ = sess.Close()
		}
	}()
	stream, err := sess.OpenStream(ctx)
	if err != nil {
		return nil, err
	}
	timeout := c.opTimeout()
	_ = stream.SetDeadline(time.Now().Add(timeout))
	if err := WriteFrame(stream, MsgRequest, EncodeRequest(&Request{DBID: dbid, Digest: digest, Offset: offset})); err != nil {
		_ = stream.Close()
		return nil, err
	}
	frame, err := ReadFrame(stream)
	if err != nil {
		_ = stream.Close()
		return nil, err
	}
	switch frame.Type {
	case MsgHeader:
		hdr, err := DecodeHeader(frame.Payload)
		if err != nil {
			_ = stream.Close()
			return nil, err
		}
		switch hdr.Status {
		case StatusOK:
			if hdr.ContainerLen < offset {
				_ = stream.Close()
				return nil, fmt.Errorf("%w: container %d below offset %d", ErrBadOffset, hdr.ContainerLen, offset)
			}
			closeSess = false
			return &FetchStream{Header: *hdr, stream: stream, sess: sess, timeout: timeout}, nil
		case StatusNotFound:
			_ = stream.Close()
			return nil, ErrNotFound
		case StatusBadOffset:
			_ = stream.Close()
			return nil, fmt.Errorf("%w: container %d", ErrBadOffset, hdr.ContainerLen)
		default:
			_ = stream.Close()
			return nil, fmt.Errorf("%w: status %d", ErrRefused, hdr.Status)
		}
	case MsgError:
		detail, derr := DecodeError(frame.Payload)
		_ = stream.Close()
		if derr != nil {
			return nil, fmt.Errorf("%w: %v", ErrRefused, derr)
		}
		switch detail.Code {
		case ErrCodeNotFound:
			return nil, ErrNotFound
		case ErrCodeBadOffset:
			return nil, ErrBadOffset
		default:
			if detail.Message != "" {
				return nil, fmt.Errorf("%w: %s", ErrRefused, detail.Message)
			}
			return nil, ErrRefused
		}
	default:
		_ = stream.Close()
		return nil, fmt.Errorf("filefetch: unexpected message %d", frame.Type)
	}
}

// Read streams container bytes until the declared length is exhausted.
func (s *FetchStream) Read(p []byte) (int, error) {
	if len(s.pending) > 0 {
		n := copy(p, s.pending)
		s.pending = s.pending[n:]
		return n, nil
	}
	if s.done {
		return 0, io.EOF
	}
	_ = s.stream.SetDeadline(time.Now().Add(s.timeout))
	frame, err := ReadFrame(s.stream)
	if err != nil {
		return 0, err
	}
	switch frame.Type {
	case MsgChunk:
		if len(frame.Payload) == 0 {
			return 0, fmt.Errorf("filefetch: empty chunk")
		}
		n := copy(p, frame.Payload)
		if n < len(frame.Payload) {
			s.pending = append(s.pending[:0], frame.Payload[n:]...)
		}
		return n, nil
	case MsgDone:
		s.done = true
		return 0, io.EOF
	case MsgError:
		detail, derr := DecodeError(frame.Payload)
		if derr != nil {
			return 0, derr
		}
		return 0, fmt.Errorf("filefetch: peer error: %s", detail.Message)
	default:
		return 0, fmt.Errorf("filefetch: unexpected message %d", frame.Type)
	}
}

// Close releases the stream and its session.
func (s *FetchStream) Close() error {
	_ = s.stream.Close()
	return s.sess.Close()
}

// ContainerLen is the total container length declared by the server.
func (s *FetchStream) ContainerLen() uint64 { return s.Header.ContainerLen }
