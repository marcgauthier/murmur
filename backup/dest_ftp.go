package backup

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/textproto"
	"strconv"
	"strings"
	"sync"
	"time"
)

// FTPOptions configures an FTP/FTPS storage destination.
type FTPOptions struct {
	Host      string
	Port      int // default 21
	Username  string
	Password  string
	RemoteDir string // directory on FTP server to store backups
	TLSConfig *tls.Config
	Timeout   time.Duration
}

// FTPDestination implements Destination for remote FTP/FTPS servers using streaming sockets.
type FTPDestination struct {
	opt FTPOptions
	mu  sync.Mutex
}

// NewFTPDestination creates a new FTP/FTPS destination.
func NewFTPDestination(opt FTPOptions) (*FTPDestination, error) {
	if opt.Host == "" {
		return nil, fmt.Errorf("backup: ftp Host cannot be empty")
	}
	if opt.Port <= 0 {
		opt.Port = 21
	}
	if opt.Timeout == 0 {
		opt.Timeout = 30 * time.Second
	}
	return &FTPDestination{opt: opt}, nil
}

func (f *FTPDestination) Type() string { return "ftp" }

// ftpSession wraps an active control connection.
type ftpSession struct {
	conn net.Conn
	tp   *textproto.Conn
}

func (f *FTPDestination) dial(ctx context.Context) (*ftpSession, error) {
	addr := net.JoinHostPort(f.opt.Host, strconv.Itoa(f.opt.Port))
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("backup: ftp dial %s: %w", addr, err)
	}

	s := &ftpSession{
		conn: conn,
		tp:   textproto.NewConn(conn),
	}

	// 1. Read initial banner (220)
	code, msg, err := s.tp.ReadResponse(220)
	if err != nil {
		s.close()
		return nil, fmt.Errorf("backup: ftp banner (%d %s): %w", code, msg, err)
	}

	// 2. AUTH TLS if configured
	if f.opt.TLSConfig != nil {
		if _, _, err := s.cmd(234, "AUTH TLS"); err != nil {
			s.close()
			return nil, fmt.Errorf("backup: ftp auth tls: %w", err)
		}
		tlsConn := tls.Client(s.conn, f.opt.TLSConfig)
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			s.close()
			return nil, fmt.Errorf("backup: ftp tls handshake: %w", err)
		}
		s.conn = tlsConn
		s.tp = textproto.NewConn(tlsConn)
		// Set Protection Buffer Size (PBSZ 0) and Data Channel Protection (PROT P)
		_, _, _ = s.cmd(200, "PBSZ 0")
		_, _, _ = s.cmd(200, "PROT P")
	}

	// 3. Authenticate
	user := f.opt.Username
	if user == "" {
		user = "anonymous"
	}
	code, msg, err = s.cmd(331, "USER %s", user)
	if err == nil {
		// Needs password
		pass := f.opt.Password
		if pass == "" {
			pass = "anonymous@"
		}
		if _, _, err := s.cmd(230, "PASS %s", pass); err != nil {
			s.close()
			return nil, fmt.Errorf("%w: ftp login: %v", ErrUnauthenticated, err)
		}
	} else if code == 230 {
		// Logged in without password
	} else {
		s.close()
		return nil, fmt.Errorf("%w: ftp user: %v", ErrUnauthenticated, err)
	}

	// 4. Binary mode
	if _, _, err := s.cmd(200, "TYPE I"); err != nil {
		s.close()
		return nil, fmt.Errorf("backup: ftp type I: %w", err)
	}

	// 5. Change to remote directory if specified
	if f.opt.RemoteDir != "" {
		if _, _, err := s.cmd(250, "CWD %s", f.opt.RemoteDir); err != nil {
			// Try creating it if not present
			_, _, _ = s.cmd(257, "MKD %s", f.opt.RemoteDir)
			if _, _, err := s.cmd(250, "CWD %s", f.opt.RemoteDir); err != nil {
				s.close()
				return nil, fmt.Errorf("backup: ftp cwd %s: %w", f.opt.RemoteDir, err)
			}
		}
	}

	return s, nil
}

func (s *ftpSession) cmd(expectCode int, format string, args ...any) (int, string, error) {
	id, err := s.tp.Cmd(format, args...)
	if err != nil {
		return 0, "", err
	}
	s.tp.StartResponse(id)
	defer s.tp.EndResponse(id)
	code, msg, err := s.tp.ReadResponse(expectCode)
	return code, msg, err
}

func (s *ftpSession) close() {
	if s.tp != nil {
		_, _, _ = s.cmd(221, "QUIT")
		_ = s.tp.Close()
	}
	if s.conn != nil {
		_ = s.conn.Close()
	}
}

// openDataConn sends PASV and connects to the passive data port.
func (s *ftpSession) openDataConn(ctx context.Context, tlsCfg *tls.Config) (net.Conn, error) {
	_, msg, err := s.cmd(227, "PASV")
	if err != nil {
		return nil, fmt.Errorf("ftp PASV: %w", err)
	}

	// Parse PASV format: (h1,h2,h3,h4,p1,p2)
	start := strings.Index(msg, "(")
	end := strings.Index(msg, ")")
	if start == -1 || end == -1 || end <= start {
		return nil, fmt.Errorf("invalid PASV response: %s", msg)
	}
	parts := strings.Split(msg[start+1:end], ",")
	if len(parts) != 6 {
		return nil, fmt.Errorf("invalid PASV fields: %s", msg)
	}

	ip := fmt.Sprintf("%s.%s.%s.%s", parts[0], parts[1], parts[2], parts[3])
	p1, _ := strconv.Atoi(parts[4])
	p2, _ := strconv.Atoi(parts[5])
	port := (p1 << 8) | p2

	addr := net.JoinHostPort(ip, strconv.Itoa(port))
	var d net.Dialer
	dataConn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("connect ftp data %s: %w", addr, err)
	}

	if tlsCfg != nil {
		tlsData := tls.Client(dataConn, tlsCfg)
		if err := tlsData.HandshakeContext(ctx); err != nil {
			_ = dataConn.Close()
			return nil, fmt.Errorf("ftp data tls handshake: %w", err)
		}
		return tlsData, nil
	}
	return dataConn, nil
}

// WriteBackup streams directly into an FTP data connection using the STOR command.
func (f *FTPDestination) WriteBackup(ctx context.Context, name string, r io.Reader, _ int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	s, err := f.dial(ctx)
	if err != nil {
		return err
	}
	defer s.close()

	dataConn, err := s.openDataConn(ctx, f.opt.TLSConfig)
	if err != nil {
		return err
	}
	defer dataConn.Close()

	// Issue STOR
	id, err := s.tp.Cmd("STOR %s", name)
	if err != nil {
		return fmt.Errorf("ftp STOR: %w", err)
	}
	s.tp.StartResponse(id)
	code, msg, err := s.tp.ReadResponse(150)
	if err != nil && code != 125 {
		s.tp.EndResponse(id)
		return fmt.Errorf("ftp STOR status (%d %s): %w", code, msg, err)
	}

	// Stream data directly over dataConn
	buf := make([]byte, 128*1024)
	for {
		select {
		case <-ctx.Done():
			s.tp.EndResponse(id)
			return ctx.Err()
		default:
		}
		n, rErr := r.Read(buf)
		if n > 0 {
			if _, wErr := dataConn.Write(buf[:n]); wErr != nil {
				s.tp.EndResponse(id)
				return fmt.Errorf("ftp data write: %w", wErr)
			}
		}
		if rErr == io.EOF {
			break
		}
		if rErr != nil {
			s.tp.EndResponse(id)
			return fmt.Errorf("ftp stream read: %w", rErr)
		}
	}

	// Close data socket so server knows upload finished
	_ = dataConn.Close()

	// Wait for transfer complete (226)
	code, msg, err = s.tp.ReadResponse(226)
	s.tp.EndResponse(id)
	if err != nil && code != 250 {
		return fmt.Errorf("ftp transfer complete (%d %s): %w", code, msg, err)
	}

	return nil
}

// ReadBackup retrieves a backup stream using RETR.
func (f *FTPDestination) ReadBackup(ctx context.Context, name string) (io.ReadCloser, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	s, err := f.dial(ctx)
	if err != nil {
		return nil, err
	}

	dataConn, err := s.openDataConn(ctx, f.opt.TLSConfig)
	if err != nil {
		s.close()
		return nil, err
	}

	id, err := s.tp.Cmd("RETR %s", name)
	if err != nil {
		_ = dataConn.Close()
		s.close()
		return nil, fmt.Errorf("ftp RETR: %w", err)
	}
	s.tp.StartResponse(id)
	code, msg, err := s.tp.ReadResponse(150)
	if err != nil && code != 125 {
		s.tp.EndResponse(id)
		_ = dataConn.Close()
		s.close()
		if code == 550 {
			return nil, fmt.Errorf("%w: %s", ErrDestinationNotFound, name)
		}
		return nil, fmt.Errorf("ftp RETR error (%d %s): %w", code, msg, err)
	}

	return &ftpReadCloser{
		dataConn: dataConn,
		s:        s,
		respID:   id,
	}, nil
}

type ftpReadCloser struct {
	dataConn net.Conn
	s        *ftpSession
	respID   uint
	closed   bool
}

func (rc *ftpReadCloser) Read(p []byte) (int, error) {
	return rc.dataConn.Read(p)
}

func (rc *ftpReadCloser) Close() error {
	if rc.closed {
		return nil
	}
	rc.closed = true
	_ = rc.dataConn.Close()
	_, _, err := rc.s.tp.ReadResponse(226)
	rc.s.tp.EndResponse(rc.respID)
	rc.s.close()
	return err
}

// ListBackups enumerates backup filenames using NLST.
func (f *FTPDestination) ListBackups(ctx context.Context, dbID string) ([]BackupInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	s, err := f.dial(ctx)
	if err != nil {
		return nil, err
	}
	defer s.close()

	dataConn, err := s.openDataConn(ctx, f.opt.TLSConfig)
	if err != nil {
		return nil, err
	}
	defer dataConn.Close()

	id, err := s.tp.Cmd("NLST")
	if err != nil {
		return nil, fmt.Errorf("ftp NLST: %w", err)
	}
	s.tp.StartResponse(id)
	code, msg, err := s.tp.ReadResponse(150)
	if err != nil && code != 125 {
		s.tp.EndResponse(id)
		return nil, fmt.Errorf("ftp NLST status (%d %s): %w", code, msg, err)
	}

	var names []string
	scanner := bufio.NewScanner(dataConn)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasSuffix(line, ".tar.gz") {
			names = append(names, line)
		}
	}
	_ = dataConn.Close()
	_, _, _ = s.tp.ReadResponse(226)
	s.tp.EndResponse(id)

	var items []BackupInfo
	for _, n := range names {
		if dbID != "" && !strings.Contains(n, dbID) {
			continue
		}
		items = append(items, BackupInfo{
			Name:      n,
			DBID:      dbID,
			CreatedAt: time.Now(),
		})
	}
	return items, nil
}

// DeleteBackup deletes a remote backup using DELE.
func (f *FTPDestination) DeleteBackup(ctx context.Context, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	s, err := f.dial(ctx)
	if err != nil {
		return err
	}
	defer s.close()

	_, _, err = s.cmd(250, "DELE %s", name)
	if err != nil {
		return fmt.Errorf("ftp DELE %s: %w", name, err)
	}
	return nil
}
