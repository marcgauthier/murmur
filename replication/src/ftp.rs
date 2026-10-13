//! Minimal FTP/FTPS upload client for bridge bundles.
//!
//! Mirrors the Go `backup.FTPDestination` behavior: banner, explicit
//! `AUTH TLS` for `ftps://`, login with anonymous defaults, `TYPE I`,
//! `CWD` with `MKD` fallback, passive data connection, `STOR`.
//! Synchronous by design; callers run it off the async runtime.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;

use crate::Error;
use crate::Result;

const DEFAULT_PORT: u16 = 21;
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_NAME_LEN: usize = 256;

/// TLS trust and timing for `ftps://` uploads.
#[derive(Debug, Clone, Default)]
pub struct FtpOptions {
    /// PEM-encoded CA certificates trusted for the control and data
    /// connections. Required: cross-domain links pin a private CA.
    pub roots_pem: Vec<u8>,
    /// Socket timeout. Zero selects the 30s default.
    pub timeout: Duration,
}

struct Target {
    tls: bool,
    host: String,
    port: u16,
    user: String,
    pass: String,
    dir: String,
    file: String,
}

fn invalid(msg: impl Into<String>) -> Error {
    Error::Invalid(format!("ftp URL: {}", msg.into()))
}

/// Percent-decode one URL component. `+` stays literal (not form data).
fn unescape(s: &str) -> Result<String> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' => {
                if i + 2 >= bytes.len() {
                    return Err(invalid("truncated escape"));
                }
                let hex = std::str::from_utf8(&bytes[i + 1..i + 3])
                    .map_err(|_| invalid("bad escape"))?;
                let b = u8::from_str_radix(hex, 16).map_err(|_| invalid("bad escape"))?;
                out.push(b);
                i += 3;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8(out).map_err(|_| invalid("non-UTF8 component"))
}

/// Portable name policy shared with the Go bridge: bounded length, no
/// separators, parent references, or hidden files.
fn check_name(name: &str) -> Result<()> {
    if name.is_empty() || name.len() > MAX_NAME_LEN {
        return Err(Error::Invalid(format!("ftp name length {}", name.len())));
    }
    if name.starts_with('.') || name.contains("..") {
        return Err(Error::Invalid(format!("ftp name {name:?} rejected")));
    }
    if !name
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
    {
        return Err(Error::Invalid(format!("ftp name {name:?} rejected")));
    }
    Ok(())
}

fn parse_url(url: &str) -> Result<(Target, String)> {
    let (scheme, rest) = url.split_once("://").ok_or_else(|| invalid("missing scheme"))?;
    let tls = match scheme {
        "ftp" => false,
        "ftps" => true,
        _ => return Err(invalid(format!("scheme {scheme:?}"))),
    };
    let (authority, path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i + 1..]),
        None => (rest, ""),
    };
    if authority.is_empty() {
        return Err(invalid("missing host"));
    }
    let (has_userinfo, userinfo, hostport) = match authority.rsplit_once('@') {
        Some((u, h)) => (true, u, h),
        None => (false, "", authority),
    };
    let (user, pass) = match userinfo.split_once(':') {
        Some((u, p)) => (unescape(u)?, unescape(p)?),
        None => (unescape(userinfo).unwrap_or_default(), String::new()),
    };
    if has_userinfo && user.is_empty() {
        return Err(invalid("empty username"));
    }
    let (host, port) = if let Some(h) = hostport.strip_prefix('[') {
        let (ip, rest) = h.split_once(']').ok_or_else(|| invalid("bad IPv6 host"))?;
        let port = match rest.strip_prefix(':') {
            Some(p) => p.parse::<u16>().map_err(|_| invalid("bad port"))?,
            None if rest.is_empty() => DEFAULT_PORT,
            None => return Err(invalid("bad IPv6 host")),
        };
        (ip.to_string(), port)
    } else {
        match hostport.rsplit_once(':') {
            Some((h, p)) if !p.is_empty() => (
                h.to_string(),
                p.parse::<u16>().map_err(|_| invalid("bad port"))?,
            ),
            _ => (hostport.to_string(), DEFAULT_PORT),
        }
    };
    if host.is_empty() {
        return Err(invalid("missing host"));
    }
    // Last path segment is the remote file; the rest is the directory.
    // An empty path (or trailing slash) keeps the caller's file name.
    let mut segments: Vec<String> = Vec::new();
    for seg in path.split('/') {
        if seg.is_empty() {
            continue;
        }
        if seg == "." || seg == ".." {
            return Err(invalid("path escapes directory"));
        }
        segments.push(unescape(seg)?);
    }
    let (dir, url_file) = match segments.split_last() {
        Some((last, _)) if !path.ends_with('/') => (
            segments[..segments.len() - 1].join("/"),
            last.clone(),
        ),
        _ => (segments.join("/"), String::new()),
    };
    Ok((
        Target {
            tls,
            host,
            port,
            user,
            pass,
            dir,
            file: String::new(),
        },
        url_file,
    ))
}

enum Conn {
    Plain(TcpStream),
    Tls(rustls::StreamOwned<rustls::ClientConnection, TcpStream>),
}

impl Conn {
    fn read_byte(&mut self) -> std::io::Result<Option<u8>> {
        let mut b = [0u8; 1];
        match self.reader().read(&mut b)? {
            0 => Ok(None),
            _ => Ok(Some(b[0])),
        }
    }

    fn reader(&mut self) -> &mut dyn Read {
        match self {
            Conn::Plain(s) => s,
            Conn::Tls(s) => s,
        }
    }

    fn writer(&mut self) -> &mut dyn Write {
        match self {
            Conn::Plain(s) => s,
            Conn::Tls(s) => s,
        }
    }

    fn send_line(&mut self, line: &str) -> Result<()> {
        self.writer()
            .write_all(format!("{line}\r\n").as_bytes())
            .map_err(|e| Error::Unavailable(format!("ftp write: {e}")))?;
        // rustls queues suburb writes until flushed; sockets ignore it.
        self.writer()
            .flush()
            .map_err(|e| Error::Unavailable(format!("ftp write: {e}")))?;
        Ok(())
    }

    /// Read one reply; the first line's code must be in `accept`.
    /// Multiline replies (`123-...` through `123 ...`) are consumed fully.
    fn read_reply(&mut self, accept: &[u16]) -> Result<(u16, String)> {
        let mut text = String::new();
        let mut code: Option<u16> = None;
        loop {
            let mut line = Vec::new();
            loop {
                match self
                    .read_byte()
                    .map_err(|e| Error::Unavailable(format!("ftp read: {e}")))?
                {
                    None => {
                        return Err(Error::Protocol("ftp: connection closed".into()));
                    }
                    Some(b'\n') => break,
                    Some(b) => {
                        if line.len() > 4096 {
                            return Err(Error::Protocol("ftp: overlong reply".into()));
                        }
                        line.push(b);
                    }
                }
            }
            while line.last() == Some(&b'\r') {
                line.pop();
            }
            let s = String::from_utf8_lossy(&line).into_owned();
            if s.len() < 4 || !s.as_bytes()[..3].iter().all(|b| b.is_ascii_digit()) {
                return Err(Error::Protocol(format!("ftp: bad reply {s:?}")));
            }
            let c: u16 = s[..3].parse().unwrap_or(0);
            let more = s.as_bytes()[3] == b'-';
            if code.is_none() {
                code = Some(c);
            }
            text.push_str(&s);
            text.push('\n');
            if !more {
                if c != code.unwrap_or(c) {
                    return Err(Error::Protocol(format!("ftp: mixed reply {s:?}")));
                }
                let c = code.unwrap_or(0);
                if !accept.contains(&c) {
                    return Err(Error::Protocol(format!("ftp: reply {c}: {}", text.trim())));
                }
                return Ok((c, text));
            }
        }
    }
}

struct Session {
    control: Option<Conn>,
    tls: Option<Arc<rustls::ClientConfig>>,
    server_name: Option<rustls::pki_types::ServerName<'static>>,
    timeout: Duration,
}

impl Session {
    fn ctl(&mut self) -> &mut Conn {
        self.control.as_mut().expect("control connection")
    }

    fn connect(
        t: &Target,
        config: Option<Arc<rustls::ClientConfig>>,
        name: Option<rustls::pki_types::ServerName<'static>>,
        timeout: Duration,
    ) -> Result<Self> {
        let addr = format!("{}:{}", t.host, t.port);
        let sock = TcpStream::connect(&addr)
            .map_err(|e| Error::Unavailable(format!("ftp dial {addr}: {e}")))?;
        sock.set_read_timeout(Some(timeout))
            .map_err(|e| Error::Unavailable(format!("ftp timeout: {e}")))?;
        sock.set_write_timeout(Some(timeout))
            .map_err(|e| Error::Unavailable(format!("ftp timeout: {e}")))?;
        let mut s = Session {
            control: Some(Conn::Plain(sock)),
            tls: config,
            server_name: name,
            timeout,
        };
        s.ctl().read_reply(&[220])?;
        if s.tls.is_some() {
            s.ctl().send_line("AUTH TLS")?;
            s.ctl().read_reply(&[234])?;
            s.upgrade_control()?;
            // Protection buffers, best effort like the Go client.
            let _ = s.cmd(&[200], "PBSZ 0");
            let _ = s.cmd(&[200], "PROT P");
        }
        let user = if t.user.is_empty() { "anonymous" } else { &t.user };
        match s.cmd(&[230, 331], &format!("USER {user}")) {
            Ok((230, _)) => {}
            Ok(_) => {
                let pass = if t.pass.is_empty() { "anonymous@" } else { &t.pass };
                s.cmd(&[230], &format!("PASS {pass}")).map_err(|e| match e {
                    Error::Protocol(m) => Error::Auth(format!("ftp login: {m}")),
                    e => e,
                })?;
            }
            Err(e) => return Err(Error::Auth(format!("ftp user: {e}"))),
        }
        s.cmd(&[200], "TYPE I")?;
        if !t.dir.is_empty() {
            if s.cmd(&[250], &format!("CWD {}", t.dir)).is_err() {
                let _ = s.cmd(&[257], &format!("MKD {}", t.dir));
                s.cmd(&[250], &format!("CWD {}", t.dir))?;
            }
        }
        Ok(s)
    }

    /// Helper: send a command and read its reply, mapping transport
    /// failures to `Unavailable` while keeping protocol rejections.
    fn cmd(&mut self, accept: &[u16], line: &str) -> Result<(u16, String)> {
        self.ctl().send_line(line)?;
        self.ctl().read_reply(accept)
    }

    fn upgrade_control(&mut self) -> Result<()> {
        let config = self.tls.clone().ok_or_else(|| Error::Invalid("ftp: no TLS config".into()))?;
        let name = self.server_name.clone().ok_or_else(|| Error::Invalid("ftp: no server name".into()))?;
        let plain = match self.control.take() {
            Some(Conn::Plain(s)) => s,
            _ => return Err(Error::Protocol("ftp: already secure".into())),
        };
        let conn = rustls::ClientConnection::new(config, name)
            .map_err(|e| Error::Unavailable(format!("ftp tls: {e}")))?;
        let mut tls = rustls::StreamOwned::new(conn, plain);
        tls.conn
            .complete_io(&mut tls.sock)
            .map_err(|e| Error::Unavailable(format!("ftp tls handshake: {e}")))?;
        self.control = Some(Conn::Tls(tls));
        Ok(())
    }

    fn open_data(&mut self) -> Result<Conn> {
        let (_, msg) = self.cmd(&[227], "PASV")?;
        let (ip, port) = parse_pasv(&msg)?;
        let addr = format!("{ip}:{port}");
        let sock = TcpStream::connect(&addr)
            .map_err(|e| Error::Unavailable(format!("ftp data dial {addr}: {e}")))?;
        sock.set_read_timeout(Some(self.timeout))
            .map_err(|e| Error::Unavailable(format!("ftp timeout: {e}")))?;
        sock.set_write_timeout(Some(self.timeout))
            .map_err(|e| Error::Unavailable(format!("ftp timeout: {e}")))?;
        if let Some(config) = &self.tls {
            let name = self.server_name.clone().ok_or_else(|| Error::Invalid("ftp: no server name".into()))?;
            let conn = rustls::ClientConnection::new(config.clone(), name)
                .map_err(|e| Error::Unavailable(format!("ftp data tls: {e}")))?;
            let mut tls = rustls::StreamOwned::new(conn, sock);
            tls.conn
                .complete_io(&mut tls.sock)
                .map_err(|e| Error::Unavailable(format!("ftp data tls handshake: {e}")))?;
            return Ok(Conn::Tls(tls));
        }
        Ok(Conn::Plain(sock))
    }

    fn store(&mut self, name: &str, data: &[u8]) -> Result<()> {
        let mut data_conn = self.open_data()?;
        self.ctl().send_line(&format!("STOR {name}"))?;
        // 150 starts the transfer; 125 means it is already underway.
        self.ctl().read_reply(&[125, 150])?;
        // Stream in 128 KiB pieces like the Go client.
        for chunk in data.chunks(128 * 1024) {
            data_conn
                .writer()
                .write_all(chunk)
                .map_err(|e| Error::Unavailable(format!("ftp data write: {e}")))?;
        }
        data_conn
            .writer()
            .flush()
            .map_err(|e| Error::Unavailable(format!("ftp data write: {e}")))?;
        close_data(data_conn);
        self.ctl().read_reply(&[226, 250])?;
        Ok(())
    }

    fn quit(&mut self) {
        // Best effort; reading the farewell also consumes any trailing
        // post-handshake bytes so the close stays a clean FIN.
        if self.ctl().send_line("QUIT").is_err() {
            return;
        }
        let _ = self.ctl().read_reply(&[221]);
    }
}

/// Close a data connection gracefully. Always drain to EOF first: closing
/// with unread bytes (e.g. TLS session tickets) sends RST instead of FIN
/// and the server reports a reset transfer.
fn close_data(c: Conn) {
    fn drain<R: Read>(r: &mut R) {
        let mut buf = [0u8; 1024];
        loop {
            match r.read(&mut buf) {
                Ok(0) | Err(_) => break,
                Ok(_) => continue,
            }
        }
    }
    match c {
        Conn::Plain(s) => {
            let _ = s.shutdown(std::net::Shutdown::Write);
            let mut s = s;
            drain(&mut s);
        }
        Conn::Tls(mut t) => {
            let _ = t.conn.send_close_notify();
            let _ = t.flush();
            drain(&mut t);
        }
    }
}

fn parse_pasv(msg: &str) -> Result<(String, u16)> {
    let start = msg.find('(').ok_or_else(|| Error::Protocol(format!("ftp: bad PASV {msg:?}")))?;
    let end = msg[start..]
        .find(')')
        .map(|i| start + i)
        .ok_or_else(|| Error::Protocol(format!("ftp: bad PASV {msg:?}")))?;
    let parts: Vec<&str> = msg[start + 1..end].split(',').collect();
    if parts.len() != 6 {
        return Err(Error::Protocol(format!("ftp: bad PASV {msg:?}")));
    }
    let mut n = [0u8; 6];
    for (i, p) in parts.iter().enumerate() {
        n[i] = p
            .trim()
            .parse::<u8>()
            .map_err(|_| Error::Protocol(format!("ftp: bad PASV {msg:?}")))?;
    }
    Ok((
        format!("{}.{}.{}.{}", n[0], n[1], n[2], n[3]),
        ((n[4] as u16) << 8) | n[5] as u16,
    ))
}

fn tls_config(roots_pem: &[u8]) -> Result<Arc<rustls::ClientConfig>> {
    let mut store = rustls::RootCertStore::empty();
    let mut pem = std::io::BufReader::new(roots_pem);
    let certs = rustls_pemfile::certs(&mut pem)
        .collect::<std::io::Result<Vec<_>>>()
        .map_err(|e| Error::Invalid(format!("ftp trust roots: {e}")))?;
    if certs.is_empty() {
        return Err(Error::Invalid("ftp trust roots: no certificates".into()));
    }
    for c in certs {
        store
            .add(c)
            .map_err(|e| Error::Invalid(format!("ftp trust roots: {e}")))?;
    }
    Ok(Arc::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(store)
            .with_no_client_auth(),
    ))
}

fn server_name(host: &str) -> Result<rustls::pki_types::ServerName<'static>> {
    use rustls::pki_types::ServerName;
    if let Ok(ip) = host.parse::<std::net::IpAddr>() {
        return Ok(ServerName::IpAddress(ip.into()));
    }
    ServerName::try_from(host.to_string()).map_err(|_| Error::Invalid(format!("ftp host {host:?}")))
}

/// Upload `data` as one file. `default_name` is used when the URL carries
/// no file name. `tls` selects explicit FTPS (`ftps://`); `None` selects
/// plain FTP (`ftp://`) and rejects `ftps://` URLs.
pub(crate) fn upload(url: &str, default_name: &str, data: &[u8], tls: Option<&FtpOptions>) -> Result<()> {
    let (mut target, url_file) = parse_url(url)?;
    if target.tls && tls.is_none() {
        return Err(Error::Invalid("ftps URL requires trust roots".into()));
    }
    if !target.tls && tls.is_some() {
        return Err(Error::Invalid("ftp URL with TLS options".into()));
    }
    target.file = if url_file.is_empty() {
        default_name.to_string()
    } else {
        url_file
    };
    check_name(&target.file)?;
    let timeout = tls.map(|o| o.timeout).filter(|t| !t.is_zero()).unwrap_or(DEFAULT_TIMEOUT);
    let config = match tls {
        Some(o) => Some(tls_config(&o.roots_pem)?),
        None => None,
    };
    let name = match config {
        Some(_) => Some(server_name(&target.host)?),
        None => None,
    };
    let mut session = Session::connect(&target, config, name, timeout)?;
    let file = std::mem::take(&mut target.file);
    let result = session.store(&file, data);
    session.quit();
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::{Bridge, Config as BridgeConfig};
    use crate::{Backend, NodeId, Record};
    use std::collections::{BTreeMap, BTreeSet};
    use std::net::{TcpListener, TcpStream};
    use std::sync::{Arc, Mutex};

    struct Stub;
    impl Backend for Stub {
        fn identity(&self) -> (NodeId, NodeId) {
            ([1; 16], [2; 16])
        }
        fn get(&self, _: &[u8]) -> Result<Option<Vec<u8>>> {
            Ok(None)
        }
        fn keys(&self, _: &[u8]) -> Result<Vec<Vec<u8>>> {
            Ok(Vec::new())
        }
        fn commit(&self, _: Vec<Record>) -> Result<()> {
            Ok(())
        }
        fn import_bridge(&self, _: crate::bridge::Header, _: Vec<u8>) -> Result<bool> {
            Ok(false)
        }
        fn release_ownership(&self, _: String, _: Vec<u8>, _: Option<String>) -> Result<bool> {
            Ok(false)
        }
        fn publish(&self, _: Vec<Record>) -> Result<()> {
            Ok(())
        }
        fn snapshot(&self) -> Result<Vec<u8>> {
            Ok(Vec::new())
        }
        fn snapshot_with_progress(&self) -> Result<(Vec<u8>, Vec<crate::protocol::Progress>)> {
            Ok((Vec::new(), Vec::new()))
        }
        fn retention_checkpoint(&self) -> Result<()> {
            Ok(())
        }
        fn prune_history(&self, _: NodeId, _: u64) -> Result<u64> {
            Ok(0)
        }
        fn reclaim_retention(&self) -> Result<()> {
            Ok(())
        }
        fn schema(&self) -> Result<Vec<u8>> {
            Ok(Vec::new())
        }
        fn apply(&self, _: Vec<Vec<u8>>, _: Vec<Record>) -> Result<bool> {
            Ok(false)
        }
        fn observe(&self, _: u64, _: u32) -> Result<()> {
            Ok(())
        }
        fn accept_schema(&self, _: Vec<u8>) -> Result<bool> {
            Ok(false)
        }
    }

    fn exporter(dir: &std::path::Path) -> Bridge {
        let cfg = BridgeConfig::exporter(
            [1; 16],
            dir.to_path_buf(),
            "s".into(),
            [30; 32],
            crate::bridge::recipient_public([40; 32]),
        );
        Bridge::open(cfg, Arc::new(Stub)).unwrap()
    }

    fn receiver(dir: &std::path::Path) -> Bridge {
        let cfg = BridgeConfig::receiver(
            [1; 16],
            dir.to_path_buf(),
            "s".into(),
            [40; 32],
            BTreeSet::from([[2; 16]]),
            BTreeMap::from([([2; 16], crate::protocol::public_key(&[30; 32]))]),
            BTreeMap::new(),
        );
        Bridge::open(cfg, Arc::new(Stub)).unwrap()
    }

    /// Self-signed CA for `ftps://` tests: (cert_der, key_der, cert_pem).
    fn test_ca() -> (Vec<u8>, Vec<u8>, Vec<u8>) {
        let mut params = rcgen::CertificateParams::new(vec![]).unwrap();
        params.subject_alt_names = vec![rcgen::SanType::IpAddress("127.0.0.1".parse().unwrap())];
        let key = rcgen::KeyPair::generate().unwrap();
        let cert = params.self_signed(&key).unwrap();
        (
            cert.der().to_vec(),
            key.serialize_der(),
            cert.pem().into_bytes(),
        )
    }

    #[derive(Default)]
    struct Capture {
        user: String,
        pass: String,
        dir: String,
        file: String,
        bytes: Vec<u8>,
        cwd_count: usize,
        tls_control: bool,
        tls_data: bool,
    }

    struct Script {
        tls: Option<(Vec<u8>, Vec<u8>)>,
        reject_pass: bool,
        fail_first_cwd: bool,
    }

    enum ServerConn {
        Plain(TcpStream),
        Tls(rustls::StreamOwned<rustls::ServerConnection, TcpStream>),
    }

    impl ServerConn {
        fn read_line(&mut self) -> String {
            let mut line = Vec::new();
            let mut b = [0u8; 1];
            let r: &mut dyn Read = match self {
                ServerConn::Plain(s) => s,
                ServerConn::Tls(s) => s,
            };
            loop {
                r.read_exact(&mut b).unwrap();
                if b[0] == b'\n' {
                    break;
                }
                line.push(b[0]);
            }
            while line.last() == Some(&b'\r') {
                line.pop();
            }
            String::from_utf8(line).unwrap()
        }

        fn write_line(&mut self, s: &str) {
            let w: &mut dyn Write = match self {
                ServerConn::Plain(s) => s,
                ServerConn::Tls(s) => s,
            };
            w.write_all(format!("{s}\r\n").as_bytes()).unwrap();
            w.flush().unwrap();
        }
    }

    /// One-shot fake FTP(S) server on loopback. Panics on protocol errors
    /// so test failures show the server side too.
    fn spawn_server(script: Script) -> (String, Arc<Mutex<Capture>>, std::thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let cap = Arc::new(Mutex::new(Capture::default()));
        let cap2 = cap.clone();
        let handle = std::thread::spawn(move || {
            let (sock, _) = listener.accept().unwrap();
            sock.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
            sock.set_write_timeout(Some(Duration::from_secs(10))).unwrap();
            let mut c = ServerConn::Plain(sock);
            // Multiline banner exercises the client's reply reader.
            c.write_line("220-fake ftp");
            c.write_line("220 ready");
            if let Some((cert_der, key_der)) = &script.tls {
                assert_eq!(c.read_line(), "AUTH TLS");
                c.write_line("234 go ahead");
                let config = rustls::ServerConfig::builder()
                    .with_no_client_auth()
                    .with_single_cert(
                        vec![rustls::pki_types::CertificateDer::from(cert_der.clone())],
                        rustls::pki_types::PrivateKeyDer::Pkcs8(
                            rustls::pki_types::PrivatePkcs8KeyDer::from(key_der.clone()),
                        ),
                    )
                    .unwrap();
                let conn = rustls::ServerConnection::new(Arc::new(config)).unwrap();
                let plain = match c {
                    ServerConn::Plain(s) => s,
                    ServerConn::Tls(_) => panic!("already secure"),
                };
                let mut tls = rustls::StreamOwned::new(conn, plain);
                if tls.conn.complete_io(&mut tls.sock).is_err() {
                    return; // client aborted the handshake (negative test)
                }
                c = ServerConn::Tls(tls);
                cap2.lock().unwrap().tls_control = true;
            }
            // PBSZ/PROT precede login, like the Go client sends them.
            if script.tls.is_some() {
                assert_eq!(c.read_line(), "PBSZ 0");
                c.write_line("200 pbsz");
                assert_eq!(c.read_line(), "PROT P");
                c.write_line("200 prot");
            }
            let mut cap = cap2.lock().unwrap();
            let line = c.read_line();
            assert!(line.starts_with("USER "), "{line}");
            cap.user = line["USER ".len()..].into();
            drop(cap);
            c.write_line("331 password please");
            let line = c.read_line();
            assert!(line.starts_with("PASS "), "{line}");
            let mut cap = cap2.lock().unwrap();
            cap.pass = line["PASS ".len()..].into();
            drop(cap);
            if script.reject_pass {
                c.write_line("530 bad password");
                return;
            }
            c.write_line("230 logged in");
            assert_eq!(c.read_line(), "TYPE I");
            c.write_line("200 type set");
            // Optional CWD (+MKD fallback on first attempt).
            let mut first_cwd = true;
            loop {
                let line = c.read_line();
                if let Some(dir) = line.strip_prefix("CWD ") {
                    let mut cap = cap2.lock().unwrap();
                    cap.cwd_count += 1;
                    cap.dir = dir.into();
                    drop(cap);
                    if script.fail_first_cwd && first_cwd {
                        first_cwd = false;
                        c.write_line("550 no such dir");
                        continue;
                    }
                    c.write_line("250 cwd ok");
                } else if let Some(_mkd) = line.strip_prefix("MKD ") {
                    c.write_line("257 mkd ok");
                } else if line == "PASV" {
                    break;
                } else {
                    panic!("unexpected command {line:?}");
                }
            }
            let data_listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let data_port = data_listener.local_addr().unwrap().port();
            c.write_line(&format!(
                "227 Entering Passive Mode (127,0,0,1,{},{}).",
                data_port >> 8,
                data_port & 0xff
            ));
            let (data_sock, _) = data_listener.accept().unwrap();
            data_sock
                .set_read_timeout(Some(Duration::from_secs(10)))
                .unwrap();
            // Data connection is encrypted under PROT P.
            let mut data: ServerConn = if script.tls.is_some() {
                let (cert_der, key_der) = script.tls.as_ref().unwrap();
                let config = rustls::ServerConfig::builder()
                    .with_no_client_auth()
                    .with_single_cert(
                        vec![rustls::pki_types::CertificateDer::from(cert_der.clone())],
                        rustls::pki_types::PrivateKeyDer::Pkcs8(
                            rustls::pki_types::PrivatePkcs8KeyDer::from(key_der.clone()),
                        ),
                    )
                    .unwrap();
                let conn = rustls::ServerConnection::new(Arc::new(config)).unwrap();
                let mut tls = rustls::StreamOwned::new(conn, data_sock);
                tls.conn.complete_io(&mut tls.sock).unwrap();
                cap2.lock().unwrap().tls_data = true;
                ServerConn::Tls(tls)
            } else {
                ServerConn::Plain(data_sock)
            };
            let line = c.read_line();
            let name = line.strip_prefix("STOR ").unwrap_or_else(|| panic!("expected STOR, got {line:?}"));
            cap2.lock().unwrap().file = name.into();
            c.write_line("150 opening data");
            let mut bytes = Vec::new();
            match &mut data {
                ServerConn::Plain(s) => s.read_to_end(&mut bytes).unwrap(),
                ServerConn::Tls(s) => s.read_to_end(&mut bytes).unwrap(),
            };
            cap2.lock().unwrap().bytes = bytes;
            drop(data);
            c.write_line("226 transfer complete");
            assert_eq!(c.read_line(), "QUIT");
            c.write_line("221 bye");
        });
        (format!("127.0.0.1:{port}"), cap, handle)
    }

    fn bundle_file(dir: &std::path::Path, name: &str, bytes: &[u8]) -> std::path::PathBuf {
        let path = dir.join(name);
        std::fs::write(&path, bytes).unwrap();
        path
    }

    #[tokio::test]
    async fn ftp_plain_upload_round_trip() {
        let temp = tempfile::tempdir().unwrap();
        let bridge = exporter(temp.path());
        let payload = vec![0xABu8; 300_000]; // spans several 128 KiB chunks
        let file = bundle_file(temp.path(), "node-00000000000000000001.bundle", &payload);
        let (addr, cap, server) = spawn_server(Script {
            tls: None,
            reject_pass: false,
            fail_first_cwd: true,
        });
        let url = format!("ftp://uploader:s3cret@{addr}/dropzone/");
        bridge.publish_ftp(&file, &url, None).await.unwrap();
        server.join().unwrap();
        let cap = cap.lock().unwrap();
        assert_eq!(cap.user, "uploader");
        assert_eq!(cap.pass, "s3cret");
        assert_eq!(cap.dir, "dropzone");
        assert_eq!(cap.file, "node-00000000000000000001.bundle");
        assert_eq!(cap.bytes, payload);
        assert_eq!(cap.cwd_count, 2, "MKD fallback path");
        assert!(!cap.tls_control && !cap.tls_data);
    }

    #[tokio::test]
    async fn ftps_upload_pins_private_ca() {
        let temp = tempfile::tempdir().unwrap();
        let bridge = exporter(temp.path());
        let (cert_der, key_der, pem) = test_ca();
        let payload = b"sealed bundle bytes".to_vec();
        let file = bundle_file(temp.path(), "node-99.bundle", &payload);
        let (addr, cap, server) = spawn_server(Script {
            tls: Some((cert_der, key_der)),
            reject_pass: false,
            fail_first_cwd: false,
        });
        let url = format!("ftps://{addr}/secure-zone/remote-name.bundle");
        let tls = FtpOptions {
            roots_pem: pem,
            timeout: Duration::from_secs(10),
        };
        bridge.publish_ftp(&file, &url, Some(&tls)).await.unwrap();
        server.join().unwrap();
        let cap = cap.lock().unwrap();
        assert_eq!(cap.user, "anonymous");
        assert_eq!(cap.pass, "anonymous@");
        assert_eq!(cap.dir, "secure-zone");
        assert_eq!(cap.file, "remote-name.bundle");
        assert_eq!(cap.bytes, payload);
        assert!(cap.tls_control && cap.tls_data);
    }

    #[tokio::test]
    async fn ftps_wrong_ca_fails() {
        let temp = tempfile::tempdir().unwrap();
        let bridge = exporter(temp.path());
        let (cert_der, key_der, _) = test_ca();
        let (_, _, other_pem) = test_ca();
        let file = bundle_file(temp.path(), "node-1.bundle", b"data");
        let (addr, _cap, server) = spawn_server(Script {
            tls: Some((cert_der, key_der)),
            reject_pass: false,
            fail_first_cwd: false,
        });
        let url = format!("ftps://{addr}");
        let tls = FtpOptions {
            roots_pem: other_pem,
            timeout: Duration::from_secs(10),
        };
        let err = bridge.publish_ftp(&file, &url, Some(&tls)).await.unwrap_err();
        assert!(
            matches!(err, crate::Error::Unavailable(_)),
            "wrong CA must fail the handshake, got {err:?}"
        );
        server.join().unwrap();
    }

    #[tokio::test]
    async fn ftp_bad_password_is_auth_error() {
        let temp = tempfile::tempdir().unwrap();
        let bridge = exporter(temp.path());
        let file = bundle_file(temp.path(), "node-1.bundle", b"data");
        let (addr, _cap, server) = spawn_server(Script {
            tls: None,
            reject_pass: true,
            fail_first_cwd: false,
        });
        let url = format!("ftp://u:wrong@{addr}");
        let err = bridge.publish_ftp(&file, &url, None).await.unwrap_err();
        assert!(matches!(err, crate::Error::Auth(_)), "got {err:?}");
        server.join().unwrap();
    }

    #[tokio::test]
    async fn ftp_url_and_role_validation() {
        let temp = tempfile::tempdir().unwrap();
        let bridge = exporter(temp.path());
        let file = bundle_file(temp.path(), "node-1.bundle", b"data");
        let tls = FtpOptions {
            roots_pem: b"bogus".to_vec(),
            timeout: Duration::from_secs(2),
        };
        // Malformed URLs never touch the network.
        for url in [
            "http://127.0.0.1/bundle",
            "ftp://",
            "ftp:///only-path",
            "ftp://:21/x",
            "ftp://127.0.0.1:bad/x",
            "ftp://127.0.0.1/../escape",
            "ftp://127.0.0.1/a/./b",
            "ftp://@127.0.0.1/x",
            "ftp://127.0.0.1/bad name!",
        ] {
            let err = bridge.publish_ftp(&file, url, None).await.unwrap_err();
            assert!(matches!(err, crate::Error::Invalid(_)), "{url}: got {err:?}");
        }
        // Scheme/TLS mismatch.
        let err = bridge
            .publish_ftp(&file, "ftps://127.0.0.1/x", None)
            .await
            .unwrap_err();
        assert!(matches!(err, crate::Error::Invalid(_)), "got {err:?}");
        let err = bridge
            .publish_ftp(&file, "ftp://127.0.0.1/x", Some(&tls))
            .await
            .unwrap_err();
        assert!(matches!(err, crate::Error::Invalid(_)), "got {err:?}");
        // Receiver role cannot publish.
        let rx = receiver(temp.path());
        let err = rx
            .publish_ftp(&file, "ftp://127.0.0.1/x", None)
            .await
            .unwrap_err();
        assert!(matches!(err, crate::Error::Invalid(_)), "got {err:?}");
        // Empty and oversized payloads.
        let empty = bundle_file(temp.path(), "empty.bundle", b"");
        let err = bridge
            .publish_ftp(&empty, "ftp://127.0.0.1/x", None)
            .await
            .unwrap_err();
        assert!(matches!(err, crate::Error::Invalid(_)), "got {err:?}");
        let mut limited = BridgeConfig::exporter(
            [1; 16],
            temp.path().join("limited"),
            "s".into(),
            [30; 32],
            crate::bridge::recipient_public([40; 32]),
        );
        limited.max_bundle_bytes = 2;
        let small = Bridge::open(limited, Arc::new(Stub)).unwrap();
        let err = small
            .publish_ftp(&file, "ftp://127.0.0.1/x", None)
            .await
            .unwrap_err();
        assert!(matches!(err, crate::Error::Overloaded), "got {err:?}");
        // Unreachable host is a transport failure, fast (refused).
        let err = bridge
            .publish_ftp(&file, "ftp://127.0.0.1:1/x", None)
            .await
            .unwrap_err();
        assert!(matches!(err, crate::Error::Unavailable(_)), "got {err:?}");
    }
}
