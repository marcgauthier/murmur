// Package transport implements QUIC connectivity with mutual TLS.
//
// Every node presents a certificate carrying its NodeID as a URI SAN
// (replicateddb://node/<uuid>). The authenticated certificate identity is
// bound to the replication-protocol NodeID so peers cannot spoof each other.
package transport

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/marcgauthier/spedsql/ids"
)

// NextProto is the QUIC/TLS ALPN for replication.
const NextProto = "replicateddb/1"

// NodeURIScheme prefixes the NodeID SAN: replicateddb://node/<uuid>.
const NodeURIScheme = "replicateddb"

// NodeURI returns the URI SAN for a node.
func NodeURI(id ids.NodeID) *url.URL {
	return &url.URL{Scheme: NodeURIScheme, Host: "node", Path: "/" + id.String()}
}

// NodeIDFromCert extracts the NodeID from the certificate's URI SAN.
func NodeIDFromCert(cert *x509.Certificate) (ids.NodeID, error) {
	for _, u := range cert.URIs {
		if u.Scheme != NodeURIScheme || u.Host != "node" {
			continue
		}
		s := strings.TrimPrefix(u.Path, "/")
		return ids.ParseNodeID(s)
	}
	return ids.NodeID{}, fmt.Errorf("transport: certificate lacks node URI SAN")
}

// CA is a cluster certificate authority (ECDSA P-256).
type CA struct {
	Cert    *x509.Certificate
	Key     *ecdsa.PrivateKey
	CertPEM []byte
	KeyPEM  []byte
}

// GenerateCA creates a self-signed cluster CA.
func GenerateCA(ttl time.Duration) (*CA, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, err
	}
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "replicateddb cluster CA"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(ttl),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, err
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, err
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, err
	}
	return &CA{
		Cert:    cert,
		Key:     key,
		CertPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		KeyPEM:  pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
	}, nil
}

// IssueNode issues a node certificate bound to id.
func (ca *CA) IssueNode(id ids.NodeID, ttl time.Duration) (certPEM, keyPEM []byte, err error) {
	return ca.IssueNodeForHosts(id, ttl, []string{"localhost"}, []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")})
}

// IssueNodeForHosts issues a NodeID-bound node certificate with DNS/IP SANs
// suitable for HTTPS when the node certificate is also used by the API.
func (ca *CA) IssueNodeForHosts(id ids.NodeID, ttl time.Duration, dnsNames []string, ips []net.IP) (certPEM, keyPEM []byte, err error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, nil, err
	}
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "replicateddb node " + id.String()},
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.Add(ttl),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		URIs:         []*url.URL{NodeURI(id)},
		DNSNames:     append([]string(nil), dnsNames...),
		IPAddresses:  append([]net.IP(nil), ips...),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.Cert, &key.PublicKey, ca.Key)
	if err != nil {
		return nil, nil, err
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, nil, err
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
		nil
}

// ParseCAPool builds a trust pool from PEM CA certificates.
func ParseCAPool(pemBytes []byte) (*x509.CertPool, error) {
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pemBytes) {
		return nil, fmt.Errorf("transport: no CA certificates in PEM")
	}
	return pool, nil
}

// ParseNodeCert parses a node certificate/key pair.
func ParseNodeCert(certPEM, keyPEM []byte) (tls.Certificate, error) {
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("transport: parse node cert: %w", err)
	}
	if len(cert.Certificate) == 0 {
		return tls.Certificate{}, fmt.Errorf("transport: empty certificate chain")
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("transport: parse leaf: %w", err)
	}
	if _, err := NodeIDFromCert(leaf); err != nil {
		return tls.Certificate{}, err
	}
	cert.Leaf = leaf
	return cert, nil
}
