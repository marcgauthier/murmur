package demo

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"math/big"
	"net/url"
	"time"

	murmur "github.com/marcgauthier/murmur/clients/go"
)

type Cluster struct {
	Database murmur.NodeID
	Nodes    []murmur.NodeID
	Configs  []murmur.ReplicationConfig
}

func NewCluster(count int) (*Cluster, error) {
	database, err := ID()
	if err != nil {
		return nil, err
	}
	c := &Cluster{Database: database}
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "Murmur example CA"}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, public, private)
	if err != nil {
		return nil, err
	}
	for i := 0; i < count; i++ {
		id, err := ID()
		if err != nil {
			return nil, err
		}
		c.Nodes = append(c.Nodes, id)
		pub, priv, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			return nil, err
		}
		uri, err := url.Parse("replicateddb://node/" + hex.EncodeToString(id[:]))
		if err != nil {
			return nil, err
		}
		cert := &x509.Certificate{SerialNumber: big.NewInt(int64(i + 2)), Subject: pkix.Name{CommonName: "Murmur example peer"}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), URIs: []*url.URL{uri}, DNSNames: []string{"localhost", "murmur"}, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth}}
		der, err := x509.CreateCertificate(rand.Reader, cert, ca, pub, private)
		if err != nil {
			return nil, err
		}
		keyDER, err := x509.MarshalPKCS8PrivateKey(priv)
		if err != nil {
			return nil, err
		}
		signing, err := Key()
		if err != nil {
			return nil, err
		}
		cfg, err := murmur.NewReplicationConfig("127.0.0.1:0", murmur.Credentials{Chain: []murmur.Bytes{der}, PrivateKey: keyDER, Roots: []murmur.Bytes{caDER}}, signing)
		if err != nil {
			return nil, err
		}
		cfg.IncomingInterval = 10 * time.Millisecond
		cfg.AntiEntropyInterval = 30 * time.Millisecond
		cfg.Membership = false
		c.Configs = append(c.Configs, cfg)
	}
	for i := range c.Configs {
		for j, id := range c.Nodes {
			pub, err := murmur.PublicKey(c.Configs[j].SigningKey)
			if err != nil {
				return nil, err
			}
			c.Configs[i].TrustedOrigins = append(c.Configs[i].TrustedOrigins, murmur.TrustEntry{Node: id, Key: pub})
			if i != j {
				c.Configs[i].AllowedPeers = append(c.Configs[i].AllowedPeers, id)
				c.Configs[i].TrustedSnapshotSources = append(c.Configs[i].TrustedSnapshotSources, id)
			}
		}
	}
	return c, nil
}
func (c *Cluster) Open(e *Env, name string, i int, configure func(*murmur.Options)) (*murmur.DB, error) {
	opts := Options()
	opts.DatabaseID = &c.Database
	opts.NodeID = &c.Nodes[i]
	opts.Replication = &c.Configs[i]
	if configure != nil {
		configure(&opts)
	}
	return e.Open(name, opts)
}
func Connect(e *Env, a, b *murmur.DB) (*murmur.Replication, *murmur.Replication, error) {
	ar, err := a.Replication()
	if err != nil {
		return nil, nil, err
	}
	e.Defer(ar.Release)
	br, err := b.Replication()
	if err != nil {
		return nil, nil, err
	}
	e.Defer(br.Release)
	status, err := br.Status()
	if err != nil {
		return nil, nil, err
	}
	if status.Address == nil {
		return nil, nil, murmur.ErrUnavailable
	}
	if err = ar.AddPeer(*status.Address); err != nil {
		return nil, nil, err
	}
	return ar, br, nil
}
