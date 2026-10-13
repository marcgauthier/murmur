package main

import (
	"context"
	"errors"
	"fmt"
	murmur "github.com/marcgauthier/murmur/clients/go"
	"github.com/marcgauthier/murmur/clients/go/examples/internal/demo"
)

func run(ctx context.Context, e *demo.Env) error {
	c, err := demo.NewCluster(2)
	if err != nil {
		return err
	}
	seed := c.Configs[0].SigningKey
	trusted := c.Configs[1].TrustedOrigins
	tx := murmur.OriginTransaction{Database: c.Database, Origin: c.Nodes[0], Sequence: 1, ID: c.Nodes[1], Payload: []byte{0, 1, 255}}
	if err = tx.Sign(seed); err != nil {
		return err
	}
	if err = tx.Verify(c.Database, trusted, 1024); err != nil {
		return err
	}
	if err = tx.VerifyHeader(c.Database, trusted); err != nil {
		return err
	}
	wire, err := murmur.Encode("Transaction", tx)
	if err != nil {
		return err
	}
	var decoded murmur.OriginTransaction
	if err = murmur.Decode("Transaction", wire, &decoded); err != nil {
		return err
	}
	digest, err := decoded.Digest()
	if err != nil {
		return err
	}
	decoded.Payload[0] ^= 1
	if err = decoded.Verify(c.Database, trusted, 1024); err == nil {
		return errors.New("tampered transaction accepted")
	}
	schema := murmur.SignedSchema{Database: c.Database, Signer: c.Nodes[0], Sequence: 1, Schema: []byte{0}}
	if err = schema.Sign(seed); err != nil {
		return err
	}
	if err = schema.Verify(c.Database, c.Nodes[0], trusted, 1024); err != nil {
		return err
	}
	if _, err = schema.Digest(); err != nil {
		return err
	}
	key, err := murmur.StorageKey(murmur.KeyRequest{Kind: "log", Origin: c.Nodes[0], Sequence: 1})
	if err != nil {
		return err
	}
	constants, err := murmur.Constants()
	if err != nil {
		return err
	}
	node, err := murmur.CertificateNodeID(c.Configs[0].Credentials.Chain[0])
	if err != nil {
		return err
	}
	if err = demo.Require(node == c.Nodes[0], "certificate identity mismatch"); err != nil {
		return err
	}
	allowed, err := murmur.NetworkAllowed(c.Configs[0], "127.0.0.1:1234")
	if err != nil {
		return err
	}
	if err = demo.Require(allowed, "loopback excluded"); err != nil {
		return err
	}
	a, err := murmur.NewEndpoint(c.Configs[0], c.Nodes[0])
	if err != nil {
		return err
	}
	e.Defer(a.Close)
	b, err := murmur.NewEndpoint(c.Configs[1], c.Nodes[1])
	if err != nil {
		return err
	}
	e.Defer(b.Close)
	// Closing either endpoint cancels a blocked accept/connect on timeout.
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			a.Close()
			b.Close()
		case <-done:
		}
	}()
	address, err := b.Address()
	if err != nil {
		return err
	}
	type acceptance struct {
		connection *murmur.Connection
		err        error
	}
	accepted := make(chan acceptance, 1)
	go func() { connection, err := b.Accept(); accepted <- acceptance{connection, err} }()
	var server *murmur.Connection
	defer func() {
		if server == nil {
			b.Close()
			result := <-accepted
			if result.connection != nil {
				result.connection.Close()
			}
		}
	}()
	client, err := a.Connect(address)
	if err != nil {
		return err
	}
	e.Defer(client.Close)
	result := <-accepted
	if result.err != nil {
		return result.err
	}
	server = result.connection
	e.Defer(server.Close)
	peer, err := client.PeerID(c.Configs[0])
	if err != nil {
		return err
	}
	if err = demo.Require(peer == c.Nodes[1], "unexpected peer"); err != nil {
		return err
	}
	send, recv, err := client.OpenBi()
	if err != nil {
		return err
	}
	e.Defer(send.Close)
	e.Defer(recv.Close)
	ping, err := murmur.NewMessage("Ping", uint64(42))
	if err != nil {
		return err
	}
	if err = send.WriteMessage(ping); err != nil {
		return err
	}
	reply, input, err := server.AcceptBi()
	if err != nil {
		return err
	}
	e.Defer(reply.Close)
	e.Defer(input.Close)
	message, err := input.ReadMessage()
	if err != nil {
		return err
	}
	var n uint64
	if err = message.DecodeBody(&n); err != nil {
		return err
	}
	if err = demo.Require(message.Kind == "Ping" && n == 42, "Ping mismatch"); err != nil {
		return err
	}
	pong, err := murmur.NewMessage("Pong", n)
	if err != nil {
		return err
	}
	if err = reply.WriteMessage(pong); err != nil {
		return err
	}
	message, err = recv.ReadMessage()
	if err != nil {
		return err
	}
	if err = message.DecodeBody(&n); err != nil {
		return err
	}
	if err = demo.Require(message.Kind == "Pong" && n == 42, "Pong mismatch"); err != nil {
		return err
	}
	cancelled := make(chan error, 1)
	go func() { _, err := recv.ReadMessage(); cancelled <- err }()
	if err = recv.Close(); err != nil {
		return err
	}
	select {
	case err := <-cancelled:
		if err == nil {
			return errors.New("closed receive stream succeeded")
		}
	case <-ctx.Done():
		return ctx.Err()
	}
	if err = reply.Finish(); err != nil {
		return err
	}
	if err = send.Finish(); err != nil {
		return err
	}
	e.Print("Authenticated QUIC Ping/Pong; signed postcard transaction %d bytes, digest=%x; protocol=%d, log=%s", len(wire), digest[:4], constants.Version, key)
	if _, err = murmur.CertificateNodeID([]byte("invalid")); !errors.Is(err, murmur.ErrReplicationAuth) {
		return fmt.Errorf("invalid certificate: %w", err)
	}
	return nil
}
func main() { demo.Main("protocol", run) }
