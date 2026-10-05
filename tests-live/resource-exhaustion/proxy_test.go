package resourceexhaustion_test

import (
	"net"
	"sync"
	"testing"
	"time"
)

// queuedDatagram is one paced datagram toward its destination socket.
type queuedDatagram struct {
	dst  *net.UDPConn
	peer *net.UDPAddr // set for listener-socket sends (replies to clients)
	data []byte
}

// bucket is a token-bucket pacer: sustained rate bytes per second with a
// burst allowance. The burst absorbs QUIC's windowed bursts (handshakes,
// stream windows) without loss; only sustained overload drops. Without
// burst tolerance every window loses its tail, loss runs at tens of
// percent, and the connection collapses instead of slowing down.
type bucket struct {
	mu     sync.Mutex
	rate   float64
	burst  float64
	tokens float64
	last   time.Time
}

func newBucket(rate, burst int) *bucket {
	return &bucket{rate: float64(rate), burst: float64(burst), tokens: float64(burst), last: time.Now()}
}

// pace blocks until n bytes fit the bucket.
func (b *bucket) pace(n int) {
	for {
		b.mu.Lock()
		now := time.Now()
		b.tokens += now.Sub(b.last).Seconds() * b.rate
		if b.tokens > b.burst {
			b.tokens = b.burst
		}
		b.last = now
		if b.tokens >= float64(n) {
			b.tokens -= float64(n)
			b.mu.Unlock()
			return
		}
		deficit := float64(n) - b.tokens
		b.tokens = 0
		b.mu.Unlock()
		time.Sleep(time.Duration(deficit / b.rate * float64(time.Second)))
	}
}

// throttleProxy is a transparent UDP forwarder with a token-bucket rate
// limit in BOTH directions: it emulates a slow link the way tc-tbf does,
// including drop-tail on queue overflow. QUIC identifies connections by
// connection ID, so NAT-style forwarding (one server-side socket per
// client address) is transparent to both ends.
//
// Two properties are load-bearing for QUIC stability:
//
//  1. Read loops never block on pacing: a pacer stall in the only reader
//     lets the kernel drop datagrams in bursts the sender never paced for.
//     Reads enqueue; sender goroutines pace the drain.
//  2. Queues are bounded with drop-tail: an unbounded queue turns
//     overload into unbounded delay (bufferbloat), which QUIC reads as a
//     dead path and stalls over. Drops are the backpressure signal its
//     congestion control is designed for.
type throttleProxy struct {
	t        *testing.T
	listener *net.UDPConn
	target   *net.UDPAddr

	mu      sync.Mutex
	servers map[string]*net.UDPConn // client addr -> server-side socket
	closed  bool

	fwdHi, fwdLo chan queuedDatagram
	bwdHi, bwdLo chan queuedDatagram
	serveDone    chan struct{}
	serveOnce    sync.Once
	fwdBucket    *bucket
	bwdBucket    *bucket
	fwdPackets   uint64
	fwdBytes     uint64
	fwdDrops     uint64
	bwdPackets   uint64
	bwdBytes     uint64
	bwdDrops     uint64
}

// proxyQueueDepth bounds each direction's queue like tc-tbf's latency
// limit: burst absorption comes from the token bucket, not the queue.
// A deep queue would add seconds of delay, tripping QUIC's ~1s RTO into
// spurious retransmit storms (goodput collapse); 32 datagrams cap delay
// around half a second at 64 KiB/s while sustained overload drops.
const proxyQueueDepth = 32

// proxyBurstBytes is the token-bucket burst allowance per direction.
const proxyBurstBytes = 256 << 10

func startThrottleProxy(t *testing.T, targetAddr string, rate int) *throttleProxy {
	t.Helper()
	target, err := net.ResolveUDPAddr("udp", targetAddr)
	if err != nil {
		t.Fatalf("resolve proxy target %s: %v", targetAddr, err)
	}
	listener, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatalf("proxy listen: %v", err)
	}
	p := &throttleProxy{
		t:         t,
		listener:  listener,
		target:    target,
		servers:   map[string]*net.UDPConn{},
		fwdHi:     make(chan queuedDatagram, proxyQueueDepth),
		fwdLo:     make(chan queuedDatagram, proxyQueueDepth),
		bwdHi:     make(chan queuedDatagram, proxyQueueDepth),
		bwdLo:     make(chan queuedDatagram, proxyQueueDepth),
		serveDone: make(chan struct{}),
		fwdBucket: newBucket(rate, proxyBurstBytes),
		bwdBucket: newBucket(rate, proxyBurstBytes),
	}
	go p.serve()
	go p.sender(p.fwdHi, p.fwdLo, p.fwdBucket, true)
	go p.sender(p.bwdHi, p.bwdLo, p.bwdBucket, false)
	return p
}

// Addr returns the proxy's listen address (what slow peers dial through).
func (p *throttleProxy) Addr() string { return p.listener.LocalAddr().String() }

func (p *throttleProxy) Close() {
	p.mu.Lock()
	already := p.closed
	p.closed = true
	for _, s := range p.servers {
		_ = s.Close()
	}
	p.mu.Unlock()
	if already {
		return
	}
	_ = p.listener.Close() // unblocks serve, which closes the queues
	<-p.serveDone          // then the senders drain and exit
}

// ProxyStats snapshots forwarding counters so a stall can be attributed
// to the proxy (flows frozen) or the endpoints (bytes flow, rows stall).
type ProxyStats struct {
	FwdPackets, FwdBytes, FwdDrops uint64
	BwdPackets, BwdBytes, BwdDrops uint64
}

func (p *throttleProxy) Stats() ProxyStats {
	p.mu.Lock()
	defer p.mu.Unlock()
	return ProxyStats{p.fwdPackets, p.fwdBytes, p.fwdDrops, p.bwdPackets, p.bwdBytes, p.bwdDrops}
}

// enqueue appends a datagram to a direction's bounded queues, dropping
// (and counting) on overflow like a tc queue limit. Small packets
// (QUIC ACKs, keepalives) take the priority lane: in a single drop-tail
// queue, bulk data starves ACKs, the sender's ACK clock stops, and the
// connection collapses instead of slowing down. Priority still consumes
// wire tokens: it is ordering, not free bandwidth.
func (p *throttleProxy) enqueue(d queuedDatagram, fwd bool) {
	hi, lo := p.fwdHi, p.fwdLo
	if !fwd {
		hi, lo = p.bwdHi, p.bwdLo
	}
	q := lo
	if len(d.data) <= 300 {
		q = hi
	}
	select {
	case q <- d:
	default:
		p.mu.Lock()
		if fwd {
			p.fwdDrops++
		} else {
			p.bwdDrops++
		}
		p.mu.Unlock()
	}
}

func (p *throttleProxy) serve() {
	defer func() {
		close(p.fwdHi)
		close(p.fwdLo)
		close(p.bwdHi)
		close(p.bwdLo)
		p.serveOnce.Do(func() { close(p.serveDone) })
	}()
	_ = p.listener.SetReadBuffer(4 << 20)
	buf := make([]byte, 65535)
	for {
		n, client, err := p.listener.ReadFromUDP(buf)
		if err != nil {
			return // closed
		}
		datagram := append([]byte(nil), buf[:n]...)
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return
		}
		server, ok := p.servers[client.String()]
		if !ok {
			var err error
			server, err = net.DialUDP("udp", nil, p.target)
			if err != nil {
				p.mu.Unlock()
				p.t.Logf("proxy: dial target: %v", err)
				continue
			}
			p.servers[client.String()] = server
			go p.backward(server, client)
		}
		p.mu.Unlock()
		p.enqueue(queuedDatagram{dst: server, data: datagram}, true)
	}
}

// sender drains one direction's queues through its token bucket, serving
// the priority lane first and blocking for either when both are empty.
func (p *throttleProxy) sender(hi, lo <-chan queuedDatagram, b *bucket, fwd bool) {
	for {
		if hi == nil && lo == nil {
			return // queues shut: pending items drop with the proxy
		}
		var d queuedDatagram
		select {
		case v, ok := <-hi:
			if !ok {
				hi = nil
			} else {
				d = v
			}
		default:
			select {
			case v, ok := <-hi:
				if !ok {
					hi = nil
					continue
				}
				d = v
			case v, ok := <-lo:
				if !ok {
					lo = nil
					continue
				}
				d = v
			}
		}
		if d.data == nil {
			continue
		}
		b.pace(len(d.data))
		var err error
		if d.peer != nil {
			_, err = p.listener.WriteToUDP(d.data, d.peer)
		} else {
			_, err = d.dst.Write(d.data)
		}
		if err != nil {
			return
		}
		p.mu.Lock()
		if fwd {
			p.fwdPackets++
			p.fwdBytes += uint64(len(d.data))
		} else {
			p.bwdPackets++
			p.bwdBytes += uint64(len(d.data))
		}
		p.mu.Unlock()
	}
}

// backward relays target->client datagrams for one server-side socket
// into the paced backward queue. It never times out: an idle relay must
// survive so a quiet connection's return path still works when traffic
// resumes. Close unblocks it.
func (p *throttleProxy) backward(server *net.UDPConn, client *net.UDPAddr) {
	buf := make([]byte, 65535)
	for {
		n, err := server.Read(buf)
		if err != nil {
			return // socket closed by Close
		}
		p.enqueue(queuedDatagram{peer: client, data: append([]byte(nil), buf[:n]...)}, false)
	}
}
