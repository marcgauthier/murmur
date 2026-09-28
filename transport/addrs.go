package transport

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
)

// ErrAddressNotAllowed indicates a peer address rejected by the configured IP
// admission policy. It complements CA, NodeID, and DBID authorization; a
// denied address is refused even when the peer identity is otherwise valid,
// and an allowed address never substitutes for identity.
var ErrAddressNotAllowed = errors.New("transport: address not allowed")

// maxDialCandidates bounds how many resolved addresses one Dial attempts.
const maxDialCandidates = 16

// AddressPolicy is an allow-list of IP addresses and networks. A nil
// *AddressPolicy permits every address, preserving the
// certificate/NodeID-only behavior. Use ParseAddressPolicy to build one from
// configuration; it is immutable and safe for concurrent use.
type AddressPolicy struct {
	nets    []net.IPNet
	entries []string
}

// ParseAddressPolicy parses allow-list entries. Each entry is either CIDR
// notation ("192.0.2.0/24", "2001:db8::/32") or a bare IP address
// ("192.0.2.1", "2001:db8::1", treated as a host route). An empty list
// returns (nil, nil): no filtering. Invalid entries fail fast with the
// offending value identified.
func ParseAddressPolicy(entries []string) (*AddressPolicy, error) {
	if len(entries) == 0 {
		return nil, nil
	}
	p := &AddressPolicy{entries: append([]string(nil), entries...)}
	for _, e := range entries {
		s := strings.TrimSpace(e)
		if s == "" {
			return nil, fmt.Errorf("transport: invalid allowed network %q: empty entry", e)
		}
		if strings.Contains(s, "/") {
			_, n, err := net.ParseCIDR(s)
			if err != nil {
				return nil, fmt.Errorf("transport: invalid allowed network %q: %w", e, err)
			}
			p.nets = append(p.nets, *n)
			continue
		}
		host, _, _ := strings.Cut(s, "%")
		ip := net.ParseIP(host)
		if ip == nil {
			return nil, fmt.Errorf("transport: invalid allowed network %q: not an IP or CIDR", e)
		}
		bits := 128
		if ip.To4() != nil {
			bits = 32
		}
		p.nets = append(p.nets, net.IPNet{IP: ip, Mask: net.CIDRMask(bits, bits)})
	}
	return p, nil
}

// Allows reports whether ip is permitted. A nil policy permits all.
// IPv4-mapped IPv6 addresses are normalized to IPv4 before matching; a nil
// IP is denied.
func (p *AddressPolicy) Allows(ip net.IP) bool {
	if p == nil {
		return true
	}
	if len(ip) == 0 {
		return false
	}
	if v4 := ip.To4(); v4 != nil {
		ip = v4
	}
	for i := range p.nets {
		if p.nets[i].Contains(ip) {
			return true
		}
	}
	return false
}

// AllowsAddr reports whether addr's IP is permitted. Unparseable addresses
// are denied (fail closed).
func (p *AddressPolicy) AllowsAddr(addr net.Addr) bool {
	if p == nil {
		return true
	}
	if addr == nil {
		return false
	}
	switch a := addr.(type) {
	case *net.UDPAddr:
		return p.Allows(a.IP)
	case *net.TCPAddr:
		return p.Allows(a.IP)
	case *net.IPAddr:
		return p.Allows(a.IP)
	default:
		host, _, err := net.SplitHostPort(addr.String())
		if err != nil {
			host = addr.String()
		}
		host, _, _ = strings.Cut(host, "%")
		ip := net.ParseIP(host)
		if ip == nil {
			return false
		}
		return p.Allows(ip)
	}
}

// Filter returns the subset of ips permitted by the policy, preserving order
// and capped at maxDialCandidates entries. A nil policy returns ips unchanged
// (still capped).
func (p *AddressPolicy) Filter(ips []net.IP) []net.IP {
	if p == nil {
		if len(ips) > maxDialCandidates {
			return ips[:maxDialCandidates]
		}
		return ips
	}
	out := make([]net.IP, 0, len(ips))
	for _, ip := range ips {
		if p.Allows(ip) {
			out = append(out, ip)
			if len(out) >= maxDialCandidates {
				break
			}
		}
	}
	return out
}

// DialAddrs resolves addr ("host:port") and returns dialable addresses
// restricted to policy-allowed IPs. Literal IPs are checked directly; names
// are resolved and mixed allowed/denied answers are filtered to the allowed
// subset. It returns ErrAddressNotAllowed when nothing remains.
func (p *AddressPolicy) DialAddrs(ctx context.Context, addr string) ([]string, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, fmt.Errorf("transport: invalid dial address %q: %w", addr, err)
	}
	bare, zone, _ := strings.Cut(host, "%")
	var ips []net.IP
	if ip := net.ParseIP(bare); ip != nil {
		ips = []net.IP{ip}
	} else {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		resolved, err := net.DefaultResolver.LookupIPAddr(ctx, host)
		if err != nil {
			return nil, fmt.Errorf("transport: resolve %q: %w", host, err)
		}
		ips = make([]net.IP, 0, len(resolved))
		for _, r := range resolved {
			ips = append(ips, r.IP)
		}
	}
	allowed := p.Filter(ips)
	if len(allowed) == 0 {
		return nil, fmt.Errorf("transport: dial %q: %w", redactAddr(host, port), ErrAddressNotAllowed)
	}
	out := make([]string, 0, len(allowed))
	for _, ip := range allowed {
		h := ip.String()
		if zone != "" {
			h += "%" + zone
		}
		out = append(out, net.JoinHostPort(h, port))
	}
	return out, nil
}

// Entries returns the configured allow-list entries.
func (p *AddressPolicy) Entries() []string {
	if p == nil {
		return nil
	}
	return append([]string(nil), p.entries...)
}

// redactAddr renders host:port for rejection diagnostics with IPv6 interface
// identifiers stripped to the /64 prefix. IPv4 is shown in full, matching
// firewall-deny log practice; no identity or key material is included.
func redactAddr(host, port string) string {
	bare, _, _ := strings.Cut(host, "%")
	if ip := net.ParseIP(bare); ip != nil && ip.To4() == nil {
		if v6 := ip.To16(); v6 != nil {
			var prefix net.IP = append(net.IP(nil), v6[:8]...)
			prefix = append(prefix, make(net.IP, 8)...)
			return net.JoinHostPort(prefix.String(), port)
		}
	}
	return net.JoinHostPort(host, port)
}

// redactAddrOf renders addr's IP for rejection diagnostics (see redactAddr).
func redactAddrOf(addr net.Addr) string {
	if addr == nil {
		return "<nil>"
	}
	switch a := addr.(type) {
	case *net.UDPAddr:
		return redactIP(a.IP)
	case *net.TCPAddr:
		return redactIP(a.IP)
	case *net.IPAddr:
		return redactIP(a.IP)
	default:
		host, _, err := net.SplitHostPort(addr.String())
		if err != nil {
			host = addr.String()
		}
		host, _, _ = strings.Cut(host, "%")
		return redactIP(net.ParseIP(host))
	}
}

// redactIP renders an IP for rejection diagnostics (see redactAddr).
func redactIP(ip net.IP) string {
	if len(ip) == 0 {
		return "<nil>"
	}
	if ip.To4() != nil {
		return ip.String()
	}
	if v6 := ip.To16(); v6 != nil {
		var prefix net.IP = append(net.IP(nil), v6[:8]...)
		prefix = append(prefix, make(net.IP, 8)...)
		return prefix.String()
	}
	return "<invalid>"
}
