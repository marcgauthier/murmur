# License

## Murmur

MIT License

Copyright (c) 2026 Marc Gauthier

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

## Third-party licenses

Murmur's own code is MIT-licensed as above. The table below lists every Go
module the build uses (direct and transitive, from `go list -m all`,
2026-10-06) and the license each module declares in its own repository.
Modules present only for module-graph resolution (never downloaded or built)
are excluded.

License mix: MIT, BSD-2-Clause, BSD-3-Clause, Apache-2.0, and MPL-2.0.
No dependency uses GPL, LGPL, AGPL, SSPL/BSL, or any other strong-copyleft
license, so MIT for Murmur is compatible with the whole set:

- MIT/BSD/ISC portions require only preservation of their copyright and
  license notices, which this file provides by attribution.
- Apache-2.0 portions are used unmodified; this file serves as the required
  attribution notice. The full Apache-2.0 text ships in each module.
- MPL-2.0 portions (HashiCorp libraries) are used unmodified as libraries,
  which the MPL expressly permits as a "Larger Work"; their file headers
  are untouched.

| Module | Version | License |
|---|---|---|
| github.com/google/btree | v1.1.3 | Apache-2.0 |
| github.com/google/uuid | v1.6.0 | BSD-3-Clause |
| github.com/hashicorp/errwrap | v1.1.0 | MPL-2.0 |
| github.com/hashicorp/go-immutable-radix | v1.3.1 | MPL-2.0 |
| github.com/hashicorp/go-metrics | v0.7.0 | MIT |
| github.com/hashicorp/go-msgpack/v2 | v2.1.5 | MIT |
| github.com/hashicorp/go-multierror | v1.1.1 | MPL-2.0 |
| github.com/hashicorp/go-sockaddr | v1.0.7 | MPL-2.0 |
| github.com/hashicorp/golang-lru | v1.0.2 | MPL-2.0 |
| github.com/hashicorp/golang-lru/v2 | v2.0.7 | MPL-2.0 |
| github.com/hashicorp/memberlist | v0.7.0 | MPL-2.0 |
| github.com/miekg/dns | v1.1.73 | BSD-3-Clause |
| github.com/quic-go/quic-go | v0.63.0 | MIT |
| github.com/sean-/seed | v0.0.0-20170313163322-e2103e2c3529 | MIT AND BSD-3-Clause (Go-runtime portion) |
| golang.org/x/crypto | v0.57.0 | BSD-3-Clause |
| golang.org/x/net | v0.58.0 | BSD-3-Clause |
| golang.org/x/sys | v0.48.0 | BSD-3-Clause |

Regenerate this table after dependency changes with `go list -m all` and a fresh read of each new module license file.
