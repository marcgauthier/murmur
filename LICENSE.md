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
2026-10-04) and the license each module declares in its own repository.
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
- SQLite (embedded via `mattn/go-sqlite3` and `modernc.org/libsqlite`) is
  public domain; the Go bindings above carry their own listed licenses.

| Module | Version | License |
|---|---|---|
| github.com/DataDog/zstd | v1.5.7 | BSD-3-Clause |
| github.com/RaduBerinde/axisds | v0.1.0 | Apache-2.0 |
| github.com/RaduBerinde/btreemap | v0.0.0-20250419174037-3d62b7205d54 | Apache-2.0 |
| github.com/aclements/go-perfevent | v0.0.0-20240301234650-f7843625020f | BSD-3-Clause |
| github.com/beorn7/perks | v1.0.1 | MIT |
| github.com/brianvoe/gofakeit/v7 | v7.17.1 | MIT |
| github.com/cespare/xxhash/v2 | v2.3.0 | MIT |
| github.com/cockroachdb/crlib | v0.0.0-20241112164430-1264a2edc35b | Apache-2.0 |
| github.com/cockroachdb/datadriven | v1.0.3-0.20250407164829-2945557346d5 | Apache-2.0 |
| github.com/cockroachdb/errors | v1.11.3 | Apache-2.0 |
| github.com/cockroachdb/logtags | v0.0.0-20230118201751-21c54148d20b | Apache-2.0 |
| github.com/cockroachdb/metamorphic | v0.0.0-20231108215700-4ba948b56895 | Apache-2.0 |
| github.com/cockroachdb/pebble/v2 | v2.1.6 | BSD-3-Clause |
| github.com/cockroachdb/redact | v1.1.5 | Apache-2.0 |
| github.com/cockroachdb/swiss | v0.0.0-20251224182025-b0f6560f979b | Apache-2.0 |
| github.com/cockroachdb/tokenbucket | v0.0.0-20230807174530-cc333fc44b06 | Apache-2.0 |
| github.com/dustin/go-humanize | v1.0.1 | MIT |
| github.com/ericlagergren/aegis | v0.0.0-20250325060835-cd0defd64358 | BSD-2-Clause |
| github.com/ericlagergren/saferand | v0.0.0-20211228043234-529f04ad6e1a | BSD-2-Clause |
| github.com/ericlagergren/subtle | v0.0.0-20220507045147-890d697da010 | BSD-3-Clause |
| github.com/getsentry/sentry-go | v0.27.0 | MIT |
| github.com/ghemawat/stream | v0.0.0-20171120220530-696b145b53b9 | Apache-2.0 |
| github.com/go-errors/errors | v1.4.2 | MIT |
| github.com/gogo/protobuf | v1.3.2 | BSD-3-Clause |
| github.com/golang/snappy | v0.0.5-0.20231225225746-43d5d4cd4e0e | BSD-3-Clause |
| github.com/google/btree | v1.1.3 | Apache-2.0 |
| github.com/google/go-cmp | v0.7.0 | BSD-3-Clause |
| github.com/google/pprof | v0.0.0-20250317173921-a4b03ec1a45e | Apache-2.0 |
| github.com/google/uuid | v1.6.0 | BSD-3-Clause |
| github.com/hashicorp/errwrap | v1.1.0 | MPL-2.0 |
| github.com/hashicorp/go-immutable-radix | v1.3.1 | MPL-2.0 |
| github.com/hashicorp/go-metrics | v0.7.0 | MIT |
| github.com/hashicorp/go-msgpack/v2 | v2.1.5 | MIT |
| github.com/hashicorp/go-multierror | v1.1.1 | MPL-2.0 |
| github.com/hashicorp/go-sockaddr | v1.0.7 | MPL-2.0 |
| github.com/hashicorp/go-uuid | v1.0.1 | MPL-2.0 |
| github.com/hashicorp/golang-lru | v1.0.2 | MPL-2.0 |
| github.com/hashicorp/golang-lru/v2 | v2.0.7 | MPL-2.0 |
| github.com/hashicorp/memberlist | v0.7.0 | MPL-2.0 |
| github.com/jinzhu/inflection | v1.0.0 | MIT |
| github.com/jinzhu/now | v1.1.5 | MIT |
| github.com/klauspost/compress | v1.19.1 | BSD-3-Clause AND MIT AND Apache-2.0 (per portion) |
| github.com/kr/pretty | v0.3.1 | MIT |
| github.com/kr/text | v0.2.0 | MIT |
| github.com/kylelemons/godebug | v1.1.0 | Apache-2.0 |
| github.com/mattn/go-isatty | v0.0.20 | MIT |
| github.com/mattn/go-sqlite3 | v1.14.32 | MIT |
| github.com/miekg/dns | v1.1.73 | BSD-3-Clause |
| github.com/minio/minlz | v1.0.1-0.20250507153514-87eb42fe8882 | Apache-2.0 |
| github.com/munnerz/goautoneg | v0.0.0-20191010083416-a7dc8b61c822 | BSD-3-Clause |
| github.com/ncruces/go-strftime | v1.0.0 | MIT |
| github.com/pingcap/errors | v0.11.4 | BSD-2-Clause |
| github.com/pkg/errors | v0.9.1 | BSD-2-Clause |
| github.com/pmezard/go-difflib | v1.0.0 | BSD-2-Clause |
| github.com/prometheus/client_golang | v1.24.1 | Apache-2.0 |
| github.com/prometheus/client_model | v0.6.3 | Apache-2.0 |
| github.com/prometheus/common | v0.71.0 | Apache-2.0 |
| github.com/prometheus/procfs | v0.21.1 | Apache-2.0 |
| github.com/quic-go/go-ossfuzz-seeds | v0.1.0 | MIT |
| github.com/quic-go/quic-go | v0.63.0 | MIT |
| github.com/remyoudompheng/bigfft | v0.0.0-20230129092748-24d4a6f8daec | BSD-3-Clause |
| github.com/rogpeppe/go-internal | v1.13.1 | BSD-3-Clause |
| github.com/sean-/seed | v0.0.0-20170313163322-e2103e2c3529 | MIT AND BSD-3-Clause (Go-runtime portion) |
| github.com/stretchr/testify | v1.12.1 | MIT |
| go.uber.org/goleak | v1.3.0 | MIT |
| go.uber.org/mock | v0.5.2 | Apache-2.0 |
| go.yaml.in/yaml/v2 | v2.4.4 | Apache-2.0 AND MIT (libyaml portion) |
| go.yaml.in/yaml/v3 | v3.0.5 | Apache-2.0 AND MIT (libyaml portion) |
| golang.org/x/crypto | v0.55.0 | BSD-3-Clause |
| golang.org/x/exp | v0.0.0-20251023183803-a4bb9ffd2546 | BSD-3-Clause |
| golang.org/x/mod | v0.38.0 | BSD-3-Clause |
| golang.org/x/net | v0.58.0 | BSD-3-Clause |
| golang.org/x/sync | v0.22.0 | BSD-3-Clause |
| golang.org/x/sys | v0.47.0 | BSD-3-Clause |
| golang.org/x/text | v0.41.0 | BSD-3-Clause |
| golang.org/x/tools | v0.48.0 | BSD-3-Clause |
| google.golang.org/protobuf | v1.36.12 | BSD-3-Clause |
| gorm.io/driver/sqlite | v1.6.0 | MIT |
| gorm.io/gorm | v1.31.2 | MIT |
| modernc.org/cc/v4 | v4.27.1 | BSD-2-Clause |
| modernc.org/ccgo/v4 | v4.30.1 | BSD-2-Clause |
| modernc.org/fileutil | v1.3.40 | BSD-2-Clause |
| modernc.org/gc/v2 | v2.6.5 | BSD-3-Clause |
| modernc.org/gc/v3 | v3.1.1 | BSD-3-Clause |
| modernc.org/goabi0 | v0.2.0 | BSD-3-Clause |
| modernc.org/libc | v1.67.6 | BSD-2-Clause |
| modernc.org/mathutil | v1.7.1 | BSD-2-Clause |
| modernc.org/memory | v1.11.0 | BSD-2-Clause |
| modernc.org/opt | v0.1.4 | BSD-2-Clause |
| modernc.org/sortutil | v1.2.1 | BSD-2-Clause |
| modernc.org/sqlite | v1.44.3 | BSD-3-Clause |
| modernc.org/strutil | v1.2.1 | BSD-2-Clause |
| modernc.org/token | v1.1.0 | BSD-3-Clause |

Regenerate this table after dependency changes with `go list -m all` and a fresh read of each new module license file.
