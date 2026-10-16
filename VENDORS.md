# VENDORS

This inventory separates crates declared directly in Murmur’s Cargo manifests from crates they bring in transitively. It covers the root workspace `Cargo.lock` and the standalone `tests-benchmark/Cargo.lock` and `clients/go/native/Cargo.lock`; locked versions are shown. Dev/test, build, optional, and platform-specific dependencies are included. Rust standard-library components and Murmur’s own path dependencies are not external crates.

“Direct” means declared in a Murmur package manifest, including dev/test or build dependencies; it does not assert that source code directly imports the crate. A crate declared directly by one Murmur package can also be transitive for another. Crates not declared in any Murmur manifest are listed as transitive.

Crate authors below come from crate manifests when available. Cargo metadata does not consistently provide an organization or country of origin, so country is marked `Not stated` rather than inferred from a maintainer’s personal location.

## Direct dependencies

| Crate | Locked version(s) | Declared by (scope) | Author(s) / organization stated by crate | Country of origin |
|---|---:|---|---|---|
| `aes-gcm` | `0.10.3` | murmur-replication (runtime), murmur-spool (runtime) | RustCrypto Developers | Not stated |
| `argon2` | `0.5.3` | murmur-spool (runtime) | RustCrypto Developers | Not stated |
| `blake3` | `1.8.7` | sqlite-session (runtime), sqlite-session-benchmark (runtime) | Jack O'Connor <oconnor663@gmail.com>, Samuel Neves | Not stated |
| `chacha20poly1305` | `0.10.1` | murmur-replication (runtime) | RustCrypto Developers | Not stated |
| `crc32c` | `0.6.8` | murmur-spool (runtime) | Zack Owens | Not stated |
| `ed25519-dalek` | `2.2.0` | murmur-replication (runtime) | isis lovecruft <isis@patternsinthevoid.net>, Tony Arcieri <bascule@gmail.com>, Michael Rosenberg <michael@mrosenberg.pub> | Not stated |
| `flate2` | `1.1.10` | murmur-spool (runtime) | Alex Crichton <alex@alexcrichton.com>, Josh Triplett <josh@joshtriplett.org> | Not stated |
| `fs2` | `0.4.3` | murmur-spool (runtime) | Dan Burkert <dan@danburkert.com> | Not stated |
| `hkdf` | `0.12.4` | murmur-replication (runtime), murmur-spool (runtime) | RustCrypto Developers | Not stated |
| `ipnet` | `2.12.2` | murmur-replication (runtime) | Kris Price <kris@krisprice.nz> | Not stated |
| `libc` | `0.2.190` | murmur-spool (runtime) | Not stated in crate metadata | Not stated |
| `postcard` | `1.1.3` | murmur-replication (runtime), sqlite-session (runtime) | James Munns <james@onevariable.com> | Not stated |
| `quinn` | `0.11.12` | murmur-replication (runtime) | Not stated in crate metadata | Not stated |
| `rand` | `0.10.3`, `0.8.8` | murmur-replication (runtime), murmur-spool (runtime), sqlite-session (runtime) | The Rand Project Developers, The Rust Project Developers | Not stated |
| `rand` | `0.10.3`, `0.8.8` | The Rand Project Developers, The Rust Project Developers | Not stated |
| `rcgen` | `0.13.2` | sqlite-session (dev/test), sqlite-session-benchmark (runtime), murmur-replication (dev/test) | Not stated in crate metadata | Not stated |
| `reqwest` | `0.12.28` | murmur-replication (runtime) | Sean McArthur <sean@seanmonstar.com> | Not stated |
| `rusqlite` | `0.40.2` | sqlite-session (runtime), sqlite-session-benchmark (runtime) | The rusqlite developers | Not stated |
| `sqlite-vec` | `0.1.6` | sqlite-session (optional runtime, `vector` feature) | Alex Garcia <alexsebastian.garcia@gmail.com>; no company stated in crate metadata | Not stated |
| `rustls` | `0.23.45` | murmur-replication (runtime) | Not stated in crate metadata | Not stated |
| `rustls-pemfile` | `2.2.0` | murmur-replication (runtime) | Not stated in crate metadata | Not stated |
| `serde` | `1.0.229` | murmur-replication (runtime), sqlite-session (runtime) | Erick Tryzelaar <erick.tryzelaar@gmail.com>, David Tolnay <dtolnay@gmail.com> | Not stated |
| `serde_json` | `1.0.154` | sqlite-session (runtime), sqlite-session-benchmark (runtime), murmur-go (runtime) | Not stated in crate metadata | Not stated |
| `sha2` | `0.10.9` | murmur-replication (runtime), murmur-spool (runtime) | RustCrypto Developers | Not stated |
| `tempfile` | `3.27.0` | murmur-spool (dev/test), sqlite-session (dev/test), sqlite-session-benchmark (runtime), murmur-replication (dev/test) | Steven Allen <steven@stebalien.com>, The Rust Project Developers, Ashley Mannix <ashleymannix@live.com.au>, Jason White <me@jasonwhite.io> | Not stated |
| `thiserror` | `2.0.21` | murmur-replication (runtime), sqlite-session (runtime) | David Tolnay <dtolnay@gmail.com> | Not stated |
| `tokio` | `1.53.2` | murmur-replication (runtime) | Tokio Contributors <team@tokio.rs> | Not stated |
| `x25519-dalek` | `2.0.1` | murmur-replication (runtime) | Isis Lovecruft <isis@patternsinthevoid.net>, DebugSteven <debugsteven@gmail.com>, Henry de Valence <hdevalence@hdevalence.ca> | Not stated |
| `x509-parser` | `0.18.1` | murmur-replication (runtime) | Pierre Chifflier <chifflier@wzdftpd.net> | Not stated |
| `zeroize` | `1.9.1` | murmur-replication (runtime), sqlite-session (runtime) | The RustCrypto Project Developers | Not stated |
| `zstd` | `0.13.3` | murmur-replication (runtime), sqlite-session (runtime) | Alexandre Bury <alexandre.bury@gmail.com> | Not stated |

## Transitive dependencies

These crates are in the lockfile but are not declared directly by any Murmur package manifest.

| Crate | Locked version | Author(s) / organization stated by crate | Country of origin |
|---|---:|---|---|
| `adler2` | `2.0.1` | Jonas Schievink <jonasschievink@gmail.com>, oyvindln <oyvindln@users.noreply.github.com> | Not stated |
| `aead` | `0.5.2` | RustCrypto Developers | Not stated |
| `aes` | `0.8.4` | RustCrypto Developers | Not stated |
| `aho-corasick` | `1.1.5` | Andrew Gallant <jamslam@gmail.com> | Not stated |
| `arrayvec` | `0.7.8` | bluss | Not stated |
| `asn1-rs` | `0.7.2` | Pierre Chifflier <chifflier@wzdftpd.net> | Not stated |
| `asn1-rs-derive` | `0.6.0` | Pierre Chifflier <chifflier@wzdftpd.net> | Not stated |
| `asn1-rs-impl` | `0.2.0` | Pierre Chifflier <chifflier@wzdftpd.net> | Not stated |
| `atomic-polyfill` | `1.0.3` | Not stated in crate metadata | Not stated |
| `atomic-waker` | `1.1.2` | Stjepan Glavina <stjepang@gmail.com>, Contributors to futures-rs | Not stated |
| `autocfg` | `1.5.1` | Josh Stone <cuviper@gmail.com> | Not stated |
| `base64` | `0.22.1`, `0.23.1` | Marshall Pierce <marshall@mpierce.org> | Not stated |
| `base64ct` | `1.8.3` | RustCrypto Developers | Not stated |
| `bindgen` | `0.72.1` | Jyun-Yan You <jyyou.tw@gmail.com>, Emilio Cobos Álvarez <emilio@crisal.io>, Nick Fitzgerald <fitzgen@gmail.com>, The Servo project developers | Not stated |
| `bitflags` | `2.13.2` | The Rust Project Developers | Not stated |
| `blake2` | `0.10.6` | RustCrypto Developers | Not stated |
| `block-buffer` | `0.10.4` | RustCrypto Developers | Not stated |
| `bumpalo` | `3.20.3` | Not stated in crate metadata | Not stated |
| `byteorder` | `1.5.0` | Andrew Gallant <jamslam@gmail.com> | Not stated |
| `bytes` | `1.12.1` | Carl Lerche <me@carllerche.com>, Sean McArthur <sean@seanmonstar.com> | Not stated |
| `cc` | `1.8.0` | Not stated in crate metadata | Not stated |
| `cexpr` | `0.6.0` | Jethro Beekman <jethro@jbeekman.nl> | Not stated |
| `cfg-if` | `1.0.5` | Alex Crichton <alex@alexcrichton.com> | Not stated |
| `cfg_aliases` | `0.2.2` | Zicklag <zicklag@katharostech.com> | Not stated |
| `chacha20` | `0.10.2`, `0.9.1` | RustCrypto Developers | Not stated |
| `cipher` | `0.4.4` | RustCrypto Developers | Not stated |
| `clang-sys` | `1.9.1` | Kyle Mayes <kyle@mayeses.com> | Not stated |
| `cobs` | `0.3.0` | Allen Welkie <>, James Munns <james@onevariable.com> | Not stated |
| `const-oid` | `0.9.6` | RustCrypto Developers | Not stated |
| `constant_time_eq` | `0.4.2` | Cesar Eduardo Barros <cesarb@cesarb.eti.br> | Not stated |
| `cpufeatures` | `0.2.17`, `0.3.1` | RustCrypto Developers | Not stated |
| `crc32fast` | `1.5.2` | Sam Rijs <srijs@airpost.net>, Alex Crichton <alex@alexcrichton.com> | Not stated |
| `critical-section` | `1.2.0` | Not stated in crate metadata | Not stated |
| `crypto-common` | `0.1.7` | RustCrypto Developers | Not stated |
| `ctr` | `0.9.2` | RustCrypto Developers | Not stated |
| `curve25519-dalek` | `4.1.3` | Isis Lovecruft <isis@patternsinthevoid.net>, Henry de Valence <hdevalence@hdevalence.ca> | Not stated |
| `curve25519-dalek-derive` | `0.1.1` | Not stated in crate metadata | Not stated |
| `data-encoding` | `2.11.1` | Not stated in crate metadata | Not stated |
| `der` | `0.7.10` | RustCrypto Developers | Not stated |
| `der-parser` | `10.0.0` | Pierre Chifflier <chifflier@wzdftpd.net> | Not stated |
| `deranged` | `0.5.8`, `0.5.9` | Jacob Pratt <jacob@jhpratt.dev> | Not stated |
| `digest` | `0.10.7` | RustCrypto Developers | Not stated |
| `displaydoc` | `0.2.7` | Jane Lusby <jlusby@yaah.dev> | Not stated |
| `ed25519` | `2.2.3` | RustCrypto Developers | Not stated |
| `either` | `1.19.0` | Not stated in crate metadata | Not stated |
| `embedded-io` | `0.4.0`, `0.6.1` | Not stated in crate metadata | Not stated |
| `errno` | `0.3.14` | Chris Wong <lambda.fairy@gmail.com>, Dan Gohman <dev@sunfishcode.online> | Not stated |
| `fallible-iterator` | `0.3.0` | Steven Fackler <sfackler@gmail.com> | Not stated |
| `fallible-streaming-iterator` | `0.1.9` | Steven Fackler <sfackler@gmail.com> | Not stated |
| `fastrand` | `2.5.0` | Stjepan Glavina <stjepang@gmail.com> | Not stated |
| `fiat-crypto` | `0.2.9` | Not stated in crate metadata | Not stated |
| `find-msvc-tools` | `0.1.14` | Not stated in crate metadata | Not stated |
| `foldhash` | `0.2.0` | Orson Peters <orsonpeters@gmail.com> | Not stated |
| `form_urlencoded` | `1.2.2` | The rust-url developers | Not stated |
| `futures-channel` | `0.3.34` | Not stated in crate metadata | Not stated |
| `futures-core` | `0.3.34` | Not stated in crate metadata | Not stated |
| `futures-task` | `0.3.34` | Not stated in crate metadata | Not stated |
| `futures-util` | `0.3.34` | Not stated in crate metadata | Not stated |
| `generic-array` | `0.14.7` | Bartłomiej Kamiński <fizyk20@gmail.com>, Aaron Trent <novacrazy@gmail.com> | Not stated |
| `getrandom` | `0.2.17`, `0.4.3` | The Rand Project Developers | Not stated |
| `ghash` | `0.5.1` | RustCrypto Developers | Not stated |
| `glob` | `0.3.4` | The Rust Project Developers | Not stated |
| `hash32` | `0.2.1` | Jorge Aparicio <jorge@japaric.io> | Not stated |
| `hashbrown` | `0.16.1`, `0.17.1` | Amanieu d'Antras <amanieu@gmail.com> | Not stated |
| `hashbrown` | `0.16.1`, `0.17.1` | Not stated in crate metadata | Not stated |
| `hashlink` | `0.12.2` | Not stated in crate metadata | Not stated |
| `heapless` | `0.7.17` | Jorge Aparicio <jorge@japaric.io>, Per Lindgren <per.lindgren@ltu.se>, Emil Fresk <emil.fresk@gmail.com> | Not stated |
| `hmac` | `0.12.1` | RustCrypto Developers | Not stated |
| `http` | `1.5.0` | Alex Crichton <alex@alexcrichton.com>, Carl Lerche <me@carllerche.com>, Sean McArthur <sean@seanmonstar.com> | Not stated |
| `http-body` | `1.1.0` | Carl Lerche <me@carllerche.com>, Lucio Franco <luciofranco14@gmail.com>, Sean McArthur <sean@seanmonstar.com> | Not stated |
| `http-body-util` | `0.1.5` | Carl Lerche <me@carllerche.com>, Lucio Franco <luciofranco14@gmail.com>, Sean McArthur <sean@seanmonstar.com> | Not stated |
| `httparse` | `1.10.1` | Sean McArthur <sean@seanmonstar.com> | Not stated |
| `hyper` | `1.12.0` | Sean McArthur <sean@seanmonstar.com> | Not stated |
| `hyper-rustls` | `0.27.10` | Not stated in crate metadata | Not stated |
| `hyper-util` | `0.1.21` | Sean McArthur <sean@seanmonstar.com> | Not stated |
| `icu_collections` | `2.3.0` | The ICU4X Project Developers | Not stated |
| `icu_locale_core` | `2.3.0` | The ICU4X Project Developers | Not stated |
| `icu_normalizer` | `2.3.0` | The ICU4X Project Developers | Not stated |
| `icu_normalizer_data` | `2.3.0` | The ICU4X Project Developers | Not stated |
| `icu_properties` | `2.3.0` | The ICU4X Project Developers | Not stated |
| `icu_properties_data` | `2.3.0` | The ICU4X Project Developers | Not stated |
| `icu_provider` | `2.3.1` | The ICU4X Project Developers | Not stated |
| `idna` | `1.1.0` | The rust-url developers | Not stated |
| `idna_adapter` | `1.2.2` | The rust-url developers | Not stated |
| `inout` | `0.1.4` | RustCrypto Developers | Not stated |
| `itertools` | `0.13.0` | bluss | Not stated |
| `itoa` | `1.0.18` | David Tolnay <dtolnay@gmail.com> | Not stated |
| `jobserver` | `0.1.35` | Alex Crichton <alex@alexcrichton.com> | Not stated |
| `js-sys` | `0.3.106` | Not stated in crate metadata | Not stated |
| `lazy_static` | `1.5.1` | Marvin Löbel <loebel.marvin@gmail.com> | Not stated |
| `libloading` | `0.8.9` | Simonas Kazlauskas <libloading@kazlauskas.me> | Not stated |
| `libsqlite3-sys` | `0.38.2` | The rusqlite developers | Not stated |
| `linux-raw-sys` | `0.12.1` | Dan Gohman <dev@sunfishcode.online> | Not stated |
| `litemap` | `0.8.3` | The ICU4X Project Developers | Not stated |
| `lock_api` | `0.4.14` | Amanieu d'Antras <amanieu@gmail.com> | Not stated |
| `log` | `0.4.34` | The Rust Project Developers | Not stated |
| `lru-slab` | `0.1.3` | Benjamin Saunders <ben.e.saunders@gmail.com> | Not stated |
| `memchr` | `2.8.3` | Andrew Gallant <jamslam@gmail.com>, bluss | Not stated |
| `minimal-lexical` | `0.2.1` | Alex Huszagh <ahuszagh@gmail.com> | Not stated |
| `miniz_oxide` | `0.9.1` | Frommi <daniil.liferenko@gmail.com>, oyvindln <oyvindln@users.noreply.github.com>, Rich Geldreich richgel99@gmail.com | Not stated |
| `mio` | `1.2.4` | Carl Lerche <me@carllerche.com>, Thomas de Zeeuw <thomasdezeeuw@gmail.com>, Tokio Contributors <team@tokio.rs> | Not stated |
| `nom` | `7.1.3` | contact@geoffroycouprie.com | Not stated |
| `num-bigint` | `0.4.8` | The Rust Project Developers | Not stated |
| `num-conv` | `0.2.2` | Jacob Pratt <jacob@jhpratt.dev> | Not stated |
| `num-integer` | `0.1.47` | The Rust Project Developers | Not stated |
| `num-traits` | `0.2.19` | The Rust Project Developers | Not stated |
| `oid-registry` | `0.8.1` | Pierre Chifflier <chifflier@wzdftpd.net> | Not stated |
| `once_cell` | `1.21.4` | Aleksey Kladov <aleksey.kladov@gmail.com> | Not stated |
| `opaque-debug` | `0.3.1` | RustCrypto Developers | Not stated |
| `password-hash` | `0.5.0` | RustCrypto Developers | Not stated |
| `pem` | `3.0.6` | Jonathan Creekmore <jonathan@thecreekmores.org> | Not stated |
| `percent-encoding` | `2.3.2` | The rust-url developers | Not stated |
| `pin-project-lite` | `0.2.17` | Not stated in crate metadata | Not stated |
| `pkcs8` | `0.10.2` | RustCrypto Developers | Not stated |
| `pkg-config` | `0.3.34` | Alex Crichton <alex@alexcrichton.com> | Not stated |
| `poly1305` | `0.8.0` | RustCrypto Developers | Not stated |
| `polyval` | `0.6.2` | RustCrypto Developers | Not stated |
| `potential_utf` | `0.1.6` | The ICU4X Project Developers | Not stated |
| `powerfmt` | `0.2.1` | Jacob Pratt <jacob@jhpratt.dev> | Not stated |
| `ppv-lite86` | `0.2.21` | The CryptoCorrosion Contributors | Not stated |
| `proc-macro2` | `1.0.107` | David Tolnay <dtolnay@gmail.com>, Alex Crichton <alex@alexcrichton.com> | Not stated |
| `quinn-proto` | `0.11.19` | Not stated in crate metadata | Not stated |
| `quinn-udp` | `0.5.16` | Not stated in crate metadata | Not stated |
| `quote` | `1.0.47` | David Tolnay <dtolnay@gmail.com> | Not stated |
| `r-efi` | `6.0.0` | Not stated in crate metadata | Not stated |
| `rand_chacha` | `0.3.1` | The Rand Project Developers, The Rust Project Developers, The CryptoCorrosion Contributors | Not stated |
| `rand_core` | `0.10.1`, `0.6.4` | The Rand Project Developers | Not stated |
| `rand_core` | `0.10.1`, `0.6.4` | The Rand Project Developers, The Rust Project Developers | Not stated |
| `rand_pcg` | `0.10.2` | The Rand Project Developers | Not stated |
| `regex` | `1.13.1` | The Rust Project Developers, Andrew Gallant <jamslam@gmail.com> | Not stated |
| `regex-automata` | `0.4.18` | The Rust Project Developers, Andrew Gallant <jamslam@gmail.com> | Not stated |
| `regex-syntax` | `0.8.11` | The Rust Project Developers, Andrew Gallant <jamslam@gmail.com> | Not stated |
| `ring` | `0.17.14` | Not stated in crate metadata | Not stated |
| `rsqlite-vfs` | `0.1.1` | Not stated in crate metadata | Not stated |
| `rustc-hash` | `2.1.3` | The Rust Project Developers | Not stated |
| `rustc_version` | `0.4.1` | Not stated in crate metadata | Not stated |
| `rusticata-macros` | `4.1.0` | Pierre Chifflier <chifflier@wzdftpd.net> | Not stated |
| `rustix` | `1.1.5` | Dan Gohman <dev@sunfishcode.online>, Jakub Konka <kubkon@jakubkonka.com> | Not stated |
| `rustls-pki-types` | `1.15.1` | Not stated in crate metadata | Not stated |
| `rustls-webpki` | `0.103.15` | Not stated in crate metadata | Not stated |
| `rustversion` | `1.0.22`, `1.0.23` | David Tolnay <dtolnay@gmail.com> | Not stated |
| `ryu` | `1.0.23` | David Tolnay <dtolnay@gmail.com> | Not stated |
| `scopeguard` | `1.2.0` | bluss | Not stated |
| `semver` | `1.0.28` | David Tolnay <dtolnay@gmail.com> | Not stated |
| `serde_core` | `1.0.229` | Erick Tryzelaar <erick.tryzelaar@gmail.com>, David Tolnay <dtolnay@gmail.com> | Not stated |
| `serde_derive` | `1.0.229` | Erick Tryzelaar <erick.tryzelaar@gmail.com>, David Tolnay <dtolnay@gmail.com> | Not stated |
| `serde_urlencoded` | `0.7.1` | Anthony Ramine <n.oxyde@gmail.com> | Not stated |
| `shlex` | `1.3.0`, `2.0.1` | comex <comexk@gmail.com>, Fenhl <fenhl@fenhl.net>, Adrian Taylor <adetaylor@chromium.org>, Alex Touchet <alextouchet@outlook.com>, Daniel Parks <dp+git@oxidized.org>, Garrett Berg <googberg@gmail.com> | Not stated |
| `signature` | `2.2.0` | RustCrypto Developers | Not stated |
| `simd-adler32` | `0.3.10` | Marvin Countryman <me@maar.vin> | Not stated |
| `slab` | `0.4.12` | Carl Lerche <me@carllerche.com> | Not stated |
| `smallvec` | `1.16.3` | The Servo Project Developers | Not stated |
| `socket2` | `0.6.5` | Alex Crichton <alex@alexcrichton.com>, Thomas de Zeeuw <thomasdezeeuw@gmail.com> | Not stated |
| `spin` | `0.9.9` | Mathijs van de Nes <git@mathijs.vd-nes.nl>, John Ericson <git@JohnEricson.me>, Joshua Barretto <joshua.s.barretto@gmail.com> | Not stated |
| `spki` | `0.7.3` | RustCrypto Developers | Not stated |
| `sqlite-wasm-rs` | `0.5.5` | Not stated in crate metadata | Not stated |
| `stable_deref_trait` | `1.2.1` | Robert Grosse <n210241048576@gmail.com> | Not stated |
| `subtle` | `2.6.1` | Isis Lovecruft <isis@patternsinthevoid.net>, Henry de Valence <hdevalence@hdevalence.ca> | Not stated |
| `syn` | `2.0.119`, `3.0.9` | David Tolnay <dtolnay@gmail.com> | Not stated |
| `sync_wrapper` | `1.0.2` | Actyx AG <developer@actyx.io> | Not stated |
| `synstructure` | `0.13.2`, `0.14.0` | Nika Layzell <nika@thelayzells.com> | Not stated |
| `thiserror-impl` | `2.0.21` | David Tolnay <dtolnay@gmail.com> | Not stated |
| `time` | `0.3.55` | Jacob Pratt <open-source@jhpratt.dev>, Time contributors | Not stated |
| `time-core` | `0.1.9` | Jacob Pratt <open-source@jhpratt.dev>, Time contributors | Not stated |
| `time-macros` | `0.2.32` | Jacob Pratt <open-source@jhpratt.dev>, Time contributors | Not stated |
| `tinystr` | `0.8.4` | The ICU4X Project Developers | Not stated |
| `tinyvec` | `1.13.3` | Lokathor <zefria@gmail.com> | Not stated |
| `tokio-macros` | `2.7.2` | Tokio Contributors <team@tokio.rs> | Not stated |
| `tokio-rustls` | `0.26.6` | Not stated in crate metadata | Not stated |
| `tower` | `0.5.3` | Tower Maintainers <team@tower-rs.com> | Not stated |
| `tower-http` | `0.6.11` | Tower Maintainers <team@tower-rs.com> | Not stated |
| `tower-layer` | `0.3.3` | Tower Maintainers <team@tower-rs.com> | Not stated |
| `tower-service` | `0.3.3` | Tower Maintainers <team@tower-rs.com> | Not stated |
| `tracing` | `0.1.44` | Eliza Weisman <eliza@buoyant.io>, Tokio Contributors <team@tokio.rs> | Not stated |
| `tracing-core` | `0.1.36` | Tokio Contributors <team@tokio.rs> | Not stated |
| `try-lock` | `0.2.5` | Sean McArthur <sean@seanmonstar.com> | Not stated |
| `typenum` | `1.20.1` | Not stated in crate metadata | Not stated |
| `unicode-ident` | `1.0.27` | David Tolnay <dtolnay@gmail.com> | Not stated |
| `universal-hash` | `0.5.1` | RustCrypto Developers | Not stated |
| `untrusted` | `0.9.0` | Brian Smith <brian@briansmith.org> | Not stated |
| `url` | `2.5.8` | The rust-url developers | Not stated |
| `utf8_iter` | `1.0.4` | Henri Sivonen <hsivonen@hsivonen.fi> | Not stated |
| `vcpkg` | `0.2.15` | Jim McGrath <jimmc2@gmail.com> | Not stated |
| `version_check` | `0.9.5` | Sergio Benitez <sb@sergio.bz> | Not stated |
| `want` | `0.3.2` | Sean McArthur <sean@seanmonstar.com> | Not stated |
| `wasi` | `0.11.1+wasi-snapshot-preview1` | Not stated in crate metadata | Not stated |
| `wasm-bindgen` | `0.2.129` | Not stated in crate metadata | Not stated |
| `wasm-bindgen-futures` | `0.4.79` | Not stated in crate metadata | Not stated |
| `wasm-bindgen-macro` | `0.2.129` | Not stated in crate metadata | Not stated |
| `wasm-bindgen-macro-support` | `0.2.129` | Not stated in crate metadata | Not stated |
| `wasm-bindgen-shared` | `0.2.129` | Not stated in crate metadata | Not stated |
| `web-sys` | `0.3.106` | Not stated in crate metadata | Not stated |
| `web-time` | `1.1.0` | Not stated in crate metadata | Not stated |
| `webpki-roots` | `1.0.9` | Not stated in crate metadata | Not stated |
| `winapi` | `0.3.9` | Peter Atashian <retep998@gmail.com> | Not stated |
| `winapi-i686-pc-windows-gnu` | `0.4.0` | Not stated in crate metadata | Not stated |
| `winapi-x86_64-pc-windows-gnu` | `0.4.0` | Peter Atashian <retep998@gmail.com> | Not stated |
| `windows-link` | `0.2.1` | Not stated in crate metadata | Not stated |
| `windows-sys` | `0.52.0`, `0.61.2` | Microsoft | Not stated |
| `windows-sys` | `0.52.0`, `0.61.2` | Not stated in crate metadata | Not stated |
| `windows-targets` | `0.52.6` | Microsoft | Not stated |
| `windows_aarch64_gnullvm` | `0.52.6` | Not stated in crate metadata | Not stated |
| `windows_aarch64_msvc` | `0.52.6` | Not stated in crate metadata | Not stated |
| `windows_i686_gnu` | `0.52.6` | Not stated in crate metadata | Not stated |
| `windows_i686_gnullvm` | `0.52.6` | Not stated in crate metadata | Not stated |
| `windows_i686_msvc` | `0.52.6` | Not stated in crate metadata | Not stated |
| `windows_x86_64_gnu` | `0.52.6` | Microsoft | Not stated |
| `windows_x86_64_gnullvm` | `0.52.6` | Not stated in crate metadata | Not stated |
| `windows_x86_64_msvc` | `0.52.6` | Not stated in crate metadata | Not stated |
| `writeable` | `0.6.4` | The ICU4X Project Developers | Not stated |
| `yasna` | `0.5.2` | Masaki Hara <ackie.h.gmai@gmail.com> | Not stated |
| `yoke` | `0.8.3` | Manish Goregaokar <manishsmail@gmail.com> | Not stated |
| `yoke-derive` | `0.8.4` | Manish Goregaokar <manishsmail@gmail.com> | Not stated |
| `zerocopy` | `0.8.62` | Not stated in crate metadata | Not stated |
| `zerocopy-derive` | `0.8.62` | Not stated in crate metadata | Not stated |
| `zerofrom` | `0.1.8` | The ICU4X Project Developers | Not stated |
| `zerofrom-derive` | `0.1.8` | Manish Goregaokar <manishsmail@gmail.com> | Not stated |
| `zeroize_derive` | `1.5.0` | The RustCrypto Project Developers | Not stated |
| `zerotrie` | `0.2.5` | The ICU4X Project Developers | Not stated |
| `zerovec` | `0.11.8` | The ICU4X Project Developers | Not stated |
| `zerovec-derive` | `0.11.6` | Manish Goregaokar <manishsmail@gmail.com> | Not stated |
| `zlib-rs` | `0.6.8` | Not stated in crate metadata | Not stated |
| `zmij` | `1.0.23` | David Tolnay <dtolnay@gmail.com> | Not stated |
| `zstd-safe` | `7.3.0` | Alexandre Bury <alexandre.bury@gmail.com> | Not stated |
| `zstd-sys` | `2.1.1+zstd.1.5.7` | Alexandre Bury <alexandre.bury@gmail.com> | Not stated |

## Murmur workspace path dependencies

These are internal packages, not third-party crates:

| Dependency | Declared by |
|---|---|
| `murmur-spool` | `sqlite-session` |
| `murmur-replication` | `sqlite-session` (optional) |
| `sqlite-session` | `sqlite-session-benchmark` |

## Embedded Go binding dependencies

The Go module uses only the Go standard library (including cgo); it has no external
Go modules. `clients/go/native/Cargo.toml` declares `rusqlite`, `serde`,
`serde_json`, `postcard`, `zeroize`, `tokio`, and `quinn` at runtime, and `syn`
for the development API inventory scanner. These crates and all locked transitive
packages are included above; authors/organizations and countries use the same
metadata-based attribution. `syn` and `serde_json` are direct dependencies of this
native binding in addition to their transitive roles elsewhere.

The native binding also depends on the internal `sqlite-session` and
`murmur-spool` path packages. Go and Rust toolchains, a C compiler (GNU/MinGW or
Apple's toolchain), LLVM/libclang, and platform system libraries are build/system
prerequisites, not downloaded Go modules or additional Cargo runtime crates.

## Optional benchmark profiling prerequisites

The Linux `tests-benchmark/tools/profile_mesh.py` helper uses Python 3's standard
library only; it adds no Python package or Cargo dependency. It optionally invokes
`iostat` and `pidstat` from an installed sysstat distribution. Versions are system
prerequisites rather than lockfile dependencies (this run used Python 3.12.3 and
sysstat 12.6.1).

| Tool | Organization / author | Country | Primary attribution |
|---|---|---|---|
| Python 3 and standard library | Python Software Foundation | United States (PSF organization) | [PSF legal information](https://www.python.org/about/legal/) |
| sysstat (`iostat`, `pidstat`) | Sébastien Godard, creator and maintainer | Not stated as country of origin in upstream biography | [Author biography](https://sysstat.github.io/about) |
| gperftools (`libprofiler`, `google-pprof`; optional component sampling) | gperftools contributors; originally developed at Google | Not stated by upstream profiling documentation | [Upstream CPU profiler documentation](https://gperftools.github.io/gperftools/cpuprofile.html) |
