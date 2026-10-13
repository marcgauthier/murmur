# Language clients

Client implementations live under `clients/<language>` so additional languages
can be added without changing the Rust database layout.

| Language | Client | Examples |
|---|---|---|
| Go | [Embedded cgo client](go/README.md) | [19 runnable feature examples](go/examples/README.md) |

The Go module is `github.com/marcgauthier/murmur/clients/go`. Its Rust native
library is built locally; application SQL, persistence, replication, and wire
formats remain implemented by Murmur's Rust crates.
