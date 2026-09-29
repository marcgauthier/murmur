# Repository guidance

## Main goal of SPeD-SQL
Create a very fast local sql database with encryption at rest on disk via KV store and that replicated quickly via QUIC with TLS, The code must work, it must be tested with live test not just test for code. 


## Keep documentation current

- When behavior, public APIs, configuration, schema rules, replication, storage,
  recovery, security, or operational requirements change, update the affected
  documents in `architecture/` as part of the same change.
- Update `README.md` when setup, usage examples, repository layout, or implementation
  status changes. Keep examples accurate and distinguish implemented behavior from
  planned capabilities.
- Use `architecture/README.md` as the documentation index. Add or update its links
  when documents are added, renamed, moved, or reorganized.
- Keep architecture documents focused on their topics. Cross-reference related
  documents instead of duplicating requirements or recreating a root monolith.
- Preserve numbered section identities when possible; if a section is renamed or
  moved, update affected cross-references and table-of-contents links.
- Before finishing documentation changes, check that relative links and section
  anchors resolve, Markdown fences are balanced, and no obsolete document paths
  remain in active references.
