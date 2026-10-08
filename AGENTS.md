# Repository guidance

## Main goal of Murmur-SQL
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

## RIME usage documentation

- When an AI agent changes code in a way that affects RIME's Go syntax, public
  API, or user-visible behavior, update `rime/USAGE.md` in the same change.
- Keep examples and function/feature syntax accurate; document new APIs and
  remove or mark obsolete forms. Check links, anchors, and examples before
  finishing.
- `rime/USAGE.md` is the complete syntax reference; `rime/README.md` remains
  the short introduction.

## Vendor dependencies policy

- Whenever a new external dependency or package is imported or added to `go.mod`, it **must** be immediately documented in `VENDORS.md`.
- Each entry in `VENDORS.md` must include:
  - Package import path and version.
  - Organization or company behind the package (or the individual developer/publisher if independent).
  - Country of origin of the entity/maintainer.
  - License type.
  - Purpose or role of the package in the codebase.
- Maintainers and agents must verify that `VENDORS.md` is updated in the same pull request or changeset that introduces any new dependency.
