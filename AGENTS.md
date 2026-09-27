# Repository guidance

## Keep documentation current

- When behavior, public APIs, configuration, schema rules, replication, storage,
  recovery, security, or operational requirements change, update the affected
  documents in `architecture/` as part of the same change.
- Update `README.md` when setup, usage examples, repository layout, or implementation
  status changes. Keep examples accurate and distinguish implemented behavior from
  planned capabilities.
- Update `architecture/implementation-roadmap.md` and acceptance criteria when a
  planned capability is implemented, changed, or removed.
- Use `architecture/README.md` as the documentation index. Add or update its links
  when documents are added, renamed, moved, or reorganized.
- Keep architecture documents focused on their topics. Cross-reference related
  documents instead of duplicating requirements or recreating a root monolith.
- Preserve numbered section identities when possible; if a section is renamed or
  moved, update affected cross-references and table-of-contents links.
- Before finishing documentation changes, check that relative links and section
  anchors resolve, Markdown fences are balanced, and no obsolete document paths
  remain in active references.
