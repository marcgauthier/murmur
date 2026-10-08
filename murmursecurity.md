# Focused Murmur security-review preparation

## 1\. Objective and workflow

Prepare an independent review answering:

> **Can an attacker violate Murmur’s confidentiality, data integrity, identity, authorization or availability guarantees under realistic abuse?**

Use **Codex Security locally** against a frozen copy of the current workspace, including uncommitted security and CRDT changes. Start with a standard scan to confirm coverage, then deepen the assessment of these boundaries. Codex Security supports local repository/component scans and reports validation evidence and coverage. [Official workflow](<https://learn.chatgpt.com/docs/security/plugin/scans>).

Keep **encrypted VFS** in scope. It protects Murmur’s authoritative files; authentication, nonce handling and crash recovery are security responsibilities.

The assessment produces findings and reproductions. Applying fixes is a separate task.

## 2\. Review boundaries

| Area | Required focus |
|---|---|
| Encrypted VFS | File/header/index authentication; nonce uniqueness across reopen and reuse; committed corruption versus torn tails; plaintext leakage; links and checkpoints; bounded allocations. |
| Key lifecycle | Application-key and data-key separation; registry authentication; interrupted rotation/rewrite; key retirement and backup references; wrong-key rejection; secret exposure. |
| High/Low bridge | Signer, recipient, domain and stream authorization; tampered artifacts; replay and receipt atomicity; ownership protections; imported causal identities; artifact paths and decompression limits. |
| Origin signatures | Canonical signed identity and payload; forwarded-origin impersonation; unknown keys; historical compatibility; verification before durable side effects; snapshot and migration trust exceptions. |
| Certificates and transport | Chain, usage, expiry and NodeID validation; expected-peer and DBID binding; admission policies; reconnect/migration behavior; authentication before privileged processing. |
| Reachable decoders/parsers | Network frames, transactions/chunks, snapshots, schema manifests, CRDT records, encrypted containers, bridge artifacts and service requests. Check lengths, counts, integer overflow, allocation, decompression and aggregate resource limits. |
| Service interfaces | Authentication and intended separation between ordinary clients and administrators; unlock, rotation, origin authorization, peer changes, bridge administration, debug exposure and request limits. |

Trace supporting code outside these areas only when necessary to establish an attack path. Exclude general query functionality, CLI workflows, broad performance tuning and unrelated correctness work.

**Service qualification:** the library currently ships no HTTP adapter. Its service endpoints are live-test tooling. Review that tooling, but classify missing client/admin separation as a deployment requirement unless an actual shipped security boundary is bypassed. Do not present fixture behavior as a production library vulnerability.

## 3\. Preparation and validation

- Freeze the source snapshot outside the working repository. Record HEAD, dirty changes, included untracked source and file hashes. Exclude runtime databases, logs, generated credentials and unrelated artifacts.
- Supply architecture documentation as claims to verify, not proof. Have the reviewer derive current formats and guarantees from code; documentation already contains some historical version references.
- Use a fresh review session. Do not treat previous implementation explanations or passing tests as evidence of security.
- Define attacker capabilities separately: unauthenticated network access; an admitted malicious peer with its own credentials; an authorized bridge signer acting maliciously; an ordinary service client; and ciphertext-file tampering without decryption keys.
- Record documented limitations separately: no Byzantine consensus, no whole-database rollback protection, explicitly trusted snapshot/baseline authorities, and current key-revocation limitations.
- Validate candidates with minimal reproductions, targeted fuzzing and isolated live daemons. Exercise the RIME and spool paths where record materialization affects the attack.
- For rejected origin/payload attacks, verify that protected state, HLC, generation, receipts and watermarks remain unchanged. For resource attacks, measure consumption and recovery while honest traffic continues.
- Require a coverage matrix listing reviewed, tested and deferred surfaces. Findings must identify attacker prerequisites, reachable code, violated guarantee, severity, evidence, reproduction and remediation. Separate confirmed issues, unvalidated candidates and deployment gaps.

## 4\. Prompt to use

> Conduct an independent, focused security assessment of the supplied frozen Murmur source snapshot.
>
> Main question: **Can an attacker violate Murmur’s confidentiality, data integrity, identity, authorization or availability guarantees under realistic abuse?**
>
> Review only these security boundaries and their necessary integration paths: encrypted VFS; encryption-key registry, rotation and rewrite; High/Low bridge; transaction-origin signatures; certificate and QUIC peer authentication; parsers/decoders reachable through those boundaries; and service authentication/administration.
>
> Include CRDT payloads only for signature coverage, actor authorization, imported identities, parser safety and resource abuse. General CRDT semantics and unrelated database functionality are outside this assessment.
>
> Treat code as authoritative. Independently verify documentation claims, implemented versions and reachable attack paths. Existing tests and previous review conclusions are starting points, not proof.
>
> Distinguish unauthenticated attackers, admitted malicious peers using their own credentials, authorized bridge signers, ordinary service clients and attackers able to alter ciphertext files without possessing storage keys. Do not assume every authenticated peer is honest.
>
> For service interfaces, the intended deployment separates ordinary clients from administrators. Identify whether each interface is shipped library functionality, test tooling or a proposed hosting interface. Label missing fixture role separation as a deployment gap unless you demonstrate a shipped-boundary violation.
>
> Prioritize authentication bypass, forwarded-origin impersonation, cross-domain authorization failures, replay, secret/plaintext leakage, nonce reuse, unsafe corruption recovery, key-loss scenarios and attacker-controlled resource exhaustion.
>
> Assess documented trust exceptions explicitly. Do not report the absence of Byzantine consensus or whole-database rollback protection as a new implementation vulnerability.
>
> Use Codex Security when available. Confirm its actual scan scope; a repository-wide scan is not proof that only these components were reviewed. If precise scope selection is unavailable, use component scans and a targeted integration review, and disclose coverage.
>
> Perform validation only in disposable local environments with synthetic credentials. Preserve the original workspace. Produce minimal reproductions without applying product fixes.
>
> Return a prioritized findings report and coverage matrix. Each finding must include source locations, attacker prerequisites, attack path, violated guarantee, practical impact, validation status, exact reproduction commands and proposed remediation. Separate confirmed vulnerabilities, unvalidated candidates, accepted limitations and deployment gaps. State explicitly what remains unreviewed; an empty findings list is not a security certification.
