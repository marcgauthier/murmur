#!/usr/bin/env python3
"""Symbolize each ASLR process separately, then partition its CPU stack samples."""
import argparse
from collections import Counter
import json
from pathlib import Path
import re
import subprocess


def category(stack):
    # Walk from leaf toward root; charge allocation/syscall helpers to callers.
    # Broad Rust runtime frames contain application types as generic parameters,
    # so skip them rather than treating those type names as executing functions.
    for frame in reversed(re.split(r";(?!\s*\d+\])", stack)):
        if re.search(r"pthread_create|std::thread::scoped::Scope>::spawn|std::thread::builder::Builder>::spawn", frame):
            return "Worker thread creation"
        if frame.startswith(("std::sync::", "<std::sync::", "std::sys::pal::unix::futex::", "<std::sys::sync::thread_parking::")):
            return "Runtime/channels/scheduling"
        if frame.startswith(("std::", "core::", "alloc::", "<std::", "<core::", "<alloc::")):
            continue
        if re.search(r"ZSTD_|HUF_|FSE_|zstd::|zstd_safe::", frame):
            if "spool::compression::" in stack or "sqlite_session::storage::Zstd" in stack:
                return "Spool decompression" if "::decompress" in stack else "Spool compression"
            return "Wire compression/decompression"
        if re.search(r"miniz_oxide::|flate2::|spool::compression::", frame):
            return "Spool decompression" if "::decompress" in stack else "Spool compression"
        if re.search(r"spool::crypto::|aes_gcm::|aes::|ghash::|polyval::|ctr::", frame):
            return "Spool encryption/decryption"
        if re.search(r"ed25519|curve25519|murmur_replication::verification::", frame):
            return "Transaction authentication/signatures"
        if re.search(r"rustls::|ring::|webpki::|aesni_gcm|chacha20_poly1305|ChaCha20|Poly1305", frame):
            return "QUIC TLS/packet crypto"
        if re.search(r"quinn::|quinn_proto::|quinn_udp::", frame):
            return "QUIC transport"
        if "murmur_replication::membership::" in frame or re.search(r"::(swim|datagram|gossip_members|membership_tick|probe|packet|gossip)::", frame):
            return "Membership/SWIM"
        if "spool::index::Index>::memory_estimate" in frame:
            return "Full-index memory metrics"
        if re.search(r"spool::(index|read_cache)::|spool::store::Store>::keys|logical_store::LogicalStore>::keys", frame):
            return "Spool index/cache lookup"
        if re.search(r"spool::(loader|segment)::|spool::store::Store>::(get|read)|logical_store::LogicalStore>::get", frame):
            return "Spool block/record reads"
        if "spool::" in frame or "sqlite_session::storage::" in frame:
            return "Spool writes/other persistence"
        if re.search(r"sqlite_session::(merge|metadata|schema|network|model|table_replication|snapshot_stream)::", frame):
            return "Merge preparation/metadata/publication"
        if re.search(r"sqlite3|sqlite_session::(database|session)::|rusqlite::|libsqlite3_sys::", frame):
            return "SQLite/session capture"
        if "murmur_replication::" in frame:
            return "Replication orchestration/framing/gossip"
        if re.search(r"serde::|postcard::|blake3::|sha2::|sha256", frame):
            if "murmur_replication::membership::Packet" in stack:
                return "Membership/SWIM"
            return "Serialization/hashing (shared)"
    if re.search(r"tokio::|mio::|std::sync::|futex|syscall|pthread", stack):
        return "Runtime/scheduling/unattributed syscalls"
    return "Other/unresolved"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--pprof", type=Path, required=True)
    parser.add_argument("--phase", type=Path, required=True)
    args = parser.parse_args()
    profiles = sorted(args.phase.glob("*.cpu.prof"))
    if not profiles:
        parser.error("no CPU profiles found")
    counts, leaves, node_totals = Counter(), Counter(), {}
    for profile in profiles:
        output = profile.with_suffix(".stacks")
        # Never combine raw PIE/ASLR profiles with the legacy Perl pprof: their
        # addresses can be resolved using the wrong process's mapping.
        if not output.exists():
            pending = output.with_suffix(".pending")
            with pending.open("w") as stream:
                subprocess.run([str(args.pprof.resolve()), "--no_strip_temp", "--collapsed",
                                str(args.binary.resolve()), str(profile.resolve())],
                               stdout=stream, check=True)
            pending.replace(output)
        total = 0
        for line in output.read_text().splitlines():
            stack, count = line.rsplit(" ", 1)
            count = int(count)
            counts[category(stack)] += count
            leaves[stack.rsplit(";", 1)[-1]] += count
            total += count
        node_totals[profile.name] = total
        print(f"{profile.name}: {total} samples", flush=True)
    total = sum(counts.values())
    if not total:
        parser.error("profiles contain no CPU samples")
    report = {"measurement": "exclusive sampled node CPU, including local persistence/application SQL",
              "classification": "closest identifiable component, leaf to root; see category() rules",
              "total_samples": total, "nodes": node_totals,
              "components": [{"component": name, "samples": count, "percent": 100 * count / total}
                             for name, count in counts.most_common()],
              "top_leaf_frames": leaves.most_common(30)}
    target = args.phase / "cpu-components.json"
    target.write_text(json.dumps(report, indent=2))
    print(json.dumps(report["components"], indent=2))


if __name__ == "__main__":
    main()
