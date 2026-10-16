#!/usr/bin/env python3
"""Linux-only, dependency-free resource sampling around the mesh benchmark."""
import argparse
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import tempfile
import time


def counters(path):
    result = {}
    for line in path.read_text().splitlines():
        parts = line.split()
        if len(parts) >= 2 and parts[1].isdigit():
            result[parts[0].rstrip(":")] = int(parts[1])
    return result


def pressure(path):
    result = {}
    try:
        for line in path.read_text().splitlines():
            parts = line.split()
            result[parts[0]] = int(dict(part.split("=") for part in parts[1:])["total"])
    except (OSError, ValueError, KeyError):
        pass
    return result


def process(pid):
    root = Path(f"/proc/{pid}")
    stat = root.joinpath("stat").read_text().rsplit(")", 1)[1].split()
    status = counters(root / "status")
    command = root.joinpath("cmdline").read_bytes().split(b"\0")
    phase = next((name for name in ("baseline", "replication")
                  if any(f"/{name}/".encode() in arg for arg in command)), None)
    io = counters(root / "io")
    return {"pid": pid, "start": int(stat[19]), "phase": phase,
            "cpu": int(stat[11]) + int(stat[12]),
            "read": io.get("read_bytes", 0), "write": io.get("write_bytes", 0),
            "rss": status.get("VmRSS", 0) * 1024, "threads": int(stat[17])}


def average(rows, name):
    values = [row[name] for row in rows if name in row]
    return sum(values) / len(values) if values else None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--nodes", type=int, default=10)
    parser.add_argument("--seconds", type=int, default=30)
    parser.add_argument("--rate", type=float, default=1000)
    parser.add_argument("--capture-ms", type=int, default=1000)
    parser.add_argument("--smoke", action="store_true")
    parser.add_argument("--device", help="physical disk for optional iostat, e.g. sda")
    parser.add_argument("--out", type=Path)
    args = parser.parse_args()
    if args.capture_ms < 1:
        parser.error("--capture-ms must be positive")
    if not Path("/proc/self/stat").exists():
        parser.error("Linux /proc is required")
    output = args.out or Path(tempfile.mkdtemp(prefix="murmur-resource-profile-"))
    output.mkdir(parents=True, exist_ok=True)
    print(f"Profile artifacts: {output}", flush=True)
    mesh = output / "mesh"
    env = dict(os.environ, MURMUR_MESH_ARTIFACTS=str(mesh.resolve()),
               MURMUR_MESH_CAPTURE_MS=str(args.capture_ms))
    tick_hz = os.sysconf("SC_CLK_TCK")
    relative = Path("/proc/self/cgroup").read_text().strip().split("::", 1)[-1]
    cgroup = Path("/sys/fs/cgroup") / relative.lstrip("/")
    quota = {}
    for parent in (cgroup, *cgroup.parents):
        if not str(parent).startswith("/sys/fs/cgroup"):
            continue
        try:
            quota[str(parent)] = parent.joinpath("cpu.max").read_text().strip()
        except OSError:
            pass
    metadata = {"nodes": args.nodes, "seconds": args.seconds, "rate": args.rate,
                "capture_ms": args.capture_ms, "smoke_only": args.smoke,
                "cpu_affinity": sorted(os.sched_getaffinity(0)), "clock_ticks": tick_hz,
                "cgroup": str(cgroup), "cpu_quotas": quota, "disk_device": args.device}
    (output / "metadata.json").write_text(json.dumps(metadata, indent=2))
    monitors = []
    files = []
    samples = []
    previous = {}
    previous_cpu = None
    previous_time = time.monotonic()
    previous_pressure = {}
    previous_throttled = None
    previous_disk = None
    benchmark = None
    try:
        for name, command in (
            ("iostat.txt", ["iostat", "-x", "-y", "-t", *([args.device] if args.device else []), "1"]),
            ("pidstat.txt", ["pidstat", "-u", "-d", "-w", "-G", "meshbench", "1"]),
        ):
            if shutil.which(command[0]):
                log = open(output / name, "w")
                files.append(log)
                monitors.append(subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT))
        log = open(output / "benchmark.log", "w")
        files.append(log)
        benchmark = subprocess.Popen([str(args.binary.resolve()), str(args.nodes),
                                      str(args.seconds), str(args.rate)] + (["--smoke"] if args.smoke else []), env=env,
                                     stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        with open(output / "samples.jsonl", "w") as trace:
            while benchmark.poll() is None:
                time.sleep(1)
                end = time.monotonic()
                elapsed = end - previous_time
                current = {}
                try:
                    children = Path(f"/proc/{benchmark.pid}/task/{benchmark.pid}/children").read_text().split()
                except OSError:
                    children = []
                for pid in children:
                    try:
                        node = process(int(pid))
                        current[(node["pid"], node["start"])] = node
                    except (OSError, ValueError, IndexError):
                        pass
                host = list(map(int, Path("/proc/stat").read_text().splitlines()[0].split()[1:9]))
                disk = None
                if args.device:
                    try:
                        disk = list(map(int, Path(f"/sys/block/{args.device}/stat").read_text().split()))
                    except (OSError, ValueError):
                        pass
                stalls = {kind: pressure(cgroup / f"{kind}.pressure") for kind in ("cpu", "io")}
                try:
                    throttled = counters(cgroup / "cpu.stat").get("throttled_usec")
                except OSError:
                    throttled = None
                for phase in ("baseline", "replication"):
                    nodes = [(key, node) for key, node in current.items() if node["phase"] == phase]
                    if not nodes:
                        continue
                    row = {"time_ms": int(time.time() * 1000), "interval_seconds": elapsed,
                           "phase": phase, "nodes": len(nodes),
                           "node_cpu_cores": 0, "read_bytes_per_second": 0,
                           "write_bytes_per_second": 0,
                           "rss_bytes": sum(node["rss"] for _, node in nodes),
                           "threads": sum(node["threads"] for _, node in nodes)}
                    for key, node in nodes:
                        if key in previous:
                            row["node_cpu_cores"] += max(0, node["cpu"] - previous[key]["cpu"]) / tick_hz / elapsed
                            for source, target in (("read", "read_bytes_per_second"), ("write", "write_bytes_per_second")):
                                row[target] += max(0, node[source] - previous[key][source]) / elapsed
                    if previous_cpu is not None:
                        delta = [a - b for a, b in zip(host, previous_cpu)]
                        total = sum(delta)
                        if total > 0:
                            row["host_cpu_busy_percent"] = 100 * (total - delta[3] - delta[4]) / total
                            row["host_iowait_percent"] = 100 * delta[4] / total
                    if disk is not None and previous_disk is not None:
                        delta = [a - b for a, b in zip(disk, previous_disk)]
                        row["disk_busy_percent"] = delta[9] / (elapsed * 10)
                        row["disk_average_queue"] = delta[10] / (elapsed * 1000)
                        row["disk_read_kib_per_second"] = delta[2] / 2 / elapsed
                        row["disk_write_kib_per_second"] = delta[6] / 2 / elapsed
                        if delta[0]:
                            row["disk_read_await_ms"] = delta[3] / delta[0]
                        if delta[4]:
                            row["disk_write_await_ms"] = delta[7] / delta[4]
                        if len(delta) >= 17 and delta[15]:
                            row["disk_flush_await_ms"] = delta[16] / delta[15]
                    for kind, values in stalls.items():
                        for mode, total in values.items():
                            before = previous_pressure.get(kind, {}).get(mode)
                            if before is not None:
                                row[f"{kind}_pressure_{mode}_percent"] = max(0, total - before) / (elapsed * 10_000)
                    if throttled is not None and previous_throttled is not None:
                        row["throttled_usec"] = max(0, throttled - previous_throttled)
                    trace.write(json.dumps(row) + "\n")
                    trace.flush()
                    samples.append(row)
                previous = current
                previous_time = end
                previous_cpu = host
                previous_pressure = stalls
                previous_throttled = throttled
                previous_disk = disk
        summary = {"benchmark_exit_code": benchmark.wait(), "metadata": metadata, "phases": {}}
        for phase in ("baseline", "replication"):
            timing_path = mesh / phase / "phase-timing.json"
            if not timing_path.exists():
                continue
            timing = json.loads(timing_path.read_text())
            rows = [row for row in samples if row["phase"] == phase and row["nodes"] == args.nodes
                    and row["time_ms"] - row["interval_seconds"] * 1000 >= timing["start_ms"]
                    and row["time_ms"] <= timing.get("end_ms", float("inf"))]
            summary["phases"][phase] = {"samples": len(rows), "timing": timing,
                "means": {key: average(rows, key) for key in (
                    "node_cpu_cores", "host_cpu_busy_percent", "host_iowait_percent",
                    "cpu_pressure_some_percent", "io_pressure_some_percent", "io_pressure_full_percent",
                    "read_bytes_per_second", "write_bytes_per_second",
                    "disk_busy_percent", "disk_average_queue", "disk_read_kib_per_second",
                    "disk_write_kib_per_second", "disk_read_await_ms",
                    "disk_write_await_ms", "disk_flush_await_ms")},
                "peak_node_cpu_cores": max((row["node_cpu_cores"] for row in rows), default=None),
                "peak_sampled_rss_bytes": max((row["rss_bytes"] for row in rows), default=None),
                "throttled_usec": sum(row["throttled_usec"] for row in rows if "throttled_usec" in row)
                    if any("throttled_usec" in row for row in rows) else None}
        (output / "summary.json").write_text(json.dumps(summary, indent=2))
        print(json.dumps(summary), flush=True)
        return benchmark.returncode
    finally:
        if benchmark and benchmark.poll() is None:
            # The Rust driver drops/kills its children on ordinary error exits.
            os.killpg(benchmark.pid, signal.SIGTERM)
            try:
                benchmark.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(benchmark.pid, signal.SIGKILL)
                benchmark.wait()
        for monitor in monitors:
            monitor.terminate()
            monitor.wait()
        for log in files:
            log.close()


if __name__ == "__main__":
    raise SystemExit(main())
