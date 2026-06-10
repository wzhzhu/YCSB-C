#!/usr/bin/env python3
import argparse
import csv
import datetime as dt
import errno
import os
import pathlib
import re
import shutil
import subprocess
import sys
import time
from typing import Dict, List, Tuple


WORKLOAD_SPECS = {
    "A": "workloads/workloada.spec",
    "B": "workloads/workloadb.spec",
    "C": "workloads/workloadc.spec",
    "D": "workloads/workloadd.spec",
    "E": "workloads/workloade.spec",
    "F": "workloads/workloadf.spec",
}

SCHEMES = {
    "lru": {
        "rocksdb.cache_type": "lru_cache",
        "rocksdb.use_multi_level_cache": "false",
    },
    "hcc": {
        "rocksdb.cache_type": "hyper_clock_cache",
        "rocksdb.use_multi_level_cache": "false",
    },
    "arc": {
        "rocksdb.cache_type": "arc_cache",
        "rocksdb.cache_pending_max_age_ops": "65536",
        "rocksdb.use_multi_level_cache": "false",
    },
    "cacheus": {
        "rocksdb.cache_type": "cacheus_cache",
        "rocksdb.cache_pending_max_age_ops": "65536",
        "rocksdb.use_multi_level_cache": "false",
    },
    "mlc_hcc_sr_bottom": {
        "rocksdb.cache_type": "hyper_clock_cache",
        "rocksdb.use_multi_level_cache": "true",
        "rocksdb.num_levels": "7",
        # Only the last level (L6) uses SR-HCC semantics.
        "rocksdb.multi_level_cache_srhcc_start_level": "6",
        "rocksdb.multi_level_cache_shared_pool_ratio": "0.0",
        "rocksdb.multi_level_cache_auto_adjust": "true",
        "rocksdb.multi_level_cache_allocator_mode": "model",
        "rocksdb.multi_level_cache_adjust_interval_ms": "1000",
        # robust_hit_rate (alpha = 1/hit_rate) instead of the default
        # constant_one: the uniform within-level model starves huge bottom
        # levels (L6 got 0 bytes at 1-2GB budgets despite 84% of traffic).
        "rocksdb.multi_level_cache_alpha_estimator": "robust_hit_rate",
    },
    "mlc_hcc_all_levels": {
        "rocksdb.cache_type": "hyper_clock_cache",
        "rocksdb.use_multi_level_cache": "true",
        "rocksdb.num_levels": "7",
        "rocksdb.multi_level_cache_srhcc_start_level": "-1",
        "rocksdb.multi_level_cache_shared_pool_ratio": "0.0",
        "rocksdb.multi_level_cache_auto_adjust": "true",
        "rocksdb.multi_level_cache_allocator_mode": "model",
        "rocksdb.multi_level_cache_adjust_interval_ms": "1000",
        "rocksdb.multi_level_cache_alpha_estimator": "robust_hit_rate",
    },
    "mlc_hcc_dynamic_srhcc": {
        "rocksdb.cache_type": "hyper_clock_cache",
        "rocksdb.use_multi_level_cache": "true",
        "rocksdb.num_levels": "7",
        "rocksdb.multi_level_cache_srhcc_start_level": "-1",
        "rocksdb.multi_level_cache_shared_pool_ratio": "0.0",
        "rocksdb.multi_level_cache_auto_adjust": "true",
        "rocksdb.multi_level_cache_allocator_mode": "model",
        "rocksdb.multi_level_cache_adjust_interval_ms": "1000",
        "rocksdb.multi_level_cache_alpha_estimator": "robust_hit_rate",
        "rocksdb.multi_level_cache_dynamic_srhcc_enable": "true",
        "rocksdb.multi_level_cache_dynamic_srhcc_check_interval_ops": "4096",
        "rocksdb.multi_level_cache_dynamic_srhcc_min_samples": "12288",
        "rocksdb.multi_level_cache_dynamic_srhcc_sample_rate_log2": "0",
        "rocksdb.multi_level_cache_dynamic_srhcc_poll_interval_ms": "100",
        "rocksdb.multi_level_cache_dynamic_srhcc_unique_ratio_enable_threshold": "0.50",
        "rocksdb.multi_level_cache_dynamic_srhcc_unique_ratio_disable_threshold": "0.30",
    },
}

# D_eff variants: same as the base MLC schemes but the allocator models each
# level with its effective working-set estimate D_eff = -(alpha*c)/ln(1-hit)
# instead of the raw level data size (ported from db_bench).
for _base in ("mlc_hcc_sr_bottom", "mlc_hcc_all_levels", "mlc_hcc_dynamic_srhcc"):
    SCHEMES[f"{_base}_deff"] = {
        **SCHEMES[_base],
        "rocksdb.multi_level_cache_use_effective_data_size": "true",
    }

COMMON_PROPS = {
    "workload": "com.yahoo.ycsb.workloads.CoreWorkload",
    # 100GB with 1024B KV assumption => 104,857,600 records.
    "recordcount": str(100 * 1024 * 1024 * 1024 // 1024),
    # 20M ops so the transaction phase reaches steady state (1M only measured
    # the cold-start warm-up transient at these cache sizes).
    "operationcount": "20000000",
    "fieldcount": "10",
    "fieldlength": "100",
    # Approximate 24-byte key with "user" + zero-padded numeric suffix.
    "zeropadding": "20",
    "field_len_dist": "constant",
    # Keep load order stable to avoid random-insert compaction corruption
    # on the current RocksDB branch under high-volume YCSB runs.
    "insertorder": "ordered",
    "rocksdb.raw_kv_mode": "true",
    "rocksdb.raw_key_size_bytes": "24",
    "rocksdb.raw_value_size_bytes": "1000",
    "rocksdb.block_cache_size_bytes": "1073741824",
    "rocksdb.cache_numshardbits": "0",
    "rocksdb.target_file_size_base": str(64 * 1024 * 1024),
    "rocksdb.write_buffer_size": str(64 * 1024 * 1024),
    "rocksdb.bloom_bits_per_key": "10",
    "rocksdb.cache_index_and_filter_blocks": "true",
    "rocksdb.cache_index_and_filter_blocks_with_high_priority": "true",
    # Bypass the OS page cache so block-cache misses pay real NVMe latency.
    # With 250GB RAM vs 100GB data, buffered reads made misses nearly free
    # (warm page cache), decoupling throughput from hit ratio and coupling
    # cases that share the reused DB.
    "rocksdb.use_direct_reads": "true",
    "rocksdb.use_direct_io_for_flush_and_compaction": "true",
}

THROUGHPUT_RE = re.compile(
    r"^rocksdb\t(?P<spec>.+)\t(?P<threads>\d+)\t(?P<ktps>[-+eE0-9\.]+)$",
    re.MULTILINE,
)
METRIC_RE = re.compile(
    r"^rocksdb\t(?P<name>[a-zA-Z0-9_]+)\t(?P<value>[-+eE0-9\.]+)$",
    re.MULTILINE,
)
LOADED_RE = re.compile(r"^# Loading records:\t(?P<count>\d+)$", re.MULTILINE)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run RocksDB cache strategy matrix on YCSB-C.")
    p.add_argument("--ycsb-root", default=".", help="Path to YCSB-C root.")
    p.add_argument("--threads", default="8,16,32,64,128")
    p.add_argument("--cache-gb", default="1,2,4,8")
    p.add_argument(
        "--shard-bits",
        default="0",
        help=(
            "Comma-separated rocksdb.cache_numshardbits values (matrix "
            "dimension). 0 = unsharded (historical default). For lru/hcc this "
            "is native RocksDB sharding; for arc/cacheus a value k>0 shards "
            "both the wrapper policy layer (2^k instances routed by key hash) "
            "and the shared backing cache."
        ),
    )
    p.add_argument("--workloads", default="A,B,C,D,E,F")
    p.add_argument(
        "--schemes",
        default=(
            "lru,hcc,arc,cacheus,"
            "mlc_hcc_sr_bottom,mlc_hcc_all_levels,mlc_hcc_dynamic_srhcc,"
            "mlc_hcc_sr_bottom_deff,mlc_hcc_all_levels_deff,"
            "mlc_hcc_dynamic_srhcc_deff"
        ),
    )
    p.add_argument("--repeats", type=int, default=1)
    p.add_argument("--results-dir", default="results/rocksdb-matrix")
    p.add_argument(
        "--db-root-dir",
        default="/mnt/rocksdb_nvme/fio/ycsbc-tmp",
        help="Root directory for per-case rocksdb.dir.",
    )
    p.add_argument("--run-id", default=None)
    p.add_argument("--recordcount", type=int, default=None)
    p.add_argument("--operationcount", type=int, default=None)
    p.add_argument("--fieldcount", type=int, default=None)
    p.add_argument("--fieldlength", type=int, default=None)
    p.add_argument(
        "--load-threads",
        type=int,
        default=128,
        help="Fill/load phase thread count. Default is 128.",
    )
    p.add_argument(
        "--insert-order",
        choices=["ordered", "hashed"],
        default="ordered",
        help="YCSB key insert order for load/transaction key mapping.",
    )
    p.add_argument(
        "--refill-policy",
        choices=["per_case", "reuse_if_readonly"],
        default="per_case",
        help="per_case: refill each case; reuse_if_readonly: reuse DB for readonly workloads.",
    )
    p.add_argument(
        "--keep-db",
        action="store_true",
        help="Keep per-case RocksDB directories after each run.",
    )
    p.add_argument("--dry-run", action="store_true")
    return p.parse_args()


def load_spec(path: pathlib.Path) -> Dict[str, str]:
    props: Dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#") or "=" not in s:
            continue
        k, v = s.split("=", 1)
        props[k.strip()] = v.strip()
    return props


def write_props(path: pathlib.Path, props: Dict[str, str]) -> None:
    lines = [f"{k}={v}" for k, v in props.items()]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def remove_tree_with_retry(path: pathlib.Path, retries: int = 5, delay_s: float = 0.2) -> None:
    """Best-effort recursive delete with retry for transient ENOTEMPTY races."""
    for attempt in range(retries):
        try:
            shutil.rmtree(path)
            return
        except FileNotFoundError:
            return
        except OSError as exc:
            if exc.errno not in (errno.ENOTEMPTY, errno.EBUSY):
                raise
            if attempt + 1 >= retries:
                raise
            time.sleep(delay_s)


def parse_metrics(output: str) -> Dict[str, float]:
    metrics: Dict[str, float] = {}
    m = THROUGHPUT_RE.search(output)
    if m:
        metrics["throughput_kops"] = float(m.group("ktps"))
    lm = LOADED_RE.search(output)
    if lm:
        metrics["loaded_records"] = float(lm.group("count"))
    for mm in METRIC_RE.finditer(output):
        metrics[mm.group("name")] = float(mm.group("value"))
    return metrics


def collect_mlc_metrics(metrics: Dict[str, float]) -> str:
    parts: List[str] = []
    for key in sorted(metrics.keys()):
        if key.startswith("mlc_"):
            parts.append(f"{key}={metrics[key]}")
    return ";".join(parts)


def get_mlc_level_metric(metrics: Dict[str, float], level: int, suffix: str) -> float:
    return metrics.get(f"mlc_level_{level}_{suffix}", 0.0)


def to_latency_ms(throughput_kops: float) -> float:
    if throughput_kops <= 0:
        return 0.0
    return 1.0 / throughput_kops


def ensure_ycsbc_binary(ycsb_root: pathlib.Path) -> Tuple[bool, str]:
    ycsbc_bin = ycsb_root / "ycsbc"
    if ycsbc_bin.exists():
        return True, ""

    rocksdb_home = os.environ.get("ROCKSDB_HOME", "/users/wzhzhu/rocksdb")
    link_flags = (
        f"-Wl,-rpath,{rocksdb_home}/build-tests "
        f"{rocksdb_home}/build-tests/librocksdb.so -lpthread -ltbb"
    )
    build = subprocess.run(
        ["make", f"ROCKSDB_HOME={rocksdb_home}", f"LDFLAGS={link_flags}"],
        cwd=str(ycsb_root),
        text=True,
        capture_output=True,
        check=False,
    )
    if ycsbc_bin.exists():
        return True, ""

    detail = (
        f"rebuild exit={build.returncode}\n"
        f"{build.stdout or ''}\n{build.stderr or ''}"
    )
    return False, detail


def run_once(
    ycsb_root: pathlib.Path,
    results_dir: pathlib.Path,
    db_root_dir: pathlib.Path,
    run_id: str,
    workload: str,
    scheme: str,
    cache_bytes: int,
    threads: int,
    repeat_idx: int,
    shard_bits: int,
    overrides: Dict[str, str],
    db_subdir: str,
    skip_load: bool,
    clean_db_before_run: bool,
    cleanup_after_run: bool,
    keep_db: bool,
) -> Dict[str, str]:
    base_spec = load_spec(ycsb_root / WORKLOAD_SPECS[workload])
    props = dict(base_spec)
    props.update(COMMON_PROPS)
    props.update(overrides)
    props.update(SCHEMES[scheme])
    props["rocksdb.block_cache_size_bytes"] = str(cache_bytes)
    props["rocksdb.cache_numshardbits"] = str(shard_bits)
    props["threadcount"] = str(threads)
    props["skipload"] = "true" if skip_load else "false"
    db_dir = db_root_dir / run_id / db_subdir
    props["rocksdb.dir"] = str(db_dir)
    db_dir.parent.mkdir(parents=True, exist_ok=True)
    if clean_db_before_run and db_dir.exists():
        remove_tree_with_retry(db_dir)

    spec_dir = results_dir / "specs"
    log_dir = results_dir / "logs"
    spec_dir.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)

    spec_path = spec_dir / (
        f"wl{workload}-{scheme}-c{cache_bytes}-t{threads}-s{shard_bits}"
        f"-r{repeat_idx}.spec"
    )
    write_props(spec_path, props)

    cmd = [
        str(ycsb_root / "ycsbc"),
        "-db",
        "rocksdb",
        "-threads",
        str(threads),
        "-P",
        str(spec_path),
    ]
    output = ""
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(ycsb_root),
            text=True,
            capture_output=True,
            check=False,
        )
        output = (proc.stdout or "") + (proc.stderr or "")
    except FileNotFoundError:
        ok, rebuild_detail = ensure_ycsbc_binary(ycsb_root)
        if ok:
            proc = subprocess.run(
                cmd,
                cwd=str(ycsb_root),
                text=True,
                capture_output=True,
                check=False,
            )
            output = (proc.stdout or "") + (proc.stderr or "")
        else:
            output = (
                "[ERR] ycsbc binary not found and auto-rebuild failed.\n"
                f"{rebuild_detail}\n"
            )
            proc = subprocess.CompletedProcess(args=cmd, returncode=127)
    log_path = log_dir / (
        f"wl{workload}-{scheme}-c{cache_bytes}-t{threads}-s{shard_bits}"
        f"-r{repeat_idx}.log"
    )
    log_path.write_text(output, encoding="utf-8")

    metrics = parse_metrics(output)
    throughput = metrics.get("throughput_kops", 0.0)
    read_attempt_kops = metrics.get("read_attempt_kops", 0.0)
    write_attempt_kops = metrics.get("write_attempt_kops", 0.0)
    read_success_kops = metrics.get("read_success_kops", 0.0)
    write_success_kops = metrics.get("write_success_kops", 0.0)
    mlc_metrics = collect_mlc_metrics(metrics)
    arc_wrapper_hit_ratio = metrics.get("arc_wrapper_hit_ratio", 0.0)
    cacheus_wrapper_hit_ratio = metrics.get("cacheus_wrapper_hit_ratio", 0.0)
    effective_hit_ratio = metrics.get("cache_hit_ratio", 0.0)
    if scheme == "arc" and "arc_wrapper_hit_ratio" in metrics:
        effective_hit_ratio = arc_wrapper_hit_ratio
    elif scheme == "cacheus" and "cacheus_wrapper_hit_ratio" in metrics:
        effective_hit_ratio = cacheus_wrapper_hit_ratio

    row = {
        "run_id": run_id,
        "workload": workload,
        "scheme": scheme,
        "cache_bytes": str(cache_bytes),
        "threads": str(threads),
        "shard_bits": str(shard_bits),
        "repeat": str(repeat_idx),
        "exit_code": str(proc.returncode),
        "avg_latency_ms": f"{to_latency_ms(throughput):.6f}",
        "read_attempt_kops": f"{read_attempt_kops:.6f}",
        "write_attempt_kops": f"{write_attempt_kops:.6f}",
        "read_success_kops": f"{read_success_kops:.6f}",
        "write_success_kops": f"{write_success_kops:.6f}",
        "read_avg_latency_ms": f"{to_latency_ms(read_success_kops):.6f}",
        "write_avg_latency_ms": f"{to_latency_ms(write_success_kops):.6f}",
        "cache_hit": f"{metrics.get('cache_hit', 0.0):.0f}",
        "cache_miss": f"{metrics.get('cache_miss', 0.0):.0f}",
        "cache_hit_ratio": f"{effective_hit_ratio:.6f}",
        "cache_data_hit_ratio": f"{metrics.get('cache_data_hit_ratio', 0.0):.6f}",
        "cache_filter_hit_ratio": f"{metrics.get('cache_filter_hit_ratio', 0.0):.6f}",
        "cache_index_hit_ratio": f"{metrics.get('cache_index_hit_ratio', 0.0):.6f}",
        "cache_data_hit": f"{metrics.get('cache_data_hit', 0.0):.0f}",
        "cache_data_miss": f"{metrics.get('cache_data_miss', 0.0):.0f}",
        "cache_filter_hit": f"{metrics.get('cache_filter_hit', 0.0):.0f}",
        "cache_filter_miss": f"{metrics.get('cache_filter_miss', 0.0):.0f}",
        "cache_index_hit": f"{metrics.get('cache_index_hit', 0.0):.0f}",
        "cache_index_miss": f"{metrics.get('cache_index_miss', 0.0):.0f}",
        "backing_cache_hit_ratio": f"{metrics.get('cache_hit_ratio', 0.0):.6f}",
        "arc_wrapper_hit_ratio": f"{arc_wrapper_hit_ratio:.6f}",
        "cacheus_wrapper_hit_ratio": f"{cacheus_wrapper_hit_ratio:.6f}",
        "mlc_total_hit_ratio": f"{metrics.get('mlc_total_hit_ratio', 0.0):.6f}",
        "mlc_l0_hit_ratio": f"{get_mlc_level_metric(metrics, 0, 'hit_ratio'):.6f}",
        "mlc_l1_hit_ratio": f"{get_mlc_level_metric(metrics, 1, 'hit_ratio'):.6f}",
        "mlc_l2_hit_ratio": f"{get_mlc_level_metric(metrics, 2, 'hit_ratio'):.6f}",
        "mlc_l3_hit_ratio": f"{get_mlc_level_metric(metrics, 3, 'hit_ratio'):.6f}",
        "mlc_l4_hit_ratio": f"{get_mlc_level_metric(metrics, 4, 'hit_ratio'):.6f}",
        "mlc_l5_hit_ratio": f"{get_mlc_level_metric(metrics, 5, 'hit_ratio'):.6f}",
        "mlc_l6_hit_ratio": f"{get_mlc_level_metric(metrics, 6, 'hit_ratio'):.6f}",
        "mlc_l0_probation_insert": f"{get_mlc_level_metric(metrics, 0, 'probation_insert'):.0f}",
        "mlc_l1_probation_insert": f"{get_mlc_level_metric(metrics, 1, 'probation_insert'):.0f}",
        "mlc_l2_probation_insert": f"{get_mlc_level_metric(metrics, 2, 'probation_insert'):.0f}",
        "mlc_l3_probation_insert": f"{get_mlc_level_metric(metrics, 3, 'probation_insert'):.0f}",
        "mlc_l4_probation_insert": f"{get_mlc_level_metric(metrics, 4, 'probation_insert'):.0f}",
        "mlc_l5_probation_insert": f"{get_mlc_level_metric(metrics, 5, 'probation_insert'):.0f}",
        "mlc_l6_probation_insert": f"{get_mlc_level_metric(metrics, 6, 'probation_insert'):.0f}",
        "mlc_metrics": mlc_metrics,
        "executed_ops": f"{metrics.get('executed_ops', 0.0):.0f}",
        "read_ops": f"{metrics.get('read_ops', 0.0):.0f}",
        "write_ops": f"{metrics.get('write_ops', 0.0):.0f}",
        "read_ok_ops": f"{metrics.get('read_ok_ops', 0.0):.0f}",
        "write_ok_ops": f"{metrics.get('write_ok_ops', 0.0):.0f}",
        "loaded_records": f"{metrics.get('loaded_records', 0.0):.0f}",
        "db_dir": str(db_dir),
        "spec_file": str(spec_path),
        "log_file": str(log_path),
    }
    if cleanup_after_run and (not keep_db) and db_dir.exists():
        remove_tree_with_retry(db_dir)
    return row


def parse_float(props: Dict[str, str], key: str, default: float = 0.0) -> float:
    try:
        return float(props.get(key, str(default)))
    except Exception:
        return default


def is_readonly_workload(props: Dict[str, str]) -> bool:
    update = parse_float(props, "updateproportion", 0.0)
    insert = parse_float(props, "insertproportion", 0.0)
    rmw = parse_float(props, "readmodifywriteproportion", 0.0)
    return (update + insert + rmw) == 0.0


def emit_markdown(rows: List[Dict[str, str]], out_path: pathlib.Path) -> None:
    if not rows:
        out_path.write_text("# No results\n", encoding="utf-8")
        return
    header = (
        "| workload | scheme | cache(GB) | threads | shard_bits | "
        "read_attempt(Kops/s) | "
        "read_success(Kops/s) | write_attempt(Kops/s) | write_success(Kops/s) | "
        "hit_ratio | data_hit_ratio | filter_hit_ratio | index_hit_ratio |\n"
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n"
    )
    lines = [header]
    for r in rows:
        cache_gb = int(r["cache_bytes"]) / (1024**3)
        lines.append(
            f"| {r['workload']} | {r['scheme']} | {cache_gb:.0f} | {r['threads']} | "
            f"{r['shard_bits']} | "
            f"{r['read_attempt_kops']} | {r['read_success_kops']} | "
            f"{r['write_attempt_kops']} | {r['write_success_kops']} | "
            f"{r['cache_hit_ratio']} | {r['cache_data_hit_ratio']} | "
            f"{r['cache_filter_hit_ratio']} | {r['cache_index_hit_ratio']} |\n"
        )
    out_path.write_text("".join(lines), encoding="utf-8")


def main() -> int:
    args = parse_args()
    ycsb_root = pathlib.Path(args.ycsb_root).resolve()
    ok, rebuild_detail = ensure_ycsbc_binary(ycsb_root)
    if not ok:
        print(f"[ERR] ycsbc binary not found under: {ycsb_root}", file=sys.stderr)
        if rebuild_detail:
            print(rebuild_detail, file=sys.stderr)
        print("Please build first: make ROCKSDB_HOME=/users/wzhzhu/rocksdb", file=sys.stderr)
        return 2

    threads = [int(x) for x in args.threads.split(",") if x]
    cache_gb = [int(x) for x in args.cache_gb.split(",") if x]
    shard_bits_list = [int(x) for x in args.shard_bits.split(",") if x.strip()]
    workloads = [x.strip().upper() for x in args.workloads.split(",") if x.strip()]
    schemes = [x.strip() for x in args.schemes.split(",") if x.strip()]

    unknown_wl = [w for w in workloads if w not in WORKLOAD_SPECS]
    unknown_scheme = [s for s in schemes if s not in SCHEMES]
    if unknown_wl:
        print(f"[ERR] Unknown workloads: {unknown_wl}", file=sys.stderr)
        return 2
    if unknown_scheme:
        print(f"[ERR] Unknown schemes: {unknown_scheme}", file=sys.stderr)
        return 2

    run_id = args.run_id or dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    overrides: Dict[str, str] = {}
    if args.recordcount is not None:
        overrides["recordcount"] = str(args.recordcount)
    if args.operationcount is not None:
        overrides["operationcount"] = str(args.operationcount)
    if args.fieldcount is not None:
        overrides["fieldcount"] = str(args.fieldcount)
    if args.fieldlength is not None:
        overrides["fieldlength"] = str(args.fieldlength)
    overrides["loadthreadcount"] = str(args.load_threads)
    overrides["insertorder"] = args.insert_order

    results_dir = (ycsb_root / args.results_dir / run_id).resolve()
    results_dir.mkdir(parents=True, exist_ok=True)
    db_root_dir = pathlib.Path(args.db_root_dir).resolve()
    db_root_dir.mkdir(parents=True, exist_ok=True)

    matrix: List[Tuple[str, str, int, int, int, int]] = []
    for wl in workloads:
        for scheme in schemes:
            for cgb in cache_gb:
                for t in threads:
                    for sb in shard_bits_list:
                        for r in range(1, args.repeats + 1):
                            matrix.append((wl, scheme, cgb * 1024**3, t, sb, r))

    print(f"[INFO] run_id={run_id}")
    print(f"[INFO] total_runs={len(matrix)}")
    if args.dry_run:
        for wl, scheme, cache_bytes, t, sb, r in matrix[:20]:
            print(
                f"dry-run: wl={wl} scheme={scheme} cache={cache_bytes}B "
                f"threads={t} shard_bits={sb} repeat={r}"
            )
        if len(matrix) > 20:
            print(f"... ({len(matrix) - 20} more)")
        return 0

    base_workload_props: Dict[str, Dict[str, str]] = {
        wl: load_spec(ycsb_root / WORKLOAD_SPECS[wl]) for wl in workloads
    }
    readonly_workloads = {
        wl: is_readonly_workload(base_workload_props[wl]) for wl in workloads
    }
    shared_loaded_workloads: set = set()
    shared_db_subdirs: Dict[str, str] = {}

    rows: List[Dict[str, str]] = []
    for idx, (wl, scheme, cache_bytes, t, sb, r) in enumerate(matrix, start=1):
        print(
            f"[{idx}/{len(matrix)}] wl={wl} scheme={scheme} "
            f"cache={cache_bytes} threads={t} shard_bits={sb} repeat={r}",
            flush=True,
        )
        reuse_db = (
            args.refill_policy == "reuse_if_readonly" and readonly_workloads.get(wl, False)
        )
        if reuse_db:
            db_subdir = shared_db_subdirs.get(wl, f"shared/wl{wl}")
            shared_db_subdirs[wl] = db_subdir
            skip_load = wl in shared_loaded_workloads
            clean_db_before_run = wl not in shared_loaded_workloads
            cleanup_after_run = False
        else:
            db_subdir = f"wl{wl}-{scheme}-c{cache_bytes}-t{t}-s{sb}-r{r}"
            skip_load = False
            clean_db_before_run = True
            cleanup_after_run = True

        row = run_once(
            ycsb_root,
            results_dir,
            db_root_dir,
            run_id,
            wl,
            scheme,
            cache_bytes,
            t,
            r,
            sb,
            overrides,
            db_subdir,
            skip_load,
            clean_db_before_run,
            cleanup_after_run,
            args.keep_db,
        )
        rows.append(row)
        if reuse_db and row["exit_code"] == "0":
            shared_loaded_workloads.add(wl)
        if row["exit_code"] != "0":
            print(f"[WARN] non-zero exit: {row['log_file']}", file=sys.stderr)

    csv_path = results_dir / "summary.csv"
    fieldnames = list(rows[0].keys()) if rows else []
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    md_path = results_dir / "summary.md"
    emit_markdown(rows, md_path)

    if (not args.keep_db) and shared_db_subdirs:
        for subdir in shared_db_subdirs.values():
            shared_dir = db_root_dir / run_id / subdir
            if shared_dir.exists():
                remove_tree_with_retry(shared_dir)

    print(f"[INFO] summary_csv={csv_path}")
    print(f"[INFO] summary_md={md_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
