#!/usr/bin/env python3
"""
mp_benchmark.py
===============
Vanilla Python multiprocessing benchmark.

Workloads:
  light   ~200  integer ops per task  (50 iters × 4 LCG/XOR)
  medium  ~1200 integer ops per task  (300 iters × 4)
  heavy   ~4000 integer ops per task  (1000 iters × 4)

Phases: 900 / 9,000 / 90,000 / 1,800,000 jobs — 8 workers.

Two scheduling modes:
  Static  — pre-partition range equally across workers (zero contention)
  Dynamic — workers batch-claim from a shared counter under lock
"""

import sys
import os
import time
from multiprocessing import Process, Value, Barrier

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

# ============================================================
# CONFIG
# ============================================================
NUM_WORKERS = 8
PHASES = [900, 9_000, 90_000, 1_800_000]
DYNAMIC_BATCH = 64


# ============================================================
# WORKLOADS — pure integer arithmetic, no I/O, no sleep
# ============================================================
def light_load(seed):
    """~200 integer arithmetic ops (50 iters × 4 ops)."""
    x = seed | 1
    for _ in range(50):
        x = (x * 1664525 + 1013904223) & 0xFFFFFFFF
        x ^= x >> 16
        x = (x * 2654435761) & 0xFFFFFFFF
        x ^= x >> 13
    return x


def medium_load(seed):
    """~1200 integer arithmetic ops (300 iters × 4 ops)."""
    x = seed | 1
    for _ in range(300):
        x = (x * 1664525 + 1013904223) & 0xFFFFFFFF
        x ^= x >> 16
        x = (x * 2654435761) & 0xFFFFFFFF
        x ^= x >> 13
    return x


def heavy_load(seed):
    """~4000 integer arithmetic ops (1000 iters × 4 ops)."""
    x = seed | 1
    for _ in range(1000):
        x = (x * 1664525 + 1013904223) & 0xFFFFFFFF
        x ^= x >> 16
        x = (x * 2654435761) & 0xFFFFFFFF
        x ^= x >> 13
    return x


WORKLOADS = [
    ("light",  light_load,  200),
    ("medium", medium_load, 1200),
    ("heavy",  heavy_load,  4000),
]


# ============================================================
# STATIC MODE — pre-partitioned, zero shared state
# ============================================================
def _static_worker(fn, start, count, barrier):
    """Each worker gets a pre-assigned [start, start+count) range."""
    barrier.wait()  # sync: all workers + main ready before work begins
    for i in range(start, start + count):
        fn(i)


def run_static(fn, total_jobs, num_workers=NUM_WORKERS):
    chunk = total_jobs // num_workers
    remainder = total_jobs % num_workers

    specs = []
    pos = 0
    for i in range(num_workers):
        c = chunk + (1 if i < remainder else 0)
        specs.append((pos, c))
        pos += c

    barrier = Barrier(num_workers + 1)

    processes = [
        Process(target=_static_worker, args=(fn, s, c, barrier))
        for s, c in specs
    ]

    for p in processes:
        p.start()
    barrier.wait()  # all workers alive and ready
    t0 = time.perf_counter_ns()
    for p in processes:
        p.join()
    t1 = time.perf_counter_ns()

    return (t1 - t0) / 1_000_000_000


# ============================================================
# DYNAMIC MODE — shared counter, batch claiming
# ============================================================
def _dynamic_worker(fn, counter, total, batch_size, barrier):
    """Claim batch_size tasks at a time from shared counter."""
    barrier.wait()  # sync: all workers + main ready before work begins
    while True:
        with counter.get_lock():
            start = counter.value
            if start >= total:
                return
            end = min(start + batch_size, total)
            counter.value = end
        for i in range(start, end):
            fn(i)


def run_dynamic(fn, total_jobs, num_workers=NUM_WORKERS,
                batch_size=DYNAMIC_BATCH):
    counter = Value("i", 0)
    barrier = Barrier(num_workers + 1)

    processes = [
        Process(target=_dynamic_worker,
                args=(fn, counter, total_jobs, batch_size, barrier))
        for _ in range(num_workers)
    ]

    for p in processes:
        p.start()
    barrier.wait()  # all workers alive and ready
    t0 = time.perf_counter_ns()
    for p in processes:
        p.join()
    t1 = time.perf_counter_ns()

    return (t1 - t0) / 1_000_000_000


# ============================================================
# PLOTTING
# ============================================================
def plot_results(results, output_path):
    fig, axes = plt.subplots(1, 3, figsize=(18, 5.5), sharey=False)
    phase_labels = [f"{p:,}" for p in PHASES]
    x = np.arange(len(PHASES))
    width = 0.30

    for idx, (name, _, ops) in enumerate(WORKLOADS):
        ax = axes[idx]
        st = [results[name]["static"][p] for p in PHASES]
        dy = [results[name]["dynamic"][p] for p in PHASES]

        b1 = ax.bar(x - width / 2, st, width, label="Static",
                     color="#2979FF", edgecolor="white", linewidth=0.5)
        b2 = ax.bar(x + width / 2, dy, width, label="Dynamic",
                     color="#FF6D00", edgecolor="white", linewidth=0.5)

        ax.set_xlabel("Jobs", fontsize=9)
        ax.set_ylabel("Time (s)", fontsize=9)
        ax.set_title(f"{name.capitalize()} (~{ops} ops)", fontsize=10)
        ax.set_xticks(x)
        ax.set_xticklabels(phase_labels, fontsize=7, rotation=25)
        ax.set_yscale("log")
        ax.legend(fontsize=7, loc="upper left")
        ax.grid(axis="y", alpha=0.3)

        for bar in list(b1) + list(b2):
            h = bar.get_height()
            if h > 0:
                label = f"{h:.3f}" if h < 10 else f"{h:.1f}"
                ax.text(bar.get_x() + bar.get_width() / 2.0, h * 1.15,
                        label, ha="center", va="bottom", fontsize=5.5,
                        rotation=45)

    fig.suptitle(
        f"multiprocessing Benchmark — Static vs Dynamic "
        f"({NUM_WORKERS} workers, batch={DYNAMIC_BATCH})",
        fontsize=12, fontweight="bold")
    fig.tight_layout()
    fig.subplots_adjust(top=0.88)
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Plot → {output_path}")


def plot_throughput(results, output_path):
    fig, axes = plt.subplots(1, 3, figsize=(18, 5.5), sharey=False)
    phase_labels = [f"{p:,}" for p in PHASES]
    x = np.arange(len(PHASES))
    width = 0.30

    for idx, (name, _, ops) in enumerate(WORKLOADS):
        ax = axes[idx]
        st_tp = [p / max(results[name]["static"][p], 1e-9) for p in PHASES]
        dy_tp = [p / max(results[name]["dynamic"][p], 1e-9) for p in PHASES]

        ax.bar(x - width / 2, st_tp, width, label="Static",
               color="#2979FF", edgecolor="white", linewidth=0.5)
        ax.bar(x + width / 2, dy_tp, width, label="Dynamic",
               color="#FF6D00", edgecolor="white", linewidth=0.5)

        ax.set_xlabel("Jobs", fontsize=9)
        ax.set_ylabel("Tasks/sec", fontsize=9)
        ax.set_title(f"{name.capitalize()} (~{ops} ops)", fontsize=10)
        ax.set_xticks(x)
        ax.set_xticklabels(phase_labels, fontsize=7, rotation=25)
        ax.set_yscale("log")
        ax.legend(fontsize=7, loc="upper left")
        ax.grid(axis="y", alpha=0.3)

    fig.suptitle(
        f"multiprocessing Throughput — Static vs Dynamic "
        f"({NUM_WORKERS} workers, batch={DYNAMIC_BATCH})",
        fontsize=12, fontweight="bold")
    fig.tight_layout()
    fig.subplots_adjust(top=0.88)
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Plot → {output_path}")


# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    results = {name: {"static": {}, "dynamic": {}} for name, _, _ in WORKLOADS}

    sep = "=" * 74
    print(sep)
    print("  multiprocessing Benchmark")
    print(f"  Workers: {NUM_WORKERS}  |  Dynamic batch: {DYNAMIC_BATCH}")
    print(sep)
    print(f"  {'Load':>8} | {'Jobs':>10} | {'Static (s)':>11} "
          f"| {'Dynamic (s)':>12} | {'Ratio':>7}")
    print("-" * 74)

    for name, fn, ops in WORKLOADS:
        for phase in PHASES:
            t_static = run_static(fn, phase)
            t_dynamic = run_dynamic(fn, phase)

            results[name]["static"][phase] = t_static
            results[name]["dynamic"][phase] = t_dynamic

            ratio = t_dynamic / t_static if t_static > 0 else float("inf")
            print(f"  {name:>8} | {phase:>10,} | {t_static:>10.4f}s "
                  f"| {t_dynamic:>11.4f}s | {ratio:>6.2f}x")
        print("-" * 74)

    # Throughput table
    print()
    print(f"  {'Load':>8} | {'Jobs':>10} | {'Static t/s':>12} "
          f"| {'Dynamic t/s':>13}")
    print("-" * 74)
    for name, _, _ in WORKLOADS:
        for phase in PHASES:
            st = results[name]["static"][phase]
            dy = results[name]["dynamic"][phase]
            print(f"  {name:>8} | {phase:>10,} | "
                  f"{phase / max(st, 1e-9):>11,.0f}/s | "
                  f"{phase / max(dy, 1e-9):>12,.0f}/s")
        print("-" * 74)
    print(sep)

    out_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                           "benchmark_output")
    os.makedirs(out_dir, exist_ok=True)

    plot_results(results, os.path.join(out_dir, "mp_benchmark_time.png"))
    plot_throughput(results,
                    os.path.join(out_dir, "mp_benchmark_throughput.png"))
    print("\nDone.")