"""
Comprehensive benchmark: compare serial, threading, multiprocessing, asyncio
for both CPU-bound and I/O-bound workloads.
"""
import time
import asyncio
import threading
import multiprocessing
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from typing import Callable


# ---------------------------------------------------------------------------
# Workloads
# ---------------------------------------------------------------------------

def cpu_task(n: int) -> int:
    """CPU-bound: compute sum of squares."""
    return sum(i * i for i in range(n))


def io_task_sync(duration: float) -> float:
    """I/O-bound: blocking sleep simulating disk/network."""
    import time as _time
    _time.sleep(duration)
    return duration


async def io_task_async(duration: float) -> float:
    """I/O-bound: async sleep (non-blocking)."""
    await asyncio.sleep(duration)
    return duration


# ---------------------------------------------------------------------------
# Runners
# ---------------------------------------------------------------------------

def run_serial_cpu(tasks: list[int]) -> list[int]:
    return [cpu_task(n) for n in tasks]


def run_threaded_cpu(tasks: list[int], workers: int = 4) -> list[int]:
    with ThreadPoolExecutor(max_workers=workers) as ex:
        return list(ex.map(cpu_task, tasks))


def run_multiprocess_cpu(tasks: list[int], workers: int = 4) -> list[int]:
    with ProcessPoolExecutor(max_workers=workers) as ex:
        return list(ex.map(cpu_task, tasks))


def run_serial_io(tasks: list[float]) -> list[float]:
    return [io_task_sync(t) for t in tasks]


def run_threaded_io(tasks: list[float], workers: int = 10) -> list[float]:
    with ThreadPoolExecutor(max_workers=workers) as ex:
        return list(ex.map(io_task_sync, tasks))


def run_multiprocess_io(tasks: list[float], workers: int = 10) -> list[float]:
    with ProcessPoolExecutor(max_workers=workers) as ex:
        return list(ex.map(io_task_sync, tasks))


def run_asyncio_io(tasks: list[float]) -> list[float]:
    async def _run():
        return await asyncio.gather(*[io_task_async(t) for t in tasks])
    return asyncio.run(_run())


# ---------------------------------------------------------------------------
# Benchmark runner
# ---------------------------------------------------------------------------

def benchmark(name: str, fn: Callable, *args, **kwargs) -> float:
    t = time.perf_counter()
    result = fn(*args, **kwargs)
    elapsed = time.perf_counter() - t
    return elapsed


def print_table(headers: list[str], rows: list[list]):
    widths = [max(len(str(row[i])) for row in [headers] + rows) for i in range(len(headers))]
    fmt = "  ".join(f"{{:<{w}}}" for w in widths)
    print(fmt.format(*headers))
    print("  ".join("-" * w for w in widths))
    for row in rows:
        print(fmt.format(*row))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    cpu_count = multiprocessing.cpu_count()
    print(f"CPU cores: {cpu_count}")
    print("=" * 60)

    # --- CPU-bound benchmark ---
    print("\n### CPU-BOUND WORKLOAD")
    print("Task: 4x sum(i*i for i in range(5_000_000))")
    print("(GIL prevents threading from helping here)\n")

    CPU_TASKS = [5_000_000] * 4

    cpu_rows = []
    for name, fn, args in [
        ("Serial", run_serial_cpu, (CPU_TASKS,)),
        ("Threading (4)", run_threaded_cpu, (CPU_TASKS,)),
        ("Multiprocessing (4)", run_multiprocess_cpu, (CPU_TASKS,)),
    ]:
        elapsed = benchmark(name, fn, *args)
        cpu_rows.append([name, f"{elapsed:.3f}s", ""])

    # Calculate speedup relative to serial
    serial_time = float(cpu_rows[0][1].rstrip("s"))
    for row in cpu_rows:
        t = float(row[1].rstrip("s"))
        row[2] = f"{serial_time/t:.1f}x" if t > 0 else "—"

    print_table(["Method", "Time", "Speedup vs Serial"], cpu_rows)

    # --- I/O-bound benchmark ---
    print("\n\n### I/O-BOUND WORKLOAD")
    print("Task: 20x sleep(0.1s) — simulates network/disk I/O")
    print("(All concurrent methods should be ~0.1s; serial takes ~2s)\n")

    IO_TASKS = [0.1] * 20

    io_rows = []
    for name, fn, args in [
        ("Serial", run_serial_io, (IO_TASKS,)),
        ("Threading (20)", run_threaded_io, (IO_TASKS, 20)),
        ("Multiprocessing (20)", run_multiprocess_io, (IO_TASKS, 20)),
        ("asyncio", run_asyncio_io, (IO_TASKS,)),
    ]:
        elapsed = benchmark(name, fn, *args)
        io_rows.append([name, f"{elapsed:.3f}s", ""])

    serial_io_time = float(io_rows[0][1].rstrip("s"))
    for row in io_rows:
        t = float(row[1].rstrip("s"))
        row[2] = f"{serial_io_time/t:.1f}x" if t > 0 else "—"

    print_table(["Method", "Time", "Speedup vs Serial"], io_rows)

    # --- Summary ---
    print("\n\n### WHEN TO USE EACH")
    print_table(
        ["Method", "Best for", "Limitation"],
        [
            ["Serial", "Simple scripts, debugging", "Slow for concurrent work"],
            ["threading", "I/O-bound (HTTP, DB, files)", "GIL blocks CPU work"],
            ["multiprocessing", "CPU-bound (math, ML, image)", "High startup overhead"],
            ["asyncio", "High-concurrency I/O (async libs)", "Requires async code"],
        ]
    )

    print("\n### GIL EXPLANATION")
    print("The Global Interpreter Lock (GIL) ensures only one Python thread")
    print("executes bytecode at a time. This means:")
    print("  - threading  → no speedup for CPU-bound work")
    print("  - threading  → DOES help for I/O (GIL released during I/O)")
    print("  - multiprocessing → bypasses GIL (separate processes)")
    print("  - asyncio    → cooperative concurrency (not parallelism)")
