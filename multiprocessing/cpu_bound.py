"""Compare serial, threading, multiprocessing for CPU-bound work."""
import time, threading, multiprocessing
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

def cpu_task(n):
    """CPU-bound: count to n."""
    return sum(i * i for i in range(n))

TASKS = [5_000_000] * 4

def run_serial():
    return [cpu_task(n) for n in TASKS]

def run_threaded():
    with ThreadPoolExecutor(max_workers=4) as ex:
        return list(ex.map(cpu_task, TASKS))

def run_multiprocess():
    with ProcessPoolExecutor(max_workers=4) as ex:
        return list(ex.map(cpu_task, TASKS))

if __name__ == "__main__":
    for name, fn in [("Serial", run_serial), ("Threading", run_threaded), ("Multiprocessing", run_multiprocess)]:
        t = time.perf_counter()
        results = fn()
        elapsed = time.perf_counter() - t
        print(f"{name:<18}: {elapsed:.3f}s")
    # Threading won't help (GIL), multiprocessing WILL help
