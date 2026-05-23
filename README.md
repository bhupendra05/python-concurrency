# Python Concurrency

Practical examples comparing Python's three concurrency models — threading, multiprocessing, and asyncio — with benchmarks and clear guidance on when to use each.

## Run the Benchmark

```bash
python benchmarks/compare_all.py
```

Sample output:
```
### CPU-BOUND WORKLOAD (4x compute sum of squares)
Method                Time     Speedup vs Serial
------                ----     -----------------
Serial                3.210s   1.0x
Threading (4)         3.195s   1.0x   ← GIL blocks parallelism!
Multiprocessing (4)   0.890s   3.6x   ← True parallelism

### I/O-BOUND WORKLOAD (20x sleep 0.1s)
Method                Time     Speedup vs Serial
------                ----     -----------------
Serial                2.003s   1.0x
Threading (20)        0.101s   19.8x
Multiprocessing (20)  0.105s   19.1x
asyncio               0.101s   19.9x
```

## Examples

### Threading (`threading/`)

```bash
python threading/producer_consumer.py  # Thread-safe queue with producer/consumer
python threading/thread_pool.py        # ThreadPoolExecutor patterns
```

**`producer_consumer.py`** — Classic producer/consumer with `queue.Queue`. Thread-safe bounded queue, sentinel values, multiple consumers.

**`thread_pool.py`** — Complete guide to ThreadPoolExecutor:
- `executor.map()` — simplest, results in input order
- `executor.submit()` + `as_completed()` — process as results arrive
- `wait(FIRST_COMPLETED)` — get first result
- Callbacks via `add_done_callback()`
- Per-future timeout with `future.result(timeout=n)`
- Rate-limited execution

### Multiprocessing (`multiprocessing/`)

```bash
python multiprocessing/cpu_bound.py     # Serial vs threading vs multiprocessing
python multiprocessing/shared_memory.py # Zero-copy data sharing between processes
```

**`cpu_bound.py`** — The definitive GIL demonstration: threading doesn't help for CPU work, multiprocessing does.

**`shared_memory.py`** — `multiprocessing.shared_memory.SharedMemory` for sharing large numpy arrays between processes with zero copies. Includes benchmark vs pickle-based copying.

### Concurrent.futures (`concurrent_futures/`)

```bash
python concurrent_futures/executor_patterns.py
```

Covers: `as_completed`, `wait` with all completion modes, future cancellation, pipeline chaining with callbacks, rate-limited execution.

## The GIL Explained

The **Global Interpreter Lock (GIL)** is a mutex in CPython that prevents multiple threads from executing Python bytecode simultaneously.

```python
# This does NOT run in parallel — GIL ensures only 1 thread at a time
import threading
def cpu_work():
    sum(i*i for i in range(10_000_000))

t1 = threading.Thread(target=cpu_work)
t2 = threading.Thread(target=cpu_work)
t1.start(); t2.start()
t1.join(); t2.join()
# Takes just as long as serial!
```

The GIL is **released** during I/O operations (socket.recv, file.read, time.sleep), which is why threading works well for I/O-bound code.

## Decision Guide

| Workload type | Example | Use |
|--------------|---------|-----|
| I/O-bound, simple | HTTP requests, file I/O | `ThreadPoolExecutor` |
| I/O-bound, high concurrency | 1000+ concurrent connections | `asyncio` |
| CPU-bound, independent tasks | Image processing, ML inference | `ProcessPoolExecutor` |
| CPU-bound, shared memory | Scientific computing, numpy | `multiprocessing` + `SharedMemory` |
| Mixed workloads | Web scraper with parsing | Thread pool for I/O, process pool for parsing |

## Quick Reference

```python
# ThreadPoolExecutor — I/O-bound
from concurrent.futures import ThreadPoolExecutor
with ThreadPoolExecutor(max_workers=10) as ex:
    results = list(ex.map(fetch_url, urls))

# ProcessPoolExecutor — CPU-bound
from concurrent.futures import ProcessPoolExecutor
with ProcessPoolExecutor(max_workers=4) as ex:
    results = list(ex.map(process_image, image_paths))

# asyncio — async I/O
import asyncio
async def main():
    results = await asyncio.gather(*[fetch(url) for url in urls])
asyncio.run(main())

# as_completed — process results in arrival order
from concurrent.futures import as_completed
futures = {ex.submit(task, arg): arg for arg in args}
for future in as_completed(futures):
    result = future.result()
```
