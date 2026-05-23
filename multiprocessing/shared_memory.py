"""SharedMemory for zero-copy inter-process data sharing."""
import numpy as np
import time
import multiprocessing
from multiprocessing import shared_memory, Process
from multiprocessing.managers import SharedMemoryManager


# ---------------------------------------------------------------------------
# Pattern 1: Raw SharedMemory with numpy
# ---------------------------------------------------------------------------

def worker_read(shm_name: str, shape: tuple, dtype, result_queue):
    """Worker that reads from shared memory without copying data."""
    # Attach to existing shared memory
    existing_shm = shared_memory.SharedMemory(name=shm_name)
    arr = np.ndarray(shape, dtype=dtype, buffer=existing_shm.buf)

    # Compute on the shared data (zero-copy read)
    total = arr.sum()
    mean = arr.mean()
    result_queue.put({"sum": float(total), "mean": float(mean)})

    # Don't delete — just unlink from this process
    existing_shm.close()


def worker_write(shm_name: str, shape: tuple, dtype, start_val: int):
    """Worker that writes to shared memory."""
    existing_shm = shared_memory.SharedMemory(name=shm_name)
    arr = np.ndarray(shape, dtype=dtype, buffer=existing_shm.buf)

    # Write to shared memory (visible to all other processes instantly)
    arr[:] = np.arange(start_val, start_val + arr.size, dtype=dtype).reshape(shape)
    existing_shm.close()


def demo_shared_memory():
    """Share a large numpy array between processes without copying."""
    shape = (1000, 1000)
    dtype = np.float64
    size_mb = np.prod(shape) * np.dtype(dtype).itemsize / 1024 / 1024

    print(f"=== SharedMemory Demo ({size_mb:.1f} MB array) ===")

    # Create shared memory
    shm = shared_memory.SharedMemory(create=True, size=np.prod(shape) * np.dtype(dtype).itemsize)
    arr = np.ndarray(shape, dtype=dtype, buffer=shm.buf)
    arr[:] = np.random.rand(*shape)

    print(f"Original array: sum={arr.sum():.2f}, mean={arr.mean():.4f}")

    # Spawn multiple readers — they all access the same memory
    result_queue: multiprocessing.Queue = multiprocessing.Queue()
    readers = [
        Process(target=worker_read, args=(shm.name, shape, dtype, result_queue))
        for _ in range(3)
    ]

    t = time.perf_counter()
    for p in readers:
        p.start()
    for p in readers:
        p.join()

    results = [result_queue.get() for _ in readers]
    elapsed = time.perf_counter() - t

    print(f"3 readers finished in {elapsed:.3f}s")
    for i, r in enumerate(results):
        print(f"  Reader {i}: sum={r['sum']:.2f}, mean={r['mean']:.4f}")

    # Cleanup
    shm.close()
    shm.unlink()


# ---------------------------------------------------------------------------
# Pattern 2: SharedMemoryManager for managed lifecycle
# ---------------------------------------------------------------------------

def worker_with_manager(shm_name: str, shape: tuple):
    """Worker using manager-created shared memory."""
    shm = shared_memory.SharedMemory(name=shm_name)
    arr = np.ndarray(shape, dtype=np.int32, buffer=shm.buf)
    # Multiply each element by 2 in-place
    arr *= 2
    shm.close()


def demo_shared_memory_manager():
    """Use SharedMemoryManager for automatic cleanup."""
    print("\n=== SharedMemoryManager Demo ===")
    shape = (100,)
    size_bytes = np.prod(shape) * np.dtype(np.int32).itemsize

    with SharedMemoryManager() as manager:
        shm = manager.SharedMemory(size=size_bytes)
        arr = np.ndarray(shape, dtype=np.int32, buffer=shm.buf)
        arr[:] = np.arange(100, dtype=np.int32)

        print(f"Before: arr[0:5] = {arr[:5].tolist()}")

        # Worker modifies in-place
        p = Process(target=worker_with_manager, args=(shm.name, shape))
        p.start()
        p.join()

        print(f"After worker (x2): arr[0:5] = {arr[:5].tolist()}")
        # Manager auto-cleans up when context exits


# ---------------------------------------------------------------------------
# Pattern 3: Benchmark — copy vs shared memory
# ---------------------------------------------------------------------------

def demo_benchmark():
    """Compare passing data by copy (pickle) vs shared memory."""
    print("\n=== Benchmark: Copy vs SharedMemory ===")
    shape = (500, 500)
    dtype = np.float64
    data = np.random.rand(*shape)

    # Method 1: multiprocessing with pickling (copy)
    def process_copy(arr):
        return arr.sum()

    t = time.perf_counter()
    with multiprocessing.Pool(4) as pool:
        # Each call pickles and copies data to worker
        results = pool.map(process_copy, [data] * 4)
    copy_time = time.perf_counter() - t
    print(f"  With pickling (copy): {copy_time:.3f}s")

    # Method 2: shared memory (zero-copy)
    size_bytes = data.nbytes
    shm = shared_memory.SharedMemory(create=True, size=size_bytes)
    shared_arr = np.ndarray(shape, dtype=dtype, buffer=shm.buf)
    shared_arr[:] = data

    result_q: multiprocessing.Queue = multiprocessing.Queue()
    t = time.perf_counter()
    workers = [
        Process(target=worker_read, args=(shm.name, shape, dtype, result_q))
        for _ in range(4)
    ]
    for w in workers:
        w.start()
    for w in workers:
        w.join()
    shm_time = time.perf_counter() - t

    shm.close()
    shm.unlink()
    print(f"  With shared memory:   {shm_time:.3f}s")
    print(f"  Speedup: {copy_time / shm_time:.1f}x" if shm_time > 0 else "  (too fast to measure)")


if __name__ == "__main__":
    demo_shared_memory()
    demo_shared_memory_manager()
    demo_benchmark()
