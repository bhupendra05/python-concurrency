"""ThreadPoolExecutor patterns: map vs submit, futures, timeouts, callbacks."""
import time
import random
import threading
from concurrent.futures import ThreadPoolExecutor, Future, as_completed, wait, FIRST_COMPLETED

# ---------------------------------------------------------------------------
# Pattern 1: executor.map() — simplest, maintains input order
# ---------------------------------------------------------------------------

def fetch_url(url: str) -> dict:
    """Simulate an HTTP GET request."""
    delay = random.uniform(0.05, 0.3)
    time.sleep(delay)
    return {"url": url, "status": 200, "size": random.randint(1000, 50000), "time_ms": int(delay * 1000)}


def demo_map():
    """executor.map() — blocks until all done, results in input order."""
    urls = [f"https://api.example.com/item/{i}" for i in range(8)]

    print("=== executor.map() ===")
    t = time.perf_counter()
    with ThreadPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(fetch_url, urls))

    elapsed = time.perf_counter() - t
    for r in results:
        print(f"  {r['url'][-15:]} → {r['status']} ({r['time_ms']}ms)")
    print(f"Total: {elapsed:.2f}s (serial would be ~{sum(r['time_ms'] for r in results)/1000:.2f}s)")
    return results


# ---------------------------------------------------------------------------
# Pattern 2: executor.submit() — fine-grained control, non-blocking
# ---------------------------------------------------------------------------

def process_file(filename: str) -> tuple[str, int, float]:
    """Simulate processing a file. Returns (name, lines, duration)."""
    delay = random.uniform(0.1, 0.5)
    time.sleep(delay)
    lines = random.randint(100, 10000)
    return filename, lines, delay


def demo_submit():
    """executor.submit() — non-blocking, futures for tracking individual tasks."""
    filenames = [f"data_{i:02d}.csv" for i in range(6)]

    print("\n=== executor.submit() with as_completed() ===")
    t = time.perf_counter()

    with ThreadPoolExecutor(max_workers=3) as executor:
        # Submit all tasks, get Future objects
        future_to_file = {executor.submit(process_file, f): f for f in filenames}

        # Process results as they complete (not in submission order)
        for future in as_completed(future_to_file):
            filename = future_to_file[future]
            try:
                name, lines, duration = future.result()
                print(f"  {name}: {lines} lines ({duration:.2f}s)")
            except Exception as e:
                print(f"  {filename} failed: {e}")

    print(f"Total: {time.perf_counter() - t:.2f}s")


# ---------------------------------------------------------------------------
# Pattern 3: wait() with conditions
# ---------------------------------------------------------------------------

def demo_wait():
    """wait() — wait for first N futures or specific conditions."""
    print("\n=== wait() FIRST_COMPLETED ===")

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(fetch_url, f"https://api.example.com/{i}") for i in range(5)]

        # Get first completed result
        done, not_done = wait(futures, return_when=FIRST_COMPLETED)
        first = list(done)[0]
        result = first.result()
        print(f"First completed: {result['url'][-20:]} ({result['time_ms']}ms)")
        print(f"Still pending: {len(not_done)} tasks")

        # Wait for ALL remaining
        wait(not_done)
        print("All tasks completed.")


# ---------------------------------------------------------------------------
# Pattern 4: Callbacks and cancellation
# ---------------------------------------------------------------------------

def demo_callbacks():
    """Add callbacks to futures and cancel pending tasks."""
    print("\n=== Futures with callbacks ===")
    completed_count = [0]
    lock = threading.Lock()

    def on_done(future: Future):
        with lock:
            completed_count[0] += 1
        if not future.cancelled() and future.exception() is None:
            result = future.result()
            print(f"  Callback: {result['url'][-20:]} done")

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = []
        for i in range(6):
            f = executor.submit(fetch_url, f"https://api.example.com/cb/{i}")
            f.add_done_callback(on_done)
            futures.append(f)

        # Cancel tasks that haven't started yet
        cancelled = sum(1 for f in futures if f.cancel())
        print(f"Cancelled {cancelled} pending tasks")

    print(f"Completed {completed_count[0]} tasks total")


# ---------------------------------------------------------------------------
# Pattern 5: Timeout handling
# ---------------------------------------------------------------------------

def slow_operation(duration: float) -> str:
    """An operation that may take too long."""
    time.sleep(duration)
    return f"Done after {duration:.1f}s"


def demo_timeout():
    """Handle futures with per-future timeouts."""
    print("\n=== Per-future timeout ===")
    durations = [0.1, 0.2, 2.0, 0.3, 3.0]  # 2.0 and 3.0 will time out

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [(duration, executor.submit(slow_operation, duration)) for duration in durations]

        for duration, future in futures:
            try:
                result = future.result(timeout=0.5)
                print(f"  duration={duration:.1f}s: {result}")
            except TimeoutError:
                future.cancel()
                print(f"  duration={duration:.1f}s: TIMED OUT (cancelled)")
            except Exception as e:
                print(f"  duration={duration:.1f}s: ERROR {e}")


if __name__ == "__main__":
    demo_map()
    demo_submit()
    demo_wait()
    demo_callbacks()
    demo_timeout()
