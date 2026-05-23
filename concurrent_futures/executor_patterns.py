"""Concurrent.futures patterns: as_completed, wait, cancel, timeout, chaining."""
import time
import random
from concurrent.futures import (
    ThreadPoolExecutor, ProcessPoolExecutor,
    as_completed, wait, FIRST_COMPLETED, FIRST_EXCEPTION, ALL_COMPLETED,
    Future,
)


# ---------------------------------------------------------------------------
# 1. as_completed — process results in completion order
# ---------------------------------------------------------------------------

def slow_task(task_id: int, duration: float) -> dict:
    time.sleep(duration)
    return {"id": task_id, "duration": duration, "result": task_id ** 2}


def demo_as_completed():
    print("=== as_completed (results arrive in completion order) ===")
    tasks = [(i, random.uniform(0.05, 0.4)) for i in range(8)]

    with ThreadPoolExecutor(max_workers=4) as executor:
        future_to_task = {
            executor.submit(slow_task, task_id, duration): task_id
            for task_id, duration in tasks
        }

        order = []
        for future in as_completed(future_to_task):
            task_id = future_to_task[future]
            result = future.result()
            order.append(task_id)
            print(f"  Task {task_id} completed: result={result['result']} ({result['duration']:.2f}s)")

    print(f"Completion order: {order}")


# ---------------------------------------------------------------------------
# 2. wait() with FIRST_EXCEPTION — stop on first error
# ---------------------------------------------------------------------------

def maybe_fail(task_id: int) -> int:
    time.sleep(random.uniform(0.05, 0.2))
    if task_id == 3:
        raise ValueError(f"Task {task_id} failed intentionally!")
    return task_id * 10


def demo_wait_first_exception():
    print("\n=== wait() FIRST_EXCEPTION ===")
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(maybe_fail, i) for i in range(8)]

        done, not_done = wait(futures, return_when=FIRST_EXCEPTION)
        print(f"Done: {len(done)}, Still pending: {len(not_done)}")

        for future in done:
            if future.exception():
                print(f"  Exception caught: {future.exception()}")
                # Cancel remaining tasks
                cancelled = sum(1 for f in not_done if f.cancel())
                print(f"  Cancelled {cancelled} pending tasks")
                break
            else:
                print(f"  Succeeded: {future.result()}")


# ---------------------------------------------------------------------------
# 3. Cancellation patterns
# ---------------------------------------------------------------------------

def long_running(task_id: int, seconds: float) -> str:
    """Task that can be cancelled before it starts."""
    time.sleep(seconds)
    return f"Task {task_id} completed"


def demo_cancellation():
    print("\n=== Future cancellation ===")
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(long_running, i, 0.5) for i in range(6)]

        # Cancel tasks not yet started (only unstarted tasks can be cancelled)
        time.sleep(0.1)  # let first 2 start
        cancelled_count = sum(1 for f in futures if f.cancel())
        print(f"Cancelled {cancelled_count} tasks (the ones still queued)")

        results = []
        for i, future in enumerate(futures):
            if future.cancelled():
                results.append(f"Task {i}: cancelled")
            else:
                try:
                    results.append(f"Task {i}: {future.result()}")
                except Exception as e:
                    results.append(f"Task {i}: error — {e}")

        for r in results:
            print(f"  {r}")


# ---------------------------------------------------------------------------
# 4. Chaining futures (pipeline)
# ---------------------------------------------------------------------------

def stage_1(data: list[int]) -> list[int]:
    """Filter even numbers."""
    time.sleep(0.05)
    return [x for x in data if x % 2 == 0]


def stage_2(data: list[int]) -> list[int]:
    """Square each number."""
    time.sleep(0.05)
    return [x ** 2 for x in data]


def stage_3(data: list[int]) -> int:
    """Sum all numbers."""
    time.sleep(0.05)
    return sum(data)


def pipeline_callback(future: Future, executor: ThreadPoolExecutor, next_fn, final_result: list):
    """Chain stages using callbacks."""
    if future.exception():
        print(f"  Pipeline error: {future.exception()}")
        return

    result = future.result()
    if next_fn is None:
        final_result.append(result)
        return

    # Determine the next stage after next_fn
    chain = [stage_1, stage_2, stage_3]
    try:
        idx = chain.index(next_fn)
        after_next = chain[idx + 1] if idx + 1 < len(chain) else None
    except (ValueError, IndexError):
        after_next = None

    next_future = executor.submit(next_fn, result)
    next_future.add_done_callback(
        lambda f: pipeline_callback(f, executor, after_next, final_result)
    )


def demo_pipeline():
    print("\n=== Chained pipeline with callbacks ===")
    data = list(range(1, 21))
    final_result: list = []

    with ThreadPoolExecutor(max_workers=3) as executor:
        f1 = executor.submit(stage_1, data)
        f1.add_done_callback(
            lambda f: pipeline_callback(f, executor, stage_2, final_result)
        )
        # Wait for pipeline completion
        time.sleep(0.5)

    if final_result:
        print(f"  Input: {data}")
        print(f"  Pipeline result (filter even → square → sum): {final_result[0]}")


# ---------------------------------------------------------------------------
# 5. Rate-limited execution
# ---------------------------------------------------------------------------

def api_call(endpoint: str) -> dict:
    """Simulate rate-limited API call."""
    time.sleep(random.uniform(0.02, 0.1))
    return {"endpoint": endpoint, "status": 200}


def rate_limited_executor(tasks: list, max_workers: int = 5, rate_per_second: float = 10.0):
    """Execute tasks respecting a rate limit."""
    print(f"\n=== Rate-limited execution ({rate_per_second} req/s) ===")
    min_interval = 1.0 / rate_per_second
    results = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        last_submit = 0.0

        for task in tasks:
            # Throttle submission rate
            elapsed = time.perf_counter() - last_submit
            if elapsed < min_interval:
                time.sleep(min_interval - elapsed)

            futures.append(executor.submit(api_call, task))
            last_submit = time.perf_counter()

        for future in as_completed(futures):
            results.append(future.result())

    print(f"  Completed {len(results)} requests")
    return results


if __name__ == "__main__":
    demo_as_completed()
    demo_wait_first_exception()
    demo_cancellation()
    demo_pipeline()

    endpoints = [f"/api/item/{i}" for i in range(15)]
    rate_limited_executor(endpoints, max_workers=3, rate_per_second=20)
