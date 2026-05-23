"""Threading: producer/consumer with thread-safe queue."""
import threading, queue, time, random

def producer(q, num_items, name):
    for i in range(num_items):
        item = f"{name}-item-{i}"
        q.put(item)
        print(f"  [producer-{name}] produced: {item}")
        time.sleep(random.uniform(0.01, 0.05))
    q.put(None)  # sentinel

def consumer(q, name):
    results = []
    while True:
        item = q.get()
        if item is None:
            q.task_done()
            break
        time.sleep(random.uniform(0.01, 0.1))  # simulate work
        results.append(item)
        q.task_done()
        print(f"  [consumer-{name}] processed: {item}")
    return results

if __name__ == "__main__":
    q = queue.Queue(maxsize=5)
    prod = threading.Thread(target=producer, args=(q, 8, "A"))
    cons1 = threading.Thread(target=consumer, args=(q, "1"))
    cons2 = threading.Thread(target=consumer, args=(q, "2"))

    t = time.perf_counter()
    prod.start(); cons1.start(); cons2.start()
    prod.join(); cons1.join(); cons2.join()
    print(f"\nDone in {time.perf_counter()-t:.2f}s")
