# ZIO — Scheduler Bounty Fork

This is a fork of [zio/zio](https://github.com/zio/zio) maintained by Dakoda Stemen. The work here represents a completed bounty contribution targeting `ZScheduler`, the internal fiber scheduler at the heart of the ZIO 2.x JVM runtime.

---

## About This Fork

### The Problem

ZIO schedules fibers across a fixed pool of worker threads. Each worker owns a bounded local run queue (`RingBufferPow2[Runnable]`, capacity 256) plus a single "next runnable" slot. When a new fiber is submitted or yielded, the runtime must decide which worker should receive it — a decision made thousands to millions of times per second under production load.

The upstream implementation uses **Power of Two Choices**: randomly sample two workers and route to whichever has the smaller `localQueue.size() + (1 if nextRunnable != null else 0)`. This is a well-known load-balancing heuristic with good average-case behavior, but it has two structural limitations:

1. **Queue size is a lagging proxy.** `localQueue.size()` reflects tasks already enqueued, not tasks actively running or about to run. Under burst workloads the sampled counts can be stale by the time a scheduling decision is acted upon.
2. **Only two workers are considered.** With N workers, 2-choice sampling leaves open the possibility of routing to a heavily loaded worker when lighter options exist elsewhere in the pool.

### The Solution

This fork replaces 2-choice random sampling with a **global least-loaded scan using per-worker atomic task counters**.

#### Core mechanism: `AtomicLongArray taskCounts`

A single `AtomicLongArray` of length `poolSize * 16` is allocated alongside the worker array. Each worker is assigned a dedicated slot at index `workerIndex * 16`. The stride of 16 `long` fields (128 bytes) exceeds a typical cache-line width, so counter reads and writes for different workers never share a cache line — false sharing is eliminated at the data-structure level.

`chooseWorker()` performs an O(n) scan over all non-blocking workers, reading each counter and tracking the minimum:

```scala
private def chooseWorker(): ZScheduler.Worker = {
  val n = poolSize
  var best    = null.asInstanceOf[ZScheduler.Worker]
  var minLoad = Long.MaxValue
  var i       = 0
  while (i < n) {
    val w = workers(i)
    if (!w.blocking) {
      val load = math.max(0L, taskCounts.get(i * 16))
      if (load < minLoad) { minLoad = load; best = w }
    }
    i += 1
  }
  best
}
```

`poolSize` is `Runtime.getRuntime.availableProcessors`, which is typically 4–32 on server hardware. An O(n) loop over this range is a handful of nanoseconds and a predictable memory-access pattern — well within budget for a scheduling hot path.

#### Counter lifecycle

Correctness depends on counters accurately tracking in-flight task load. Every code path that adds or removes a task from a worker adjusts the counter:

| Operation | Counter effect |
|---|---|
| `submit` routes to a chosen worker's local queue | `getAndIncrement(best.workerIndex * 16)` |
| `submitAndYield` enqueues the yielded fiber | increment on target worker |
| `stealWork` polls from local queue | `getAndDecrement(worker.workerIndex * 16)` |
| Work-stealing across workers | decrement on the victim, increment on the thief (net zero across pool) |
| Worker marked as blocking, queue flushed to global | `taskCounts.set(idx * 16, 0L)` — hard reset since tasks migrated out |
| Local queue overflow, tasks pushed to global queue | counter decremented for each migrated task |

The `workerIndex` field on `Worker` gives each worker O(1) access to its own counter slot without a linear scan of the `workers` array.

#### False sharing on `Worker` fields

`chooseWorker()` reads `workers(i).blocking` across the worker array on every scheduling decision. Hot fields on `Worker` — `blocking`, `localQueue`, `nextRunnable` — are separated by explicit padding arrays (`pad1_*`, `pad2_*`, `pad3_*`, each 16 `Long` fields = 128 bytes) so that reads of one worker's fields do not pull a neighboring worker's fields into the same cache line. This mirrors the same false-sharing discipline applied to the `taskCounts` array.

---

## Branches

### `nio-ll-clean`

The final, clean implementation. Contains a single focused commit on top of the upstream `series/2.x` base:

> `Implement NIO Least-Loaded Scheduler via per-worker atomic task tracking`

This branch is the reference for the completed bounty work. The diff is intentionally minimal — only `ZScheduler.scala` is modified, with no changes to tests, benchmarks, or build configuration beyond what the scheduler change requires.

### `nio-scheduler-least-loaded`

The working branch capturing the full development history. The commit log shows the evolution of the approach:

1. **`Refactor ZScheduler routing to use Power of Two Choices strategy`** — reproduced and isolated the upstream routing strategy as a baseline, establishing a clear before/after for the benchmark.
2. **`Implement NIO Least-Loaded Scheduler via Strided Atomic Tracking`** — introduced the `AtomicLongArray` with stride-16 layout and wired up the initial increment/decrement sites.
3. **`Implement NIO Least-Loaded Scheduler via per-worker atomic task tracking`** — completed the counter lifecycle (work-stealing adjustment, blocking-worker reset, overflow handling) and added the `Worker` padding fields.

---

## Design Considerations

**Why not a concurrent priority queue or per-worker atomic rather than a strided array?**
Per-worker atomics (`AtomicLong[]`) would require an object reference dereference per worker on each scan. A strided `AtomicLongArray` keeps all counters in a single array object, giving `chooseWorker()` a single sequential memory scan with predictable prefetching behavior.

**Why not maintain a sorted structure and pick the minimum in O(1)?**
Maintaining sorted order under concurrent increments and decrements would require locks or a CAS-heavy sorted data structure, both of which are more expensive than a linear scan over 4–32 integers.

**Why hard-reset the counter to 0 when a worker blocks?**
When a worker transitions to blocking, its entire local queue is flushed to the global queue and the worker slot is replaced. Any in-flight counter value no longer reflects real load on that slot, so a hard reset is cheaper and safer than trying to reconcile the count against the number of tasks actually migrated.

---

## Upstream ZIO

This fork tracks [zio/zio](https://github.com/zio/zio) `series/2.x`. ZIO is a zero-dependency Scala library for asynchronous and concurrent programming built on a high-performance fiber runtime.

- Homepage: https://zio.dev/
- Upstream repository: https://github.com/zio/zio
- License: [Apache 2.0](LICENSE)