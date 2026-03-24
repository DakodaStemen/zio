# DakodaStemen/zio

A fork of [zio/zio](https://github.com/zio/zio), maintained as part of a bounty contribution targeting `ZScheduler` — the internal fiber scheduler at the heart of the ZIO 2.x JVM runtime. The contribution replaces the upstream Power-of-Two-Choices random sampling with a global least-loaded scan using per-worker atomic task counters and false-sharing-free memory layout.

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![Scala](https://img.shields.io/badge/Scala-2.13%20%2F%203.x-red.svg)](https://www.scala-lang.org/)

---

## Table of Contents

- [Background — ZScheduler and the Fiber Scheduling Problem](#background--zscheduler-and-the-fiber-scheduling-problem)
- [The Upstream Approach: Power-of-Two Choices](#the-upstream-approach-power-of-two-choices)
- [Limitations of Two-Choice Sampling](#limitations-of-two-choice-sampling)
- [The Contribution: Least-Loaded Scan with Atomic Counters](#the-contribution-least-loaded-scan-with-atomic-counters)
- [False-Sharing Prevention](#false-sharing-prevention)
- [Counter Lifecycle and Correctness](#counter-lifecycle-and-correctness)
- [Blocking Worker Handling](#blocking-worker-handling)
- [Performance Characteristics](#performance-characteristics)
- [Implementation](#implementation)
- [Running the Tests](#running-the-tests)
- [Upstream](#upstream)
- [License](#license)

---

## Background — ZScheduler and the Fiber Scheduling Problem

ZIO schedules fibers across a fixed pool of worker threads (`poolSize = Runtime.getRuntime.availableProcessors` by default). Each worker owns:

- A bounded local run queue: `RingBufferPow2[Runnable]` (capacity 256)
- A single "next runnable" slot for the immediately yielded fiber

When a new fiber is submitted or an existing fiber yields, the runtime must decide which worker should receive it. This decision is made millions of times per second under production load.

The goal: distribute fibers as evenly as possible to minimize queue buildup on any individual worker, while keeping the scheduling decision itself fast enough to not become a bottleneck.

---

## The Upstream Approach: Power-of-Two Choices

The upstream `ZScheduler.chooseWorker()` implements **Power-of-Two Choices** (2C):

```scala
// Upstream implementation (simplified)
def chooseWorker(): Worker = {
  val i = ThreadLocalRandom.current().nextInt(poolSize)
  val j = ThreadLocalRandom.current().nextInt(poolSize - 1)
  val jj = if (j >= i) j + 1 else j
  val wi = workers(i)
  val wj = workers(jj)
  val li = wi.localQueue.size() + (if (wi.nextRunnable != null) 1 else 0)
  val lj = wj.localQueue.size() + (if (wj.nextRunnable != null) 1 else 0)
  if (li <= lj) wi else wj
}
```

2C is a well-studied load-balancing heuristic. It provides O(log log n) maximum load rather than the O(log n / log log n) of pure random placement, with only two queue reads per decision.

---

## Limitations of Two-Choice Sampling

Two structural limitations motivate this contribution:

**1. Queue size is a lagging proxy.**
`localQueue.size()` counts tasks already enqueued, not tasks actively executing or about to run. Under burst workloads — many short-lived fibers submitted in rapid succession — sampled counts can be stale by the time the scheduling decision is acted upon. A worker that just dequeued a batch of tasks will read as lightly loaded even if it is about to be busy.

**2. Only two workers are considered.**
With N workers, 2C leaves open the possibility of routing to a heavily loaded worker when lighter options exist elsewhere in the pool. The probability of choosing the minimum-loaded worker approaches certainty only as the number of choices approaches N.

For ZIO's typical `poolSize` of 4–32 (number of available processors), an O(N) scan over all workers is a handful of nanoseconds — well within scheduling budget — and eliminates both limitations simultaneously.

---

## The Contribution: Least-Loaded Scan with Atomic Counters

This fork replaces 2C with a **global least-loaded scan using per-worker atomic task counters**.

A single `AtomicLongArray taskCounts` of length `poolSize * 16` is allocated alongside the worker array. Each worker is assigned a dedicated slot at index `workerIndex * 16`.

`chooseWorker()` performs an O(N) scan:

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

The scan reads N atomic longs sequentially. Each element is on its own cache line (stride of 16 `long` fields = 128 bytes, see below). The pattern is a single linear sweep with no branching beyond the minimum comparison — predictable for the CPU's branch predictor and prefetcher.

---

## False-Sharing Prevention

The stride of 16 is deliberate. A cache line on x86 is 64 bytes. A `long` is 8 bytes. To ensure that adjacent workers' counters are never on the same cache line, each counter must be separated by at least 8 longs (64 bytes). Using a stride of 16 (128 bytes) provides 2× cache line separation, protecting against both 64-byte and 128-byte cache line widths found on different processor microarchitectures.

Without this separation, concurrent reads and writes to counters for different workers would invalidate shared cache lines, causing false sharing: a performance hazard where threads on different cores are serializing on cache-coherence traffic for data they do not logically share.

```
Array layout (stride = 16, longs of 8 bytes each):
Index 0    → worker 0 counter (64 bytes to next worker)
Index 16   → worker 1 counter
Index 32   → worker 2 counter
...
```

---

## Counter Lifecycle and Correctness

Correctness depends on counters accurately tracking in-flight task load at all times. Every code path that adds or removes a task adjusts the counter atomically:

| Operation | Counter effect |
|-----------|----------------|
| `submit` routes fiber to a worker | `getAndIncrement(best.workerIndex * 16)` |
| `submitAndYield` enqueues the yielded fiber | increment on target worker |
| Worker dequeues from local queue (`stealWork`) | `getAndDecrement(worker.workerIndex * 16)` |
| Work-stealing: victim dequeues, thief receives | decrement on victim, increment on thief (net zero across pool) |
| Worker transitions to blocking mode | `taskCounts.set(idx * 16, 0L)` — hard reset; tasks migrated to global queue |
| Local queue overflow to global queue | decrement per migrated task |

The counter is a lower bound on actual load, not a precise count of queued fibers. Near-zero values are snapped to zero (the `math.max(0L, ...)` in `chooseWorker`) to prevent counter drift from causing negative load readings.

---

## Blocking Worker Handling

When a worker executes a blocking operation, it transitions into blocking mode: its local queue is flushed to the global queue for other workers to steal, and its counter is hard-reset to zero. `chooseWorker` skips workers in blocking mode (`if (!w.blocking)`). This prevents the scheduler from routing new fibers to a worker that is stuck in a blocking call and unable to process them promptly.

---

## Performance Characteristics

For a pool of N workers:

| Metric | Upstream (2C) | This contribution |
|--------|--------------|-------------------|
| Workers sampled | 2 | N (all non-blocking) |
| Scheduling decision cost | O(1) + 2 random reads | O(N) sequential atomic reads |
| Worst-case balance | 2-choice distribution | Optimal (always routes to minimum) |
| False sharing | Possible (queue metadata) | Eliminated (128-byte counter stride) |
| Staleness | Queue size (lagging) | Atomic counter (updated at enqueue/dequeue) |

For typical ZIO pool sizes (4–32), the O(N) scan is 4–32 sequential atomic reads from hot cache lines — on the order of 10–50 nanoseconds. Fiber scheduling on a ZIO runtime typically takes hundreds of nanoseconds to microseconds end-to-end, so the O(N) cost is not a bottleneck.

---

## Implementation

The changes are confined to `core-jvm/src/main/scala/zio/internal/ZScheduler.scala`:

- `taskCounts: AtomicLongArray` field added to `ZScheduler`
- `chooseWorker()` replaced with least-loaded scan
- `submit`, `submitAndYield`, `stealWork`, blocking-mode transition, and queue-overflow paths updated to maintain counter invariants
- Existing tests pass unchanged; new property-based tests verify counter balance under concurrent submit/complete cycles

---

## Running the Tests

```bash
sbt "coreJVM/test"
# or run only scheduler tests
sbt "coreJVM/testOnly *ZScheduler*"
```

---

## Upstream

This fork tracks the `series/2.x` branch of [zio/zio](https://github.com/zio/zio). The scheduler contribution is isolated to the `zscheduler-least-loaded` branch. The `series/2.x` branch contains only this documentation update.

---

## License

Apache License, Version 2.0 — see upstream [LICENSE](https://github.com/zio/zio/blob/series/2.x/LICENSE).

This is a fork and contribution to the ZIO project. All code contributed here is offered under the same Apache 2.0 license as the upstream project.
