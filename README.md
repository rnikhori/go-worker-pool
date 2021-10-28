# go-worker-pool

A minimal generic worker pool / job queue built on goroutines and channels (stdlib only).

## Build

```sh
go build ./...
```

## Run

```sh
go run .
```

Runs 20 fake jobs across 5 workers and prints the sorted squares of 1..20.

## Job priorities

Jobs are submitted as a `pool.Job[T]` struct with a `Priority` field:

```go
p.Submit(pool.Job[int]{
	Priority: 10,
	Fn: func(ctx context.Context) (int, error) {
		return doImportantWork(ctx)
	},
})
```

Internally the pool keeps queued jobs in a `container/heap` priority
queue. A worker always dequeues the highest-priority job available;
jobs of equal priority are run in the order they were submitted
(first-in, first-out). The default priority is `0`, so submitting
without setting `Priority` behaves exactly like a plain FIFO queue.

## Context and per-job timeouts

Each job's `Fn` receives a `context.Context`:

```go
p.Submit(pool.Job[int]{
	Timeout: 2 * time.Second,
	Fn: func(ctx context.Context) (int, error) {
		return fetch(ctx)
	},
})
```

If `Timeout` is set, the pool derives a `context.WithTimeout` deadline
for that job alone and cancels it once the timeout elapses, so a
well-behaved `Fn` that checks `ctx.Done()` (or passes `ctx` down to
anything it calls) fails fast with `context.DeadlineExceeded` instead
of blocking a worker indefinitely. A zero `Timeout` (the default) means
no deadline is applied.

## Retries with exponential backoff

A job can ask to be retried when its `Fn` returns an error:

```go
p.Submit(pool.Job[int]{
	MaxRetries: 3,
	Fn: func(ctx context.Context) (int, error) {
		return callFlakyService(ctx)
	},
})
```

After a failed attempt, the worker sleeps for an exponentially
increasing delay (doubling each time, starting at 50ms and capped at
2s by default) before trying again, up to `MaxRetries` additional
attempts. The `Result` delivered on `Results()` reflects the outcome
of the last attempt. The default schedule can be overridden pool-wide:

```go
p := pool.NewPool[int](numWorkers, pool.WithRetryBackoff[int](100*time.Millisecond, 10*time.Second))
```

## Panic recovery

If a job's `Fn` panics, the pool recovers the panic in the worker
goroutine that ran it, converts it into an error (wrapping the
recovered value's message), and delivers it as a normal `Result` with
`Err` set. The worker itself is unaffected and continues picking up
and running subsequent jobs — a single misbehaving job cannot take
down the pool.

## Metrics via Stats()

A pool tracks its own execution metrics and exposes them through a
thread-safe `Stats()` method:

```go
stats := p.Stats()
fmt.Printf("completed=%d failed=%d retried=%d avg=%v\n",
	stats.Completed, stats.Failed, stats.Retried, stats.AvgDuration)
```

`Completed` and `Failed` count finished jobs (after all retries) by
outcome, `Retried` counts individual retry attempts, and `AvgDuration`
is the mean wall-clock time spent per finished job, including any
retries and backoff sleeps. `Stats()` can be called at any time,
including while jobs are still running.

## Error aggregation

Every job error (after retries, if any, are exhausted) is also
recorded internally. `Errors()` returns a snapshot of all of them, in
completion order:

```go
for _, err := range p.Errors() {
	log.Println("job failed:", err)
}
```

This is useful for a batch-style workload where you want to drain
`Results()` for successful values but still review everything that
went wrong afterward, without threading error handling through every
call site that consumes results.

## Dynamic worker scaling

The number of workers is not fixed for the lifetime of the pool:

```go
p.AddWorker()           // scale up by one
err := p.RemoveWorker() // scale down by one; returns ErrMinWorkers if only one remains
n := p.WorkerCount()    // current active worker count
```

`AddWorker` starts a new goroutine immediately. `RemoveWorker` asks one
worker to stop after it finishes whatever job it is currently running
(or immediately, if it is idle); it never removes the last worker,
since a pool always needs at least one to make progress.

## Graceful shutdown

`Shutdown(ctx)` is the recommended way to stop a pool:

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
if err := p.Shutdown(ctx); err != nil {
	log.Printf("pool did not drain in time: %v", err)
}
```

It immediately stops the pool from accepting new jobs (`Submit`
returns `ErrPoolClosed` from then on), lets every worker finish
draining the queued and in-flight jobs, and closes `Results()` once
they do. If `ctx` is cancelled or its deadline passes first, `Shutdown`
returns `ctx.Err()` right away without waiting further — workers keep
running in the background and still close `Results()` whenever they
eventually finish.

`Close()` and `Wait()` remain available as lower-level primitives:
`Close()` just stops accepting new jobs, and `Wait()` blocks
unconditionally (no deadline) until every worker is done.

## Benchmarks

`BenchmarkPool_Throughput` measures end-to-end throughput for cheap
jobs across a range of worker counts (1 through 32):

```sh
go test ./pool/ -run ^$ -bench BenchmarkPool_Throughput -benchmem
```

Use it to see where adding workers stops paying off for your workload
and hardware; for trivial jobs that do almost no work, coordination
overhead eventually outweighs the benefit of more workers.

## Examples

- `main.go` (run with `go run .`): a minimal demo that computes squares
  of 1..20 across 5 workers.
- `examples/batchfetch` (run with `go run ./examples/batchfetch`): a
  more realistic use case that "fetches" a batch of URLs from a flaky
  fake backend, combining job priorities (critical endpoints ahead of
  bulk assets), per-job timeouts and retries, and a graceful,
  deadline-bounded `Shutdown`, then reports `Stats()` and `Errors()`.
