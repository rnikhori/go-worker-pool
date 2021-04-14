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
