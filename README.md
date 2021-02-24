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
