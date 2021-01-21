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
