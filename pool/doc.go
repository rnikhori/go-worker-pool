// Package pool implements a generic worker pool for running jobs
// concurrently and collecting their results.
//
// A Pool runs a configurable number of worker goroutines pulling from a
// single priority queue. On top of plain concurrent execution it adds:
//
//   - Job priorities, so urgent work is dequeued before less important
//     work already waiting.
//   - Per-job timeouts and context-based cancellation.
//   - Retries with exponential backoff for jobs that fail transiently.
//   - Panic recovery, so one misbehaving job cannot take down a worker.
//   - Thread-safe Stats() (completed/failed/retried counts, average
//     duration) and Errors() (every failure seen so far).
//   - Dynamic scaling via AddWorker/RemoveWorker.
//   - Graceful, deadline-bounded shutdown via Shutdown(ctx).
//
// Basic usage:
//
//	p := pool.NewPool[int](4)
//
//	go func() {
//		for i := 0; i < 10; i++ {
//			i := i
//			p.Submit(pool.Job[int]{
//				Priority:   0,
//				MaxRetries: 2,
//				Timeout:    time.Second,
//				Fn: func(ctx context.Context) (int, error) {
//					return i * i, nil
//				},
//			})
//		}
//		p.Close()
//	}()
//
//	go p.Wait()
//	for r := range p.Results() {
//		if r.Err != nil {
//			log.Println("job failed:", r.Err)
//			continue
//		}
//		fmt.Println(r.Value)
//	}
//
// Prefer Shutdown(ctx) over Close()+Wait() when the caller wants an
// upper bound on how long shutdown can take; see the Shutdown docs for
// details.
package pool
