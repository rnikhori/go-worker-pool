// Package pool implements a generic, fixed-size worker pool for running
// jobs concurrently and collecting their results.
package pool

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// ErrMinWorkers is returned by RemoveWorker when the pool only has one
// worker left; a pool must always keep at least one worker running.
var ErrMinWorkers = errors.New("pool: cannot remove the last worker")

// ErrPoolClosed is returned by Submit once the pool has been closed via
// Close or Shutdown; no further jobs are accepted after that point.
var ErrPoolClosed = errors.New("pool: pool is closed")

// Job is a unit of work submitted to a Pool. Fn is called by a worker
// goroutine, with a context the worker controls, to produce a value of
// type T or an error. Priority controls queue ordering: jobs with a
// higher Priority are dequeued before jobs with a lower one; jobs with
// equal priority run in submission order. Timeout, if non-zero, bounds
// how long Fn is allowed to run: the context passed to Fn is cancelled
// with context.DeadlineExceeded once Timeout elapses. MaxRetries sets
// how many additional attempts are made if Fn returns a non-nil error;
// the default of 0 means a failing job is not retried.
type Job[T any] struct {
	Fn         func(ctx context.Context) (T, error)
	Priority   int
	Timeout    time.Duration
	MaxRetries int
}

// Result pairs a job's return value with any error it produced.
type Result[T any] struct {
	Value T
	Err   error
}

// Pool runs submitted jobs across a fixed number of worker goroutines.
//
// Jobs are held in a priority queue (container/heap) guarded by a mutex
// and condition variable.
type Pool[T any] struct {
	mu      sync.Mutex
	cond    *sync.Cond
	queue   jobQueue[T]
	seq     int64
	closing bool

	results        chan Result[T]
	closeResults   sync.Once
	wg             sync.WaitGroup
	workerCount    int
	removeRequests int

	retryBaseDelay time.Duration
	retryMaxDelay  time.Duration

	statsMu       sync.Mutex
	stats         Stats
	totalDuration time.Duration

	errsMu sync.Mutex
	errs   []error
}

// Stats holds a snapshot of a Pool's execution metrics.
type Stats struct {
	Completed   uint64
	Failed      uint64
	Retried     uint64
	AvgDuration time.Duration
}

// Stats returns a thread-safe snapshot of the pool's execution metrics
// as of the call. It may be called concurrently with job submission and
// execution.
func (p *Pool[T]) Stats() Stats {
	p.statsMu.Lock()
	defer p.statsMu.Unlock()
	snapshot := p.stats
	if n := snapshot.Completed + snapshot.Failed; n > 0 {
		snapshot.AvgDuration = p.totalDuration / time.Duration(n)
	}
	return snapshot
}

// Option configures optional Pool behavior at construction time.
type Option[T any] func(*Pool[T])

// WithRetryBackoff overrides the default exponential backoff schedule
// used between job retry attempts. base is the delay before the first
// retry; the delay doubles on each subsequent attempt up to max.
func WithRetryBackoff[T any](base, max time.Duration) Option[T] {
	return func(p *Pool[T]) {
		p.retryBaseDelay = base
		p.retryMaxDelay = max
	}
}

// NewPool starts numWorkers goroutines ready to process jobs.
func NewPool[T any](numWorkers int, opts ...Option[T]) *Pool[T] {
	p := &Pool[T]{
		results:        make(chan Result[T]),
		retryBaseDelay: defaultRetryBaseDelay,
		retryMaxDelay:  defaultRetryMaxDelay,
	}
	for _, opt := range opts {
		opt(p)
	}
	p.cond = sync.NewCond(&p.mu)
	p.workerCount = numWorkers

	p.wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go p.worker()
	}

	return p
}

// AddWorker starts one additional worker goroutine, increasing the
// pool's capacity to process jobs concurrently. It can be called at any
// time while the pool is running.
func (p *Pool[T]) AddWorker() {
	p.mu.Lock()
	p.workerCount++
	p.mu.Unlock()

	p.wg.Add(1)
	go p.worker()
}

// WorkerCount returns the current number of worker goroutines, including
// any started by AddWorker and excluding any pending removal via
// RemoveWorker.
func (p *Pool[T]) WorkerCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.workerCount
}

// RemoveWorker asks one worker goroutine to stop after it finishes any
// job it is currently running (or immediately, if it is idle). It
// returns ErrMinWorkers rather than remove the pool's last worker.
func (p *Pool[T]) RemoveWorker() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.workerCount <= 1 {
		return ErrMinWorkers
	}
	p.workerCount--
	p.removeRequests++
	p.cond.Broadcast()
	return nil
}

func (p *Pool[T]) worker() {
	defer p.wg.Done()
	for {
		p.mu.Lock()
		for p.queue.Len() == 0 && !p.closing && p.removeRequests == 0 {
			p.cond.Wait()
		}
		if p.removeRequests > 0 {
			p.removeRequests--
			p.mu.Unlock()
			return
		}
		if p.queue.Len() == 0 {
			p.mu.Unlock()
			return
		}
		item := heap.Pop(&p.queue).(*jobItem[T])
		p.mu.Unlock()

		start := time.Now()
		value, err := p.runWithRetries(item.job)
		p.recordOutcome(err, time.Since(start))
		p.results <- Result[T]{Value: value, Err: err}
	}
}

// defaultRetryBaseDelay and defaultRetryMaxDelay set the exponential
// backoff schedule used between retry attempts: delay doubles on each
// attempt starting from the base, capped at the max.
const (
	defaultRetryBaseDelay = 50 * time.Millisecond
	defaultRetryMaxDelay  = 2 * time.Second
)

// backoffDelay returns the delay to sleep before retry attempt n (n
// starting at 1 for the first retry).
func backoffDelay(n int, base, max time.Duration) time.Duration {
	delay := base
	for i := 1; i < n; i++ {
		delay *= 2
		if delay >= max {
			return max
		}
	}
	if delay > max {
		return max
	}
	return delay
}

// runWithRetries runs job, retrying up to job.MaxRetries times with
// exponential backoff if Fn returns a non-nil error. It returns the
// value and error from the last attempt made.
func (p *Pool[T]) runWithRetries(job Job[T]) (T, error) {
	var value T
	var err error
	for attempt := 0; ; attempt++ {
		value, err = p.runJob(job)
		if err == nil || attempt >= job.MaxRetries {
			return value, err
		}
		p.statsMu.Lock()
		p.stats.Retried++
		p.statsMu.Unlock()
		time.Sleep(backoffDelay(attempt+1, p.retryBaseDelay, p.retryMaxDelay))
	}
}

// runJob executes a single job, applying its Timeout (if any) as a
// context deadline and recovering from any panic inside Fn so that one
// bad job cannot take down its worker goroutine. A recovered panic is
// reported as an error on the returned Result.
func (p *Pool[T]) runJob(job Job[T]) (value T, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("pool: job panicked: %v", r)
		}
	}()

	ctx := context.Background()
	if job.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, job.Timeout)
		defer cancel()
	}
	return job.Fn(ctx)
}

// recordOutcome updates Stats after a job finishes (after all retries).
// duration is the total wall time spent on the job, including any
// retries and backoff sleeps. A non-nil err is also appended to the
// pool's error log, retrievable via Errors().
func (p *Pool[T]) recordOutcome(err error, duration time.Duration) {
	p.statsMu.Lock()
	if err != nil {
		p.stats.Failed++
	} else {
		p.stats.Completed++
	}
	p.totalDuration += duration
	p.statsMu.Unlock()

	if err != nil {
		p.errsMu.Lock()
		p.errs = append(p.errs, err)
		p.errsMu.Unlock()
	}
}

// Errors returns a copy of every error produced by jobs run on this pool
// so far (after retries are exhausted), in the order they finished. It
// is safe to call concurrently with job submission and execution.
func (p *Pool[T]) Errors() []error {
	p.errsMu.Lock()
	defer p.errsMu.Unlock()
	out := make([]error, len(p.errs))
	copy(out, p.errs)
	return out
}

// Submit queues a job for execution. It returns immediately; the job runs
// once a worker becomes available, ahead of any already-queued job with a
// lower Priority. Submit returns ErrPoolClosed if the pool has already
// been closed via Close or Shutdown.
func (p *Pool[T]) Submit(job Job[T]) error {
	p.mu.Lock()
	if p.closing {
		p.mu.Unlock()
		return ErrPoolClosed
	}
	p.seq++
	heap.Push(&p.queue, &jobItem[T]{job: job, seq: p.seq})
	p.cond.Signal()
	p.mu.Unlock()
	return nil
}

// Results returns the channel on which job results are delivered.
func (p *Pool[T]) Results() <-chan Result[T] {
	return p.results
}

// Close signals that no more jobs will be submitted. Call it once all
// Submit calls are done, typically from the same goroutine that submits.
func (p *Pool[T]) Close() {
	p.mu.Lock()
	p.closing = true
	p.cond.Broadcast()
	p.mu.Unlock()
}

// Wait blocks until every worker has finished, then closes the results
// channel so range loops over Results() terminate. Run it in its own
// goroutine if the caller is still draining Results() concurrently.
func (p *Pool[T]) Wait() {
	p.wg.Wait()
	p.closeResults.Do(func() { close(p.results) })
}

// Shutdown stops the pool from accepting further work, lets every
// worker drain the remaining queue and finish any job already in
// flight, then closes the results channel. If ctx is done before all
// workers finish, Shutdown returns ctx.Err() immediately; workers keep
// running in the background and will still close the results channel
// once they eventually finish.
func (p *Pool[T]) Shutdown(ctx context.Context) error {
	p.mu.Lock()
	p.closing = true
	p.cond.Broadcast()
	p.mu.Unlock()

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		p.closeResults.Do(func() { close(p.results) })
		return nil
	case <-ctx.Done():
		go func() {
			<-done
			p.closeResults.Do(func() { close(p.results) })
		}()
		return ctx.Err()
	}
}
