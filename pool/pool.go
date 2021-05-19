// Package pool implements a generic, fixed-size worker pool for running
// jobs concurrently and collecting their results.
package pool

import (
	"container/heap"
	"context"
	"fmt"
	"sync"
	"time"
)

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

// jobItem wraps a submitted Job as it waits in the queue.
type jobItem[T any] struct {
	job   Job[T]
	seq   int64
	index int
}

// jobQueue is a container/heap.Interface implementation that orders items
// by descending priority, breaking ties by submission order (seq).
type jobQueue[T any] []*jobItem[T]

func (q jobQueue[T]) Len() int { return len(q) }

func (q jobQueue[T]) Less(i, j int) bool {
	if q[i].job.Priority != q[j].job.Priority {
		return q[i].job.Priority > q[j].job.Priority
	}
	return q[i].seq < q[j].seq
}

func (q jobQueue[T]) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
	q[i].index = i
	q[j].index = j
}

func (q *jobQueue[T]) Push(x any) {
	item := x.(*jobItem[T])
	item.index = len(*q)
	*q = append(*q, item)
}

func (q *jobQueue[T]) Pop() any {
	old := *q
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*q = old[:n-1]
	return item
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

	results chan Result[T]
	wg      sync.WaitGroup

	retryBaseDelay time.Duration
	retryMaxDelay  time.Duration

	statsMu       sync.Mutex
	stats         Stats
	totalDuration time.Duration
}

// Stats holds a snapshot of a Pool's execution metrics.
type Stats struct {
	Completed   uint64
	Failed      uint64
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

	p.wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go p.worker()
	}

	return p
}

func (p *Pool[T]) worker() {
	defer p.wg.Done()
	for {
		p.mu.Lock()
		for p.queue.Len() == 0 && !p.closing {
			p.cond.Wait()
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
// retries and backoff sleeps.
func (p *Pool[T]) recordOutcome(err error, duration time.Duration) {
	p.statsMu.Lock()
	if err != nil {
		p.stats.Failed++
	} else {
		p.stats.Completed++
	}
	p.totalDuration += duration
	p.statsMu.Unlock()
}

// Submit queues a job for execution. It returns immediately; the job runs
// once a worker becomes available, ahead of any already-queued job with a
// lower Priority.
func (p *Pool[T]) Submit(job Job[T]) {
	p.mu.Lock()
	p.seq++
	heap.Push(&p.queue, &jobItem[T]{job: job, seq: p.seq})
	p.cond.Signal()
	p.mu.Unlock()
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
	close(p.results)
}
