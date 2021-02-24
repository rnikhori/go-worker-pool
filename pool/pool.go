// Package pool implements a generic, fixed-size worker pool for running
// jobs concurrently and collecting their results.
package pool

import (
	"container/heap"
	"context"
	"sync"
)

// Job is a unit of work submitted to a Pool. Fn is called by a worker
// goroutine, with a context the worker controls, to produce a value of
// type T or an error. Priority controls queue ordering: jobs with a
// higher Priority are dequeued before jobs with a lower one; jobs with
// equal priority run in submission order.
type Job[T any] struct {
	Fn       func(ctx context.Context) (T, error)
	Priority int
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
}

// NewPool starts numWorkers goroutines ready to process jobs.
func NewPool[T any](numWorkers int) *Pool[T] {
	p := &Pool[T]{
		results: make(chan Result[T]),
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

		value, err := item.job.Fn(context.Background())
		p.results <- Result[T]{Value: value, Err: err}
	}
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
