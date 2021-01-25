// Package pool implements a generic, fixed-size worker pool for running
// jobs concurrently and collecting their results.
package pool

import "sync"

// Job is a unit of work that produces a value of type T or an error.
type Job[T any] func() (T, error)

// Result pairs a job's return value with any error it produced.
type Result[T any] struct {
	Value T
	Err   error
}

// jobItem wraps a submitted Job as it waits in the queue.
type jobItem[T any] struct {
	fn Job[T]
}

// Pool runs submitted jobs across a fixed number of worker goroutines.
//
// Jobs are held in an internal queue guarded by a mutex and condition
// variable rather than an unbuffered channel. This is functionally
// equivalent to the previous channel-based design but gives the queue a
// concrete, inspectable data structure that later work (priority
// ordering, dynamic scaling) can build on.
type Pool[T any] struct {
	mu      sync.Mutex
	cond    *sync.Cond
	queue   []*jobItem[T]
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
		for len(p.queue) == 0 && !p.closing {
			p.cond.Wait()
		}
		if len(p.queue) == 0 {
			p.mu.Unlock()
			return
		}
		item := p.queue[0]
		p.queue = p.queue[1:]
		p.mu.Unlock()

		value, err := item.fn()
		p.results <- Result[T]{Value: value, Err: err}
	}
}

// Submit queues a job for execution. It returns immediately; the job runs
// once a worker becomes available.
func (p *Pool[T]) Submit(job Job[T]) {
	p.mu.Lock()
	p.queue = append(p.queue, &jobItem[T]{fn: job})
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
