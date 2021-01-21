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

// Pool runs submitted jobs across a fixed number of worker goroutines.
type Pool[T any] struct {
	jobs    chan Job[T]
	results chan Result[T]
	wg      sync.WaitGroup
}

// NewPool starts numWorkers goroutines ready to process jobs.
func NewPool[T any](numWorkers int) *Pool[T] {
	p := &Pool[T]{
		jobs:    make(chan Job[T]),
		results: make(chan Result[T]),
	}

	p.wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go p.worker()
	}

	return p
}

func (p *Pool[T]) worker() {
	defer p.wg.Done()
	for job := range p.jobs {
		value, err := job()
		p.results <- Result[T]{Value: value, Err: err}
	}
}

// Submit queues a job for execution. It blocks until a worker accepts it.
func (p *Pool[T]) Submit(job Job[T]) {
	p.jobs <- job
}

// Results returns the channel on which job results are delivered.
func (p *Pool[T]) Results() <-chan Result[T] {
	return p.results
}

// Close signals that no more jobs will be submitted. Call it once all
// Submit calls are done, typically from the same goroutine that submits.
func (p *Pool[T]) Close() {
	close(p.jobs)
}

// Wait blocks until every worker has finished, then closes the results
// channel so range loops over Results() terminate. Run it in its own
// goroutine if the caller is still draining Results() concurrently.
func (p *Pool[T]) Wait() {
	p.wg.Wait()
	close(p.results)
}
