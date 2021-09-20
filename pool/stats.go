package pool

import "time"

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
