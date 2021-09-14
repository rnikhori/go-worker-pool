package pool_test

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rnikhori/go-worker-pool/pool"
)

func TestPriorityOrdering(t *testing.T) {
	p := pool.NewPool[int](1)

	var mu sync.Mutex
	var order []int

	block := make(chan struct{})
	p.Submit(pool.Job[int]{Fn: func(ctx context.Context) (int, error) {
		<-block
		return 0, nil
	}})

	// Give the single worker time to pick up the blocking job so that
	// everything submitted below queues up behind it in one batch,
	// making priority order observable.
	time.Sleep(20 * time.Millisecond)

	submissions := []struct {
		id       int
		priority int
	}{
		{id: 1, priority: 0},
		{id: 2, priority: 5},
		{id: 3, priority: 1},
		{id: 4, priority: 10},
		{id: 5, priority: 0},
	}
	for _, s := range submissions {
		s := s
		p.Submit(pool.Job[int]{
			Priority: s.priority,
			Fn: func(ctx context.Context) (int, error) {
				mu.Lock()
				order = append(order, s.id)
				mu.Unlock()
				return s.id, nil
			},
		})
	}

	close(block)
	p.Close()
	go p.Wait()
	for range p.Results() {
	}

	want := []int{4, 2, 3, 1, 5}
	if !reflect.DeepEqual(order, want) {
		t.Fatalf("completion order = %v, want %v", order, want)
	}
}

func TestJobTimeout(t *testing.T) {
	p := pool.NewPool[int](1)

	p.Submit(pool.Job[int]{
		Timeout: 20 * time.Millisecond,
		Fn: func(ctx context.Context) (int, error) {
			select {
			case <-time.After(200 * time.Millisecond):
				return 1, nil
			case <-ctx.Done():
				return 0, ctx.Err()
			}
		},
	})
	p.Close()
	go p.Wait()

	r := <-p.Results()
	if !errors.Is(r.Err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", r.Err)
	}
}

func TestRetriesEventuallySucceed(t *testing.T) {
	p := pool.NewPool[int](1, pool.WithRetryBackoff[int](time.Millisecond, 5*time.Millisecond))

	var attempts int32
	p.Submit(pool.Job[int]{
		MaxRetries: 3,
		Fn: func(ctx context.Context) (int, error) {
			n := atomic.AddInt32(&attempts, 1)
			if n < 3 {
				return 0, errors.New("not yet")
			}
			return 42, nil
		},
	})
	p.Close()
	go p.Wait()

	r := <-p.Results()
	if r.Err != nil {
		t.Fatalf("unexpected error: %v", r.Err)
	}
	if r.Value != 42 {
		t.Fatalf("value = %d, want 42", r.Value)
	}
	if got := atomic.LoadInt32(&attempts); got != 3 {
		t.Fatalf("attempts = %d, want 3", got)
	}
}

func TestRetriesExhausted(t *testing.T) {
	p := pool.NewPool[int](1, pool.WithRetryBackoff[int](time.Millisecond, 5*time.Millisecond))

	var attempts int32
	wantErr := errors.New("always fails")
	p.Submit(pool.Job[int]{
		MaxRetries: 2,
		Fn: func(ctx context.Context) (int, error) {
			atomic.AddInt32(&attempts, 1)
			return 0, wantErr
		},
	})
	p.Close()
	go p.Wait()

	r := <-p.Results()
	if !errors.Is(r.Err, wantErr) {
		t.Fatalf("err = %v, want %v", r.Err, wantErr)
	}
	if got := atomic.LoadInt32(&attempts); got != 3 {
		t.Fatalf("attempts = %d, want 3 (1 initial + 2 retries)", got)
	}
}

func TestPanicRecovery(t *testing.T) {
	p := pool.NewPool[int](2)

	p.Submit(pool.Job[int]{Fn: func(ctx context.Context) (int, error) {
		panic("boom")
	}})
	p.Submit(pool.Job[int]{Fn: func(ctx context.Context) (int, error) {
		return 7, nil
	}})
	p.Close()
	go p.Wait()

	var sawPanicErr bool
	var sawGoodResult bool
	for r := range p.Results() {
		switch {
		case r.Err != nil:
			sawPanicErr = true
		case r.Value == 7:
			sawGoodResult = true
		}
	}

	if !sawPanicErr {
		t.Fatal("expected the panicking job to surface as an error result")
	}
	if !sawGoodResult {
		t.Fatal("expected the second job to complete normally after the panic")
	}
}

func TestStats(t *testing.T) {
	p := pool.NewPool[int](2, pool.WithRetryBackoff[int](time.Millisecond, 2*time.Millisecond))

	p.Submit(pool.Job[int]{Fn: func(ctx context.Context) (int, error) {
		time.Sleep(5 * time.Millisecond)
		return 1, nil
	}})
	p.Submit(pool.Job[int]{Fn: func(ctx context.Context) (int, error) {
		return 0, errors.New("nope")
	}})

	var attempts int32
	p.Submit(pool.Job[int]{
		MaxRetries: 2,
		Fn: func(ctx context.Context) (int, error) {
			if atomic.AddInt32(&attempts, 1) < 2 {
				return 0, errors.New("try again")
			}
			return 2, nil
		},
	})

	p.Close()
	go p.Wait()
	for range p.Results() {
	}

	stats := p.Stats()
	if stats.Completed != 2 {
		t.Fatalf("Completed = %d, want 2", stats.Completed)
	}
	if stats.Failed != 1 {
		t.Fatalf("Failed = %d, want 1", stats.Failed)
	}
	if stats.Retried != 1 {
		t.Fatalf("Retried = %d, want 1", stats.Retried)
	}
	if stats.AvgDuration <= 0 {
		t.Fatalf("AvgDuration = %v, want > 0", stats.AvgDuration)
	}
}

func TestErrorsAggregation(t *testing.T) {
	p := pool.NewPool[int](2)

	errA := errors.New("job a failed")
	errB := errors.New("job b failed")
	p.Submit(pool.Job[int]{Fn: func(ctx context.Context) (int, error) { return 0, errA }})
	p.Submit(pool.Job[int]{Fn: func(ctx context.Context) (int, error) { return 0, errB }})
	p.Submit(pool.Job[int]{Fn: func(ctx context.Context) (int, error) { return 1, nil }})

	p.Close()
	go p.Wait()
	for range p.Results() {
	}

	got := p.Errors()
	if len(got) != 2 {
		t.Fatalf("len(Errors()) = %d, want 2: %v", len(got), got)
	}

	var foundA, foundB bool
	for _, err := range got {
		switch {
		case errors.Is(err, errA):
			foundA = true
		case errors.Is(err, errB):
			foundB = true
		}
	}
	if !foundA || !foundB {
		t.Fatalf("Errors() = %v, want both errA and errB", got)
	}
}

func TestDynamicWorkerScaling(t *testing.T) {
	p := pool.NewPool[int](2)

	if got := p.WorkerCount(); got != 2 {
		t.Fatalf("WorkerCount() = %d, want 2", got)
	}

	p.AddWorker()
	p.AddWorker()
	if got := p.WorkerCount(); got != 4 {
		t.Fatalf("WorkerCount() after AddWorker x2 = %d, want 4", got)
	}

	if err := p.RemoveWorker(); err != nil {
		t.Fatalf("RemoveWorker() error = %v", err)
	}
	if got := p.WorkerCount(); got != 3 {
		t.Fatalf("WorkerCount() after RemoveWorker = %d, want 3", got)
	}

	for i := 0; i < 20; i++ {
		p.Submit(pool.Job[int]{Fn: func(ctx context.Context) (int, error) {
			return 1, nil
		}})
	}
	p.Close()
	go p.Wait()

	total := 0
	for range p.Results() {
		total++
	}
	if total != 20 {
		t.Fatalf("processed %d jobs, want 20", total)
	}
}

func TestShutdownWaitsForInFlightJobs(t *testing.T) {
	p := pool.NewPool[int](2)

	var finished int32
	p.Submit(pool.Job[int]{Fn: func(ctx context.Context) (int, error) {
		time.Sleep(50 * time.Millisecond)
		atomic.AddInt32(&finished, 1)
		return 1, nil
	}})
	time.Sleep(5 * time.Millisecond) // let the worker pick it up

	go func() {
		for range p.Results() {
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := p.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}

	if got := atomic.LoadInt32(&finished); got != 1 {
		t.Fatalf("finished = %d, want 1 (job should have completed before Shutdown returned)", got)
	}

	if err := p.Submit(pool.Job[int]{Fn: func(ctx context.Context) (int, error) { return 0, nil }}); !errors.Is(err, pool.ErrPoolClosed) {
		t.Fatalf("Submit() after Shutdown error = %v, want ErrPoolClosed", err)
	}
}

func TestShutdownDeadlineExceeded(t *testing.T) {
	p := pool.NewPool[int](1)

	block := make(chan struct{})
	defer close(block)
	p.Submit(pool.Job[int]{Fn: func(ctx context.Context) (int, error) {
		<-block
		return 0, nil
	}})
	time.Sleep(5 * time.Millisecond) // let the worker pick it up

	go func() {
		for range p.Results() {
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := p.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown() error = %v, want context.DeadlineExceeded", err)
	}
}

func TestRemoveWorkerRefusesLastWorker(t *testing.T) {
	p := pool.NewPool[int](1)
	if err := p.RemoveWorker(); !errors.Is(err, pool.ErrMinWorkers) {
		t.Fatalf("RemoveWorker() error = %v, want ErrMinWorkers", err)
	}
	p.Close()
	go p.Wait()
	for range p.Results() {
	}
}
