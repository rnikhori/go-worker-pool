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
