package pool_test

import (
	"context"
	"errors"
	"reflect"
	"sync"
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
