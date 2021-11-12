package main

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/rnikhori/go-worker-pool/pool"
)

func main() {
	runSquaresDemo()
	runPriorityDemo()
}

// runSquaresDemo computes the squares of 1..20 across 5 concurrent
// workers. Because multiple workers race to pick up jobs, the order
// results arrive in is not the submission order, so the squares are
// sorted before printing.
func runSquaresDemo() {
	const numJobs = 20
	const numWorkers = 5

	p := pool.NewPool[int](numWorkers)

	go func() {
		for i := 1; i <= numJobs; i++ {
			i := i
			p.Submit(pool.Job[int]{
				Fn: func(ctx context.Context) (int, error) {
					time.Sleep(100 * time.Millisecond)
					return i * i, nil
				},
			})
		}
		p.Close()
	}()

	go p.Wait()

	squares := make([]int, 0, numJobs)
	for r := range p.Results() {
		if r.Err != nil {
			fmt.Println("job error:", r.Err)
			continue
		}
		squares = append(squares, r.Value)
	}

	sort.Ints(squares)
	fmt.Printf("collected %d results:\n%v\n", len(squares), squares)
}

// runPriorityDemo shows job priorities taking effect. It uses a single
// worker so completion order is deterministic: all jobs below are
// queued up front (behind one short blocking job so the worker can't
// drain them one at a time as they're submitted), then the worker
// processes them highest Priority first, breaking ties in submission
// order.
func runPriorityDemo() {
	fmt.Println("\npriority demo (single worker, higher Priority runs first):")

	p := pool.NewPool[string](1)

	block := make(chan struct{})
	p.Submit(pool.Job[string]{Fn: func(ctx context.Context) (string, error) {
		<-block
		return "", nil
	}})
	time.Sleep(10 * time.Millisecond) // let the worker pick up the blocking job

	labeled := []struct {
		name     string
		priority int
	}{
		{"low-a", 0},
		{"high", 10},
		{"low-b", 0},
		{"medium", 5},
	}
	for _, job := range labeled {
		job := job
		p.Submit(pool.Job[string]{
			Priority: job.priority,
			Fn: func(ctx context.Context) (string, error) {
				return job.name, nil
			},
		})
	}

	close(block)
	p.Close()
	go p.Wait()

	for r := range p.Results() {
		if r.Value != "" {
			fmt.Println(" -", r.Value)
		}
	}
}
