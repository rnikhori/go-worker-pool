package pool_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/rnikhori/go-worker-pool/pool"
)

// benchmarkThroughput submits numJobs trivial jobs to a pool of the given
// size and drains all results, measuring end-to-end throughput.
func benchmarkThroughput(b *testing.B, numWorkers, numJobs int) {
	for i := 0; i < b.N; i++ {
		p := pool.NewPool[int](numWorkers)

		go func() {
			for j := 0; j < numJobs; j++ {
				j := j
				p.Submit(pool.Job[int]{Fn: func(ctx context.Context) (int, error) {
					return j * j, nil
				}})
			}
			p.Close()
		}()

		go p.Wait()
		for range p.Results() {
		}
	}
}

func BenchmarkPool_Throughput(b *testing.B) {
	for _, n := range []int{1, 2, 4, 8, 16, 32} {
		n := n
		b.Run(fmt.Sprintf("workers=%d", n), func(b *testing.B) {
			benchmarkThroughput(b, n, 1000)
		})
	}
}
