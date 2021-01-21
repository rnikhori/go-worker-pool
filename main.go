package main

import (
	"fmt"
	"sort"
	"time"

	"github.com/rnikhori/go-worker-pool/pool"
)

func main() {
	const numJobs = 20
	const numWorkers = 5

	p := pool.NewPool[int](numWorkers)

	go func() {
		for i := 1; i <= numJobs; i++ {
			i := i
			p.Submit(func() (int, error) {
				time.Sleep(100 * time.Millisecond)
				return i * i, nil
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
