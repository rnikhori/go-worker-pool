// Command batchfetch demonstrates using the pool for a batch of
// independent "fetches" against a flaky, latency-variable fake backend:
// some URLs matter more than others (priority), slow backends shouldn't
// hang a worker forever (timeout), and transient failures are worth a
// second try (retries). It also shows Stats(), Errors(), and a
// deadline-bounded graceful Shutdown.
package main

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/rnikhori/go-worker-pool/pool"
)

type fetchResult struct {
	url  string
	body string
}

// fakeFetch simulates an HTTP GET: it takes a random amount of time and
// fails roughly a third of the time, honoring ctx cancellation the way a
// real net/http request would.
func fakeFetch(ctx context.Context, url string) (string, error) {
	latency := time.Duration(20+rand.Intn(120)) * time.Millisecond

	select {
	case <-time.After(latency):
	case <-ctx.Done():
		return "", ctx.Err()
	}

	if rand.Intn(3) == 0 {
		return "", fmt.Errorf("fetching %s: connection reset", url)
	}
	return fmt.Sprintf("<html>%s OK</html>", url), nil
}

func main() {
	rand.Seed(1)

	criticalURLs := []string{
		"https://api.example.com/health",
		"https://api.example.com/config",
	}
	bulkURLs := []string{
		"https://cdn.example.com/a.png",
		"https://cdn.example.com/b.png",
		"https://cdn.example.com/c.png",
		"https://cdn.example.com/d.png",
		"https://cdn.example.com/e.png",
		"https://cdn.example.com/f.png",
	}

	p := pool.NewPool[fetchResult](4, pool.WithRetryBackoff[fetchResult](30*time.Millisecond, 500*time.Millisecond))

	for _, url := range criticalURLs {
		url := url
		p.Submit(pool.Job[fetchResult]{
			Priority:   10,
			Timeout:    1 * time.Second,
			MaxRetries: 3,
			Fn: func(ctx context.Context) (fetchResult, error) {
				body, err := fakeFetch(ctx, url)
				return fetchResult{url: url, body: body}, err
			},
		})
	}
	for _, url := range bulkURLs {
		url := url
		p.Submit(pool.Job[fetchResult]{
			Priority:   0,
			Timeout:    1 * time.Second,
			MaxRetries: 1,
			Fn: func(ctx context.Context) (fetchResult, error) {
				body, err := fakeFetch(ctx, url)
				return fetchResult{url: url, body: body}, err
			},
		})
	}

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := p.Shutdown(ctx); err != nil {
			fmt.Println("shutdown:", err)
		}
	}()

	ok := 0
	for r := range p.Results() {
		if r.Err != nil {
			continue
		}
		ok++
		fmt.Printf("fetched %s\n", r.Value.url)
	}

	stats := p.Stats()
	fmt.Printf("\n%d/%d fetches succeeded\n", ok, len(criticalURLs)+len(bulkURLs))
	fmt.Printf("stats: completed=%d failed=%d retried=%d avgDuration=%v\n",
		stats.Completed, stats.Failed, stats.Retried, stats.AvgDuration)

	if errs := p.Errors(); len(errs) > 0 {
		fmt.Println("errors:")
		for _, err := range errs {
			fmt.Println(" -", err)
		}
	}
}
