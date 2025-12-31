package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cmmasaba/deduplication"
	"github.com/cmmasaba/deduplication/keyvalue"
)

func keyValueExample(wg *sync.WaitGroup, vals []string) {
	defer wg.Done()

	var readLimit int64 = 64

	kvDedup, err := deduplication.NewKeyValDeduplicator(
		keyvalue.NewValueHasherSHA256(readLimit),
		1*time.Minute,
		15*time.Second,
	)
	if err != nil {
		fmt.Println("[kv]: failed to initialize key-value deduplicator")

		return
	}

	ctx := context.Background()

	timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	ticker := time.NewTicker(10 * time.Second)

	var count int

outer:
	for {
		select {
		case <-timeoutCtx.Done():
			break outer
		case <-ticker.C:
			for i := range len(vals) {
				ok, err := kvDedup.IsDuplicate(ctx, vals[i])
				if err != nil {
					fmt.Printf("[kv]: error: %s", err)
				}

				if ok {
					count++
				}
			}
		}
	}

	fmt.Printf("[kv]: total duplicates count: %d \n\n", count)
}

func bloomFilterExample(wg *sync.WaitGroup, vals []string) {
	defer wg.Done()

	bfDedup, err := deduplication.NewBloomFilterDeduplicator(
		1*time.Minute,
		"example_bf",
		0.001,
		4,
		2,
	)
	if err != nil {
		return
	}

	ctx := context.Background()

	timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	ticker := time.NewTicker(10 * time.Second)

	var count int

outer:
	for {
		select {
		case <-timeoutCtx.Done():
			break outer
		case <-ticker.C:
			for i := range len(vals) {
				ok, err := bfDedup.IsDuplicate(ctx, vals[i])
				if err != nil {
					fmt.Printf("[bf]: error: %s", err)
					break outer
				}

				if ok {
					count++
				}
			}
		}
	}

	fmt.Printf("[bf]: total duplicates count: %d \n\n", count)
}

func cuckooFilterExample(wg *sync.WaitGroup, vals []string) {
	defer wg.Done()

	cfDedup, err := deduplication.NewCuckooFilterDeduplicator(
		1*time.Minute,
		"example_cf",
		64,
		4,
		15*time.Second,
	)
	if err != nil {
		return
	}

	ctx := context.Background()

	timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	ticker := time.NewTicker(10 * time.Second)

	var count int

outer:
	for {
		select {
		case <-timeoutCtx.Done():
			break outer
		case <-ticker.C:
			for i := range len(vals) {
				ok, err := cfDedup.IsDuplicate(ctx, vals[i])
				if err != nil {
					fmt.Printf("[cf]: error: %s", err)
				}

				if ok {
					count++
				}
			}
		}
	}

	fmt.Printf("[cf]: total duplicates count: %d \n\n", count)
}

func main() {
	var wg sync.WaitGroup

	vals := []string{"Nairobi", "Mombasa", "Kisumu", "Nakuru"}

	wg.Add(3)

	go keyValueExample(&wg, vals)
	go bloomFilterExample(&wg, vals)
	go cuckooFilterExample(&wg, vals)

	wg.Wait()
}
