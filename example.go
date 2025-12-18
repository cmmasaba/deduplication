package main

import (
	"context"
	"fmt"
	"time"
)

func main() {
	var (
		dedup     Deduplicator
		readLimit int64 = 64
	)

	dedup.applyDefaults()
	dedup.KeyFactory = NewValueHasherSHA256(int64(readLimit))

	ctx := context.Background()
	timeoutCtx, _ := context.WithTimeout(ctx, 5*time.Minute)
	vals := []string{"Nairobi", "Mombasa", "Kisumu", "Nakuru"}
	ticker := time.NewTicker(20 * time.Second)

outer:
	for {
		select {
		case <-timeoutCtx.Done():
			fmt.Println("\n\nDone")
			break outer
		case <-ticker.C:
			for i := range 4 {
				ok, err := dedup.IsDuplicate(ctx, vals[i])
				if err != nil {
					fmt.Printf("Error: %s", err)
				}

				if ok {
					fmt.Printf("%s is a duplicate\n", vals[i])
				}
			}
		}
	}
}
