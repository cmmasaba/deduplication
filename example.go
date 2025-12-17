package main

import (
	"context"
	"fmt"
	"time"
)

func main() {
	ctx := context.Background()
	var dedup Deduplicator
	dedup.applyDefaults()
	vals := []string{"Nairobi", "Mombasa", "Kisumu", "Nakuru"}

	timeoutCtx, _ := context.WithTimeout(ctx, 5*time.Minute)
	ticker := time.NewTicker(20 * time.Second)
outer:
	for {
		select {
		case <-timeoutCtx.Done():
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
