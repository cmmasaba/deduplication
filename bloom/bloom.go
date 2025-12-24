package bloomfilter

import (
	"context"

	"github.com/cmmasaba/deduplication/cache"
)

type BFStore interface {
	BFAdd(context.Context, string, string) (bool, error)
	BFExists(context.Context, string, string) (bool, error)
	BFInit(context.Context, string, float64, int64, int64) (bool, error)
}

type BloomFilter struct {
	store BFStore
	bfKey string
}

// NewCuckooFilter creates and returns a [BloomFilter] backed by Redis.
func NewCuckooFilter(
	ctx context.Context,
	connStr, bfKey string,
	errorRate float64,
	capacity, bucketSize int64,
) (*BloomFilter, error) {
	c, err := cache.NewCache(connStr)
	if err != nil {
		return nil, err
	}

	bf := &BloomFilter{store: c, bfKey: bfKey}

	_, err = bf.store.BFInit(ctx, bfKey, errorRate, capacity, bucketSize)
	if err != nil {
		return nil, err
	}

	return bf, nil
}

// IsDuplicate checks if the key is present in the bloom filter.
func (bf *BloomFilter) IsDuplicate(ctx context.Context, key string) (bool, error) {
	exists, err := bf.store.BFExists(ctx, bf.bfKey, key)
	if err != nil {
		return false, err
	}

	if exists {
		return true, nil
	}

	_, err = bf.store.BFAdd(ctx, bf.bfKey, key)
	if err != nil {
		return false, err
	}

	return false, nil
}
