package bloomfilter

import (
	"context"
	"fmt"

	"github.com/cmmasaba/deduplication/cache"
)

type bfStore interface {
	BFAdd(context.Context, string, string) (bool, error)
	// BFDel(context.Context, string, string) (bool, error)
	BFExists(context.Context, string, string) (bool, error)
	BFInit(context.Context, string, float64, int64, int64) (bool, error)
}

type BloomFilter struct {
	store bfStore
	Key   string
}

// NewBloomFilter creates and returns a [BloomFilter] backed by Redis.
func NewBloomFilter(
	connStr, bfKey string,
	errorRate float64,
	capacity, expansion int64,
) (*BloomFilter, error) {
	c, err := cache.NewCache(connStr)
	if err != nil {
		return nil, err
	}

	bf := &BloomFilter{store: c, Key: bfKey}

	_, err = bf.store.BFInit(context.Background(), bfKey, errorRate, capacity, expansion)
	if err != nil {
		return nil, err
	}

	return bf, nil
}

// IsDuplicate checks if the key is present in the bloom filter.
func (bf *BloomFilter) IsDuplicate(ctx context.Context, data any) (bool, error) {
	key, ok := data.(string)
	if !ok {
		return false, fmt.Errorf("bf-isduplicate: bad data, expected string")
	}

	exists, err := bf.store.BFExists(ctx, bf.Key, key)
	if err != nil {
		return false, err
	}

	if exists {
		return true, nil
	}

	_, err = bf.store.BFAdd(ctx, bf.Key, key)
	if err != nil {
		return false, err
	}

	return false, nil
}
