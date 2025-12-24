package cuckoofilter

import (
	"context"

	"github.com/cmmasaba/deduplication/cache"
)

type CFStore interface {
	CFAdd(context.Context, string, string) (bool, error)
	CFExists(context.Context, string, string) (bool, error)
	CFInit(ctx context.Context, cfKey string, capacity, bucketSize int64) (bool, error)
}

type CuckooFilter struct {
	store CFStore
	cfKey string
}

// NewCuckooFilter creates and returns a [CuckooFilter] backed by Redis.
func NewCuckooFilter(
	ctx context.Context,
	connStr, cfKey string,
	capacity, bucketSize int64,
) (*CuckooFilter, error) {
	c, err := cache.NewCache(connStr)
	if err != nil {
		return nil, err
	}

	cf := &CuckooFilter{store: c, cfKey: cfKey}

	_, err = cf.store.CFInit(ctx, cfKey, capacity, bucketSize)
	if err != nil {
		return nil, err
	}

	return cf, nil
}

// IsDuplicate checks if the key is present in the cuckoo filter.
func (cf *CuckooFilter) IsDuplicate(ctx context.Context, key string) (bool, error) {
	exists, err := cf.store.CFExists(ctx, cf.cfKey, key)
	if err != nil {
		return false, err
	}

	if exists {
		return true, nil
	}

	_, err = cf.store.CFAdd(ctx, cf.cfKey, key)
	if err != nil {
		return false, err
	}

	return false, nil
}
