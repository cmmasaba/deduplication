package cuckoofilter

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/cmmasaba/deduplication/cache"
)

type cfStore interface {
	CFAdd(context.Context, string, string) (bool, error)
	CFDel(context.Context, string, string) (bool, error)
	CFExists(context.Context, string, string) (bool, error)
	CFInit(ctx context.Context, cfKey string, capacity, bucketSize int64) (bool, error)
}

type CuckooFilter struct {
	store  cfStore
	cfKey  string
	window time.Duration
}

var logger = slog.New(slog.NewTextHandler(os.Stdout, nil))

// NewCuckooFilter creates and returns a [CuckooFilter] backed by Redis.
func NewCuckooFilter(
	connStr, cfKey string,
	capacity, bucketSize int64,
	window time.Duration,
) (*CuckooFilter, error) {
	c, err := cache.NewCache(connStr)
	if err != nil {
		logger.Error("[cf] error initializing cache", "error", err)

		return nil, err
	}

	cf := &CuckooFilter{store: c, cfKey: cfKey, window: window}

	_, err = cf.store.CFInit(context.Background(), cfKey, capacity, bucketSize)
	if err != nil {
		logger.Error("[cf] error initializing cuckoo filter", "error", err)

		return nil, err
	}

	return cf, nil
}

// IsDuplicate checks if the key is present in the cuckoo filter.
func (cf *CuckooFilter) IsDuplicate(ctx context.Context, data any) (bool, error) {
	key, ok := data.(string)
	if !ok {
		logger.Error("[cf] error checking duplicate", "error", "expected string")

		return false, fmt.Errorf("bad data, expected string")
	}

	exists, err := cf.store.CFExists(ctx, cf.cfKey, key)
	if err != nil {
		logger.Error("[cf] error perfoming cf lookup", "error", err)

		return false, err
	}

	if exists {
		return true, nil
	}

	_, err = cf.store.CFAdd(ctx, cf.cfKey, key)
	if err != nil {
		logger.Error("[cf] error perfoming cf insertion", "error", err)

		return false, err
	}

	return false, nil
}
