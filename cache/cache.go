package cache

import (
	"context"
	"time"

	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
)

// Cache is a wrapper for the Redis cache
type Cache struct {
	cache *redis.Client
}

func NewCache(connStr string) (*Cache, error) {
	opt, err := redis.ParseURL(connStr)
	if err != nil {
		return nil, err
	}

	client := redis.NewClient(opt)

	// Enable tracing instrumentation.
	if err := redisotel.InstrumentTracing(client); err != nil {
		return nil, err
	}

	// Enable metrics instrumentation.
	if err := redisotel.InstrumentMetrics(client); err != nil {
		return nil, err
	}

	return &Cache{cache: client}, nil
}

func (c *Cache) Exists(ctx context.Context, keys ...string) *redis.IntCmd {
	return c.cache.Exists(ctx, keys...)
}

func (c *Cache) SetEx(
	ctx context.Context,
	key string,
	val any,
	duration time.Duration,
) *redis.StatusCmd {
	return c.cache.SetEx(ctx, key, val, duration)
}

func (c *Cache) BFInit(
	ctx context.Context,
	bfKey string,
	errorRate float64,
	capacity, expansion int64,
) (bool, error) {
	err := c.cache.Do(ctx, "BF.RESERVE", bfKey, errorRate, capacity, "EXPANSION", expansion).Err()
	if err != nil {
		return false, err
	}

	return true, nil
}

func (c *Cache) BFAdd(ctx context.Context, bfKey string, key any) (bool, error) {
	ok, err := c.cache.Do(ctx, "BF.ADD", bfKey, key).Bool()
	if err != nil {
		return false, err
	}

	return ok, nil
}

func (c *Cache) BFExists(ctx context.Context, bfKey string, key any) (bool, error) {
	exists, err := c.cache.Do(ctx, "BF.EXISTS", bfKey, key).Bool()
	if err != nil {
		return false, err
	}

	return exists, nil
}

func (c *Cache) CFInit(
	ctx context.Context,
	cfKey string,
	capacity, bucketSize int64,
) (bool, error) {
	err := c.cache.Do(ctx, "CF.RESERVE", cfKey, capacity, "BUCKETSIZE", bucketSize).Err()
	if err != nil {
		return false, err
	}

	return true, nil
}

func (c *Cache) CFAdd(ctx context.Context, cfKey string, key any) (bool, error) {
	ok, err := c.cache.Do(ctx, "CF.ADD", cfKey, key).Bool()
	if err != nil {
		return false, err
	}

	return ok, nil
}

func (c *Cache) CFExists(ctx context.Context, cfKey string, key any) (bool, error) {
	exists, err := c.cache.Do(ctx, "CF.EXISTS", cfKey, key).Bool()
	if err != nil {
		return false, err
	}

	return exists, nil
}

func (c *Cache) CFDel(ctx context.Context, cfKey string, key any) (bool, error) {
	ok, err := c.cache.Do(ctx, "CF.DEL", cfKey, key).Bool()
	if err != nil {
		return false, err
	}

	return ok, nil
}
