package cache

import (
	"context"
	"fmt"
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
	cmd := fmt.Sprintf("EXPANSION %v", expansion)

	ok, err := c.cache.Do(ctx, "BF.RESERVE", bfKey, errorRate, capacity, cmd).Bool()
	if err != nil {
		return false, err
	}

	return ok, nil
}

func (c *Cache) BFAdd(ctx context.Context, bfKey, key string) (bool, error) {
	ok, err := c.cache.Do(ctx, "BF.ADD", bfKey, key).Bool()
	if err != nil {
		return false, err
	}

	return ok, nil
}

func (c *Cache) BFExists(ctx context.Context, bfKey, key string) (bool, error) {
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
	cmd := fmt.Sprintf("BUCKETSIZE %v", bucketSize)

	ok, err := c.cache.Do(ctx, "CF.RESERVE", cfKey, capacity, cmd).Bool()
	if err != nil {
		return false, err
	}

	return ok, nil
}

func (c *Cache) CFAdd(ctx context.Context, cfKey, hash string) (bool, error) {
	ok, err := c.cache.Do(ctx, "CF.ADD", cfKey, hash).Bool()
	if err != nil {
		return false, err
	}

	return ok, nil
}

func (c *Cache) CFExists(ctx context.Context, cfKey, key string) (bool, error) {
	exists, err := c.cache.Do(ctx, "CF.EXISTS", cfKey, key).Bool()
	if err != nil {
		return false, err
	}

	return exists, nil
}
