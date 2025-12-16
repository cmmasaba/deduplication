package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/gob"
	"hash/adler32"
	"io"
	"math"
	"time"

	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
)

// HasherLimitMinimum is the least number of bytes used for
// calculating the hash value of a key
const ValueHasherLimitMinimum = 64

type Cache interface {
	Exists(ctx context.Context, keys ...string) *redis.IntCmd
	Get(context.Context, string) *redis.StringCmd
	SetEx(context.Context, string, interface{}, time.Duration) *redis.StatusCmd
}

type ExpiringKeyRepository interface {
	// IsDuplicate returns `true` if the key
	// was not checked in recent past.
	// The key must expire in a certain time window.
	IsDuplicate(ctx context.Context, key string) (ok bool, err error)
}

type ValueHasher func(value []byte) (string, error)

type Deduplicator struct {
	KeyFactory ValueHasher
	Repository ExpiringKeyRepository
	Timeout    time.Duration
}

func (d *Deduplicator) IsDuplicate(ctx context.Context, value any) (bool, error) {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(value)
	if err != nil {
		return false, err
	}

	key, err := d.KeyFactory(buf.Bytes())
	if err != nil {
		return false, err
	}

	ctx, cancel := context.WithTimeout(ctx, d.Timeout)
	defer cancel()

	return d.Repository.IsDuplicate(ctx, key)
}

func applyDefaults(d *Deduplicator) *Deduplicator {
	if d == nil {
		r, err := NewRedisExpiringKeyRepo()
		if err != nil {
			panic(err)
		}

		return &Deduplicator{
			KeyFactory: NewValueHasherAdler32(math.MaxInt64),
			Repository: r,
			Timeout:    time.Minute,
		}
	}
	if d.KeyFactory == nil {
		d.KeyFactory = NewValueHasherAdler32(math.MaxInt64)
	}

	if d.Repository == nil {
		r, err := NewRedisExpiringKeyRepo()
		if err != nil {
			panic(err)
		}
		d.Repository = r
	}

	if d.Timeout < 5*time.Millisecond {
		d.Timeout = 5 * time.Millisecond
	}

	return d
}

func NewCache(connStr string) (Cache, error) {
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

	return client, nil
}

type RedisExpiringKeyRepo struct {
	cache Cache
}

func NewRedisExpiringKeyRepo() (*RedisExpiringKeyRepo, error) {
	return nil, nil
}

func (r *RedisExpiringKeyRepo) Set(ctx context.Context, key string, val any, expiry time.Duration) error {
	if err := r.cache.SetEx(ctx, key, val, expiry).Err(); err != nil {
		return err
	}

	return nil
}

func (r *RedisExpiringKeyRepo) Get(ctx context.Context, key string) (string, error) {
	val, err := r.cache.Get(ctx, "foo").Result()
	if err != nil {
		return "", err
	}

	return val, nil
}

func (r *RedisExpiringKeyRepo) IsDuplicate(ctx context.Context, key string) (bool, error) {
	exists, err := r.cache.Exists(ctx, key).Result()
	if err != nil {
		return false, err
	}

	if exists <= 0 {
		return false, nil
	}

	return true, nil
}

// NewValueHasherAdler32 generates messages using Adler-32 checksum of the [Value].
// Read limit specifies how many bytes of the [Value] are used to compute the hash.
// Lower limits improve performance but results in more false positives. Read limit
// must be greater than [HasherLimitMinimum]
func NewValueHasherAdler32(readLimit int64) ValueHasher {
	if readLimit < ValueHasherLimitMinimum {
		readLimit = ValueHasherLimitMinimum
	}

	return func(value []byte) (string, error) {
		h := adler32.New()
		_, err := io.CopyN(h, bytes.NewReader(value), readLimit)
		if err != nil && err != io.EOF {
			return "", err
		}

		return string(h.Sum(nil)), nil
	}
}

// NewValueHasherSHA256 is slower but more efficient. Read limit specifies how many
// bytes of the [Value] are used to compute the hash.
// Lower limits improve performance but results in more false positives. Read limit
// must be greater than [HasherLimitMinimum]
func NewValueHasherSHA256(readLimit int64) ValueHasher {
	if readLimit < ValueHasherLimitMinimum {
		readLimit = ValueHasherLimitMinimum
	}

	return func(value []byte) (string, error) {
		h := sha256.New()
		_, err := io.CopyN(h, bytes.NewReader(value), readLimit)
		if err != nil && err != io.EOF {
			return "", err
		}

		return string(h.Sum(nil)), nil
	}
}
