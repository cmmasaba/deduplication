package deduplication

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/gob"
	"errors"
	"hash/adler32"
	"io"
	"math"
	"os"
	"time"

	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
)

// ValueHasherLimitMinimum is the least number of bytes used for
// calculating the hash value of a key
const ValueHasherLimitMinimum = 64

// Payload is a value to be deduplicated.
// Key is a hash of the value
// Value is the actual data
type Payload struct {
	Key   string
	Value any
}

type Cache interface {
	Exists(context.Context, ...string) *redis.IntCmd
	SetEx(context.Context, string, any, time.Duration) *redis.StatusCmd
	DBSize(context.Context) *redis.IntCmd
}

type ExpiringKeyRepository interface {
	// IsDuplicate returns `true` if the key
	// was not checked in recent past.
	// The key must expire in a certain time window.
	IsDuplicate(ctx context.Context, data Payload) (ok bool, err error)
}

// ValueHasher returns a hash that identifies a value.
// The hash should be unique per value but avoiding collisions
// completely is not practical.
type ValueHasher func(value []byte) (string, error)

// Deduplicator drops similar values if they are present in a
// [ExpiringKeyRepository]. The similarity is determined by
// [ValueHasher]. Timeout is applied using [context.WithTimeout]
//
// KeyFactory defaults to [NewValueHasherAdler32] with read limit
// set to [math.MaxInt64]. Use [NewValueHasherSHA256] for less
// collisions.
// Timeout defaults to one minute.
type Deduplicator struct {
	KeyFactory ValueHasher
	Repository ExpiringKeyRepository
	Timeout    time.Duration
}

// IsDuplicate returns `true` if the value hash calculated by [ValueHasher]
// was seen in a deduplication time window.
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

	return d.Repository.IsDuplicate(ctx, Payload{Key: key, Value: value})
}

func (d *Deduplicator) applyDefaults() {
	if d == nil {
		r, err := NewRedisExpiringKeyRepo(time.Minute, os.Getenv("REDIS_HOST_URL"))
		if err != nil {
			panic(err)
		}

		d = &Deduplicator{
			KeyFactory: NewValueHasherAdler32(math.MaxInt64),
			Repository: r,
			Timeout:    5*time.Minute,
		}
	}
	if d.KeyFactory == nil {
		d.KeyFactory = NewValueHasherAdler32(math.MaxInt64)
	}

	if d.Repository == nil {
		r, err := NewRedisExpiringKeyRepo(5*time.Minute, os.Getenv("REDIS_HOST_URL"))
		if err != nil {
			panic(err)
		}
		d.Repository = r
	}

	if d.Timeout < 5*time.Millisecond {
		d.Timeout = 5 * time.Millisecond
	}
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
	cache  Cache
	window time.Duration
}

func NewRedisExpiringKeyRepo(window time.Duration, redisURL string) (*RedisExpiringKeyRepo, error) {
	if window < time.Millisecond*5 {
		return nil, errors.New("deduplication window cannot be less than 5 milliseconds")
	}

	c, err := NewCache(redisURL)
	if err != nil {
		return nil, err
	}

	return &RedisExpiringKeyRepo{
		cache:  c,
		window: window,
	}, nil
}

// IsDuplicate checks if the `data` is duplicate within a given time window.
func (r *RedisExpiringKeyRepo) IsDuplicate(ctx context.Context, data Payload) (bool, error) {
	exists, err := r.cache.Exists(ctx, data.Key).Result()
	if err != nil {
		return false, err
	}

	if exists > 0 {
		return true, nil
	}

	err = r.cache.SetEx(ctx, data.Key, data.Value, r.window).Err()
	if err != nil {
		return false, err
	}

	return false, nil
}

// Len returns the number of keys in the repository that have not expired.
func (r *RedisExpiringKeyRepo) Len(ctx context.Context) (int64, error) {
	count, err := r.cache.DBSize(ctx).Result()
	if err != nil {
		return 0, err
	}

	return count, nil
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
