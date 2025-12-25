package keyvalue

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/sha512"
	"errors"
	"hash/adler32"
	"io"
	"time"

	"github.com/cmmasaba/deduplication/cache"
	"github.com/redis/go-redis/v9"
)

// ValueHasherLimitMinimum is the least number of bytes used for
// calculating the hash value of a key
const ValueHasherLimitMinimum = 64

// ValueHasher returns a hash that identifies a value.
type ValueHasher func(value []byte) (string, error)

type KeyStore interface {
	Exists(context.Context, ...string) *redis.IntCmd
	SetEx(context.Context, string, any, time.Duration) *redis.StatusCmd
}

type RedisExpiringKeyRepo struct {
	cache  KeyStore
	window time.Duration
}

// Payload is a value to be deduplicated.
// Key is a hash of the value
// Value is the actual data
type Payload struct {
	Key   string
	Value any
}

func NewRedisExpiringKeyRepo(window time.Duration, redisURL string) (*RedisExpiringKeyRepo, error) {
	if window < time.Millisecond*5 {
		return nil, errors.New("deduplication window cannot be less than 5 milliseconds")
	}

	c, err := cache.NewCache(redisURL)
	if err != nil {
		return nil, err
	}

	return &RedisExpiringKeyRepo{
		cache:  c,
		window: window,
	}, nil
}

// IsDuplicate checks if the `data` is duplicate within a given time window.
func (r *RedisExpiringKeyRepo) IsDuplicate(ctx context.Context, data any) (bool, error) {
	payload, ok := data.(Payload)
	if !ok {
		return false, errors.New("bad data")
	}

	exists, err := r.cache.Exists(ctx, payload.Key).Result()
	if err != nil {
		return false, err
	}

	if exists > 0 {
		return true, nil
	}

	err = r.cache.SetEx(ctx, payload.Key, payload.Value, r.window).Err()
	if err != nil {
		return false, err
	}

	return false, nil
}

// NewValueHasherAdler32 computes Adler32 checksum of a value.
// Read limit must be greater than [HasherLimitMinimum]
func NewValueHasherAdler32(readLimit int64) ValueHasher {
	if readLimit < ValueHasherLimitMinimum {
		readLimit = ValueHasherLimitMinimum
	}

	return func(value []byte) (string, error) {
		h := adler32.New()

		_, err := io.CopyN(h, bytes.NewReader(value), readLimit)
		if err != nil && !errors.Is(err, io.EOF) {
			return "", err
		}

		return string(h.Sum(nil)), nil
	}
}

// NewValueHasherSHA256 computes SHA256 checksum of a value.
// Read limit must be greater than [HasherLimitMinimum]
func NewValueHasherSHA256(readLimit int64) ValueHasher {
	if readLimit < ValueHasherLimitMinimum {
		readLimit = ValueHasherLimitMinimum
	}

	return func(value []byte) (string, error) {
		h := sha256.New()

		_, err := io.CopyN(h, bytes.NewReader(value), readLimit)
		if err != nil && !errors.Is(err, io.EOF) {
			return "", err
		}

		return string(h.Sum(nil)), nil
	}
}

// NewValueHasherSHA512 computes SHA512 checksum of a value.
// Read limit must be greater than [HasherLimitMinimum]
func NewValueHasherSHA512(readLimit int64) ValueHasher {
	if readLimit < ValueHasherLimitMinimum {
		readLimit = ValueHasherLimitMinimum
	}

	return func(value []byte) (string, error) {
		h := sha512.New()

		_, err := io.CopyN(h, bytes.NewReader(value), readLimit)
		if err != nil && !errors.Is(err, io.EOF) {
			return "", err
		}

		return string(h.Sum(nil)), nil
	}
}
