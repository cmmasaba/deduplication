package deduplication

import (
	"bytes"
	"context"
	"encoding/gob"
	"math"
	"os"
	"time"

	bloomfilter "github.com/cmmasaba/deduplication/bloom"
	cuckoofilter "github.com/cmmasaba/deduplication/cuckoo"
	"github.com/cmmasaba/deduplication/keyvalue"
)

type KeyRepository interface {
	IsDuplicate(ctx context.Context, data any) (ok bool, err error)
}

// KeyValDeduplicator drops similar values if they are present in a
// KeyValue store. The similarity is determined by
// [keyvalue.ValueHasher]. Timeout is applied using [context.WithTimeout]
//
// KeyFactory defaults to [keyvalue.NewValueHasherAdler32] with read limit
// set to [math.MaxInt64]. Other options are [keyvalue.NewValueHasherSHA256] or
// [keyvalue.NewValueHasherSHA512].
type KeyValDeduplicator struct {
	KeyFactory keyvalue.ValueHasher
	Repository KeyRepository
	Timeout    time.Duration
}

func NewKeyValDeduplicator(
	keyFactory keyvalue.ValueHasher, ctxTimeout, window time.Duration,
) (*KeyValDeduplicator, error) {
	r, err := keyvalue.NewRedisExpiringKeyRepo(window, os.Getenv("REDIS_HOST_URL"))
	if err != nil {
		return nil, err
	}

	if keyFactory == nil {
		keyFactory = keyvalue.NewValueHasherAdler32(math.MaxInt64)
	}

	if ctxTimeout < 5*time.Millisecond {
		ctxTimeout = 5 * time.Millisecond
	}

	d := &KeyValDeduplicator{
		KeyFactory: keyFactory,
		Repository: r,
		Timeout:    ctxTimeout,
	}

	return d, nil
}

// IsDuplicate returns `true` if the value hash calculated by [ValueHasher]
// was seen in a deduplication time window.
func (d *KeyValDeduplicator) IsDuplicate(ctx context.Context, data any) (bool, error) {
	var buf bytes.Buffer

	err := gob.NewEncoder(&buf).Encode(data)
	if err != nil {
		return false, err
	}

	key, err := d.KeyFactory(buf.Bytes())
	if err != nil {
		return false, err
	}

	ctx, cancel := context.WithTimeout(ctx, d.Timeout)
	defer cancel()

	return d.Repository.IsDuplicate(ctx, keyvalue.Payload{Key: key, Value: data})
}

type BloomFilterDeduplicator struct {
	Repository KeyRepository
	Timeout    time.Duration
}

func NewBloomFilterDeduplicator(
	ctxTimeout time.Duration,
	filterKey string,
	errorRate float64,
	capacity, expansion int64,
) (*BloomFilterDeduplicator, error) {
	bf, err := bloomfilter.New(
		os.Getenv("REDIS_HOST_URL"),
		filterKey,
		errorRate,
		capacity,
		expansion,
	)
	if err != nil {
		return nil, err
	}

	if ctxTimeout < 5*time.Millisecond {
		ctxTimeout = 5 * time.Millisecond
	}

	d := &BloomFilterDeduplicator{
		Repository: bf,
		Timeout:    ctxTimeout,
	}

	return d, nil
}

// IsDuplicate returns `true` if the data is present in the bloom filter.
func (d *BloomFilterDeduplicator) IsDuplicate(ctx context.Context, data any) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, d.Timeout)
	defer cancel()

	return d.Repository.IsDuplicate(ctx, data)
}

type CuckooFilterDeduplicator struct {
	Repository KeyRepository
	Timeout    time.Duration
}

func NewCuckooFilterDeduplicator(
	ctxTimeout time.Duration,
	filterKey string,
	capacity, bucketSize int64,
	window time.Duration,
) (*CuckooFilterDeduplicator, error) {
	cf, err := cuckoofilter.New(
		os.Getenv("REDIS_HOST_URL"),
		filterKey,
		capacity,
		bucketSize,
		window,
	)
	if err != nil {
		return nil, err
	}

	if ctxTimeout < 5*time.Millisecond {
		ctxTimeout = 5 * time.Millisecond
	}

	d := &CuckooFilterDeduplicator{
		Repository: cf,
		Timeout:    ctxTimeout,
	}

	return d, nil
}

// IsDuplicate returns `true` if the data is present in the cuckoo filter.
func (d *CuckooFilterDeduplicator) IsDuplicate(ctx context.Context, data any) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, d.Timeout)
	defer cancel()

	return d.Repository.IsDuplicate(ctx, data)
}
