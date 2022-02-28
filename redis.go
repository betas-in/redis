package rediscache

import (
	"context"
	"fmt"
	"time"

	"github.com/betas-in/logger"
	"github.com/go-redis/redis/v8"
)

// Cache ...
type Cache interface {
	GetClient() *redis.Client
	Ping(ctx context.Context) (string, error)
	Get(ctx context.Context, key string) (string, error)
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) (string, error)
	Del(ctx context.Context, keys ...string) (int64, error)
	ZAdd(ctx context.Context, key string, members ...*redis.Z) (int64, error)
	ZCard(ctx context.Context, key string) (int64, error)
	ZPopMin(ctx context.Context, key string, count ...int64) ([]redis.Z, error)
	ZScore(ctx context.Context, key, member string) (float64, error)
	ZRem(ctx context.Context, key, member string) (int64, error)
	ZRange(ctx context.Context, key string, start, stop int64) ([]string, error)
	ZRangeByScore(ctx context.Context, key, min, max string, offset, count int64) ([]string, error)
	ZRangeByScoreWithScores(ctx context.Context, key, min, max string, offset, count int64) ([]redis.Z, error)
	RPush(ctx context.Context, key string, values ...interface{}) (int64, error)
	LLen(ctx context.Context, key string) (int64, error)
	LRem(ctx context.Context, key string, count int64, value interface{}) (int64, error)
	LSet(ctx context.Context, key string, index int64, value interface{}) (string, error)
	LPop(ctx context.Context, key string) (string, error)
	LRange(ctx context.Context, key string, start int64, stop int64) ([]string, error)
	LMove(ctx context.Context, source string, destination string, srcpos string, destpos string) (string, error)
	SMembers(ctx context.Context, key string) ([]string, error)
	SAdd(ctx context.Context, key string, members ...interface{}) (int64, error)
	Incr(ctx context.Context, key string) (int64, error)
	Expire(ctx context.Context, key string, expiration time.Duration) (bool, error)
	Close() error
}

type cache struct {
	client *redis.Client
	log    *logger.Logger
	config *Config
}

type Config struct {
	Host       string
	Port       int
	Password   string
	DB         int
	MaxRetries int
	PoolSize   int
}

// NewCache ...
func NewCache(conf *Config, log *logger.Logger) (Cache, error) {
	c := cache{
		log:    log,
		config: conf,
	}
	c.defaults()

	client := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", c.config.Host, c.config.Port),
		Password: c.config.Password,
		DB:       c.config.DB,
		OnConnect: func(ctx context.Context, conn *redis.Conn) error {
			log.Info("redis").Msgf("connected to %s:%d", c.config.Host, c.config.Port)
			return nil
		},
		MaxRetries: c.config.MaxRetries,
		PoolSize:   c.config.PoolSize,
	})

	ctx := context.Background()
	_, err := client.Ping(ctx).Result()
	if err != nil {
		log.Fatal("redis").Msgf("Could not ping redis %+v", err)
	}
	c.client = client

	return &c, nil
}

func (c *cache) defaults() {
	if c.config.Host == "" {
		c.config.Host = "127.0.0.1"
	}
	if c.config.Port == 0 {
		c.config.Port = 6379
	}
	if c.config.DB == 0 {
		c.config.DB = 0
	}
	if c.config.MaxRetries == 0 {
		c.config.MaxRetries = 3
	}
	if c.config.PoolSize == 0 {
		c.config.PoolSize = 10
	}
}

// GetClient the redis object
func (c *cache) GetClient() *redis.Client {
	return c.client
}

// Close the redis object
func (c *cache) Close() error {
	return c.client.Close()
}

// Get from redis
func (c *cache) Get(ctx context.Context, key string) (string, error) {
	response, err := c.client.Get(ctx, key).Result()
	if err != nil && err != redis.Nil {
		return response, err
	}
	return response, nil
}

// Set in redis
func (c *cache) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) (string, error) {
	response, err := c.client.Set(ctx, key, value, expiration).Result()
	if err != nil && err != redis.Nil {
		return response, err
	}
	return response, nil
}

// Del in redis
func (c *cache) Del(ctx context.Context, keys ...string) (int64, error) {
	response, err := c.client.Del(ctx, keys...).Result()
	if err != nil && err != redis.Nil {
		return response, err
	}
	return response, nil
}

// ZAdd in redis
func (c *cache) ZAdd(ctx context.Context, key string, members ...*redis.Z) (int64, error) {
	response, err := c.client.ZAdd(ctx, key, members...).Result()
	if err != nil && err != redis.Nil {
		return response, err
	}
	return response, nil
}

// ZCard in redis
func (c *cache) ZCard(ctx context.Context, key string) (int64, error) {
	response, err := c.client.ZCard(ctx, key).Result()
	if err != nil && err != redis.Nil {
		return response, err
	}
	return response, nil
}

// ZPopMin in redis
func (c *cache) ZPopMin(ctx context.Context, key string, count ...int64) ([]redis.Z, error) {
	response, err := c.client.ZPopMin(ctx, key, count...).Result()
	if err != nil && err != redis.Nil {
		return response, err
	}
	return response, nil
}

// ZScore in redis
func (c *cache) ZScore(ctx context.Context, key, member string) (float64, error) {
	response, err := c.client.ZScore(ctx, key, member).Result()
	if err != nil && err != redis.Nil {
		return response, err
	}
	return response, nil
}

// ZRem in redis
func (c *cache) ZRem(ctx context.Context, key, member string) (int64, error) {
	response, err := c.client.ZRem(ctx, key, member).Result()
	if err != nil && err != redis.Nil {
		return response, err
	}
	return response, nil
}

func (c *cache) ZRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	response, err := c.client.ZRange(ctx, key, start, stop).Result()
	if err != nil && err != redis.Nil {
		return response, err
	}
	return response, nil
}

func (c *cache) ZRangeByScoreWithScores(ctx context.Context, key, min, max string, offset, count int64) ([]redis.Z, error) {
	response, err := c.client.ZRangeByScoreWithScores(
		ctx,
		key,
		&redis.ZRangeBy{Min: min, Max: max, Offset: offset, Count: count},
	).Result()
	if err != nil && err != redis.Nil {
		return response, err
	}
	return response, nil
}

func (c *cache) ZRangeByScore(ctx context.Context, key, min, max string, offset, count int64) ([]string, error) {
	response, err := c.client.ZRangeByScore(
		ctx,
		key,
		&redis.ZRangeBy{Min: min, Max: max, Offset: offset, Count: count},
	).Result()
	if err != nil && err != redis.Nil {
		return response, err
	}
	return response, nil
}

// RPush in redis
func (c *cache) RPush(ctx context.Context, key string, values ...interface{}) (int64, error) {
	response, err := c.client.RPush(ctx, key, values...).Result()
	if err != nil && err != redis.Nil {
		return response, err
	}
	return response, nil
}

// LLen in redis
func (c *cache) LLen(ctx context.Context, key string) (int64, error) {
	response, err := c.client.LLen(ctx, key).Result()
	if err != nil && err != redis.Nil {
		return response, err
	}
	return response, nil
}

func (c *cache) LRem(ctx context.Context, key string, count int64, value interface{}) (int64, error) {
	response, err := c.client.LRem(ctx, key, count, value).Result()
	if err != nil && err != redis.Nil {
		return response, err
	}
	return response, nil
}

func (c *cache) LSet(ctx context.Context, key string, index int64, value interface{}) (string, error) {
	response, err := c.client.LSet(ctx, key, index, value).Result()
	if err != nil && err != redis.Nil {
		return response, err
	}
	return response, nil
}

func (c *cache) LPop(ctx context.Context, key string) (string, error) {
	response, err := c.client.LPop(ctx, key).Result()
	if err != nil && err != redis.Nil {
		return response, err
	}
	return response, nil
}

func (c *cache) LRange(ctx context.Context, key string, start int64, stop int64) ([]string, error) {
	response, err := c.client.LRange(ctx, key, start, stop).Result()
	if err != nil && err != redis.Nil {
		return response, err
	}
	return response, nil
}

func (c *cache) LMove(ctx context.Context, source string, destination string, srcpos string, destpos string) (string, error) {
	response, err := c.client.LMove(ctx, source, destination, srcpos, destpos).Result()
	if err != nil && err != redis.Nil {
		return response, err
	}
	return response, nil
}

func (c *cache) SMembers(ctx context.Context, key string) ([]string, error) {
	response, err := c.client.SMembers(ctx, key).Result()
	if err != nil && err != redis.Nil {
		return response, err
	}
	return response, nil
}

func (c *cache) SAdd(ctx context.Context, key string, members ...interface{}) (int64, error) {
	response, err := c.client.SAdd(ctx, key, members...).Result()
	if err != nil && err != redis.Nil {
		return response, err
	}
	return response, nil
}

func (c *cache) Ping(ctx context.Context) (string, error) {
	response, err := c.client.Ping(ctx).Result()
	if err != nil && err != redis.Nil {
		return response, err
	}
	return response, nil
}

func (c *cache) Incr(ctx context.Context, key string) (int64, error) {
	response, err := c.client.Incr(ctx, key).Result()
	if err != nil && err != redis.Nil {
		return response, err
	}
	return response, nil
}

func (c *cache) Expire(ctx context.Context, key string, expiration time.Duration) (bool, error) {
	response, err := c.client.Expire(ctx, key, expiration).Result()
	if err != nil && err != redis.Nil {
		return response, err
	}
	return response, nil
}
