package redis

import (
	"context"
	"errors"
	"time"

	"github.com/anshu4sharma/fraud_payment_detector/pkg/utils"
	"github.com/redis/go-redis/v9"
)

type RedisClient struct {
	client  *redis.Client
	logger  *utils.Logger
	isReady bool
}

func NewRedisClient(url string, logger *utils.Logger) *RedisClient {
	opt, err := redis.ParseURL(url)
	if err != nil {
		logger.Errorf("Invalid Redis URL: %v", err)
		return nil
	}
	return &RedisClient{
		client: redis.NewClient(opt),
		logger: logger,
	}
}

func (r *RedisClient) Connect(maxRetries int, retryDelay time.Duration) error {
	var err error
	for i := 0; i < maxRetries; i++ {
		_, err = r.client.Ping(context.Background()).Result()
		if err == nil {
			r.isReady = true
			r.logger.Infof("Connected to Redis")
			return nil
		}
		r.logger.Errorf("Failed to connect to Redis (attempt %d): %v", i+1, err)
		time.Sleep(retryDelay)
		retryDelay *= 2
	}
	return errors.New("max retries reached, could not connect to Redis")
}

func (r *RedisClient) Close() error {
	if r.client != nil {
		if err := r.client.Close(); err != nil {
			r.logger.Errorf("Error closing Redis connection: %v", err)
			return err
		}
		r.logger.Infof("Redis connection closed")
	}
	return nil
}

func (r *RedisClient) IsReady() bool {
	return r.isReady
}

func (r *RedisClient) Get(ctx context.Context, key string) (string, error) {
	if !r.isReady {
		return "", errors.New("Redis is not connected")
	}
	return r.client.Get(ctx, key).Result()
}

func (r *RedisClient) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	if !r.isReady {
		return errors.New("Redis is not connected")
	}
	return r.client.Set(ctx, key, value, expiration).Err()
}

func (r *RedisClient) Delete(ctx context.Context, key string) error {
	if !r.isReady {
		return errors.New("Redis is not connected")
	}
	return r.client.Del(ctx, key).Err()
}
