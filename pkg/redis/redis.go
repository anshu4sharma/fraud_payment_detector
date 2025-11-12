package redis

import (
	"context"
	"errors"
	"time"

	"github.com/anshu4sharma/fraud_payment_detector/pkg/utils"
	"github.com/redis/go-redis/v9"
)

type RedisClient struct {
	*redis.Client       // Embedded redis.Client for full method access
	logger  *utils.Logger
	isReady bool
}

// NewRedisClient creates a new RedisClient
func NewRedisClient(url string, logger *utils.Logger) *RedisClient {
	opt, err := redis.ParseURL(url)
	if err != nil {
		logger.Errorf("Invalid Redis URL: %v", err)
		return nil
	}

	return &RedisClient{
		Client: redis.NewClient(opt),
		logger: logger,
	}
}

// Connect establishes connection with retry logic
func (r *RedisClient) Connect(maxRetries int, retryDelay time.Duration) error {
	var err error
	for i := 0; i < maxRetries; i++ {
		_, err = r.Ping(context.Background()).Result() // Call embedded Client method
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

// Close closes the Redis connection
func (r *RedisClient) Close() error {
	if r.Client != nil {
		if err := r.Client.Close(); err != nil {
			r.logger.Errorf("Error closing Redis connection: %v", err)
			return err
		}
		r.logger.Infof("Redis connection closed")
	}
	return nil
}

// IsReady checks if Redis is connected
func (r *RedisClient) IsReady() bool {
	return r.isReady
}

// Wrapper methods (optional, for convenience)
func (r *RedisClient) GetValue(ctx context.Context, key string) (string, error) {
	if !r.isReady {
		return "", errors.New("Redis is not connected")
	}
	return r.Get(ctx, key).Result() // Call embedded Get
}

func (r *RedisClient) SetValue(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	if !r.isReady {
		return errors.New("Redis is not connected")
	}
	return r.Set(ctx, key, value, expiration).Err()
}

func (r *RedisClient) DeleteKey(ctx context.Context, key string) error {
	if !r.isReady {
		return errors.New("Redis is not connected")
	}
	return r.Del(ctx, key).Err()
}
