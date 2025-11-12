package repository

import (
	"github.com/anshu4sharma/fraud_payment_detector/pkg/redis"
	"github.com/anshu4sharma/fraud_payment_detector/pkg/utils"
	"github.com/gofiber/fiber/v2"
)

type PaymentRepository interface {
	InsertPayment(ctx *fiber.Ctx, id string) (string, error)
}

type RedisPaymentRepository struct {
	client *redis.RedisClient
	logger *utils.Logger
}

func NewRedisPaymentRepository(client *redis.RedisClient, logger *utils.Logger) PaymentRepository {
	return &RedisPaymentRepository{client: client, logger: logger}
}

func (r *RedisPaymentRepository) InsertPayment(ctx *fiber.Ctx, id string) (string, error) {
	r.client.Set(ctx.Context(), "hello", "anshu", 0)
	r.logger.Infof("Fetching user with ID %s from Redis", id)
	r.logger.Warnf("Fetching user with ID %s from Redis", id)
	return id, nil
}
