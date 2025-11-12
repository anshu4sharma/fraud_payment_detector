package repository

import (
	"fmt"
	"log"

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
	paymentKey := fmt.Sprintf("payments-%s", id)
	err := r.client.LPush(ctx.Context(), paymentKey).Err()
	if err != nil {
		log.Fatal(err)
	}
	return id, nil
}
