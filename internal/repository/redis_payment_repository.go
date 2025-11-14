package repository

import (
	"fmt"

	"github.com/anshu4sharma/fraud_payment_detector/internal/structs"
	"github.com/anshu4sharma/fraud_payment_detector/pkg/redis"
	"github.com/anshu4sharma/fraud_payment_detector/pkg/utils"
	"github.com/gofiber/fiber/v2"
)

type PaymentRepository interface {
	InsertPayment(ctx *fiber.Ctx, payment structs.PaymentReq, strPay []byte) (string, error)
}

type RedisPaymentRepository struct {
	client *redis.RedisClient
	logger *utils.Logger
}

func NewRedisPaymentRepository(client *redis.RedisClient, logger *utils.Logger) PaymentRepository {
	return &RedisPaymentRepository{client: client, logger: logger}
}

func (r *RedisPaymentRepository) InsertPayment(ctx *fiber.Ctx, payment structs.PaymentReq, strPay []byte) (string, error) {
	paymentKey := fmt.Sprintf("payments-%s", payment.UserId)

	err := r.client.LPush(ctx.Context(), paymentKey, strPay).Err()

	if err != nil {
		r.logger.Errorf("redis list push failed %s", err.Error())
		return "", fiber.ErrInternalServerError
	}
	if err := r.client.LTrim(ctx.Context(), paymentKey, 0, 9).Err(); err != nil {
		r.logger.Errorf("redis LTRIM failed: %v", err)
		return "", fiber.ErrInternalServerError
	}
	return payment.ID, nil
}
