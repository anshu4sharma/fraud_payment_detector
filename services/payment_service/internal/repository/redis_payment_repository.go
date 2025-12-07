package repository

import (
	"fmt"

	"github.com/anshu4sharma/fraud_payment_detector/shared/lib/pkg/redis"
	"github.com/anshu4sharma/fraud_payment_detector/shared/lib/pkg/utils"
	"github.com/anshu4sharma/fraud_payment_detector/shared/structs"
	"github.com/gofiber/fiber/v2"
	"go.uber.org/zap"
)

type PaymentRepository interface {
	InsertPayment(ctx *fiber.Ctx, payment structs.PaymentReq, payload []byte) (string, error)
}

type RedisPaymentRepository struct {
	client *redis.RedisClient
	logger *utils.Logger
}

func NewRedisPaymentRepository(
	client *redis.RedisClient,
	logger *utils.Logger,
) PaymentRepository {
	return &RedisPaymentRepository{
		client: client,
		logger: logger,
	}
}

func (r *RedisPaymentRepository) InsertPayment(
	ctx *fiber.Ctx,
	payment structs.PaymentReq,
	payload []byte,
) (string, error) {

	paymentKey := fmt.Sprintf("payments-%s", payment.UserId)

	if err := r.client.LPush(ctx.Context(), paymentKey, payload).Err(); err != nil {
		r.logger.Error(
			"redis LPUSH failed",
			zap.String("key", paymentKey),
			zap.String("payment_id", payment.ID),
			zap.Error(err),
		)
		return "", fiber.ErrInternalServerError
	}

	if err := r.client.LTrim(ctx.Context(), paymentKey, 0, 9).Err(); err != nil {
		r.logger.Error(
			"redis LTRIM failed",
			zap.String("key", paymentKey),
			zap.String("payment_id", payment.ID),
			zap.Error(err),
		)
		return "", fiber.ErrInternalServerError
	}

	r.logger.Debug(
		"payment stored in redis",
		zap.String("key", paymentKey),
		zap.String("payment_id", payment.ID),
	)

	return payment.ID, nil
}
