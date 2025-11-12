package bootstrap

import (
	"github.com/anshu4sharma/fraud_payment_detector/internal/handlers"
	"github.com/anshu4sharma/fraud_payment_detector/internal/repository"
	"github.com/anshu4sharma/fraud_payment_detector/internal/services"
	"github.com/anshu4sharma/fraud_payment_detector/pkg/redis"
	"github.com/anshu4sharma/fraud_payment_detector/pkg/utils"
)

func InitializeApp(redisClient *redis.RedisClient, logger *utils.Logger) (*handlers.Handlers, error) {
	paymentRepo := repository.NewRedisPaymentRepository(redisClient, logger)

	paymentService := services.NewPaymentService(paymentRepo, logger)

	handlers := handlers.NewHandlers(
		paymentService,
		logger,
	)

	return handlers, nil
}
