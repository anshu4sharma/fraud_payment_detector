package bootstrap

import (
	"github.com/anshu4sharma/fraud_payment_detector/shared/lib/pkg/kafka"
	"github.com/anshu4sharma/fraud_payment_detector/shared/lib/pkg/redis"
	"github.com/anshu4sharma/fraud_payment_detector/shared/lib/pkg/utils"
	"github.com/anshu4sharma/services/payment_service/internal/handlers"
	"github.com/anshu4sharma/services/payment_service/internal/repository"
	"github.com/anshu4sharma/services/payment_service/internal/services"
)

func InitializeApp(redisClient *redis.RedisClient, logger *utils.Logger, kafkaClient *kafka.KafkaClient) (*handlers.Handlers, error) {
	paymentRepo := repository.NewRedisPaymentRepository(redisClient, logger)

	paymentService := services.NewPaymentService(paymentRepo, logger, kafkaClient)

	handlers := handlers.NewHandlers(
		paymentService,
		logger,
	)

	return handlers, nil
}
