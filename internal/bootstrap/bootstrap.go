package bootstrap

import (
	"github.com/anshu4sharma/fraud_payment_detector/internal/handlers"
	"github.com/anshu4sharma/fraud_payment_detector/internal/repository"
	"github.com/anshu4sharma/fraud_payment_detector/internal/services"
	"github.com/anshu4sharma/fraud_payment_detector/pkg/kafka"
	"github.com/anshu4sharma/fraud_payment_detector/pkg/redis"
	"github.com/anshu4sharma/fraud_payment_detector/pkg/utils"
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
