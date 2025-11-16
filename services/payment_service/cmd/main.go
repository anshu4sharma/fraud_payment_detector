package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/anshu4sharma/fraud_payment_detector/shared/config"
	"github.com/anshu4sharma/fraud_payment_detector/shared/constant"
	"github.com/anshu4sharma/fraud_payment_detector/shared/lib/pkg/kafka"
	"github.com/anshu4sharma/fraud_payment_detector/shared/lib/pkg/redis"
	"github.com/anshu4sharma/fraud_payment_detector/shared/lib/pkg/utils"
	"github.com/anshu4sharma/services/payment_service/internal/bootstrap"
	"github.com/anshu4sharma/services/payment_service/internal/router"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/recover"
)

func main() {
	constant.Init()
	cfg := config.Load()

	logger := utils.NewLogger()

	redisClient := redis.NewRedisClient(cfg.RedisURL, logger)
	err := redisClient.Connect(constant.RetryLimit, constant.DefaultTimeout)
	if err != nil {
		logger.Errorf("Failed to connect to Redis: %v", err)
		os.Exit(1)
	}
	defer redisClient.Close()

	kafkaClient := kafka.NewKafkaClient(logger)
	err = kafkaClient.ConnectProducer(cfg.KafkaBrokers)
	if err != nil {
		logger.Errorf("Failed to connect Kafka producer: %v", err)
		os.Exit(1)
	}
	defer kafkaClient.Close()

	app := fiber.New(fiber.Config{
		AppName:   "FraudPaymentDetector v1.0",
		BodyLimit: 10 * 1024,
	})
	app.Use(recover.New())

	handlers, err := bootstrap.InitializeApp(redisClient, logger, kafkaClient)
	if err != nil {
		logger.Errorf("Failed to initialize app: %v", err)
		os.Exit(1)
	}
	router.SetupRoutes(app, handlers)

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-shutdown
		logger.Infof("Shutting down server...")
		if err := app.Shutdown(); err != nil {
			logger.Errorf("Error shutting down server: %v", err)
		}
	}()

	log.Fatal(app.Listen(cfg.ServerPort))
}
