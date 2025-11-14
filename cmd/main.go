package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/gofiber/fiber/v2/middleware/recover"

	"github.com/anshu4sharma/fraud_payment_detector/internal/bootstrap"
	"github.com/anshu4sharma/fraud_payment_detector/internal/config"
	"github.com/anshu4sharma/fraud_payment_detector/internal/constant"
	"github.com/anshu4sharma/fraud_payment_detector/internal/router"
	"github.com/anshu4sharma/fraud_payment_detector/pkg/kafka"
	"github.com/anshu4sharma/fraud_payment_detector/pkg/redis"
	"github.com/anshu4sharma/fraud_payment_detector/pkg/utils"
	"github.com/gofiber/fiber/v2"
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
	err = kafkaClient.ConnectProducer(cfg.KafkaBrokers, constant.RetryLimit, constant.DefaultTimeout)
	if err != nil {
		logger.Errorf("Failed to connect Kafka producer: %v", err)
		os.Exit(1)
	}
	defer kafkaClient.Close()

	// Optional: Connect consumer if you want to consume messages
	err = kafkaClient.ConnectConsumer(cfg.KafkaBrokers, cfg.KafkaGroupID, []string{constant.PaymentTopic}, constant.RetryLimit, constant.DefaultTimeout)
	if err != nil {
		logger.Errorf("Failed to connect Kafka consumer: %v", err)
		os.Exit(1)
	}

	app := fiber.New(fiber.Config{
		AppName:   "FraudPaymentDetector v1.0",
		BodyLimit: 10 * 1024,
	})
	app.Use(recover.New())

	handlers, err := bootstrap.InitializeApp(redisClient, logger ,kafkaClient)
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
