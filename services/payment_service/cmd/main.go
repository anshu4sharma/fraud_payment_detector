package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/anshu4sharma/fraud_payment_detector/shared/config"
	"github.com/anshu4sharma/fraud_payment_detector/shared/constant"
	"github.com/anshu4sharma/fraud_payment_detector/shared/lib/pkg/kafka"
	"github.com/anshu4sharma/fraud_payment_detector/shared/lib/pkg/redis"
	"github.com/anshu4sharma/fraud_payment_detector/shared/lib/pkg/utils"
	"github.com/anshu4sharma/services/payment_service/internal/bootstrap"
	"github.com/anshu4sharma/services/payment_service/internal/router"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/recover"
	"go.uber.org/zap"
)

func main() {
	constant.Init()
	cfg := config.Load()

	logger := utils.NewLogger(cfg.Env, "payment_service")
	defer func() {
		logger.Info("syncing logger before exit")
		logger.Sync()
	}()

	redisClient := redis.NewRedisClient(cfg.RedisURL, logger)
	if redisClient == nil {
		logger.Error("redis client initialization failed")
		os.Exit(1)
	}

	if err := redisClient.Connect(constant.RetryLimit, constant.DefaultTimeout); err != nil {
		logger.Error("failed to connect to redis",
			zap.String("redis_url", cfg.RedisURL),
			zap.Error(err),
		)
		os.Exit(1)
	}
	defer func() {
		logger.Info("closing redis connection")
		redisClient.Close()
	}()

	kafkaClient := kafka.NewKafkaClient(logger)
	if err := kafkaClient.ConnectProducer(cfg.KafkaBrokers); err != nil {
		logger.Error("failed to connect kafka producer",
			zap.String("brokers", cfg.KafkaBrokers),
			zap.Error(err),
		)
		os.Exit(1)
	}
	defer func() {
		logger.Info("closing kafka producer")
		kafkaClient.Close()
	}()

	app := fiber.New(fiber.Config{
		AppName:               "FraudPaymentDetector v1.0",
		BodyLimit:             10 * 1024,
		DisableStartupMessage: true,
	})
	app.Use(recover.New())

	handlers, err := bootstrap.InitializeApp(redisClient, logger, kafkaClient)
	if err != nil {
		logger.Error("failed to initialize application",
			zap.Error(err),
		)
		os.Exit(1)
	}

	router.SetupRoutes(app, handlers)

	serverErrors := make(chan error, 1)

	go func() {
		logger.Info("http server starting",
			zap.String("port", cfg.ServerPort),
		)
		serverErrors <- app.Listen(cfg.ServerPort)
	}()

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-serverErrors:
		logger.Error("server failed to start",
			zap.Error(err),
		)
		os.Exit(1)

	case sig := <-shutdown:
		logger.Info("shutdown signal received",
			zap.String("signal", sig.String()),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if err := app.ShutdownWithContext(ctx); err != nil {
			logger.Error("error during graceful shutdown",
				zap.Error(err),
			)

			if err := app.Shutdown(); err != nil {
				logger.Error("error during force shutdown",
					zap.Error(err),
				)
			}
		}

		logger.Info("server stopped successfully")
	}
}
