package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/anshu4sharma/fraud_payment_detector/shared/config"
	"github.com/anshu4sharma/fraud_payment_detector/shared/constant"
	"github.com/anshu4sharma/fraud_payment_detector/shared/lib/pkg/kafka"
	"github.com/anshu4sharma/fraud_payment_detector/shared/lib/pkg/redis"
	"github.com/anshu4sharma/fraud_payment_detector/shared/lib/pkg/utils"
	ruleengine "github.com/anshu4sharma/services/fraud_detection/rule_engine"
	"go.uber.org/zap"
)

func main() {
	constant.Init()
	cfg := config.Load()

	// logger expects env ("prod" or anything else)
	logger := utils.NewLogger(cfg.Env,"fraud_detection")
	defer logger.Sync()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Redis
	redisClient := redis.NewRedisClient(cfg.RedisURL, logger)
	if redisClient == nil {
		logger.Error("redis client initialization failed")
		os.Exit(1)
	}

	if err := redisClient.Connect(constant.RetryLimit, constant.DefaultTimeout); err != nil {
		logger.Error(
			"failed to connect to redis",
			zap.Error(err),
		)
		os.Exit(1)
	}
	defer redisClient.Close()

	// Kafka
	kafkaClient := kafka.NewKafkaClient(logger)

	if err := kafkaClient.ConnectProducer(cfg.KafkaBrokers); err != nil {
		logger.Error(
			"failed to connect kafka producer",
			zap.String("brokers", cfg.KafkaBrokers),
			zap.Error(err),
		)
		os.Exit(1)
	}

	if err := kafkaClient.ConnectConsumer(
		cfg.KafkaBrokers,
		cfg.KafkaGroupID,
		[]string{constant.PaymentTopic},
	); err != nil {
		logger.Error(
			"failed to connect kafka consumer",
			zap.String("brokers", cfg.KafkaBrokers),
			zap.String("group_id", cfg.KafkaGroupID),
			zap.String("topic", constant.PaymentTopic),
			zap.Error(err),
		)
		os.Exit(1)
	}
	defer kafkaClient.Close()

	// Rule engine
	go ruleengine.RunRuleEngine(ctx, logger, kafkaClient, redisClient)

	// Graceful shutdown
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)

	<-shutdown

	logger.Info("shutdown signal received, initiating graceful shutdown")

	cancel()

	logger.Info("shutdown complete")
}
