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
)

func main() {
	constant.Init()
	cfg := config.Load()

	logger := utils.NewLogger()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	redisClient := redis.NewRedisClient(cfg.RedisURL, logger)
	if err := redisClient.Connect(constant.RetryLimit, constant.DefaultTimeout); err != nil {
		logger.Errorf("Failed to connect to Redis: %v", err)
		os.Exit(1)
	}
	defer redisClient.Close()

	kafkaClient := kafka.NewKafkaClient(logger)
	if err := kafkaClient.ConnectProducer(cfg.KafkaBrokers); err != nil {
		logger.Errorf("Failed to connect Kafka producer: %v", err)
		os.Exit(1)
	}
	defer kafkaClient.Close()

	if err := kafkaClient.ConnectConsumer(cfg.KafkaBrokers, cfg.KafkaGroupID, []string{constant.PaymentTopic}); err != nil {
		logger.Errorf("Failed to connect Kafka consumer: %v", err)
		os.Exit(1)
	}
	defer kafkaClient.Close()

	go ruleengine.RunRuleEngine(ctx, logger, kafkaClient, redisClient)

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)

	<-shutdown
	logger.Infof("Received shutdown signal... initiating graceful shutdown")

	cancel()

	logger.Infof("Shutdown complete")
}
