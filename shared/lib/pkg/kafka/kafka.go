package kafka

import (
	"context"
	"errors"
	"time"

	"github.com/anshu4sharma/fraud_payment_detector/shared/lib/pkg/utils"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"go.uber.org/zap"
)

type KafkaClient struct {
	producer *kafka.Producer
	consumer *kafka.Consumer
	logger   *utils.Logger
	ready    bool
}

func NewKafkaClient(logger *utils.Logger) *KafkaClient {
	return &KafkaClient{logger: logger}
}

func (k *KafkaClient) ConnectProducer(brokers string) error {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":  brokers,
		"enable.idempotence": true,
		"acks":               "all",
		"compression.type":   "lz4",
		"linger.ms":          5,
		"batch.num.messages": 10000,
	})
	if err != nil {
		k.logger.Error(
			"failed to create kafka producer",
			zap.String("brokers", brokers),
			zap.Error(err),
		)
		return err
	}

	k.producer = p
	k.ready = true

	k.logger.Info(
		"kafka producer initialized",
		zap.String("brokers", brokers),
	)

	go func() {
		for e := range p.Events() {
			if msg, ok := e.(*kafka.Message); ok && msg.TopicPartition.Error != nil {
				k.logger.Error(
					"kafka produce error",
					zap.String("topic", *msg.TopicPartition.Topic),
					zap.Error(msg.TopicPartition.Error),
				)
			}
		}
	}()

	return nil
}

func (k *KafkaClient) ConnectConsumer(brokers, groupID string, topics []string) error {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":        brokers,
		"group.id":                 groupID,
		"auto.offset.reset":        "earliest",
		"enable.auto.commit":       false,
		"fetch.min.bytes":          1_000_000,
		"fetch.wait.max.ms":        25,
		"go.events.channel.enable": false,
	})
	if err != nil {
		k.logger.Error(
			"failed to create kafka consumer",
			zap.String("brokers", brokers),
			zap.String("group_id", groupID),
			zap.Error(err),
		)
		return err
	}

	if err := c.SubscribeTopics(topics, nil); err != nil {
		k.logger.Error(
			"failed to subscribe to kafka topics",
			zap.Strings("topics", topics),
			zap.Error(err),
		)
		return err
	}

	k.consumer = c
	k.ready = true

	k.logger.Info(
		"kafka consumer subscribed",
		zap.String("group_id", groupID),
		zap.Strings("topics", topics),
	)

	return nil
}

func (k *KafkaClient) Produce(topic, key string, value []byte) error {
	if !k.ready || k.producer == nil {
		return errors.New("producer not ready")
	}

	err := k.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Key:   []byte(key),
		Value: value,
	}, nil)

	if err != nil {
		k.logger.Error(
			"failed to produce kafka message",
			zap.String("topic", topic),
			zap.String("key", key),
			zap.Error(err),
		)
	}

	return err
}

func (k *KafkaClient) Consume(ctx context.Context, handler func(string, []byte)) error {
	if !k.ready || k.consumer == nil {
		return errors.New("consumer not ready")
	}

	for {
		select {
		case <-ctx.Done():
			k.logger.Info("kafka consumer context canceled")
			return nil
		default:
			msg, err := k.consumer.ReadMessage(200 * time.Millisecond)
			if err != nil {
				continue
			}

			handler(string(msg.Key), msg.Value)

			if _, err := k.consumer.CommitMessage(msg); err != nil {
				k.logger.Error(
					"failed to commit kafka message",
					zap.String("topic", *msg.TopicPartition.Topic),
					zap.Int32("partition", msg.TopicPartition.Partition),
					zap.Int64("offset", int64(msg.TopicPartition.Offset)),
					zap.Error(err),
				)
			}
		}
	}
}

func (k *KafkaClient) Close() {
	if k.producer != nil {
		k.producer.Flush(5000)
		k.producer.Close()
		k.logger.Info("kafka producer closed")
	}

	if k.consumer != nil {
		k.consumer.Close()
		k.logger.Info("kafka consumer closed")
	}

	k.ready = false
}

func (k *KafkaClient) IsReady() bool {
	return k.ready
}
