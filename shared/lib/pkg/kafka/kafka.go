package kafka

import (
	"context"
	"errors"
	"time"

	"github.com/anshu4sharma/fraud_payment_detector/shared/lib/pkg/utils"
	"github.com/confluentinc/confluent-kafka-go/kafka"
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
		return err
	}

	k.producer = p
	k.ready = true
	k.logger.Infof("Kafka producer initialized")

	go func() {
		for e := range p.Events() {
			if msg, ok := e.(*kafka.Message); ok && msg.TopicPartition.Error != nil {
				k.logger.Errorf("Produce error: %v", msg.TopicPartition.Error)
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
		return err
	}

	if err := c.SubscribeTopics(topics, nil); err != nil {
		return err
	}

	k.consumer = c
	k.ready = true
	k.logger.Infof("Kafka consumer subscribed to %v", topics)
	return nil
}

func (k *KafkaClient) Produce(topic, key string, value []byte) error {
	if !k.ready || k.producer == nil {
		return errors.New("producer not ready")
	}

	return k.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(key),
		Value:          value,
	}, nil)
}

func (k *KafkaClient) Consume(ctx context.Context, handler func(string, []byte)) error {
	if !k.ready || k.consumer == nil {
		return errors.New("consumer not ready")
	}

	for {
		select {
		case <-ctx.Done():
			k.logger.Infof("Consumer context canceled")
			return nil
		default:
			msg, err := k.consumer.ReadMessage(200 * time.Millisecond)
			if err != nil {
				continue
			}
			handler(string(msg.Key), msg.Value)
			_, _ = k.consumer.CommitMessage(msg)
		}
	}
}

func (k *KafkaClient) Close() {
	if k.producer != nil {
		k.producer.Flush(5000)
		k.producer.Close()
		k.logger.Infof("Producer closed")
	}
	if k.consumer != nil {
		k.consumer.Close()
		k.logger.Infof("Consumer closed")
	}
	k.ready = false
}

func (k *KafkaClient) IsReady() bool {
	return k.ready
}
