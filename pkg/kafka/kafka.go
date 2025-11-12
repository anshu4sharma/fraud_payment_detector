package kafka

import (
	"context"
	"errors"
	"time"

	"github.com/anshu4sharma/fraud_payment_detector/pkg/utils"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type KafkaClient struct {
	producer *kafka.Producer
	consumer *kafka.Consumer
	logger   *utils.Logger
	isReady  bool
}

func NewKafkaClient(logger *utils.Logger) *KafkaClient {
	return &KafkaClient{
		logger: logger,
	}
}

func (k *KafkaClient) ConnectProducer(brokers string, maxRetries int, retryDelay time.Duration) error {
	var err error
	for i := 0; i < maxRetries; i++ {
		p, e := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": brokers})
		if e == nil {
			k.producer = p
			k.isReady = true
			k.logger.Infof("Kafka producer connected")
			return nil
		}
		err = e
		k.logger.Errorf("Failed to connect Kafka producer (attempt %d): %v", i+1, err)
		time.Sleep(retryDelay)
		retryDelay *= 2
	}
	return errors.New("max retries reached, could not connect Kafka producer")
}

func (k *KafkaClient) ConnectConsumer(brokers, groupID string, topics []string, maxRetries int, retryDelay time.Duration) error {
	var err error
	for i := 0; i < maxRetries; i++ {
		c, e := kafka.NewConsumer(&kafka.ConfigMap{
			"bootstrap.servers":        brokers,
			"group.id":                 groupID,
			"auto.offset.reset":        "earliest",
			"enable.auto.commit":       false,
			"go.events.channel.enable": true,
		})
		if e == nil {
			if e := c.SubscribeTopics(topics, nil); e != nil {
				err = e
				k.logger.Errorf("Error subscribing topics: %v", err)
				continue
			}
			k.consumer = c
			k.isReady = true
			k.logger.Infof("Kafka consumer connected and subscribed to topics: %v", topics)
			return nil
		}
		err = e
		k.logger.Errorf("Failed to connect Kafka consumer (attempt %d): %v", i+1, err)
		time.Sleep(retryDelay)
		retryDelay *= 2
	}
	return errors.New("max retries reached, could not connect Kafka consumer")
}

func (k *KafkaClient) Close() {
	if k.producer != nil {
		k.producer.Flush(5000)
		k.producer.Close()
		k.logger.Infof("Kafka producer closed")
	}
	if k.consumer != nil {
		k.consumer.Close()
		k.logger.Infof("Kafka consumer closed")
	}
	k.isReady = false
}

func (k *KafkaClient) IsReady() bool {
	return k.isReady
}

func (k *KafkaClient) Produce(topic string, key string, value []byte) error {
	if !k.isReady || k.producer == nil {
		return errors.New("Kafka producer not ready")
	}

	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(key),
		Value:          value,
	}

	return k.producer.Produce(msg, nil)
}

func (k *KafkaClient) Consume(ctx context.Context, handler func(key string, value []byte)) error {
	if !k.isReady || k.consumer == nil {
		return errors.New("Kafka consumer not ready")
	}

	for {
		select {
		case <-ctx.Done():
			k.logger.Infof("Kafka consumer stopping due to context cancellation")
			return nil
		case ev := <-k.consumer.Events():
			switch e := ev.(type) {
			case *kafka.Message:
				handler(string(e.Key), e.Value)
				k.consumer.CommitMessage(e) 
			case kafka.Error:
				k.logger.Errorf("Kafka error: %v", e)
			default:
			}
		}
	}
}
