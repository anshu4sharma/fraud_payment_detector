package services

import (
	"encoding/json"
	"time"

	"github.com/anshu4sharma/fraud_payment_detector/shared/constant"
	"github.com/anshu4sharma/fraud_payment_detector/shared/lib/pkg/kafka"
	"github.com/anshu4sharma/fraud_payment_detector/shared/lib/pkg/utils"
	"github.com/anshu4sharma/fraud_payment_detector/shared/structs"
	"github.com/anshu4sharma/services/payment_service/internal/repository"
	"github.com/gofiber/fiber/v2"
	"go.uber.org/zap"
)

type PaymentService struct {
	repo   repository.PaymentRepository
	logger *utils.Logger
	kafka  *kafka.KafkaClient
}

func NewPaymentService(
	repo repository.PaymentRepository,
	logger *utils.Logger,
	kafkaClient *kafka.KafkaClient,
) *PaymentService {
	return &PaymentService{
		repo:   repo,
		logger: logger,
		kafka:  kafkaClient,
	}
}

func (s *PaymentService) InsertPayment(
	ctx *fiber.Ctx,
	payment structs.PaymentReq,
) (string, error) {

	fullPayment := structs.PaymentStruct{
		ID:        payment.ID,
		UserId:    payment.UserId,
		Amount:    payment.Amount,
		Location:  payment.Location,
		TimeStamp: time.Now().UTC().Format(time.RFC3339),
	}

	payload, err := json.Marshal(fullPayment)
	if err != nil {
		s.logger.Error(
			"failed to marshal payment",
			zap.String("payment_id", payment.ID),
			zap.String("user_id", payment.UserId),
			zap.Error(err),
		)
		return "", err
	}

	if err := s.kafka.Produce(
		constant.PaymentTopic,
		payment.UserId,
		payload,
	); err != nil {
		s.logger.Error(
			"failed to publish payment event",
			zap.String("topic", constant.PaymentTopic),
			zap.String("payment_id", payment.ID),
			zap.String("user_id", payment.UserId),
			zap.Error(err),
		)
		return "", err
	}

	s.logger.Info(
		"payment event published",
		zap.String("payment_id", payment.ID),
		zap.String("user_id", payment.UserId),
		zap.Int64("amount", int64(payment.Amount)),
	)

	return s.repo.InsertPayment(ctx, payment, payload)
}
