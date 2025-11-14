package services

import (
	"encoding/json"
	"time"

	"github.com/anshu4sharma/fraud_payment_detector/internal/constant"
	"github.com/anshu4sharma/fraud_payment_detector/internal/repository"
	"github.com/anshu4sharma/fraud_payment_detector/internal/structs"
	"github.com/anshu4sharma/fraud_payment_detector/pkg/kafka"
	"github.com/anshu4sharma/fraud_payment_detector/pkg/utils"
	"github.com/gofiber/fiber/v2"
)

type PaymentService struct {
	repo   repository.PaymentRepository
	logger *utils.Logger
	kafka  *kafka.KafkaClient
}

func NewPaymentService(repo repository.PaymentRepository, logger *utils.Logger, kafkaClient *kafka.KafkaClient) *PaymentService {
	return &PaymentService{repo: repo, logger: logger, kafka: kafkaClient}
}

func (s *PaymentService) InsertPayment(ctx *fiber.Ctx, payment structs.PaymentReq) (string, error) {

	fullPayment := structs.PaymentStruct{
		ID:        payment.ID,
		UserId:    payment.UserId,
		Amount:    payment.Amount,
		Location:  payment.Location,
		TimeStamp: time.Now().UTC().Format(time.RFC3339),
	}
	strPay, errm := json.Marshal(fullPayment)

	if errm != nil {
		s.logger.Errorf("failed to marshal payment: %v", errm)
		return "", errm
	}

	if err := s.kafka.Produce(constant.PaymentTopic, payment.UserId, strPay); err != nil {
		s.logger.Errorf("failed to publish payment event: %v", err)
		return "", errm
	}
	return s.repo.InsertPayment(ctx, payment, strPay)
}
