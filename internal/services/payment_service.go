package services

import (
	"github.com/anshu4sharma/fraud_payment_detector/internal/repository"
	"github.com/anshu4sharma/fraud_payment_detector/pkg/utils"
	"github.com/gofiber/fiber/v2"
)

type PaymentService struct {
	repo   repository.PaymentRepository
	logger *utils.Logger
}

func NewPaymentService(repo repository.PaymentRepository, logger *utils.Logger) *PaymentService {
	return &PaymentService{repo: repo, logger: logger}
}

func (s *PaymentService) InsertPayment(ctx *fiber.Ctx, id string) (string, error) {
	return s.repo.InsertPayment(ctx, id)
}
