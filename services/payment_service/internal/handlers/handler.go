package handlers

import (
	"github.com/anshu4sharma/fraud_payment_detector/shared/lib/pkg/utils"
	"github.com/anshu4sharma/services/payment_service/internal/services"
)

type Handlers struct {
	PaymentHandler *PaymentHandler
}

func NewHandlers(paymentService *services.PaymentService, logger *utils.Logger) *Handlers {
	return &Handlers{
		PaymentHandler: NewPaymentHandler(paymentService, logger),
	}
}
