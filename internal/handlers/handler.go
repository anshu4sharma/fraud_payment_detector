package handlers

import (
	"github.com/anshu4sharma/fraud_payment_detector/internal/services"
	"github.com/anshu4sharma/fraud_payment_detector/pkg/utils"
)

type Handlers struct {
	PaymentHandler *PaymentHandler
}

func NewHandlers(paymentService *services.PaymentService, logger *utils.Logger) *Handlers {
	return &Handlers{
		PaymentHandler: NewPaymentHandler(paymentService, logger),
	}
}
