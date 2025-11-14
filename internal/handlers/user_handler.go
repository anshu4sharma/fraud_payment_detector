package handlers

import (
	"github.com/anshu4sharma/fraud_payment_detector/internal/services"
	"github.com/anshu4sharma/fraud_payment_detector/internal/structs"
	"github.com/anshu4sharma/fraud_payment_detector/pkg/utils"
	"github.com/gofiber/fiber/v2"
)

type PaymentHandler struct {
	service *services.PaymentService
	logger  *utils.Logger
}

func NewPaymentHandler(service *services.PaymentService, logger *utils.Logger) *PaymentHandler {
	return &PaymentHandler{service: service, logger: logger}
}

func (h *PaymentHandler) InsertPayment(c *fiber.Ctx) error {
	req := c.Locals("validated_body").(structs.PaymentReq)

	_, err := h.service.InsertPayment(c, req)
	if err != nil {
		h.logger.Errorf("Failed to insert payment: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error":   true,
			"message": "Failed to insert payment",
		})
	}

	return c.JSON(fiber.Map{
		"error": false,
		"data":  req,
	})
}
