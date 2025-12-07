package handlers

import (
	"github.com/anshu4sharma/fraud_payment_detector/shared/lib/pkg/utils"
	"github.com/anshu4sharma/fraud_payment_detector/shared/structs"
	"github.com/anshu4sharma/services/payment_service/internal/services"
	"github.com/gofiber/fiber/v2"
	"go.uber.org/zap"
)

type PaymentHandler struct {
	service *services.PaymentService
	logger  *utils.Logger
}

func NewPaymentHandler(service *services.PaymentService, logger *utils.Logger) *PaymentHandler {
	return &PaymentHandler{
		service: service,
		logger:  logger,
	}
}

func (h *PaymentHandler) InsertPayment(c *fiber.Ctx) error {
	req := c.Locals("validated_body").(structs.PaymentReq)

	_, err := h.service.InsertPayment(c, req)
	if err != nil {
		h.logger.Error(
			"failed to insert payment",
			zap.String("payment_id", req.ID),
			zap.String("user_id", req.UserId),
			zap.Error(err),
		)

		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error":   true,
			"message": "Failed to insert payment",
		})
	}

	h.logger.Info(
		"payment inserted successfully",
		zap.String("payment_id", req.ID),
		zap.String("user_id", req.UserId),
	)

	return c.JSON(fiber.Map{
		"error": false,
		"data":  req,
	})
}
