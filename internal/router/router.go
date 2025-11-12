package router

import (
	"github.com/anshu4sharma/fraud_payment_detector/internal/handlers"
	"github.com/anshu4sharma/fraud_payment_detector/internal/structs"
	"github.com/anshu4sharma/fraud_payment_detector/internal/validation"
	"github.com/gofiber/fiber/v2"
)

func SetupRoutes(app *fiber.App, h *handlers.Handlers) {
	api := app.Group("/api/v1")
	payment := api.Group("/payment")

	payment.Post("/",
		validation.ValidateBody[structs.PaymentReq](validation.Validator),
		h.PaymentHandler.InsertPayment,
	)
}
