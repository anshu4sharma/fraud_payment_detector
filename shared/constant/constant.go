package constant

import "time"

var (
	FraudThreshold   float64
	RetryLimit       int
	MaxPaymentAmount uint32
	DefaultTimeout   time.Duration
	PaymentTopic     string
)

func Init() {
	FraudThreshold = 0.8
	RetryLimit = 5
	MaxPaymentAmount = 100000
	DefaultTimeout = 30 * time.Second
	PaymentTopic = "payments"
}
