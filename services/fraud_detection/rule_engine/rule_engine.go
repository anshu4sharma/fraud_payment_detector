package ruleengine

import (
	"context"
	"time"

	"github.com/anshu4sharma/fraud_payment_detector/shared/lib/pkg/kafka"
	"github.com/anshu4sharma/fraud_payment_detector/shared/lib/pkg/redis"
	"github.com/anshu4sharma/fraud_payment_detector/shared/lib/pkg/utils"
	"github.com/anshu4sharma/fraud_payment_detector/shared/structs"
	"go.uber.org/zap"
)

type RuleResult struct {
	Name    string
	Passed  bool
	Message string
}

func RunRuleEngine(
	ctx context.Context,
	logger *utils.Logger,
	kafkaClient *kafka.KafkaClient,
	redisClient *redis.RedisClient,
) {
	logger.Info("rule engine initialized and listening for messages")

	_ = kafkaClient.Consume(ctx, func(key string, value []byte) {
		select {
		case <-ctx.Done():
			return
		default:
			payment := utils.UnmarshalJSONToMap(value)

			logger.Debug(
				"payment received",
				zap.String("payment_id", payment.ID),
				zap.String("user_id", payment.UserId),
			)

			ruleReport := evaluateRules(ctx, payment, redisClient)

			for _, r := range ruleReport {
				if !r.Passed {
					logger.Warn(
						"rule violation detected",
						zap.String("rule", r.Name),
						zap.String("payment_id", payment.ID),
						zap.String("message", r.Message),
					)
				} else {
					logger.Debug(
						"rule passed",
						zap.String("rule", r.Name),
						zap.String("payment_id", payment.ID),
					)
				}
			}

			if hasViolation(ruleReport) {
				logger.Warn(
					"fraud detected",
					zap.String("payment_id", payment.ID),
					zap.String("user_id", payment.UserId),
				)
			} else {
				logger.Info(
					"payment cleared",
					zap.String("payment_id", payment.ID),
				)
			}
		}
	})

	logger.Info("rule engine stopped")
}

func evaluateRules(
	ctx context.Context,
	payment structs.PaymentStruct,
	redisClient *redis.RedisClient,
) []RuleResult {
	results := []RuleResult{
		checkLargeAmount(payment),
		checkHighFrequency(ctx, payment, redisClient),
		checkLocationMismatch(ctx, payment, redisClient),
	}

	return results
}

func hasViolation(results []RuleResult) bool {
	for _, r := range results {
		if !r.Passed {
			return true
		}
	}
	return false
}

func checkLargeAmount(p structs.PaymentStruct) RuleResult {
	if p.Amount > 10000 {
		return RuleResult{
			Name:    "LargeAmount",
			Passed:  false,
			Message: "amount exceeds threshold",
		}
	}
	return RuleResult{Name: "LargeAmount", Passed: true}
}

func checkHighFrequency(
	ctx context.Context,
	p structs.PaymentStruct,
	redisClient *redis.RedisClient,
) RuleResult {
	freqKey := "user:" + p.UserId + ":freq"

	count, err := redisClient.Client.Incr(ctx, freqKey).Result()
	if err != nil {
		return RuleResult{
			Name:    "HighFrequency",
			Passed:  false,
			Message: "redis INCR failed",
		}
	}

	if count == 1 {
		if err := redisClient.Client.Expire(ctx, freqKey, 30*time.Second).Err(); err != nil {
			return RuleResult{
				Name:    "HighFrequency",
				Passed:  false,
				Message: "failed to set TTL for frequency counter",
			}
		}
	}

	if count >= 5 {
		return RuleResult{
			Name:    "HighFrequency",
			Passed:  false,
			Message: "high transaction frequency detected",
		}
	}

	return RuleResult{Name: "HighFrequency", Passed: true}
}

func checkLocationMismatch(
	ctx context.Context,
	p structs.PaymentStruct,
	redisClient *redis.RedisClient,
) RuleResult {
	locationKey := "user:" + p.UserId + ":last_location"

	lastLocation, err := redisClient.Client.Get(ctx, locationKey).Result()
	if err == nil && lastLocation != "" && lastLocation != p.Location {
		return RuleResult{
			Name:    "LocationMismatch",
			Passed:  false,
			Message: "location mismatch detected",
		}
	}

	if err := redisClient.Client.Set(ctx, locationKey, p.Location, 0).Err(); err != nil {
		return RuleResult{
			Name:    "LocationMismatch",
			Passed:  false,
			Message: "failed to update user location",
		}
	}

	return RuleResult{Name: "LocationMismatch", Passed: true}
}
