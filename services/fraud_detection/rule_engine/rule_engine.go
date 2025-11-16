package ruleengine

import (
	"context"
	"fmt"
	"time"

	"github.com/anshu4sharma/fraud_payment_detector/shared/lib/pkg/kafka"
	"github.com/anshu4sharma/fraud_payment_detector/shared/lib/pkg/redis"
	"github.com/anshu4sharma/fraud_payment_detector/shared/lib/pkg/utils"
	"github.com/anshu4sharma/fraud_payment_detector/shared/structs"
)

type RuleResult struct {
	Name    string
	Passed  bool
	Message string
}

func RunRuleEngine(ctx context.Context, logger *utils.Logger, kafka *kafka.KafkaClient, redis *redis.RedisClient) {
	logger.Infof("Rule engine initialized and listening for messages")

	kafka.Consume(ctx, func(key string, value []byte) {
		select {
		case <-ctx.Done():
			return
		default:
			payment := utils.UnmarshalJSONToMap(value)
			ruleReport := evaluateRules(ctx, payment, redis, logger)
			// logger.Infof("Received message with key: %s, value: %s", key, string(value))

			for _, r := range ruleReport {
				if !r.Passed {
					logger.Errorf("[Rule Violation] %s: %s", r.Name, r.Message)
				} else {
					logger.Infof("[Rule Passed] %s", r.Name)
				}
			}
			if hasViolation(ruleReport) {
				logger.Warnf("Fraud detected for payment ID: %s", payment.ID)
			} else {
				logger.Infof("No fraud detected for payment ID: %s", payment.ID)
			}

		}
	})

	logger.Infof("Rule engine stopped")
}

func evaluateRules(ctx context.Context, payment structs.PaymentStruct, redis *redis.RedisClient, logger *utils.Logger) []RuleResult {
	var res []RuleResult

	res = append(res, checkLargeAmount(ctx, payment))
	res = append(res, checkHighFrequency(ctx, payment, redis))
	res = append(res, checkLocationMismatch(ctx, payment, redis))

	return res
}

func hasViolation(results []RuleResult) bool {
	for _, r := range results {
		if !r.Passed {
			return true
		}
	}
	return false
}

func checkLargeAmount(ctx context.Context, p structs.PaymentStruct) RuleResult {
	if p.Amount > 10000 {
		return RuleResult{
			Name:    "LargeAmount",
			Passed:  false,
			Message: fmt.Sprintf("Amount %d exceeds threshold", p.Amount),
		}
	}
	return RuleResult{Name: "LargeAmount", Passed: true}
}

func checkHighFrequency(ctx context.Context, p structs.PaymentStruct, redis *redis.RedisClient) RuleResult {
	freqKey := fmt.Sprintf("user:%s:freq", p.UserId)
	count, err := redis.Client.Incr(ctx, freqKey).Result()
	if err != nil {
		return RuleResult{
			Name:    "HighFrequency",
			Passed:  false,
			Message: fmt.Sprintf("Redis INCR failed: %v", err),
		}
	}
	if count == 1 {
		if err := redis.Client.Expire(ctx, freqKey, 30*time.Second).Err(); err != nil {
			return RuleResult{
				Name:    "HighFrequency",
				Passed:  false,
				Message: fmt.Sprintf("TTL assignment failed: %v", err),
			}
		}
	}

	if count >= 5 {
		return RuleResult{
			Name:    "HighFrequency",
			Passed:  false,
			Message: fmt.Sprintf("High frequency detected. Count=%d in 30s window", count),
		}
	}

	return RuleResult{Name: "HighFrequency", Passed: true}
}

func checkLocationMismatch(ctx context.Context, p structs.PaymentStruct, redis *redis.RedisClient) RuleResult {
	locationKey := fmt.Sprintf("user:%s:last_location", p.UserId)
	lastLocation := redis.Client.Get(ctx, locationKey).Val()
	if lastLocation != "" && lastLocation != p.Location {
		return RuleResult{
			Name:    "LocationMismatch",
			Passed:  false,
			Message: fmt.Sprintf("Location mismatch: last %s, current %s", lastLocation, p.Location),
		}
	}

	err := redis.Set(ctx, locationKey, p.Location, 0).Err()
	if err != nil {
		return RuleResult{
			Name:    "LocationMismatch",
			Passed:  false,
			Message: fmt.Sprintf("Error updating location: %v", err),
		}
	}
	return RuleResult{Name: "LocationMismatch", Passed: true}
}
