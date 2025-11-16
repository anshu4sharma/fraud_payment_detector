package utils

import (
	"encoding/json"

	"github.com/anshu4sharma/fraud_payment_detector/shared/structs"
)

func UnmarshalJSONToMap(data []byte) structs.PaymentStruct {
	var result structs.PaymentStruct
	if err := json.Unmarshal(data, &result); err != nil {
		return structs.PaymentStruct{}
	}
	return result
}
