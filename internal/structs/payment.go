package structs

type PaymentStruct struct {
	ID        string `json:"id" validate:"required"`
	UserId    string `json:"userid" validate:"required"`
	Amount    uint32 `json:"amount" default:"0" validate:"min=0"`
	TimeStamp string `json:"time_stamp"`
	Location  string `json:"location"`
}

type PaymentReq struct {
	ID       string `json:"id" validate:"required"`
	UserId   string `json:"userid" validate:"required"`
	Amount   uint32 `json:"amount" default:"0" validate:"min=0"`
	Location string `json:"location"`
}
