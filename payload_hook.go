package main

import (
	"encoding/json"
	"fmt"
)

// PubPayloadRewriting 发布消息转换规则
func PubPayloadRewriting(topic_from string, payload []byte) []byte {
	// 发布消息转换规则编写...
	fmt.Println("pub转换规则-原消息", topic_from, " ", string(payload))
	switch topic_from {
	case "/sys/+/+/thing/event/consumption/post":
		// 将payload为json字符串，将其解析为特定的结构体
		var payloadStruct struct {
			Params struct {
				Value map[string]interface{} `json:"value"`
			} `json:"params"`
		}
		err := json.Unmarshal(payload, &payloadStruct)
		if err != nil {
			fmt.Println(err)
		}
		// 将payloadStruct.Params.Value转换为json字符串
		valueStr, err := json.Marshal(payloadStruct.Params.Value)
		if err != nil {
			fmt.Println(err)
		}
		payload = valueStr
	}
	return payload
}

// SubPayloadRewriting 订阅消息转换规则
func SubPayloadRewriting(topic_from string, payload []byte) []byte {
	// 订阅消息转换规则编写...
	fmt.Println("sub转换规则-原消息", topic_from, " ", string(payload))
	switch topic_from {
	case "test":
		payload = []byte("test")
	}
	return payload
}
