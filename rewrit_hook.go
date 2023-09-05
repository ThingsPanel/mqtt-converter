package main

import (
	"log"
	"regexp"
	"strings"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/spf13/viper"
)

func mqttOptions() *mqtt.ClientOptions {
	opts := mqtt.NewClientOptions().AddBroker("tcp://" + viper.GetString("mqtt_client.broker"))
	opts.SetKeepAlive(60 * time.Second)
	opts.SetPingTimeout(10 * time.Second)
	return opts
}

// ConnectPacket 连接mqtt服务器
func ConnectPacket(c *MqttBrokerClient) mqtt.Client {
	// 创建mqtt客户端
	var client mqtt.Client
	opts := mqttOptions()
	opts.SetClientID(c.ClientID)
	opts.SetUsername(c.Username)
	opts.SetPassword(c.Password)
	//连接转换规则编写...

	// 连接mqtt服务器
	client = mqtt.NewClient(opts)
	return client
}

// PublishPacket 发布消息
func PublishPacket(client mqtt.Client, p *packets.PublishPacket, brokerClient MqttBrokerClient) {
	// 发布转换规则编写...
	var topic string = p.TopicName
	var payload []byte = p.Payload
	for _, v := range TopicPubRewriting {
		from := strings.Replace(v.From, "{username}", brokerClient.Username, -1)
		to := v.To
		pattern := strings.Replace(from, "+", "([^/]+)", -1)
		re := regexp.MustCompile(pattern)
		if re.MatchString(topic) {
			topic = re.ReplaceAllString(topic, to)
			payload = PubPayloadRewriting(v.From, p.Payload)
			break
		}
	}
	// 发布消息
	log.Println("发布平台主题：", topic)
	token := client.Publish(topic, p.Qos, false, payload)
	token.Wait()
}

// SubscribePacket 订阅消息
func SubscribePacket(client mqtt.Client, p *packets.SubscribePacket, brokerClient MqttBrokerClient) {
	var topicList = make([]string, len(p.Topics))
	var topicFrom string = p.Topics[0]
	copy(topicList, p.Topics)
	// 订阅转换规则编写...
	for i, topic := range topicList {
		for _, v := range TopicSubRewriting {
			from := strings.Replace(v.From, "{username}", brokerClient.Username, -1)
			to := v.To
			to = strings.Replace(to, "{username}", brokerClient.Username, -1)
			pattern := strings.Replace(from, "+", "([^/]+)", -1)
			re := regexp.MustCompile(pattern)
			if re.MatchString(topic) {
				topicFrom = v.From
				topicList[i] = re.ReplaceAllString(topic, to)
				break
			}
		}
	}
	// 订阅消息
	log.Println("订阅平台主题：", topicList[0])
	token := client.Subscribe(topicList[0], p.Qoss[0], func(c mqtt.Client, d mqtt.Message) {
		log.Println("接收来自平台的消息: ", d.Topic(), "Message: ", string(d.Payload()))
		var messageId uint16 = d.MessageID()
		// 转换消息
		payload := SubPayloadRewriting(topicFrom, d.Payload())
		PublishMessageToTopic(p.Topics[0], payload, p.Qoss[0], messageId)

	})
	token.Wait()
}

// 分发消息
func PublishMessageToTopic(topic string, message []byte, Qos byte, MessageID uint16) {
	// 获取主题的订阅者列表
	subscribers, ok := TopicSubscribers[topic]
	if !ok {
		// 没有订阅者，直接返回
		return
	}

	// 创建一个新的PUBLISH数据包
	publishPacket := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	publishPacket.TopicName = topic
	publishPacket.Payload = message
	// 设置其他必要的PUBLISH包字段，如QoS等...
	log.Println(Qos)
	publishPacket.Qos = Qos
	publishPacket.MessageID = MessageID
	for _, subscriberConn := range subscribers {
		// 将消息发送给每个订阅者
		publishPacket.Write(subscriberConn)
	}
}
