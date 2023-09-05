package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"strings"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/spf13/viper"
)

var RewritFlag bool
var TopicSubscribers = make(map[string][]net.Conn)

type PubTopicRewriting struct {
	From string `mapstructure:"from"`
	To   string `mapstructure:"to"`
}
type SubTopicRewriting struct {
	From string `mapstructure:"from"`
	To   string `mapstructure:"to"`
}

var TopicPubRewriting []PubTopicRewriting
var TopicSubRewriting []SubTopicRewriting

func main() {
	// 日志格式
	log.SetPrefix("[MQTT-Converter] ")
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	viper.SetConfigFile("config.yaml")
	err := viper.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}
	RewritFlag = viper.GetBool("rewrit_flag")
	err = viper.UnmarshalKey("pub_topic_rewriting", &TopicPubRewriting)
	if err != nil {
		log.Println("Unable to decode into struct,", err)
	}
	err = viper.UnmarshalKey("sub_topic_rewriting", &TopicSubRewriting)
	if err != nil {
		log.Println("Unable to decode into struct, ", err)
	}

	// 是否关闭日志输出
	if viper.GetBool("log.disable") {
		log.SetOutput(ioutil.Discard)
	}
	listener, err := net.Listen("tcp", viper.GetString("broker"))
	if err != nil {
		log.Fatalf("Error setting up listener: %s", err)
	}

	log.Println("MQTT broker started on ", viper.GetString("broker"))

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %s", err)
			continue
		}

		go handleConnection(conn)
	}
}

// handleConnection 用于处理每一个到达的连接。
func handleConnection(conn net.Conn) {
	// 未确认消息映射
	var unacknowledgedMessages = make(map[uint16]interface{})
	// MQTT Broker 客户端结构体
	var MqttBrokerClient MqttBrokerClient
	var client mqtt.Client

	// 使用 defer 确保连接关闭，及处理断开连接的相关操作。
	defer func() {
		conn.Close()
		if RewritFlag {

			client.Disconnect(250) // 从 MQTT broker 断开连接
			log.Println("平台MQTT broker 断开连接")
		}
		clientDisconnected() // 客户端断开连接事件
	}()

	// 持续读取并处理来自客户端的数据包
	for {
		log.Println("等待数据包...")
		packet, err := packets.ReadPacket(conn)
		if err != nil {
			if err != io.EOF {
				log.Printf("读取数据包出错: %s", err)
			}
			log.Printf("客户端断开连接: %s", err)
			return
		}
		//fmt.Println(packet.String())
		// 根据数据包的类型进行相应的处理
		switch p := packet.(type) {
		// 当收到一个连接数据包时
		case *packets.ConnectPacket:
			handleConnectPacket(conn, p, &client, &MqttBrokerClient)
		// 当收到一个发布数据包时
		case *packets.PublishPacket:
			handlePublishPacket(conn, p, client, &MqttBrokerClient, unacknowledgedMessages)
		// 当收到一个发布收到确认数据包时
		case *packets.PubrecPacket:
			handlePubrecPacket(conn, p, unacknowledgedMessages)
		// 当收到一个发布释放数据包时
		case *packets.PubrelPacket:
			handlePubrelPacket(conn, p, unacknowledgedMessages)
		// 当收到一个订阅数据包时
		case *packets.SubscribePacket:
			handleSubscribePacket(conn, p, client, &MqttBrokerClient)
		// 当收到一个断开连接数据包时
		case *packets.DisconnectPacket:
			handleDisconnectPacket(conn, p)
		// 当收到一个心跳请求数据包时
		case *packets.PingreqPacket:
			handlePingreqPacket(conn, p)
		// 当收到一个取消订阅数据包时
		case *packets.UnsubscribePacket:
			handleUnsubscribePacket(conn, p)
		}

		// Handle other MQTT packet types as needed...
	}
}

// This function gets called when a client disconnects
func clientDisconnected() {
	log.Println("Client has disconnected.")
	// Add any other logic you want here
}

// PublishMessageToTopic 分发消息
func handleConnectPacket(conn net.Conn, p *packets.ConnectPacket, client *mqtt.Client, MqttBrokerClient *MqttBrokerClient) {
	connack := packets.NewControlPacket(packets.Connack).(*packets.ConnackPacket)
	connack.SessionPresent = false

	log.Println("Client ID:", p.ClientIdentifier)
	MqttBrokerClient.ClientID = p.ClientIdentifier
	if p.UsernameFlag {
		log.Println("Username:", p.Username)
		MqttBrokerClient.Username = p.Username
	}
	if p.PasswordFlag {
		log.Println("Password:", string(p.Password))
		MqttBrokerClient.Password = string(p.Password)
	}

	if RewritFlag {
		*client = ConnectPacket(MqttBrokerClient)
		if token := (*client).Connect(); token.Wait() && token.Error() != nil {
			if strings.Contains(token.Error().Error(), "not Authorized") {
				connack.ReturnCode = 0x05
				connack.Write(conn)
			} else {
				// 连接失败
				log.Println(token.Error().Error())
				connack.ReturnCode = 0x03
				connack.Write(conn)
			}
		}
	}
	// 连接成功
	connack.ReturnCode = 0x00
	connack.Write(conn)
}
func handlePublishPacket(conn net.Conn, p *packets.PublishPacket, client mqtt.Client, MqttBrokerClient *MqttBrokerClient, unacknowledgedMessages map[uint16]interface{}) {
	if p.Qos == 1 {
		puback := packets.NewControlPacket(packets.Puback).(*packets.PubackPacket)
		puback.MessageID = p.MessageID
		puback.Write(conn)
	} else if p.Qos == 2 {
		pubrec := packets.NewControlPacket(packets.Pubrec).(*packets.PubrecPacket)
		pubrec.MessageID = p.MessageID
		unacknowledgedMessages[p.MessageID] = pubrec
		pubrec.Write(conn)
	}

	log.Printf("客户端发布消息: %s\nMessage: %s\n", p.TopicName, p.Payload)

	PublishMessageToTopic(p.TopicName, p.Payload, p.Qos, p.MessageID)

	if RewritFlag {
		PublishPacket(client, p, *MqttBrokerClient)
	}
}

func handleSubscribePacket(conn net.Conn, p *packets.SubscribePacket, client mqtt.Client, MqttBrokerClient *MqttBrokerClient) {
	for i, topic := range p.Topics {
		qos := p.Qoss[i]
		log.Printf("客户端订阅: %s QoS %d\n", topic, qos)
		TopicSubscribers[topic] = append(TopicSubscribers[topic], conn)
	}

	suback := packets.NewControlPacket(packets.Suback).(*packets.SubackPacket)
	suback.MessageID = p.MessageID
	suback.ReturnCodes = make([]byte, len(p.Topics))
	for i := range p.Topics {
		suback.ReturnCodes[i] = p.Qoss[i]
	}
	suback.Write(conn)

	if RewritFlag {
		SubscribePacket(client, p, *MqttBrokerClient)
	}
}

func handlePubrecPacket(conn net.Conn, p *packets.PubrecPacket, unacknowledgedMessages map[uint16]interface{}) {
	// Remove the PUBLISH message from the list of unacknowledged messages
	delete(unacknowledgedMessages, p.MessageID)
	// Send PUBREL packet
	pubrel := packets.NewControlPacket(packets.Pubrel).(*packets.PubrelPacket)
	pubrel.MessageID = p.MessageID
	err := pubrel.Write(conn)
	if err != nil {
		log.Printf("Error sending PUBREL packet: %s", err)
		return
	}
	// Add the PUBREL message to the list of unacknowledged messages
	unacknowledgedMessages[p.MessageID] = pubrel
}

func handlePubrelPacket(conn net.Conn, p *packets.PubrelPacket, unacknowledgedMessages map[uint16]interface{}) {
	// Remove the PUBREL message from the list of unacknowledged messages
	delete(unacknowledgedMessages, p.MessageID)
	// Send PUBCOMP packet
	pubcomp := packets.NewControlPacket(packets.Pubcomp).(*packets.PubcompPacket)
	pubcomp.MessageID = p.MessageID
	err := pubcomp.Write(conn)
	if err != nil {
		log.Printf("Error sending PUBCOMP packet: %s", err)
		return
	}
}

func handleDisconnectPacket(conn net.Conn, p *packets.DisconnectPacket) {
	log.Printf("Client %s requested a clean disconnect.", conn.RemoteAddr())
}

func handlePingreqPacket(conn net.Conn, p *packets.PingreqPacket) {
	pingresp := packets.NewControlPacket(packets.Pingresp).(*packets.PingrespPacket)
	err := pingresp.Write(conn)
	if err != nil {
		log.Printf("Failed to send PINGRESP to client %s: %s", conn.RemoteAddr(), err)
	}
}

func handleUnsubscribePacket(conn net.Conn, p *packets.UnsubscribePacket) {
	// handle unsubscribe packet
	// ...
	topics := p.Topics
	for _, topic := range topics {
		log.Println("Unsubscribing from:", topic)
		// Remove the connection from the list of subscribers for the topic
		subscribers := TopicSubscribers[topic]
		for i, subscriber := range subscribers {
			if subscriber == conn {
				// Remove the subscriber from the list
				TopicSubscribers[topic] = append(subscribers[:i], subscribers[i+1:]...)
				break
			}
		}
	}
	// Send UNSUBACK packet
	unsuback := packets.NewControlPacket(packets.Unsuback).(*packets.UnsubackPacket)
	unsuback.MessageID = p.MessageID
	err := unsuback.Write(conn)
	if err != nil {
		log.Printf("Error sending UNSUBACK packet: %s", err)
		return
	}

}
