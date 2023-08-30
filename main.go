package main

import (
	"fmt"
	"io"
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
	viper.SetConfigFile("config.yaml")
	err := viper.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}
	RewritFlag = viper.GetBool("rewrit_flag")
	err = viper.UnmarshalKey("pub_topic_rewriting", &TopicPubRewriting)
	if err != nil {
		fmt.Printf("Unable to decode into struct, %v", err)
	}
	err = viper.UnmarshalKey("sub_topic_rewriting", &TopicSubRewriting)
	if err != nil {
		fmt.Printf("Unable to decode into struct, %v", err)
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

func handleConnection(conn net.Conn) {
	var unacknowledgedMessages = make(map[uint16]interface{})
	var MqttBrokerClient MqttBrokerClient
	var client mqtt.Client
	defer func() {
		conn.Close()
		if RewritFlag {
			client.Disconnect(250) // Disconnect from MQTT broker
		}
		clientDisconnected() // Client disconnected event
	}()

	for {
		fmt.Println("Waiting for packet...")
		packet, err := packets.ReadPacket(conn)
		if err != nil {
			if err != io.EOF {
				log.Printf("Error reading packet: %s", err)
			}
			return
		}
		fmt.Println("Packet type:", packet)
		switch p := packet.(type) {

		case *packets.ConnectPacket:
			connack := packets.NewControlPacket(packets.Connack).(*packets.ConnackPacket)
			connack.SessionPresent = false

			fmt.Println("Client ID:", p.ClientIdentifier)
			MqttBrokerClient.ClientID = p.ClientIdentifier
			if p.UsernameFlag { // Check if username is present
				fmt.Println("Username:", p.Username)
				MqttBrokerClient.Username = p.Username
			}
			if p.PasswordFlag { // Check if password is present
				fmt.Println("Password:", string(p.Password))
				MqttBrokerClient.Password = string(p.Password)
			}
			// hook
			if RewritFlag {
				client = ConnectPacket(&MqttBrokerClient)
				if token := client.Connect(); token.Wait() && token.Error() != nil {
					if strings.Contains(token.Error().Error(), "not authorized") {
						connack.ReturnCode = 0x05 // Connection refused, not authorized
						connack.Write(conn)
					} else {
						connack.ReturnCode = 0x03 // Connection refused, server unavailable
						connack.Write(conn)
					}
				}
			}
			connack.ReturnCode = 0x00 // Connection accepted
			connack.Write(conn)

		case *packets.PublishPacket:
			// If QoS 1 or 2, send PUBACK or PUBREC respectively
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
			fmt.Printf("客户端发布消息: %s\nMessage: %s\n", p.TopicName, p.Payload)
			// 分发消息
			PublishMessageToTopic(p.TopicName, p.Payload, p.Qos, p.MessageID)
			// hook
			if RewritFlag {
				PublishPacket(client, p, MqttBrokerClient)
			}
		case *packets.PubrecPacket:
			fmt.Println("Received PUBREC packet")
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
		case *packets.PubrelPacket:
			// If QoS 2, send PUBCOMP
			pubcomp := packets.NewControlPacket(packets.Pubcomp).(*packets.PubcompPacket)
			pubcomp.MessageID = p.MessageID
			delete(unacknowledgedMessages, p.MessageID)
			pubcomp.Write(conn)

		case *packets.SubscribePacket:
			// 在这里，你可以检查客户端希望订阅的主题
			for i, topic := range p.Topics {
				qos := p.Qoss[i]
				fmt.Printf("客户端订阅: %s QoS %d\n", topic, qos)
				// 如果需要，你可以在这里检查和管理客户端的订阅权限
				// 例如，你可能想基于客户端提供的用户名或客户端ID来决定是否允许订阅

				// 添加客户端到主题订阅者列表
				TopicSubscribers[topic] = append(TopicSubscribers[topic], conn)
			}

			// 创建并发送SUBACK数据包以确认订阅
			suback := packets.NewControlPacket(packets.Suback).(*packets.SubackPacket)
			suback.MessageID = p.MessageID

			// 这里，我们简单地假设所有的订阅请求都被接受并使用了相同的QoS等级
			// 在实际应用中，你可能需要根据实际情况修改这部分
			suback.ReturnCodes = make([]byte, len(p.Topics))
			for i := range p.Topics {
				suback.ReturnCodes[i] = p.Qoss[i]
			}

			suback.Write(conn)
			if RewritFlag {
				SubscribePacket(client, p, MqttBrokerClient)
			}
		case *packets.DisconnectPacket:
			log.Printf("Client %s requested a clean disconnect.", conn.RemoteAddr())
			return // 结束处理，连接将被defer中的代码关闭
			// client_id
		case *packets.PingreqPacket:
			pingresp := packets.NewControlPacket(packets.Pingresp).(*packets.PingrespPacket)
			err := pingresp.Write(conn)
			if err != nil {
				log.Printf("Failed to send PINGRESP to client %s: %s", conn.RemoteAddr(), err)
			}
		case *packets.UnsubscribePacket:
			// handle unsubscribe packet
			// ...
			topics := p.Topics
			for _, topic := range topics {
				fmt.Println("Unsubscribing from:", topic)
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

		// Handle other MQTT packet types as needed...
	}
}

// This function gets called when a client disconnects
func clientDisconnected() {
	fmt.Println("Client has disconnected.")
	// Add any other logic you want here
}
