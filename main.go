package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func mqtest() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"hello", // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	failOnError(err, "Failed to declare a queue")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	var forever chan struct{}

	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s", d.Body)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}

type RechargeResult struct {
	ChannelId          int32  `json:"channelId"`        //渠道ID，共有字段
	Platform           int32  `json:"platform"`         //充值平台，0-冰川，1-U8
	BCUserId           uint32 `json:"userId"`           //用户ID，共有字段
	RoleID             uint32 `json:"roleId"`           //角色ID
	Amount             int32  `json:"amount"`           //充值金额：共有字段，冰川-单位为元，U8单位为分
	CheckProductIDFlag int32  `json:"checkProduct"`     //检查产品ID标记，为1表示检查
	ProductId          string `json:"productId"`        //商品ID，U8字段，以空字符结尾
	Currency           string `json:"currency"`         //货币类型，U8字段，默认RMB，冰川也填充RMB，以空字符结尾
	PlatformOrderId    string `json:"platformOrderId"`  //平台订单号， 共有字段，以空字符结尾
	GameOrderId        string `json:"gameOrderId"`      //游戏订单号， 共有字段，以空字符结尾
	BankCode           string `json:"bankCode"`         //BankCode
	WorldId            int32  `json:"worldId"`          //worldId
	Param              string `json:"developerPayLoad"` //param
}

var mqCli_ *RabbitMQClient
var mqChannelNum_ int

func initmq() {
	mqChannelNum_ = 1
	mqCli_ = &RabbitMQClient{
		conn:                    nil,
		channel:                 nil,
		amqpurl:                 "",
		reconnectFlag:           false,
		channelReconnectFlag:    false,
		MQListData:              nil,
		arrChannel:              make([]*amqp.Channel, mqChannelNum_),
		arrChannelReconnectFlag: make([]bool, mqChannelNum_),
	}
}

func main() {
	//初始化mq
	initmq()
	//创建mq连接
	mqUrl := "amqp://gouser:DEIro34KE%40%23%24@172.16.124.61:5672/Y"
	mqCli_.Create(mqUrl, 1)
	queuename := "Game:9527|Business:Recharge"
	//mqCli_.Subscribe(queuename, "direct", queuename, "", false)
	//发消息测试
	result := &RechargeResult{
		ChannelId:          0,
		Platform:           0,
		BCUserId:           0,
		RoleID:             129143,
		Amount:             6800,
		CheckProductIDFlag: 0,
		ProductId:          "",
		Currency:           "CNY",
		PlatformOrderId:    "",
		GameOrderId:        "0001f877652de60400009c47000000a1",
		BankCode:           "",
		WorldId:            1002,
		Param:              "0",
	}
	str, _ := json.Marshal(result)
	_ = str
	//str = []byte(string(str) + string(str) + string(str) + string(str) + string(str) + string(str) + string(str) + string(str) + string(str) + string(str))
	tm1 := time.Now()
	tick1 := tm1.UnixMilli()
	for i := 0; i < 10000; i++ {
		//val := strconv.Itoa(i)
		mqCli_.Publish(queuename, queuename, []byte(str))
	}
	tm2 := time.Now()
	tick2 := tm2.UnixMilli()

	//转发消息
	go mqCli_.DispatchMsg()

	fmt.Println("wait for exit! str size=", len(str), " tick=", (tick2 - tick1))

	InstallSignal(func() {
		fmt.Println("bye")
		os.Exit(0)
	})
}
