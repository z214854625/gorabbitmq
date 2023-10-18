package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/smallnest/chanx"
	"github.com/streadway/amqp"
)

type sMqMsg struct {
	channelIndex int
	queueName    string
	deliveryTag  uint64
	data         []byte
	objId        int
}

type RabbitMQClient struct {
	conn                    *amqp.Connection              //connection
	channel                 *amqp.Channel                 //channel
	amqpurl                 string                        //url链接
	reconnectFlag           bool                          //连接重连标识
	channelReconnectFlag    bool                          //channel重连标识
	MQListData              *chanx.UnboundedChan[*sMqMsg] //无长度限制chan
	clientLock              sync.RWMutex
	arrChannel              []*amqp.Channel //channel
	arrChannelReconnectFlag []bool          //channel重连标识
	objId                   int             //对象id
}

func (c *RabbitMQClient) Create(amqpURI string, id int) bool {
	c.objId = id
	c.amqpurl = amqpURI
	ret := c.Connnect(amqpURI)
	if !ret {
		fmt.Println("RabbitMQClient Create failed!", " amqpURI=", amqpURI, " id=", c.objId)
		return false
	}
	ctx, cancel := context.WithCancel(context.Background())
	_ = cancel
	//defer cancel()
	c.MQListData = chanx.NewUnboundedChan[*sMqMsg](ctx, 100)

	const interval = 3                          //3秒
	t := time.NewTicker(interval * time.Second) //断线重连定时器
	go c.OnTimer(t)
	fmt.Println("RabbitMQClient Create suc!", " amqpURI=", amqpURI, " id=", c.objId)
	return true
}

func (c *RabbitMQClient) Connnect(amqpURI string) bool {
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
	var err error
	//fmt.Println("amqpURI=", amqpURI, ", channel len=", len(c.arrChannel), " id=", c.objId)
	c.conn, err = amqp.Dial(amqpURI)
	if err != nil {
		fmt.Println("Dial err=", err.Error(), " id=", c.objId)
		return false
	}
	//创建channel数组
	for i := 0; i < len(c.arrChannel); i++ {
		c.arrChannel[i], err = c.conn.Channel()
		if err != nil {
			c.conn.Close() //关闭conn和channel
			for j := 0; j < i; j++ {
				c.arrChannel[i].Close()
			}
			fmt.Println("RabbitMQClient.Connnect open channel failed! index=", i, " id=", c.objId)
			return false
		}
		//fmt.Println("RabbitMQClient.Connnect open channel suc! index=", i, " id=", c.objId)
	}
	//连接关闭
	go c.ConnectionNotifyClose()
	//channel关闭
	for i := 0; i < len(c.arrChannel); i++ {
		if c.arrChannel[i] == nil {
			fmt.Println("RabbitMQClient.Connnect c.arrChannel[i] nil! index=", i, " id=", c.objId)
			continue
		}
		go c.ChannelNotifyClose(i)
	}
	//fmt.Println("RabbitMQClient Connnect suc!", " id=", c.objId)
	return true
}

//连接关闭
func (c *RabbitMQClient) ConnectionNotifyClose() {
	ret := <-c.conn.NotifyClose(make(chan *amqp.Error))
	if ret == nil { //主动关闭
		fmt.Println("ConnectionNotifyClose connection closed!", " id=", c.objId)
	} else { //异常关闭
		fmt.Println("ConnectionNotifyClose connection abnormal!", " id=", c.objId)
		c.clientLock.Lock()
		c.reconnectFlag = true
		c.clientLock.Unlock()
	}
}

//channel关闭
func (c *RabbitMQClient) ChannelNotifyClose(index int) {
	if index >= len(c.arrChannel) || c.arrChannel[index] == nil {
		fmt.Println("ChannelNotifyClose index err! index=", index, " len=", len(c.arrChannel), " id=", c.objId)
		return
	}
	ret1 := <-c.arrChannel[index].NotifyClose(make(chan *amqp.Error))
	if ret1 == nil { //主动关闭
		fmt.Println("ChannelNotifyClose channel closed! index= ", index, " id=", c.objId)
	} else { //异常关闭
		fmt.Println("ChannelNotifyClose channel abnormal! index=", index, " id=", c.objId)
		c.clientLock.Lock()
		c.arrChannelReconnectFlag[index] = true
		c.clientLock.Unlock()
	}
}

func (c *RabbitMQClient) Shutdown() error {
	for i := 0; i < len(c.arrChannel); i++ {
		if c.arrChannel[i] != nil {
			c.arrChannel[i].Close()
			c.arrChannel[i] = nil
		}
	}
	if c.conn != nil {
		c.conn.Close()
	}
	c.conn = nil
	fmt.Println("AMQP shutdown OK! id=", c.objId)
	return nil
}

func (c *RabbitMQClient) Reconnect(onlychannel bool, index int) bool {
	fmt.Println("RabbitMQClient Reconnect start- onlychannel=", onlychannel, " index=", index, " id=", c.objId)
	if onlychannel {
		if c.OpenChannel(index) == false {
			fmt.Println("RabbitMQClient Reconnect 1 failed! id=", c.objId)
			return false
		}
	} else {
		if c.Connnect(c.amqpurl) == false {
			fmt.Println("RabbitMQClient Reconnect 2 failed! id=", c.objId)
			return false
		}
	}
	//重新订阅队列
	//CS.SubscribeQueue(c.objId)
	fmt.Println("RabbitMQClient Reconnect suc! id=", c.objId)
	return true
}

func (c *RabbitMQClient) OpenChannel(index int) bool {
	if c.conn == nil {
		fmt.Println("OpenChannel conn nil. index=", index, " id=", c.objId)
		return false
	}
	err := c.CloseChannel(index)
	if err != nil {
		fmt.Println("OpenChannel close channel fail. error=", err.Error(), " index=", index, " id=", c.objId)
		return false
	}
	c.arrChannel[index], err = c.conn.Channel()
	if err != nil {
		fmt.Println("OpenChannel Channel err=", err.Error(), " index=", index, " id=", c.objId)
		return false
	}
	return true
}

func (c *RabbitMQClient) CloseChannel(index int) error {
	if c.arrChannel[index] == nil {
		fmt.Println("CloseChannel channel nil. index= ", index, " id=", c.objId)
		return nil
	}
	err := c.arrChannel[index].Close()
	if err != nil {
		fmt.Println("CloseChannel channel close failed! error=", err.Error(), " index=", index, " id=", c.objId)
		return err
	}
	c.arrChannel[index] = nil
	fmt.Println("CloseChannel OK! index=", index, " id=", c.objId)

	return nil
}

func (c *RabbitMQClient) Subscribe(exchange, exchangeType, queueName, routingkey string, autoAck bool) bool {
	//fmt.Println("Subscribe exchange=", exchange, " queue=", queueName, " routing=", routingkey, " etype=", exchangeType, " ack=", autoAck, " id=", c.objId)
	var err error
	for i := 0; i < len(c.arrChannel); i++ {
		channel := c.arrChannel[i]
		//fmt.Println("Subscribe channel-- index=", i, " id=", c.objId)
		if channel == nil {
			//basic.Log.Error("Subscribe channel nil! index=", i, " id=", c.objId)
			continue
		}
		err = channel.Qos(20, 0, false)
		if err != nil {
			fmt.Println("Subscribe Qos err=", err.Error(), " index=", i, " id=", c.objId)
			continue
		}
		err = channel.ExchangeDeclare(
			exchange,     // name of the exchange
			exchangeType, // type
			true,         // durable
			false,        // delete when complete
			false,        // internal
			false,        // noWait
			nil,          // arguments
		)
		if err != nil {
			fmt.Println("Subscribe ExchangeDeclare err=", err.Error(), " index=", i, " id=", c.objId)
			continue
		}
		queue, err := channel.QueueDeclare(
			queueName, // name of the queue
			true,      // durable
			false,     // delete when unused
			false,     // exclusive
			false,     // noWait
			nil,       // arguments
		)
		if err != nil {
			fmt.Println("Subscribe QueueDeclare err=", err.Error(), " index=", i, " id=", c.objId)
			continue
		}
		err = channel.QueueBind(
			queue.Name, // name of the queue
			routingkey, // bindingKey
			exchange,   // sourceExchange
			false,      // noWait
			nil,        // arguments
		)
		if err != nil {
			fmt.Println("Subscribe QueueBind err=", err.Error(), " index=", i, " id=", c.objId)
			continue
		}
		deliveries, err := channel.Consume(
			queue.Name, // name
			"",         // consumerTag,
			autoAck,    // noAck
			false,      // exclusive
			false,      // noLocal
			false,      // noWait
			nil,        // arguments
		)
		if err != nil {
			fmt.Println("Subscribe Consume err! error=", err.Error(), " queueName=", queueName, " index=", i, " id=", c.objId)
			continue
		}
		//接收消息协程
		go c.DeliveryMsg(i, queueName, deliveries)
	}

	return true
}

//接收消息
func (c *RabbitMQClient) DeliveryMsg(index int, queueName string, deliveries <-chan amqp.Delivery) {
	fmt.Println("DeliveryMsg call! queueName=", queueName, " index=", index, " id=", c.objId)
	for d := range deliveries {
		c.MQListData.In <- &sMqMsg{channelIndex: index, queueName: queueName, deliveryTag: d.DeliveryTag, data: d.Body, objId: c.objId}
	}
	fmt.Println("DeliveryMsg channel closed! queueName=", queueName, " index=", index, " id=", c.objId)
}

func (c *RabbitMQClient) Publish(exchange, routingKey string, data []byte) bool {
	idx := rand.Intn(mqChannelNum_)
	channel := c.arrChannel[idx]
	if channel == nil {
		fmt.Println("RabbitMQClient.Publish channel nil! index=", idx, " id=", c.objId)
		return false
	}
	err := channel.Publish(
		exchange,   // publish to an exchange
		routingKey, // routing to 0 or more queues
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			Headers:         amqp.Table{},
			ContentType:     "text/plain",
			ContentEncoding: "",
			Body:            data,
			DeliveryMode:    amqp.Transient, // 1=non-persistent, 2=persistent
			Priority:        0,              // 0-9
			// a bunch of application/implementation-specific fields
		},
	)
	fmt.Println("Publish- idx=", idx, " data= ", string(data))
	if err != nil {
		fmt.Println("RabbitMQClient.Publish err=", err.Error(), " id=", c.objId)
		return false
	}
	return true
}

func (c *RabbitMQClient) DispatchMsg() {
	for {
		msg := <-c.MQListData.Out
		if msg != nil {
			fmt.Println("dispatch msg! ", msg.queueName, " idx=", msg.channelIndex, " objId=", msg.objId, " data=", string(msg.data))
			c.Ack(msg.channelIndex, msg.deliveryTag, false)
		}
	}
}

func (c *RabbitMQClient) Ack(index int, tag uint64, multiple bool) error {
	if index >= len(c.arrChannel) {
		fmt.Println("RabbitMQClient.Ack index err! index=", index, " len=", len(c.arrChannel), " id=", c.objId)
		return nil
	}
	channel := c.arrChannel[index]
	if channel == nil {
		fmt.Println("RabbitMQClient.Ack channel nil! index=", index, " len=", len(c.arrChannel), " id=", c.objId)
		return nil
	}
	err := channel.Ack(tag, multiple)
	if err != nil {
		fmt.Println("RabbitMQClient Ack err=", err.Error(), " index=", index, " id=", c.objId)
		return err
	}
	return nil
}

func (c *RabbitMQClient) NAck(index int, tag uint64, multiple bool, requeue bool) error {
	if index >= len(c.arrChannel) {
		fmt.Println("RabbitMQClient.NAck index err! index=", index, " len=", len(c.arrChannel), " id=", c.objId)
		return nil
	}
	channel := c.arrChannel[index]
	if channel == nil {
		fmt.Println("RabbitMQClient.NAck channel nil! index=", index, " len=", len(c.arrChannel), " id=", c.objId)
		return nil
	}
	err := channel.Nack(tag, multiple, requeue)
	if err != nil {
		fmt.Println("RabbitMQClient NAck err=", err.Error(), " index=", index, " id=", c.objId)
		return err
	}
	return nil
}

func (c *RabbitMQClient) OnTimer(t *time.Ticker) {
	for {
		<-t.C
		//fmt.Println("OnTimer ", time.Now(), c.reconnectFlag, c.channelReconnectFlag)
		c.clientLock.Lock()
		//connection和channel都断开，直接con重连
		if c.reconnectFlag {
			for i := 0; i < len(c.arrChannelReconnectFlag); i++ {
				if c.arrChannelReconnectFlag[i] {
					c.arrChannelReconnectFlag[i] = false
				}
			}
		}
		if c.reconnectFlag {
			c.Shutdown()
			if c.Reconnect(false, 0) {
				c.reconnectFlag = false
			}
		}
		for i := 0; i < len(c.arrChannelReconnectFlag); i++ {
			if c.arrChannelReconnectFlag[i] {
				if c.Reconnect(true, i) {
					c.arrChannelReconnectFlag[i] = false
				}

			}
		}
		c.clientLock.Unlock()
	}
	//fmt.Println("OnTimer end!")
}
