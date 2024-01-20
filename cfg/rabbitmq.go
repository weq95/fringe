package cfg

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
	"os"
	"strconv"
)

// RabbitMQQueue 普通队列
type RabbitMQQueue interface {
	Callback(data []byte)
	ToByte() []byte
	InitQueue(mq RabbitMQQueue) error
	Produce(mq RabbitMQQueue, expiration uint) error
	Consume(mq RabbitMQQueue, queueName string) error
	Queue() string
	Exchange() string
	RouterKey() string
	ConsumerTag() string
	Channel() (*amqp.Channel, error)
}

// RabbitMQDlxQueue 死信队列
type RabbitMQDlxQueue interface {
	RabbitMQQueue
	InitDlxQueue(mq RabbitMQDlxQueue) error
	DlxQueue(mq RabbitMQDlxQueue) string
	DlxRouterKey(mq RabbitMQDlxQueue) string
	DlxExchange(mq RabbitMQDlxQueue) string
}

var conn *amqp.Connection
var MqManager *MQManager

func RabbitMqConnect(mqHost string) (*amqp.Connection, error) {
	var err error
	conn, err = amqp.Dial(mqHost)

	MqManager = NewMQManager()
	MqManager.Empty()
	return conn, err
}

type MQManager struct {
	queue    map[string]RabbitMQQueue
	dlxQueue map[string]RabbitMQDlxQueue
}

func NewMQManager() *MQManager {
	return &MQManager{
		queue:    make(map[string]RabbitMQQueue, 0),
		dlxQueue: make(map[string]RabbitMQDlxQueue, 0),
	}
}

func (m *MQManager) Empty() {

}

func (m *MQManager) AddQueue(plugin RabbitMQQueue) {
	if _, ok := m.queue[plugin.Queue()]; ok {
		return
	}

	m.queue[plugin.Queue()] = plugin
	if err := plugin.InitQueue(plugin); err != nil {
		zap.L().Error("AddQueue InitQueue", zap.Error(err))
		os.Exit(1)
		return
	}
	go func() {
		var err = plugin.Consume(plugin, plugin.Queue())
		if err != nil {
			zap.L().Error(fmt.Sprintf("Queue: [%s] Consume Err", plugin.Queue()),
				zap.Error(err))
		}
	}()
}

func (m *MQManager) AddDlxQueue(plugin RabbitMQDlxQueue) {
	if _, ok := m.queue[plugin.Queue()]; ok {
		return
	}

	m.dlxQueue[plugin.DlxQueue(plugin)] = plugin
	if err := plugin.InitDlxQueue(plugin); err != nil {
		zap.L().Error("AddDlxQueue InitDlxQueue", zap.Error(err))
		os.Exit(1)
		return
	}

	go func() {
		var err = plugin.Consume(plugin, plugin.DlxQueue(plugin))
		if err != nil {
			zap.L().Error(fmt.Sprintf("DlxQueue: [%s] Consume Err",
				plugin.DlxQueue(plugin)),
				zap.Error(err))
		}
	}()
}

type CommQueueInterface struct{}

func (c CommQueueInterface) Channel() (*amqp.Channel, error) {
	return conn.Channel()
}

func (c CommQueueInterface) ConsumerTag() string {
	return ""
}

func (c CommQueueInterface) InitQueue(mq RabbitMQQueue) error {
	var channel, err = c.Channel()
	if err != nil {
		return err
	}

	defer func() {
		_ = channel.Close()
	}()

	_, err = channel.QueueDeclare(
		mq.Queue(),
		true,
		false,
		false,
		false,
		amqp.Table{},
	)
	if err != nil {
		return err
	}

	err = channel.ExchangeDeclare(
		mq.Exchange(),
		amqp.ExchangeDirect,
		true,
		false,
		false,
		false,
		amqp.Table{},
	)
	if err != nil {
		return err
	}

	return channel.QueueBind(
		mq.Queue(),
		mq.RouterKey(),
		mq.Exchange(),
		false,
		nil,
	)
}

// Produce
// expiration: 过期时间, 单位: 秒
func (c CommQueueInterface) Produce(mq RabbitMQQueue, expiration uint) error {
	var channel, err = c.Channel()
	if err != nil {
		return err
	}

	defer func() {
		_ = channel.Close()
	}()
	return channel.Publish(
		mq.Exchange(),
		mq.RouterKey(),
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        mq.ToByte(),
			Expiration:  strconv.Itoa(int(expiration * 1000)),
		},
	)
}

func (c CommQueueInterface) Consume(mq RabbitMQQueue, queueName string) error {
	var channel, err = c.Channel()
	if err != nil {
		return err
	}

	var msgCh, err01 = channel.Consume(queueName, mq.ConsumerTag(), false, false, false, false, nil)
	if err01 != nil {
		return err01
	}
	for message := range msgCh {
		mq.Callback(message.Body)
		_ = message.Ack(false)
	}

	return nil
}

// DlxCommQueueInterface 死信队列默认实现
type DlxCommQueueInterface struct {
	CommQueueInterface
}

func (d DlxCommQueueInterface) InitDlxQueue(mq RabbitMQDlxQueue) error {
	var channel, err = d.Channel()
	if err != nil {
		return err
	}
	defer func() {
		_ = channel.Close()
	}()

	err = channel.ExchangeDeclare(mq.Exchange(), amqp.ExchangeDirect, true, false, false, false, nil)
	if err != nil {
		return err
	}

	_, err = channel.QueueDeclare(mq.Queue(), true, false, false,
		false, amqp.Table{
			"x-message-ttl":             30 * 60 * 1000,      // 消息过期时间（队列级别）,毫秒, 默认30分钟过期
			"x-dead-letter-exchange":    mq.DlxExchange(mq),  // 指定死信交换机
			"x-dead-letter-routing-key": mq.DlxRouterKey(mq), // 指定死信routing-key
		})

	if err != nil {
		return err
	}

	err = channel.QueueBind(mq.Queue(), mq.RouterKey(), mq.Exchange(), false, nil)
	if err != nil {
		return err
	}

	// 交换机
	err = channel.ExchangeDeclare(mq.DlxExchange(mq), amqp.ExchangeDirect,
		true, false, false, false, nil)
	if err != nil {
		return err
	}
	// 队列
	_, err = channel.QueueDeclare(mq.DlxQueue(mq), true, false, false, false, nil)
	if err != nil {
		return err
	}
	// 绑定
	err = channel.QueueBind(mq.DlxQueue(mq), mq.DlxRouterKey(mq), mq.DlxExchange(mq), false, nil)
	return err
}

func (d DlxCommQueueInterface) DlxQueue(mq RabbitMQDlxQueue) string {
	return "dlx-" + mq.Queue()
}

func (d DlxCommQueueInterface) DlxRouterKey(mq RabbitMQDlxQueue) string {
	return "dlx-" + mq.RouterKey()
}

func (d DlxCommQueueInterface) DlxExchange(mq RabbitMQDlxQueue) string {
	return "dlx-" + mq.Exchange()
}
