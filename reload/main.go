package main

import (
	"encoding/json"
	"fmt"
	"fringe/cfg"
	"go.uber.org/zap"
	"time"
)

func main() {
	var log = cfg.NewLogger(-1)
	defer log.CloseFile()

	zap.L().Debug("这是Debug调试信息...")
	zap.L().Warn("这是Warn调试信息...")
	zap.L().Error("这是Error调试信息...")
	zap.L().DPanic("这是Panic调试信息...")
	//var h = new(cfg.MyHandler)
	//h.StartApp()

	var qq, _ = cfg.RabbitMqConnect("amqps://iscyijgb:ClkbDxGCp9g0fpETJB3K8SXsq6VLzY_b@rattlesnake.rmq.cloudamqp.com/iscyijgb")
	defer qq.Close()
	var order = new(Order)
	var mq = cfg.NewMQManager()
	mq.AddDlxQueue(order)

	var i = 1
	for {

		order.Id = i
		order.Age = int8(i)
		order.Name = fmt.Sprintf("username-%d", i)
		order.Created = time.Now().Format(time.DateTime)

		_ = order.Produce(order, 15)
		i += 1
		<-time.After(time.Millisecond * 30)
	}
}

type Order struct {
	cfg.DlxCommQueueInterface
	Id      int    `json:"id"`
	Name    string `json:"name"`
	Age     int8   `json:"age"`
	Created string `json:"created"`
}

func (o Order) ToByte() []byte {
	var b, _ = json.Marshal(o)

	return b
}

func (o Order) Queue() string {
	return "order-test-001"
}
func (o Order) Exchange() string {
	return "order-test-001"
}
func (o Order) RouterKey() string {
	return "order-test-001"
}

func (o Order) Callback(data []byte) {
	fmt.Println(fmt.Sprintf("[%s]返回的信息：", time.Now().Format(time.DateTime)), string(data))
}
