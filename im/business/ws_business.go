package business

import (
	"context"
	"encoding/json"
	"fmt"
	"fringe/cfg"
	"fringe/models"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
	"strconv"
)

type User struct {
	Id     uint64
	Client string
	ch     chan<- *UserResponse
	Conn   *websocket.Conn
}

type UserResponse struct {
	Conn     *websocket.Conn
	Client   string
	Protocol uint32
	Data     []byte
}

func NewUser(id uint64, client string, conn *websocket.Conn, ch chan<- *UserResponse) *User {
	return &User{
		Id:     id,
		Client: client,
		ch:     ch,
		Conn:   conn,
	}
}

// UserLoginEvent 用户登录事件
func UserLoginEvent(user *User, _ []byte) {
	SetUser(user)

	var pageSize = 30
	for true {
		var _, message = models.HistoryInfo(user.Id, pageSize)
		if len(message) == 0 {
			break
		}

		var byteData, err = json.Marshal(message)
		if err != nil {
			cfg.Log.Error(err.Error())
			break
		}

		user.ch <- &UserResponse{
			Conn:     user.Conn,
			Client:   user.Client,
			Protocol: 20000,
			Data:     byteData,
		}

		var ids = make([]uint, 0, len(message))
		for _, chat := range message {
			ids = append(ids, chat.Id)
		}
		_ = models.UpdateHaveRead(ids)
	}

}

// UserLogOutEvent 用户离线事件
func UserLogOutEvent(user *User, _ []byte) {
	cfg.GetRedis().HDel(context.TODO(), cfg.UserServer, strconv.FormatUint(user.Id, 10))
	DeleteUser(user.Id)
}

// GroupMessage 群消息
func GroupMessage(_ *User, data []byte) {
	var message models.UserChat
	if err := json.Unmarshal(data, &message); err != nil {
		cfg.Log.Error(fmt.Sprintf("消息格式错误: %s", err.Error()))
		return
	}

	var err = cfg.GetRedis().Publish(context.TODO(), cfg.GroupChatPubSub, message).Err()
	if err != nil {
		cfg.Log.Error(err.Error(), zap.Any("data", message))
	}
}

// SingleChat 个人消息
func SingleChat(_ *User, data []byte) {
	var message models.UserChat
	if err := json.Unmarshal(data, &message); err != nil {
		cfg.Log.Error(fmt.Sprintf("消息格式错误: %s", err.Error()))
		return
	}

	var pubSubFn = func(message models.UserChat) {
		var err = cfg.GetRedis().Publish(context.TODO(), cfg.SingleChatPubSub, message).Err()
		if err != nil {
			cfg.Log.Error(err.Error(), zap.Any("data", message))
		}
	}
	var info, ok = GetUser(message.ToUser)
	if !ok {
		pubSubFn(message)
		return
	}

	info.ch <- &UserResponse{
		Conn:     info.Conn,
		Client:   info.Client,
		Protocol: 10001,
		Data:     data,
	}
	message.Read = 1
	pubSubFn(message)
}
