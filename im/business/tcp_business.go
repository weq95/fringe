package business

import (
	"encoding/json"
	"fmt"
	"fringe/cfg"
	"fringe/models"
	"go.uber.org/zap"
)

// Heartbeat 心跳包
func Heartbeat(data []byte, _ func(uint32, []byte)) {
	zap.L().Info(fmt.Sprintf("心跳: %s", string(data)))
}

// GetSrvPort 向中心服务器提供本机ws port
func GetSrvPort(data []byte, w func(uint32, []byte)) {
	zap.L().Info(fmt.Sprintf("本机公网IP: %s", string(data)))

	_ = cfg.Val(func(cfg *cfg.AppCfg) interface{} {
		var result = fmt.Sprintf(`{"ip": "%s", "port": %d}`, string(data), cfg.IMPort)

		w(1001, []byte(result))

		return nil
	})
}

func SingleChatForward(data []byte, _ func(uint32, []byte)) {
	var info models.UserChat
	if err := json.Unmarshal(data, &info); err != nil {
		zap.L().Error(err.Error(), zap.Binary("data", data))
		return
	}

	if user, ok := GetUser(info.ToUser); ok {
		user.ch <- &UserResponse{
			Conn:     user.Conn,
			Client:   user.Client,
			Protocol: 10001,
			Data:     data,
		}
	}
}

func GroupChatForward(data []byte, _ func(uint32, []byte)) {

}
