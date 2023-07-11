package business

import (
	"context"
	"fringe/cfg"
	"fringe/models"
	"fringe/netsrv"
	"go.uber.org/zap"
	"strconv"
)

// SingleChatForwarding 个人对个人聊天转发
func SingleChatForwarding(msg *models.UserChat, b []byte) {
	if msg.Read == 1 {
		return
	}

	var ctx = context.TODO()
	var cmd = cfg.GetRedis().HGet(ctx, cfg.UserServer, strconv.FormatUint(msg.ToUser, 10))
	if cmd.Err() != nil {
		cfg.Log.Error(cmd.Err().Error(), zap.Binary("data", b))
		return
	}

	var srv, ok = netsrv.GetManagerServer()
	if !ok {
		cfg.Log.Warn("服务不存在")
		return
	}

	if srv.TcpClients.CenterForwarding(cmd.Val(), 2001, b) != nil {
		return
	}

	// 设置已读
	msg.Read = 1
}

// GroupChatForwarding 群消息转发
func GroupChatForwarding(fromUserid uint64, data []byte) {
	var srv, ok = netsrv.GetManagerServer()
	if !ok {
		cfg.Log.Warn("服务不存在", zap.Binary("data", data))
		return
	}

	// todo: 获取该群所有玩家 对在线玩家统一进行转发
	var userIds []string
	var cmd, err = cfg.GetRedis().HMGet(context.TODO(), cfg.UserServer, userIds...).Result()
	if err != nil {
		cfg.Log.Error(err.Error(), zap.Strings("ids", userIds), zap.Binary("data", data))
		return
	}
	var onlineUser = make(map[uint64]string, len(cmd))
	delete(onlineUser, fromUserid)
	for userid, clientIp := range onlineUser {
		if srv.CenterForwarding(clientIp, 2002, data) == nil {
			delete(onlineUser, userid)
		}
	}

	// 处理未成功转发的用户
}
