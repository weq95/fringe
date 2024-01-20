package business

import (
	"context"
	"encoding/json"
	"fringe/cfg"
	"fringe/models"
	"go.uber.org/zap"
	"time"
)

func SyncData() {
	go ChatInfoSyncDB()   // 同步到数据库
	go SingleChatSubPub() // 单人聊天
	go GroupChatSubPub()  // 群聊天
}

// ChatInfoSyncDB 同步信息到数据库
func ChatInfoSyncDB() {
	var timeDiff float64 = 3
	var ticker = time.NewTicker(time.Second * time.Duration(timeDiff))
	defer ticker.Stop()

	type OneInfo struct {
		SyncTime time.Time
		Info     []*models.UserChat
	}
	type GroupInfo struct {
		SyncTime time.Time
		Info     []*models.GroupChat
	}

	var limit = 30
	var one = OneInfo{Info: make([]*models.UserChat, 0, limit), SyncTime: time.Now()}
	var group = GroupInfo{Info: make([]*models.GroupChat, 0, limit), SyncTime: time.Now()}
	var oneSyncFn = func() {
		if len(one.Info) == 0 {
			return
		}
		_ = models.OneChatBatchInsert(one.Info)
		one.SyncTime = time.Now()
		one.Info = one.Info[:0]
	}
	var groupSyncFn = func() {
		if len(group.Info) == 0 {
			return
		}
		_ = models.GroupChatBatchInsert(group.Info)
		group.SyncTime = time.Now()
		group.Info = group.Info[:0]
	}
	for true {
		select {
		case msg, ok := <-cfg.IMChatInfo:
			if !ok {
				return
			}
			zap.L().Debug("", zap.Any("msg", msg))
			switch msg.(type) {
			case *models.UserChat:
				one.Info = append(one.Info, msg.(*models.UserChat))
				if len(one.Info) >= limit {
					oneSyncFn()
				}
			case *models.GroupChat:
				group.Info = append(group.Info, msg.(*models.GroupChat))
				if len(one.Info) >= limit {
					groupSyncFn()
				}
			}

		case <-ticker.C:
			if time.Now().Sub(one.SyncTime).Seconds() >= timeDiff {
				oneSyncFn()
			}
			if time.Now().Sub(group.SyncTime).Seconds() >= timeDiff {
				groupSyncFn()
			}
		}
	}
}

// SingleChatSubPub 单人聊天redis监听频道
func SingleChatSubPub() {
	var ctx = context.Background()
	var pubsub = cfg.GetRedis().Subscribe(ctx, cfg.SingleChatPubSub)
	defer pubsub.Close()

	for true {
		var msg, err = pubsub.ReceiveMessage(ctx)
		if err != nil {
			zap.L().Error(err.Error())
			break
		}

		var info models.UserChat
		if err = json.Unmarshal([]byte(msg.Payload), &info); err != nil {
			zap.L().Error(err.Error(), zap.String("info", msg.Payload))
			continue
		}

		info.Ctime = time.Now()
		//消息转发
		SingleChatForwarding(&info, []byte(msg.Payload))
		cfg.IMChatInfo <- &info
	}
}

// GroupChatSubPub 群聊天redis监听频道
func GroupChatSubPub() {
	var ctx = context.Background()
	var pubsub = cfg.GetRedis().Subscribe(ctx, cfg.GroupChatPubSub)
	defer pubsub.Close()

	for true {
		var msg, err = pubsub.ReceiveMessage(ctx)
		if err != nil {
			zap.L().Error(err.Error())
			break
		}

		var info models.GroupChat
		if err = json.Unmarshal([]byte(msg.Payload), &info); err != nil {
			zap.L().Error(err.Error(), zap.String("info", msg.Payload))
			continue
		}

		// 消息转发
		GroupChatForwarding(info.FromUser, []byte(msg.Payload))
		info.Ctime = time.Now()
		cfg.IMChatInfo <- &info
	}
}
