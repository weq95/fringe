package models

import (
	"encoding/json"
	"fringe/cfg"
	"go.uber.org/zap"
	"time"
)

type GroupChat struct {
	Id       uint      `json:"id" gorm:"column:id;primaryKey"`
	FromUser uint64    `json:"from_user" gorm:"column:fromuser"`
	GroupId  uint      `json:"group_id" gorm:"column:groupid"`
	MsgType  uint8     `json:"msg_type" gorm:"column:msgtype"`
	Text     string    `json:"text"`
	Ctime    time.Time `json:"ctime"`
}

func (g GroupChat) TableName() string {
	return "group_chat"
}

func (u *GroupChat) MarshalBinary() (data []byte, err error) {
	return json.Marshal(u)
}

func (u *GroupChat) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, &u)
}
func GroupChatBatchInsert(message []*GroupChat) error {
	var err = cfg.GetDB().Create(message).Error
	if err != nil {
		zap.L().Error(err.Error(), zap.Any("group", message))
	}
	return nil
}
