package models

import (
	"encoding/json"
	"fringe/cfg"
	"go.uber.org/zap"
	"time"
)

type UserChat struct {
	Id       uint      `json:"id" gorm:"column:id;primaryKey"`
	FromUser uint64    `json:"from_user" gorm:"column:fromuser"`
	ToUser   uint64    `json:"to_user" gorm:"column:touser"`
	MsgType  uint8     `json:"msg_type" gorm:"column:msgtype"`
	Text     string    `json:"text"`
	Read     uint8     `json:"read"`
	Ctime    time.Time `json:"ctime"`
}

func (u UserChat) TableName() string {
	return "user_chat"
}

func (u UserChat) MarshalBinary() (data []byte, err error) {
	return json.Marshal(u)
}

func (u *UserChat) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, &u)
}

func OneChatBatchInsert(message []*UserChat) error {
	var err = cfg.GetDB().Create(message).Error
	if err != nil {
		zap.L().Error(err.Error(), zap.Any("one", message))
	}
	return nil
}

// HistoryInfo 用户历史未读信息
func HistoryInfo(userid uint64, pageSize int) (int64, []*UserChat) {
	var message = make([]*UserChat, 0, pageSize)
	var count int64
	var err = cfg.GetDB().Model(&UserChat{}).Where("`read`=? and `touser`=?", 0, userid).
		Count(&count).Error
	if err != nil || count == 0 {
		return count, message
	}
	err = cfg.GetDB().Where("`read`=? and `touser`=?", 0, userid).
		Order("id ASC").Limit(pageSize).Offset(0).
		Find(&message).Error
	if err != nil {
		zap.L().Error(err.Error())
	}
	return count, message
}

// UpdateHaveRead 更新已读状态
func UpdateHaveRead(ids []uint) error {
	var err = cfg.GetDB().Table(new(UserChat).TableName()).Where("id IN ?", ids).
		Updates(map[string]interface{}{"read": 1}).Error
	if err != nil {
		zap.L().Error(err.Error())
	}

	return err
}
