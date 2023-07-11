package cfg

import (
	"crypto/rand"
	"encoding/json"
	"io"
	"math/big"
	"net/http"
)

const JsonFilePath = "./temp/json/" // excel-json 下载文件目录
// UserServer 用户所在服务器RedisKey
const UserServer = "user-server"

// SingleChatPubSub 用户一对一聊天订阅频道
const SingleChatPubSub = "single-user-chat"

// GroupChatPubSub 群消息订阅频道
const GroupChatPubSub = "group-user-chat"

// IMChatInfo 聊天信息传递媒介
var IMChatInfo = make(chan interface{}, 300)

func CryptRandom(max int64) uint64 {
	var pro, _ = rand.Int(rand.Reader, big.NewInt(max))
	return pro.Uint64()
}

func HttpGet(url string, result interface{}) error {
	var response, err = http.Get(url)
	if err != nil {
		return err
	}

	defer response.Body.Close()
	var body []byte
	body, err = io.ReadAll(response.Body)

	return json.Unmarshal(body, result)
}
