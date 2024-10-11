package cfg

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/big"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
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

// ExitApp 手动退出应用
func ExitApp() {
	_ = syscall.Kill(os.Getpid(), syscall.SIGINT)
}

// WaitCtrlC 创建等待退出管道
func WaitCtrlC() <-chan os.Signal {
	var sigCh = make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM, syscall.SIGINT, syscall.SIGKILL)

	return sigCh
}

// YMDHMS 转换为：年月日.时分秒
func YMDHMS(d time.Duration) string {
	var (
		tsecs = d / time.Second
		tmins = tsecs / 60
		thrs  = tmins / 60
		tdays = thrs / 24
		tyrs  = tdays / 365
	)

	if tyrs > 0 {
		return fmt.Sprintf("%dy%dd%dh%dm%ds", tyrs, tdays%365, thrs%24, tmins%60, tsecs%60)
	}
	if tdays > 0 {
		return fmt.Sprintf("%dd%dh%dm%ds", tdays, thrs%24, tmins%60, tsecs%60)
	}
	if thrs > 0 {
		return fmt.Sprintf("%dh%dm%ds", thrs, tmins%60, tsecs%60)
	}
	if tmins > 0 {
		return fmt.Sprintf("%dm%ds", tmins, tsecs%60)
	}
	return fmt.Sprintf("%ds", tsecs)
}

// Yuan2Fen CNY：元转分
func Yuan2Fen(yuan float64) int64 {
	return int64(math.Ceil(yuan * 100))
}
