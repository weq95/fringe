package router

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"fringe/cfg"
	"fringe/im/business"
	"fringe/netsrv"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
	"net/http"
	"strings"
)

var testClient = "postman"

type WsHandler = func(*business.User, []byte)

type WSRouter struct {
	actions map[uint32]WsHandler
	ch      chan *business.UserResponse
}

func (w *WSRouter) Name() string {
	return "ws"
}

func (w *WSRouter) IsTcpClient() bool {
	return false
}

func (w *WSRouter) Init() {
	w.actions[1001] = business.SingleChat
	w.actions[1002] = business.GroupMessage
}

func (w *WSRouter) Callback(args ...interface{}) {
	if len(args) != 3 {
		return
	}

	var protocol = args[0].(uint32)
	var handler, ok = w.actions[protocol]
	if !ok {
		zap.L().Error(fmt.Sprintf("(%d) 协议号不存在", protocol))
		return
	}

	var userInfo, ok01 = business.GetUser(args[1].(uint64))
	if !ok01 {
		return
	}

	handler(userInfo, args[2].([]byte))
}

func NewWsRouter() *WSRouter {
	var router = &WSRouter{
		actions: make(map[uint32]WsHandler, 0),
		ch:      make(chan *business.UserResponse, 2000),
	}

	go router.Write()
	return router
}

func (w *WSRouter) HeaderLen() uint {
	return 6
}

func (w *WSRouter) ProtocolLen() uint {
	return 4
}

func WsUpGrader(ctx *gin.Context) {
	if !websocket.IsWebSocketUpgrade(ctx.Request) {
		ctx.JSON(http.StatusOK, gin.H{
			"code": http.StatusMisdirectedRequest,
			"msg":  "不受支持的请求",
		})
		return
	}

	var ws, wok = netsrv.GetManagerClient(new(WSRouter).Name())
	if !wok {
		ctx.JSON(http.StatusOK, gin.H{
			"code": http.StatusNotFound,
			"msg":  "服务器错误, 未找到服务",
		})
		return
	}

	var token, ok = ctx.Get("user")
	var user, ok01 = token.(*cfg.CustomClaims)
	if !ok || !ok01 {
		ctx.JSON(http.StatusOK, gin.H{
			"code": http.StatusInternalServerError,
			"msg":  "服务器错误, 无法处理该请求",
		})
		return
	}

	var conn, err = (&websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}).Upgrade(ctx.Writer, ctx.Request, nil)
	if err != nil {
		zap.L().Error("ws 升级失败: ", zap.Error(err))
		return
	}

	go ws.InitRouter.(*WSRouter).ReadMsg(conn, user)
}

func (w *WSRouter) Write() {
	var err error
	var logErrFn = func(id uint32, data []byte) {
		zap.L().Error(err.Error(), zap.Uint32("id", id), zap.ByteString("data", data))
	}
	for true {
		select {
		case resp, ok := <-w.ch:
			if !ok {
				zap.L().Warn("chan closed")
				return
			}

			if resp.Client == testClient {
				var result interface{}
				var message = map[string]interface{}{
					"protocol": resp.Protocol,
					"result":   "",
				}
				if err = json.Unmarshal(resp.Data, &result); err != nil {
					message["result"] = string(resp.Data)
				} else {
					message["result"] = result
				}

				var byteData []byte
				if byteData, err = json.Marshal(message); err != nil {
					logErrFn(resp.Protocol, resp.Data)
					continue
				}
				if err = resp.Conn.WriteMessage(websocket.TextMessage, byteData); err != nil {
					logErrFn(resp.Protocol, resp.Data)
				}

				continue
			}

			var header = make([]byte, w.HeaderLen()+uint(len(resp.Data)))
			binary.BigEndian.PutUint32(header[:w.ProtocolLen()], resp.Protocol)
			binary.BigEndian.PutUint16(header[w.ProtocolLen():w.HeaderLen()], uint16(len(resp.Data)))
			if err = resp.Conn.WriteMessage(websocket.BinaryMessage, append(header, resp.Data...)); err != nil {
				logErrFn(resp.Protocol, resp.Data)
			}
		}
	}
}

func (w *WSRouter) ReadMsg(conn *websocket.Conn, info *cfg.CustomClaims) {
	var userInfo = business.NewUser(info.Userid, info.Client, conn, w.ch)
	var err = cfg.GetRedis().HSet(context.TODO(), cfg.UserServer, info.Userid,
		strings.Split(info.ServeIp, ":")[0]).Err()
	if err != nil {
		zap.L().Error("链接升级失败: " + err.Error())
		return
	}
	go business.UserLoginEvent(userInfo, []byte{})
	defer func() {
		_ = conn.Close()
		go business.UserLogOutEvent(userInfo, []byte{})
	}()
	for true {
		var msgType, byteData, err01 = conn.ReadMessage()
		if err01 != nil {
			return
		}

		if info.Client == testClient {
			w.PostmanDebugApi(conn, info.Userid, byteData)
			continue
		}
		if msgType != websocket.BinaryMessage {
			_ = conn.WriteMessage(websocket.TextMessage, []byte("消息格式错误"))
			return
		}

		if uint(len(byteData)) < w.HeaderLen() {
			continue
		}
		var length = binary.BigEndian.Uint16(byteData[w.ProtocolLen():w.HeaderLen()])
		if uint16(len(byteData)) < length+uint16(w.HeaderLen()) {
			continue
		}

		var protocol = binary.BigEndian.Uint32(byteData[:w.ProtocolLen()])

		// 获取 HeaderLen  + bodyLen
		var data = byteData[w.HeaderLen() : length+uint16(w.HeaderLen())]

		w.SendMsg(&Req{
			Protocol: protocol,
			Data:     data,
			Other:    info.Userid,
		})
	}
}

type Req struct {
	Protocol uint32
	Data     []byte
	Other    interface{}
}

func (w *WSRouter) SendMsg(req *Req) {
	if handler, ok := netsrv.GetManagerClient(w.Name()); ok {
		handler.Callback(req.Protocol, req.Other, req.Data)
		return
	}

	zap.L().Error(fmt.Sprintf("%s 驱动不存在", w.Name()))
}

func (w *WSRouter) PostmanDebugApi(conn *websocket.Conn, userid uint64, message []byte) {
	var data map[string]interface{}
	if err := json.Unmarshal(message, &data); err != nil {
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`PostmanDebugApi 数据未成功读取`))
		return
	}

	var protocol, ok = data["protocol"]
	if !ok {
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`PostmanDebugApi 协议号缺失`))
		return
	}

	w.SendMsg(&Req{
		Protocol: uint32(protocol.(float64)),
		Data:     message,
		Other:    userid,
	})
}
