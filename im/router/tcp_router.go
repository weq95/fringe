package router

import (
	"fmt"
	"fringe/cfg"
	"fringe/im/business"
	"fringe/netsrv"
	"go.uber.org/zap"
)

type TcpRouter struct {
	actions map[uint32]interface{}
}

func (t TcpRouter) Name() string {
	return "client"
}

func (t TcpRouter) Init() {
	t.actions[1000] = business.Heartbeat
	t.actions[1001] = business.GetSrvPort
	t.actions[2001] = business.SingleChatForward
	t.actions[2002] = business.GroupChatForward
}

func (t TcpRouter) IsTcpClient() bool {
	return true
}

func (w TcpRouter) HeaderLen() uint {
	return 6
}

func (t TcpRouter) ProtocolLen() uint {
	return 4
}

func (t TcpRouter) Callback(args ...interface{}) {
	if len(args) != 2 {
		return
	}

	var protocol = args[0].(uint32)
	var handler, ok = t.actions[protocol]
	if !ok {
		cfg.Log.Error(fmt.Sprintf("(%d) 协议号不存在", protocol))
		return
	}

	switch fn := handler.(type) {
	case func([]byte, func(uint32, []byte)):
		fn(args[1].([]byte), t.Write)
	case func(uint64, []byte, func(uint32, []byte)):
		fn(0, args[1].([]byte), t.Write)
	}
}

func NewTcpRouter() *TcpRouter {
	return &TcpRouter{
		actions: make(map[uint32]interface{}, 0),
	}
}

func (t TcpRouter) Write(protocol uint32, b []byte) {
	var srv, ok = netsrv.GetManagerClient(t.Name())
	if !ok {
		cfg.Log.Error(fmt.Sprintf("client(%s) not found.", t.Name()))
		return
	}

	if _, err := srv.Write(protocol, b); err != nil {
		cfg.Log.Error(err.Error(),
			zap.Uint32("protocol", protocol),
			zap.Binary("data", b))
	}
}
