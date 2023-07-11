package router

import (
	"errors"
	"fmt"
	"fringe/center/business"
	"fringe/cfg"
	"fringe/netsrv"
	"go.uber.org/zap"
	"net"
	"strconv"
	"sync"
)

type SrvRouter struct {
	actions map[uint32]func(data []byte, responseWriter func(uint32, []byte))
	clients map[string]*netsrv.Client
}

func (s *SrvRouter) GetClientIps() []string {
	var ips = make([]string, 0, len(s.clients))

	for _, c := range s.clients {
		if !c.Closed {
			ips = append(ips, c.Addr)
		}
	}

	return ips
}

func (s *SrvRouter) Serviceable(ip string, port uint16) {
	if _, ok := s.clients[ip]; !ok {
		return
	}

	s.clients[ip].Closed = false
	s.clients[ip].Addr = ip + ":" + strconv.Itoa(int(port))

	cfg.Log.Info(fmt.Sprintf("%s:%d 服务已开启", ip, port))
}

func (s *SrvRouter) AddClient(key string, conn net.Conn) {
	s.DeleteClient(key)

	var client = &netsrv.Client{
		Conn:   conn,
		Lock:   new(sync.Mutex),
		Closed: true,
	}

	client.InitRouter = s
	s.clients[key] = client
}

func (s *SrvRouter) DeleteClient(key string) {
	// 当一台服务器出现多个客户端时 会被互相挤掉
	// 在这个情况下并没有主动关闭链接
	delete(s.clients, key)
}

// CenterForwarding 中心转发服务
func (s *SrvRouter) CenterForwarding(clientIp string, protocol uint32, data []byte) error {
	var client, ok = s.clients[clientIp]
	if !ok || client.Closed {
		return errors.New(fmt.Sprintf("%s: 服务未开启", clientIp))
	}

	if _, err := client.Write(protocol, data); err != nil {
		cfg.Log.Error(err.Error(), zap.Uint32("id", protocol), zap.Binary("data", data))
		return err
	}

	return nil
}

func (s *SrvRouter) Name() string {
	return "srv-router"
}

func (s *SrvRouter) IsTcpClient() bool {
	return false
}

func (s *SrvRouter) Init() {
	s.actions[1000] = business.Heartbeat
	s.actions[1001] = business.UpdateSrvStatus
}

func (s *SrvRouter) Callback(args ...interface{}) {
	if len(args) != 3 {
		return
	}

	var protocol = args[0].(uint32)
	var handler, ok = s.actions[protocol]
	if !ok {
		cfg.Log.Error(fmt.Sprintf("(%d) 协议号不存在", protocol))
		return
	}

	handler(args[1].([]byte), args[2].(func(uint32, []byte)))
}

func (s *SrvRouter) HeaderLen() uint {
	return 6
}

func (s *SrvRouter) ProtocolLen() uint {
	return 4
}

func NewSrvRouter() *SrvRouter {
	var srv = &SrvRouter{
		actions: make(map[uint32]func([]byte, func(uint32, []byte)), 0),
		clients: make(map[string]*netsrv.Client, 0),
	}

	srv.Init()

	return srv
}
