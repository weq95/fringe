package netsrv

import (
	"errors"
	"fmt"
	"fringe/cfg"
	"go.uber.org/zap"
	"os"
	"strings"
	"time"
)

const MaxBuffSize = 8192

type Manager struct {
	clientConn map[string]*Client
	serverConn map[string]*Server
}

type InitRouter interface {
	Name() string
	IsTcpClient() bool
	Init()
	//头部占用字节: 协议号占用字节 + 内容占用字节
	HeaderLen() uint
	// 协议号占用字节
	ProtocolLen() uint
	// Callback 示例: Callback(protoId uint32, data []byte)
	Callback(args ...interface{})
}

var root = &Manager{
	clientConn: map[string]*Client{},
	serverConn: map[string]*Server{},
}

func GetManager() *Manager {
	return root
}

func GetManagerClient(name string) (*Client, bool) {
	var srv, ok = root.clientConn[name]

	return srv, ok
}

func GetManagerServer() (*Server, bool) {
	var name = cfg.Val(func(cfg *cfg.AppCfg) interface{} {
		return cfg.ServerTcpAddr
	}).(string)
	var srv, ok = root.serverConn[name]

	return srv, ok
}

func (m *Manager) AddClient(ipRouter map[string]InitRouter) (*Manager, error) {
	for addr, router := range ipRouter {
		if "" == strings.Trim(addr, " ") {
			return m, errors.New("链接地址必须填写")
		}

		if _, ok := m.clientConn[router.Name()]; ok {
			var message = fmt.Sprintf("[%s]名称已存在", router.Name())
			return m, errors.New(message)
		}

		router.Init()
		var client, err = NewTcpClient(addr, router)
		if err != nil {
			return m, err
		}

		m.clientConn[router.Name()] = client
	}

	return m.loopRead()
}

func (m *Manager) loopRead() (*Manager, error) {
	for key, srv := range m.clientConn {
		go func(key string, srv *Client) {
		Reconnect:
			if err := srv.HandleConn(); err != nil {
				srv.Lock.Lock()
				m.clientConn[srv.Name()].Closed = true
				m.clientConn[srv.Name()].RetriesTimes += 1
				if srv.Conn != nil {
					_ = srv.Conn.Close()
				}
				srv.Lock.Unlock()
				_ = cfg.Val(func(c *cfg.AppCfg) interface{} {
					if m.clientConn[srv.Name()].RetriesTimes > c.RetriesTimes {
						zap.L().Error("tcp拨号超出最大重试次数, 程序退出中...")
						os.Exit(2)
					}
					return nil
				})

				<-time.After(time.Second * 3)
				srv, _ = srv.connect()
				m.clientConn[key] = srv
				goto Reconnect
			}
		}(key, srv)
	}

	return m.heartbeat()
}

func (m *Manager) heartbeat() (*Manager, error) {
	go func() {
		var interval = time.Second * 15
		var timeDate = time.NewTicker(interval)
		defer timeDate.Stop()

		for true {
			select {
			case <-timeDate.C:
				for _, server := range m.clientConn {
					if server.IsTcpClient() {
						if !server.Closed {
							_, _ = server.Write(1000, []byte("ping"))
						}
					}
				}
			}
		}
	}()

	zap.L().Info("tcp 心跳检测已开启...")
	return m, nil
}

func (m *Manager) ClientClosed() {
	for _, server := range m.clientConn {
		if server.Conn != nil {
			m.clientConn[server.Name()].Closed = true
			_ = server.Conn.Close()
		}
	}
}

func (m *Manager) AddServer(ipRouter map[string]TcpClients) (*Manager, error) {
	for addr, initRouter := range ipRouter {
		var srv, err = NewServer(addr, initRouter)
		if err != nil {
			return m, err
		}

		m.serverConn[addr] = srv
	}

	return m, nil
}

func (m *Manager) ServerClosed() {
	for _, server := range m.serverConn {
		_ = server.Close()
	}
}
