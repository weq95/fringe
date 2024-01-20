package netsrv

import (
	"encoding/binary"
	"errors"
	"fmt"
	"fringe/cfg"
	"go.uber.org/zap"
	"net"
	"os"
	"strings"
	"sync"
)

type Server struct {
	Addr string
	net.Listener
	lock *sync.RWMutex
	TcpClients
}

type TcpClients interface {
	InitRouter
	AddClient(string, net.Conn)
	Serviceable(ip string, port uint16)
	GetClientIps() []string
	DeleteClient(string)
	CenterForwarding(string, uint32, []byte) error
}

func NewServer(addr string, router TcpClients) (*Server, error) {
	var listener, err = net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	var server = &Server{
		Addr:       addr,
		Listener:   listener,
		lock:       new(sync.RWMutex),
		TcpClients: router,
	}

	go server.TcpAccept()

	return server, nil
}

func (s *Server) TcpAccept() {
	for true {
		var conn, err = s.Accept()
		if err != nil {
			zap.L().Error(err.Error())
			return
		}

		go func() {
			var clientAddr = strings.Split(conn.RemoteAddr().String(), ":")[0]
			if status, _ := cfg.LocalIp(clientAddr); status {
				type PublicIP struct {
					Origin string
				}

				var ipInfo PublicIP
				if err = cfg.HttpGet("https://httpbin.org/ip", &ipInfo); err != nil {
					zap.L().Error("公网IP获取错误, 程序退出...", zap.Error(err))
					os.Exit(2)
				}

				clientAddr = ipInfo.Origin
			}

			s.TcpClients.AddClient(clientAddr, conn)
			zap.L().Info(fmt.Sprintf("%s client接入", clientAddr))
			defer func(clientAddr string) {
				_ = conn.Close()
				s.TcpClients.DeleteClient(clientAddr)
			}(clientAddr)

			_, _ = s.Write(1001, []byte(clientAddr), conn) // 获取服务端口
			_ = NewServerReader(s).Read(conn)
		}()
	}
}

func (s *Server) Write(protocol uint32, b []byte, conn net.Conn) (int, error) {
	var header = make([]byte, s.HeaderLen())
	var pLen = s.ProtocolLen()
	switch s.ProtocolLen() {
	case 2:
		binary.LittleEndian.PutUint16(header[:pLen], uint16(protocol))
	case 4:
		binary.LittleEndian.PutUint32(header[:pLen], protocol)
	case 8:
		binary.LittleEndian.PutUint64(header[:pLen], uint64(protocol))
	default:
		return 0, errors.New(fmt.Sprintf("ProtocolLen = %d 设置错误", pLen))
	}

	switch s.HeaderLen() - s.ProtocolLen() {
	case 2:
		binary.LittleEndian.PutUint16(header[pLen:], uint16(len(b)))
	case 4:
		binary.LittleEndian.PutUint32(header[pLen:], uint32(len(b)))
	case 8:
		binary.LittleEndian.PutUint64(header[pLen:], uint64(len(b)))
	default:
		return 0, errors.New(fmt.Sprintf("contentLen = %d 设置错误", s.HeaderLen()-pLen))
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	return conn.Write(append(header, b...))
}

type ServerReader struct {
	srv         InitRouter
	buff        []byte
	start       uint
	end         uint
	maxBuffSize uint
}

func NewServerReader(srv *Server) *ServerReader {
	return &ServerReader{
		srv:         srv,
		buff:        make([]byte, MaxBuffSize),
		start:       0,
		end:         0,
		maxBuffSize: MaxBuffSize,
	}
}

func (r *ServerReader) Read(conn net.Conn) error {
	for {
		if r.start > 0 {
			copy(r.buff, r.buff[r.start:r.end])
			r.end -= r.start
			r.start = 0
		}

		if r.end > r.maxBuffSize {
			return fmt.Errorf("read buffer overflow")
		}

		n, err := conn.Read(r.buff[r.end:])
		if err != nil {
			return err
		}

		r.end += uint(n)
		if err = r.processData(conn); err != nil {
			return err
		}
	}
}

func (r *ServerReader) processData(conn net.Conn) error {
	var hLen = r.srv.HeaderLen()
	var pLen = r.srv.ProtocolLen()
	for {
		if r.end-r.start < hLen {
			return nil
		}

		var headerData = r.buff[r.start : r.start+hLen]
		var contentLen uint
		switch hLen - pLen {
		case 2:
			contentLen = uint(binary.LittleEndian.Uint16(headerData[pLen:hLen]))
		case 4:
			contentLen = uint(binary.LittleEndian.Uint32(headerData[pLen:hLen]))
		case 8:
			contentLen = uint(binary.LittleEndian.Uint64(headerData[pLen:hLen]))
		default:
			return errors.New(fmt.Sprintf("contentLength [%s]tcp无法正常解析", r.srv.Name()))
		}
		if r.end-r.start-hLen < contentLen {
			return nil
		}

		var start = r.start + hLen
		var protocol uint32
		switch pLen {
		case 2:
			protocol = uint32(binary.LittleEndian.Uint16(headerData[:pLen]))
		case 4:
			protocol = binary.LittleEndian.Uint32(headerData[:pLen])
		case 8:
			protocol = uint32(binary.LittleEndian.Uint64(headerData[:pLen]))
		default:
			return errors.New(fmt.Sprintf("HeaderLen [%s]tcp无法正常解析", r.srv.Name()))

		}
		var bodyData = make([]byte, contentLen)
		copy(bodyData, r.buff[start:start+contentLen])

		r.srv.Callback(protocol, bodyData, func(protocol uint32, data []byte) {
			if _, err := r.srv.(*Server).Write(protocol, data, conn); err != nil {
				zap.L().Error(err.Error(), zap.Binary("data", data))
			}
		})

		r.start += hLen + contentLen
		if r.end-r.start >= r.maxBuffSize {
			return fmt.Errorf("processData buffer overflow")
		}

		if r.start == r.end {
			r.start = 0
			r.end = 0
			break
		}
	}

	return nil
}
