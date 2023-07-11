package netsrv

import (
	"encoding/binary"
	"errors"
	"fmt"
	"fringe/cfg"
	"io"
	"net"
	"regexp"
	"sync"
	"time"
)

type Client struct {
	Addr   string
	Closed bool
	Lock   *sync.Mutex
	net.Conn
	InitRouter
	RetriesTimes uint8
}

func NewTcpClient(addr string, core InitRouter) (*Client, error) {
	var client = &Client{
		Addr:       addr,
		InitRouter: core,
		Lock:       new(sync.Mutex),
		Closed:     true,
	}

	return client.connect()
}

func (t *Client) connect() (*Client, error) {
	if !t.IsTcpClient() {
		return t, nil
	}

	var ipRegex = regexp.MustCompile(`\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b`)
	if len(ipRegex.FindString(t.Addr)) == 0 {
		return t, errors.New(fmt.Sprintf("ip[%s]地址无效", t.Addr))
	}

	t.Lock.Lock()
	defer t.Lock.Unlock()
	t.Closed = true
	var conn, err = net.DialTimeout("tcp", t.Addr, time.Second*15)
	if err != nil {
		return t, errors.New(err.Error())
	}
	cfg.Log.Info(fmt.Sprintf("[%s]: [%s]链接已建立", t.Name(), t.Addr))
	t.Conn = conn
	t.Closed = false
	t.RetriesTimes = 0

	return t, nil
}

func (t *Client) Write(protocol uint32, b []byte) (int, error) {
	if t.Closed {
		return 0, errors.New("tcp connection closed")
	}

	var header = make([]byte, t.HeaderLen())
	var pLen = t.ProtocolLen()
	switch t.ProtocolLen() {
	case 2:
		binary.LittleEndian.PutUint16(header[:pLen], uint16(protocol))
	case 4:
		binary.LittleEndian.PutUint32(header[:pLen], protocol)
	case 8:
		binary.LittleEndian.PutUint64(header[:pLen], uint64(protocol))
	default:
		return 0, errors.New(fmt.Sprintf("ProtocolLen = %d 设置错误", pLen))
	}

	switch t.HeaderLen() - t.ProtocolLen() {
	case 2:
		binary.LittleEndian.PutUint16(header[pLen:t.HeaderLen()], uint16(len(b)))
	case 4:
		binary.LittleEndian.PutUint32(header[pLen:t.HeaderLen()], uint32(len(b)))
	case 8:
		binary.LittleEndian.PutUint64(header[pLen:t.HeaderLen()], uint64(len(b)))
	default:
		return 0, errors.New(fmt.Sprintf("contentLen = %d 设置错误", t.HeaderLen()-pLen))
	}

	t.Lock.Lock()
	defer t.Lock.Unlock()
	return t.Conn.Write(append(header, b...))
}

func (t *Client) HandleConn() error {
	if t.Conn == nil {
		return nil
	}
	var reader = NewClientReader(t)
	for {
		var err = reader.Read()
		if err != nil && err != io.EOF {
			cfg.Log.Error(fmt.Sprintf("[%s]: %s", t.Name(), err.Error()))
			return err
		}
	}
}

type ClientReader struct {
	srv         *Client
	buff        []byte
	start       uint
	end         uint
	maxBuffSize uint
}

func NewClientReader(srv *Client) *ClientReader {
	return &ClientReader{
		srv:         srv,
		buff:        make([]byte, MaxBuffSize),
		start:       0,
		end:         0,
		maxBuffSize: MaxBuffSize,
	}
}

func (r *ClientReader) Read() error {
	for {
		if r.start > 0 {
			copy(r.buff, r.buff[r.start:r.end])
			r.end -= r.start
			r.start = 0
		}

		if r.end > r.maxBuffSize {
			return fmt.Errorf("read buffer overflow")
		}

		n, err := r.srv.Read(r.buff[r.end:])
		if err != nil {
			return err
		}

		r.end += uint(n)
		if err = r.processData(); err != nil {
			return err
		}
	}
}

func (r *ClientReader) processData() error {
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
		var protoId uint32
		switch pLen {
		case 2:
			protoId = uint32(binary.LittleEndian.Uint16(headerData[:pLen]))
		case 4:
			protoId = binary.LittleEndian.Uint32(headerData[:pLen])
		case 8:
			protoId = uint32(binary.LittleEndian.Uint64(headerData[:pLen]))
		default:
			return errors.New(fmt.Sprintf("HeaderLen [%s]tcp无法正常解析", r.srv.Name()))

		}
		var bodyData = make([]byte, contentLen)
		copy(bodyData, r.buff[start:start+contentLen])

		r.srv.Callback(protoId, bodyData)

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
