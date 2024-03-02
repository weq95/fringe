package raft

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Server struct {
	mu          sync.Mutex
	id          int                 //该机器编号
	peerIds     []int               //集群中所有的节点
	raft        *Raft               //集群选举算法
	storage     Storage             //存储器
	rpcProxy    *RPCProxy           //转发器
	rpcServer   *rpc.Server         //服务器
	peerClients map[int]*rpc.Client //多个客户端
	listener    net.Listener
	commitChan  chan<- CommitEntry
	quit        chan any
	wg          sync.WaitGroup
}

func NewServer(sid int, peerIds []int, commitChan chan<- CommitEntry) *Server {
	return &Server{
		id:          sid,
		peerIds:     peerIds,
		peerClients: make(map[int]*rpc.Client),
		storage:     NewStorage(),
		commitChan:  commitChan,
		quit:        make(chan any),
	}
}

func (s *Server) Serve() {
	s.mu.Lock()
	s.raft = NewRaft(s)

	s.rpcServer = rpc.NewServer()
	s.rpcProxy = &RPCProxy{
		rf: s.raft,
	}
	_ = s.rpcServer.RegisterName("Raft", s.rpcProxy)

	var err error

	// 操作系统会选择一个可用的端口并将其分配给监听器
	s.listener, err = net.Listen("tcp", ":0")
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("[%v] listening at %s", s.id, s.listener.Addr())
	s.mu.Unlock()

	go func() {
		s.wg.Add(1)
		defer s.wg.Done()

		for {
			var conn, err01 = s.listener.Accept()
			if err01 != nil {
				select {
				case <-s.quit:
					return
				default:
					log.Fatal("accept error:", err01)
				}
			}

			go func() {
				s.wg.Add(1)
				s.rpcServer.ServeConn(conn)
				s.wg.Done()
			}()
		}
	}()
}

func (s *Server) DisconnectAll() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.raft.state = Dead
	s.raft.dlog("%d 服务器中止服务", s.id)

	for i, client := range s.peerClients {
		if client != nil {
			_ = client.Close()
			delete(s.peerClients, i)
		}
	}
}

func (s *Server) Shutdown() {
	s.raft.Stop()
	close(s.quit)
	_ = s.listener.Close()
}

func (s *Server) Call(id int, srvMethod string, args, reply interface{}) error {
	s.mu.Lock()
	var peer, ok = s.peerClients[id]
	s.mu.Unlock()
	if !ok {
		return nil
	}

	return peer.Call(srvMethod, args, reply)
}

func (s *Server) GetListenAddr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.listener.Addr()
}

// ConnectToPeer 加入集群
func (s *Server) ConnectToPeer(peerId int, addr net.Addr) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.peerClients[peerId]; !ok {
		var c, err = rpc.Dial(addr.Network(), addr.String())
		if err != nil {
			return err
		}

		s.peerClients[peerId] = c
	}
	return nil

}

// DisconnectPeer 退出集群
func (s *Server) DisconnectPeer(peerId int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var c, ok = s.peerClients[peerId]
	if !ok {
		return nil
	}

	delete(s.peerClients, peerId)
	return c.Close()
}

type RPCProxy struct {
	rf *Raft
}

// RequestVote 通过 rpc 远程调用的
// dice 是为模拟真实环境使用
// 随机延迟（1-5 毫秒）
// 随机丢包或延迟（10% 概率）
func (r *RPCProxy) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	if len(os.Getenv("RAFT_UNRELIABLE_RPC")) == 0 {
		time.Sleep(time.Duration(1+rand.Intn(5)) * time.Millisecond)
		return r.rf.RequestVote(args, reply)
	}

	var dice = rand.Intn(10)
	if dice == 9 {
		r.rf.dlog("drop RequestVote")
		return fmt.Errorf("RPC faield")
	}

	if dice == 8 {
		r.rf.dlog("delay RequestVote")
		time.Sleep(75 * time.Millisecond)
	}

	return r.rf.RequestVote(args, reply)
}

// AppendEntries 通过 rpc 远程调用的
// dice 是为模拟真实环境使用
// 随机延迟（1-5 毫秒）
// 随机丢包或延迟（10% 概率）
func (r *RPCProxy) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	if len(os.Getenv("RAFT_UNRELIABLE_RPC")) == 0 {
		time.Sleep(time.Duration(1+rand.Intn(5)) * time.Millisecond)
		return r.rf.AppendEntries(args, reply)
	}

	var dice = rand.Intn(10)
	if dice == 9 {
		r.rf.dlog("drop AppendEntries")
		return fmt.Errorf("RPC faield")
	}

	if dice == 8 {
		r.rf.dlog("delay AppendEntries")
		time.Sleep(75 * time.Millisecond)
	}

	return r.rf.AppendEntries(args, reply)
}
