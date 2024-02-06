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
	serverId    int
	peerIds     []int
	cm          *Raft
	storage     Storage
	rpcProxy    *RPCProxy
	rpcServer   *rpc.Server
	peerClients map[int]*rpc.Client
	listener    net.Listener
	commitChan  chan<- CommitEntry
	ready       <-chan any
	quit        chan any
	wg          sync.WaitGroup
}

func NewServer(sid int, peerIds []int, ready <-chan any, commitChan chan<- CommitEntry) *Server {
	return &Server{
		serverId:    sid,
		peerIds:     peerIds,
		peerClients: make(map[int]*rpc.Client),
		storage:     NewStorage(),
		ready:       ready,
		commitChan:  commitChan,
		quit:        make(chan any),
	}
}

func (s *Server) Serve() {
	s.mu.Lock()
	s.cm = NewConsensusModule(s)

	s.rpcServer = rpc.NewServer()
	s.rpcProxy = &RPCProxy{
		rf: s.cm,
	}
	_ = s.rpcServer.RegisterName("Raft", s.rpcProxy)

	var err error
	s.listener, err = net.Listen("tcp", ":0")
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("[%v] listening at %s", s.serverId, s.listener.Addr())
	s.mu.Unlock()

	go func() {
		s.wg.Add(1)
		defer s.wg.Done()

		for true {
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

	for i, client := range s.peerClients {
		if client != nil {
			_ = client.Close()
			s.peerClients[i] = nil
		}
	}
}

func (s *Server) Shutdown() {
	s.cm.Stop()
	close(s.quit)
	_ = s.listener.Close()
}

func (s *Server) Call(id int, srvMethod string, args, reply interface{}) error {
	s.mu.Lock()
	var peer = s.peerClients[id]
	s.mu.Unlock()

	if peer != nil {
		return peer.Call(srvMethod, args, reply)
	}

	return fmt.Errorf("call client %d after is's closed", id)
}

func (s *Server) GetListenAddr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.listener.Addr()
}

func (s *Server) ConnectToPeer(peerId int, addr net.Addr) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.peerClients[peerId] == nil {
		var c, err = rpc.DialHTTP(addr.Network(), addr.String())
		if err != nil {
			return err
		}

		s.peerClients[peerId] = c
	}
	return nil

}

func (s *Server) DisconnectPeer(peerId int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var c, ok = s.peerClients[peerId]
	if !ok || c == nil {
		return nil
	}

	s.peerClients[peerId] = nil
	return c.Close()
}

type RPCProxy struct {
	rf *Raft
}

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
