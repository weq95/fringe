package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"
)

type CmState int

func (c CmState) String() string {
	switch c {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	case Dead:
		return "Dead"
	default:
		panic("unreachable")
	}
}

var DebugCM = 1

const (
	Follower = iota
	Candidate
	Leader
	Dead
)

type CommitEntry struct {
	// 正在提交的客户端命令
	Command interface{}
	// 提交客户端命令的日志索引
	Index int
	// 提交客户端名的 Raft 术语
	Term int
}

type LogEntry struct {
	Command interface{}
	Term    int
}

type Raft struct {
	mu                 sync.Mutex
	id                 int // 服务器id
	peerIds            []int
	server             *Server            // 是此服务器，负责发起RPC调用
	storage            Storage            // 存储器
	commitChan         chan<- CommitEntry //将报告已提交日志
	newCommitReadyChan chan struct{}      //内部通知通道，用于提交新日志
	triggerAEChan      chan struct{}      //内部通知通道，用于向关注着发送AE通知
	currentTerm        int                //Raft集群当前领导者
	votedFor           int                //其实就是服务器id
	log                []LogEntry
	commitIndex        int //当前已提交的index
	lastApplied        int
	state              CmState
	electionResetEvent time.Time
	nextIndex          map[int]int
	matchIndex         map[int]int
}

func NewConsensusModule(srv *Server) *Raft {
	var cm = &Raft{
		id:                 srv.serverId,
		peerIds:            srv.peerIds,
		server:             srv,
		storage:            srv.storage,
		commitChan:         srv.commitChan,
		newCommitReadyChan: make(chan struct{}),
		triggerAEChan:      make(chan struct{}, 1),
		state:              Follower,
		votedFor:           -1,
		commitIndex:        -1,
		lastApplied:        -1,
		nextIndex:          make(map[int]int),
		matchIndex:         make(map[int]int),
	}
	if cm.storage.HasData() {
		cm.restoreFromStorage()
	}

	go func() {
		// 开始睡眠，直到收到领导人选举信号， 进行选举倒计时
		<-srv.ready
		cm.mu.Lock()
		cm.electionResetEvent = time.Now()
		cm.mu.Unlock()
		cm.runElectionTimer()
	}()

	go cm.commitChanSender()
	return cm
}

func (cm *Raft) Stop() {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	cm.state = Dead
	cm.dlog("becomes Dead")
	close(cm.newCommitReadyChan)
}

func DebugClose() {
	DebugCM = 0
}

func (cm *Raft) dlog(format string, args ...interface{}) {
	if DebugCM > 0 {
		format = fmt.Sprintf("[%d] ", cm.id) + format
		log.Printf(format, args...)
	}
}

func (cm *Raft) restoreFromStorage() {
	var decodeFn = func(key string, value interface{}) {
		var termData, ok = cm.storage.Get(key)
		if !ok {
			log.Fatal(fmt.Sprintf("%s not found in storage", key))
			return
		}
		var d = gob.NewDecoder(bytes.NewBuffer(termData))
		if err := d.Decode(value); err != nil {
			log.Fatal(err)
		}
	}
	decodeFn("currentTerm", &cm.currentTerm)
	decodeFn("votedFor", &cm.votedFor)
	decodeFn("log", &cm.log)
}

func (cm *Raft) persistToStorage() {
	var setFn = func(key string, data interface{}) {
		var buffer bytes.Buffer
		var err = gob.NewEncoder(&buffer).Encode(data)
		if err != nil {
			log.Fatal(err)
		}

		cm.storage.Set(key, buffer.Bytes())
	}

	setFn("currentTerm", cm.currentTerm)
	setFn("votedFor", cm.votedFor)
	setFn("log", cm.log)
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (cm *Raft) lastLogIndexAndTerm() (int, int) {
	if len(cm.log) == 0 {
		return -1, -1
	}

	var lastIndex = len(cm.log) - 1

	return lastIndex, cm.log[lastIndex].Term
}

// 变更为追随者
func (cm *Raft) becomeFollower(term int) {
	cm.dlog("becomes Follower with term=%d; log=%v", term, cm.log)
	cm.state = Follower
	cm.currentTerm = term
	cm.votedFor = -1
	cm.electionResetEvent = time.Now()

	go cm.runElectionTimer()
}

func (cm *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// 已停机不在通信
	if cm.state == Dead {
		return nil
	}
	var lastLogIndex, lastLogTerm = cm.lastLogIndexAndTerm()
	cm.dlog("RequestVote: %+v [currentTerm=%d, votedFor=%d, log index/term=(%d, %d)]",
		args, cm.currentTerm, cm.votedFor, lastLogIndex, lastLogTerm)

	if args.Term > cm.currentTerm {
		// 变更位追随者
		cm.dlog("... term out of date in RequestVote")
		cm.becomeFollower(args.Term)
	}

	reply.VoteGranted = false
	if cm.currentTerm == args.Term && // 是领导者
		(cm.votedFor == -1 || cm.votedFor == args.CandidateId) &&
		// 提交记录是最新的
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
		reply.VoteGranted = true
		cm.votedFor = args.CandidateId
		cm.electionResetEvent = time.Now()
	}

	reply.Term = cm.currentTerm
	cm.persistToStorage()
	cm.dlog(".. Request reply: %+v", reply)
	return nil
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

func (cm *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.state == Dead {
		return nil
	}

	cm.dlog("AppendEntries: %+v", args)
	if args.Term > cm.currentTerm {
		cm.dlog("... term out of date in AppendEntries")
		cm.becomeFollower(args.Term)
	}

	if args.Term == cm.currentTerm {
		if cm.state != Follower {
			cm.becomeFollower(args.Term)
		}
		cm.electionResetEvent = time.Now()

		if args.PrevLogIndex == -1 ||
			(args.PrevLogIndex < len(cm.log) && args.PrevLogTerm == cm.log[args.PrevLogIndex].Term) {
			reply.Success = true

			var logInsertIndex = args.PrevLogIndex + 1
			var newEntriesIndex = 0

			for {
				if logInsertIndex >= len(cm.log) || newEntriesIndex >= len(args.Entries) {
					break
				}

				// 领导者不一样
				if cm.log[logInsertIndex].Term != args.Entries[newEntriesIndex].Term {
					break
				}

				logInsertIndex++
				newEntriesIndex++
			}

			// 添加领导者数据不一致的记录
			if newEntriesIndex < len(args.Entries) {
				cm.dlog("... inserting entries %v from index %d", args.Entries[newEntriesIndex:], logInsertIndex)
				cm.log = append(cm.log[:logInsertIndex], args.Entries[newEntriesIndex:]...)
				cm.dlog("... log is now: %v", cm.log)
			}

			// 重置提交记录的index
			if args.LeaderCommit > cm.commitIndex {
				cm.commitIndex = min(args.LeaderCommit, len(cm.log)-1)
				cm.dlog("... setting commitIndex=%d", cm.commitIndex)
				cm.newCommitReadyChan <- struct{}{}
			}
		} else {
			if args.PrevLogIndex >= len(cm.log) {
				// 重置请求的领导者， 以便快速进入选举流程
				reply.ConflictIndex = len(cm.log)
				reply.ConflictTerm = -1
			} else {
				reply.ConflictTerm = cm.log[args.PrevLogIndex].Term
				var i int
				for i = args.PrevLogIndex - 1; i >= 0; i-- {
					if cm.log[i].Term != reply.ConflictTerm {
						break
					}
				}

				reply.ConflictIndex = i + 1
			}
		}
	}

	reply.Term = cm.currentTerm
	cm.persistToStorage()
	cm.dlog("AppendEntries reply: &+v", *reply)
	return nil
}

func (cm *Raft) electionTimeout() time.Duration {
	if len(os.Getenv("RAFT_FORCE_MORE_REELECTION")) > 0 && rand.Intn(3) == 0 {
		return time.Duration(150) + time.Millisecond
	}

	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

// 选举定时器
func (cm *Raft) runElectionTimer() {
	var timeout = cm.electionTimeout()
	cm.mu.Lock()
	var termStarted = cm.currentTerm
	cm.mu.Unlock()
	cm.dlog("election timer started (%v), term=%d", timeout, termStarted)
	var ticker = time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		<-ticker.C
		cm.mu.Lock()

		// 候选人和追随者
		if cm.state != Candidate && cm.state != Follower {
			cm.dlog("in election timer state=%s, bailing out", cm.state)
			cm.mu.Unlock()
			return
		}

		if termStarted != cm.currentTerm {
			cm.dlog("in election timer term changed from %d to %d, bailing out",
				termStarted, cm.currentTerm)
			cm.mu.Unlock()
			return
		}

		cm.mu.Unlock()
	}
}

// 开始选举
func (cm *Raft) startElection() {
	cm.state = Candidate //直接成为候选人
	cm.currentTerm += 1
	var savedCurrentTerm = cm.currentTerm
	cm.electionResetEvent = time.Now()
	cm.votedFor = cm.id
	cm.dlog("becomes Candidate (currentTerm=%d); log=%v", savedCurrentTerm, cm.log)

	var votesReceived = 1
	for _, peerId := range cm.peerIds {
		go func(peerId int) {
			cm.mu.Lock()
			var savedLastIndex, savedLastLogTerm = cm.lastLogIndexAndTerm()
			cm.mu.Unlock()
			var args = RequestVoteArgs{
				Term:         savedCurrentTerm,
				CandidateId:  cm.id,
				LastLogIndex: savedLastIndex,
				LastLogTerm:  savedLastLogTerm,
			}

			cm.dlog("sending RequestVote to %d: %+v", peerId, args)
			var reply RequestVoteReply
			var err = cm.server.Call(peerId, "Raft.RequestVote", args, &reply)
			if err != nil {
				return
			}

			cm.mu.Lock()
			defer cm.mu.Unlock()

			cm.dlog("received RequestVoteReply %+v", reply)
			if cm.state != Candidate {
				cm.dlog("while waiting for reply, state = %v", cm.state)
				return
			}

			if reply.Term > savedCurrentTerm {
				cm.dlog("term out of date in RequestVoteReply")
				cm.becomeFollower(reply.Term)
				return
			}

			if reply.Term == savedCurrentTerm {
				if reply.VoteGranted {
					votesReceived += 1
					if votesReceived*2 > len(cm.peerIds)+1 {
						cm.dlog("wins election with %d votes", votesReceived)
						cm.startLeader()
					}
				}
			}
		}(peerId)
	}
}

// 开始成为领导者
func (cm *Raft) startLeader() {
	cm.state = Leader

	for _, id := range cm.peerIds {
		cm.nextIndex[id] = len(cm.log)
		cm.matchIndex[id] = -1
	}
	cm.dlog("becomes Leader; term=%d, nextIndex=%v, matchIndex=%v; log=%v",
		cm.currentTerm, cm.nextIndex, cm.matchIndex, cm.log)

	go func(heartbeat time.Duration) {
		cm.leaderSendAEs()
		var t = time.NewTimer(heartbeat)
		defer t.Stop()

		for {
			var doSend = false
			select {
			case <-t.C:
				doSend = true
				t.Stop()
				t.Reset(heartbeat)

			case _, ok := <-cm.triggerAEChan:
				if !ok {
					return
				}
				doSend = true
				if !t.Stop() {
					<-t.C
				}
				t.Reset(heartbeat)
			}

			if doSend {
				cm.mu.Lock()
				if cm.state != Leader {
					cm.mu.Unlock()
					return
				}

				cm.mu.Unlock()
				cm.leaderSendAEs()
			}
		}
	}(50 * time.Millisecond)
}

// 向所有等待着发送一轮AE并等待结果
func (cm *Raft) leaderSendAEs() {
	cm.mu.Lock()
	if cm.state != Leader {
		cm.mu.Unlock()
		return
	}

	var savedCurrentTerm = cm.currentTerm
	cm.mu.Unlock()

	for _, peerId := range cm.peerIds {
		go func(peerId int) {
			cm.mu.Lock()
			var ni = cm.nextIndex[peerId]
			var prevLogIndex = ni - 1
			var prevLogTerm = -1

			if prevLogIndex >= 0 {
				prevLogTerm = cm.log[prevLogIndex].Term
			}
			var entries = cm.log[ni:]
			var args = AppendEntriesArgs{
				Term:         savedCurrentTerm,
				LeaderId:     cm.id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: cm.commitIndex,
			}
			cm.mu.Unlock()
			cm.dlog("sending AppendEntries to %v: ni=%d, args=%+v", peerId, ni, args)
			var reply AppendEntriesReply
			var err = cm.server.Call(peerId, "Raft.AppendEntries", args, &reply)
			if err != nil {
				log.Fatal(err)
				return
			}

			cm.mu.Lock()
			defer cm.mu.Unlock()
			// 执行降级，本节点不在是领导者
			if reply.Term > cm.currentTerm {
				cm.dlog("term out of date in heartbeat reply")
				cm.becomeFollower(reply.Term)
				return
			}

			// 是领导者 && 最新term
			if !(cm.state == Leader && savedCurrentTerm == reply.Term) {
				return
			}

			if reply.Success {
				cm.nextIndex[peerId] = ni + len(entries)
				cm.matchIndex[peerId] = cm.nextIndex[peerId] - 1

				var savedCommitIndex = cm.commitIndex
				for i := cm.commitIndex + 1; i < len(cm.log); i++ {
					if cm.log[i].Term == cm.currentTerm {
						var matchCount = 1
						for _, id := range cm.peerIds {
							if cm.matchIndex[id] >= i {
								matchCount++
							}
						}

						if matchCount*2 > len(cm.peerIds)+1 {
							cm.commitIndex = 1
						}
					}
				}

				cm.dlog("AppendEntries reply from %d success: nextIndex := %v, matchIndex := %v; commitIndex := %d",
					peerId, cm.nextIndex, cm.matchIndex, cm.commitIndex)
				if cm.commitIndex != savedCommitIndex {
					// 领导者认为需要提交新条目
					// 在提交通道上向该领导者的客户发送新条目，并通过向追随者发送 AE 来通知追随者
					cm.dlog("leader sets commitIndex = %d", cm.commitIndex)
					cm.newCommitReadyChan <- struct{}{}
					cm.triggerAEChan <- struct{}{}
				}

				return
			}

			cm.nextIndex[peerId] = reply.ConflictIndex
			if reply.ConflictTerm >= 0 {
				// 更新任期
				var lastIndexOfTerm = -1
				for i := len(cm.log) - 1; i >= 0; i-- {
					if cm.log[i].Term == reply.ConflictTerm {
						lastIndexOfTerm = i
						break
					}
				}

				if lastIndexOfTerm >= 0 {
					cm.nextIndex[peerId] = lastIndexOfTerm + 1
				}
			}
			cm.dlog("AppendEntries reply from %d !success: nextIndex := %d", peerId, ni-1)
		}(peerId)
	}
}

func (cm *Raft) commitChanSender() {
	for range cm.newCommitReadyChan {
		cm.mu.Lock()
		var savedTerm = cm.currentTerm
		var savedApplied = cm.lastApplied

		var entries []LogEntry
		if cm.commitIndex > cm.lastApplied {
			entries = cm.log[cm.lastApplied+1 : cm.commitIndex+1]
			cm.lastApplied = cm.commitIndex
		}
		cm.mu.Unlock()
		cm.dlog("commitChanSender entries=%v, savedApplied=%d", entries, savedApplied)

		for i, entry := range entries {
			cm.commitChan <- CommitEntry{
				Command: entry.Command,
				Index:   savedApplied + i + 1,
				Term:    savedTerm,
			}
		}
	}

	cm.dlog("commitChanSender done")
}

// Report 报告该节点状态
func (cm *Raft) Report() (int, int, bool) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	return cm.id, cm.currentTerm, cm.state == Leader
}

// Submit 当且仅当该 CM 是领导者时，它返回 true - 在这种情况下，命令被接受。
// 如果返回 false，客户端将必须找到不同的 CM 来提交此命令
func (cm *Raft) Submit(cmd interface{}) bool {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.dlog("Submit received by %v: %v", cm.state, cmd)

	if cm.state != Leader {
		return false
	}

	cm.log = append(cm.log, LogEntry{Command: cmd, Term: cm.currentTerm})
	cm.persistToStorage()
	cm.dlog("... log=%v", cm.log)
	cm.triggerAEChan <- struct{}{}

	return true
}
