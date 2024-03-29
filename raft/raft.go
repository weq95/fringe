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
	Follower  = iota //追随者
	Candidate        //候选人
	Leader           //领导者
	Dead             //已下线
)

type CommitEntry struct {
	// 正在提交的客户端命令
	Command interface{}
	// 已提交[commit]的日志索引
	Index int
	// 提交客户端名的 Raft 术语，任期
	Term int
}

type LogEntry struct {
	Command interface{} //提交的命令
	Term    int         //任期
}

type Raft struct {
	mu                 sync.Mutex
	id                 int                // 服务器id
	peerIds            []int              // 所有的服务器ID
	server             *Server            // 是此服务器，负责发起RPC调用
	storage            Storage            // 存储器
	commitChan         chan<- CommitEntry // 将报告已提交日志
	newCommitReadyChan chan struct{}      // 内部通知通道，用于提交新日志
	triggerAEChan      chan struct{}      // 内部通知通道，用于向关注着发送AE通知
	currentTerm        int                // Raft集群当前任期
	votedFor           int                // 领导者服务器id
	log                []LogEntry         // 目前未实现日志清理逻辑
	commitIndex        int                // 所有节点都认可的日志index
	applied            int                // 等待同步到数据库的startIndex
	state              CmState            // 服务状态
	electionResetEvent time.Time          // 选举重置时间
	nextIndex          map[int]int        // 追随者下次需要commitIndex数据索引
	matchIndex         map[int]int        // 每台机器已提交的日志条目索引
}

func NewRaft(srv *Server) *Raft {
	var cm = &Raft{
		id:                 srv.id,
		peerIds:            srv.peerIds,
		server:             srv,
		storage:            srv.storage,
		commitChan:         srv.commitChan,
		newCommitReadyChan: make(chan struct{}, 16),
		triggerAEChan:      make(chan struct{}, 1),
		state:              Follower,
		votedFor:           -1,
		commitIndex:        -1,
		applied:            -1,
		nextIndex:          make(map[int]int),
		matchIndex:         make(map[int]int),
	}
	if cm.storage.HasData() {
		cm.restoreFromStorage()
	}

	go func() {
		// 开始睡眠，直到收到领导人选举信号， 进行选举倒计时
		<-time.After(cm.electionTimeout())
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
	Term         int //任期
	CandidateId  int //候选人ID
	LastLogIndex int //最新的日志索引
	LastLogTerm  int //最新的任期
}

type RequestVoteReply struct {
	Term        int  //任期
	VoteGranted bool //是否同意
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
	cm.dlog("[%d]成为追随者 term=%d; log=%v", cm.id, term, cm.log)
	cm.state = Follower
	cm.currentTerm = term
	cm.votedFor = -1
	cm.electionResetEvent = time.Now()

	go cm.runElectionTimer()
}

// RequestVote 任期投票表决
func (cm *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// 已下线
	if cm.state == Dead {
		return nil
	}
	var lastLogIndex, lastLogTerm = cm.lastLogIndexAndTerm()
	cm.dlog("开始选举: %+v [cTeam=%d, vFor=%d, log index/term=(%d, %d)]",
		args, cm.currentTerm, cm.votedFor, lastLogIndex, lastLogTerm)

	if args.Term > cm.currentTerm {
		// 变更位追随者
		cm.dlog("... 主动降级为追随者 RequestVote")
		cm.becomeFollower(args.Term)
	}

	reply.VoteGranted = false
	if cm.currentTerm == args.Term && // 是领导者
		(cm.votedFor == -1 || cm.votedFor == args.CandidateId) && //给自己投票或是投给往期领导者
		// 是最新的任期并且日志也是最新的
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
		reply.VoteGranted = true
		cm.votedFor = args.CandidateId
		cm.electionResetEvent = time.Now()
	}

	// 把自身这边的任期返回
	reply.Term = cm.currentTerm
	cm.persistToStorage()
	cm.dlog(".. 选举结果 reply: %+v", reply)
	return nil
}

type AppendEntriesArgs struct {
	Term         int        //任期
	LeaderId     int        //领导者机器ID
	PrevLogIndex int        //上一条日志index
	PrevLogTerm  int        //上一条日志任期
	Entries      []LogEntry //leader期待follower复制的日志条目
	LeaderCommit int        //已提交日志的index
}

type AppendEntriesReply struct {
	Term          int  //任期
	Success       bool //是否赞成
	ConflictIndex int  //再次选举时，follower日志长度，如果 PrevLogIndex>len(log)时，此时重置为没有任期
	ConflictTerm  int  //正常情况下为再次选举的日志节点和任期
}

// AppendEntries 领导者给追随者同步日志条目接口
func (cm *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.state == Dead {
		return nil
	}

	cm.dlog("AppendEntries: %+v", args)
	var legal = args.Term == cm.currentTerm
	// 任期不一致，发起选举
	// 是同一任期，并且不是追随者，发起选举
	if args.Term > cm.currentTerm || (legal && cm.state != Follower) {
		cm.dlog("... 任期太小(%d) || 不是追随者(%s)，再次发起选举请求 AppendEntries", cm.currentTerm, cm.state.String())
		cm.becomeFollower(args.Term)
	}

	reply.Term = cm.currentTerm
	defer cm.persistToStorage()
	if !legal {
		return nil
	}

	// 更新选举重置时间，选举定时器一直在检测这个过期时间
	cm.electionResetEvent = time.Now()

	// args.PrevLogIndex == -1 说明没有数据
	if args.PrevLogIndex == -1 ||
		// 如果 Follower 节点的上一个日志长度不足（args.PrevLogIndex < len(raft.log)） 则会进行拒绝
		// 并且保证上一届日志任期相同
		(args.PrevLogIndex < len(cm.log) && args.PrevLogTerm == cm.log[args.PrevLogIndex].Term) {
		reply.Success = true

		var logInsertIndex = args.PrevLogIndex + 1 //这里保证了，返回的日志条目一定是从下一笔开始
		var newEntriesIndex = 0

		for {
			// 达到本地日志最大索引 或 提交日志读取完成
			if logInsertIndex >= len(cm.log) || newEntriesIndex >= len(args.Entries) {
				break
			}

			// 任期不一致，终止插入的索引
			if cm.log[logInsertIndex].Term != args.Entries[newEntriesIndex].Term {
				break
			}

			logInsertIndex++
			newEntriesIndex++
		}

		// follower(跟随者)和leader(领导者) 数据不一致的记录，日志少了，把少的日志添加到当前节点
		// 出现不一致的原因：follower 节点会将leader节点发送的日志条目添加到自己的日志中，并将自己节点任期更新到leader节点的任期
		// 1.leader节点的任期 > follower节点的任期
		// 2.leader节点发送的日志条目包含follower节点尚未复制的已提交日志条目
		if newEntriesIndex < len(args.Entries) {
			cm.dlog("... 添加日志：%+v entries %v from index %d", cm.log, args.Entries[newEntriesIndex:], logInsertIndex)

			// 当 follower 节点收到 leader 节点发送的 AppendEntries RPC 时，如果发现任期不一致，则需要根据具体情况判断是否需要将 leader 节点发送的日志条目添加到自己的日志中。
			// 如果 leader 节点的任期大于 follower 节点的任期，则 follower 节点需要将 leader 节点发送的所有日志条目添加到自己的日志中，即使任期不一致
			cm.log = append(cm.log[:logInsertIndex], args.Entries[newEntriesIndex:]...)
			cm.dlog("... 最新日志数据： %+v", cm.log)
		}

		// 重置提交记录的index，发起日志同步信号
		// 这里会把会把数据同步到 commitChan 中
		if args.LeaderCommit > cm.commitIndex {
			cm.commitIndex = min(args.LeaderCommit, len(cm.log)-1)
			cm.dlog("... 节点设置commit日志索引 commitIndex=%d", cm.commitIndex)
			cm.newCommitReadyChan <- struct{}{}
		}

		return nil
	}

	//follower 拒绝了 leader本次的请求
	//reply.ConflictIndex
	//reply.ConflictTerm
	//这里是为帮助leader快速找到冲突而存在的
	if args.PrevLogIndex >= len(cm.log) {
		// 回复自己的日志长度， 以便快速进入选举流程
		reply.ConflictIndex = len(cm.log)
		reply.ConflictTerm = -1
	} else {
		reply.ConflictTerm = cm.log[args.PrevLogIndex].Term
		var i int

		//follower寻找冲突点的index，就是任期不同的数据
		for i = args.PrevLogIndex - 1; i >= 0; i-- {
			if cm.log[i].Term != reply.ConflictTerm {
				break
			}
		}

		reply.ConflictIndex = i + 1
	}

	return nil
}

// 获取选举超时时间
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
	var termStarted = cm.currentTerm //当前任期
	cm.mu.Unlock()
	cm.dlog("election timer started (%v), term=%d", timeout, termStarted)
	var ticker = time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		<-ticker.C
		cm.mu.Lock()

		// 候选人和追随者
		// 这里是自己被选为leader
		if cm.state != Candidate && cm.state != Follower {
			cm.dlog("退选：state=%s, 即不会追随者也不是候选人", cm.state)
			cm.mu.Unlock()
			return
		}

		// 已经出结果了，终止选举流程
		if termStarted != cm.currentTerm {
			cm.dlog("任期(term)已更新，主动退选： %d to %d",
				termStarted, cm.currentTerm)
			cm.mu.Unlock()
			return
		}

		// 选举超时无回复，则开始推荐自身成为领导者
		var elapsed = time.Since(cm.electionResetEvent)
		if elapsed >= timeout {
			cm.startElection()
			cm.mu.Unlock()
			return
		}
		cm.mu.Unlock()
	}
}

// 开始选举
func (cm *Raft) startElection() {
	cm.state = Candidate //成为候选人
	cm.currentTerm += 1
	var currentTerm = cm.currentTerm   //当前任期
	cm.electionResetEvent = time.Now() //重置时间
	cm.votedFor = cm.id                //投票给自己
	cm.dlog("成为候选人 (currentTerm=%d); log=%+v", currentTerm, cm.log)

	var votesReceived = 1
	for _, peerId := range cm.peerIds {
		go func(peerId int) {
			cm.mu.Lock()
			var savedLastIndex, savedLastLogTerm = cm.lastLogIndexAndTerm()
			cm.mu.Unlock()
			var args = RequestVoteArgs{
				Term:         currentTerm,
				CandidateId:  cm.id,
				LastLogIndex: savedLastIndex,
				LastLogTerm:  savedLastLogTerm,
			}

			cm.dlog("发起候选人请求： RequestVote to %d: %+v", peerId, args)
			var reply RequestVoteReply
			var err = cm.server.Call(peerId, "Raft.RequestVote", args, &reply)
			if err != nil {
				return
			}

			cm.mu.Lock()
			defer cm.mu.Unlock()

			cm.dlog("其他人投票回复信息 RequestVoteReply： %+v", reply)
			if cm.state != Candidate {
				cm.dlog("本节点已不是候选人，主动退出选举流程, state = %v", cm.state)
				return
			}

			// 别人任期大于自己，主动降级退选
			if reply.Term > currentTerm {
				cm.dlog("候选人任期已过期 RequestVoteReply")
				cm.becomeFollower(reply.Term)
				return
			}

			if reply.Term == currentTerm && reply.VoteGranted {
				// 集群内一半以上投票赞成，则将自身升级为leader
				votesReceived += 1
				if votesReceived*2 > len(cm.peerIds)+1 {
					cm.dlog("[%d]当选为领导者，获得票数： %d", cm.id, votesReceived)
					cm.startLeader()
				}

			}
		}(peerId)
	}

	// 运行另一个选举定时器，以防止本次选举不成功
	go cm.runElectionTimer()
}

// 开始成为领导者
// 这里默认 50*time.Millisecond 进行日志同步
func (cm *Raft) startLeader() {
	// 将自己设置为领导者
	cm.state = Leader

	//进行日志同步准备
	for _, id := range cm.peerIds {
		cm.nextIndex[id] = len(cm.log)
		cm.matchIndex[id] = -1
	}
	cm.dlog("正式成为领导者：term=%d, nextIndex=%v, matchIndex=%v; log=%v",
		cm.currentTerm, cm.nextIndex, cm.matchIndex, cm.log)

	go func(heartbeat time.Duration) {
		//发起日志同步请求
		//如果本任期不合法，会自动将自己降级为追随者
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

// 这个请求必然是领导者发起，向所有等待着发送一轮AE并等待结果
func (cm *Raft) leaderSendAEs() {
	cm.mu.Lock()
	// 只能有leader发起
	if cm.state != Leader {
		cm.mu.Unlock()
		return
	}

	var currentTerm = cm.currentTerm
	cm.mu.Unlock()

	// 领导者向每个节点同步信息
	for _, peerId := range cm.peerIds {
		go func(peerId int) {
			cm.mu.Lock()
			var ni = cm.nextIndex[peerId]
			var prevLogIndex = ni - 1 //上一条日志的索引
			var prevLogTerm = -1      //上一条日志的任期

			if prevLogIndex >= 0 {
				prevLogTerm = cm.log[prevLogIndex].Term
			}
			var entries = cm.log[ni:] //leader希望follower复制的下一个日志条目
			var args = AppendEntriesArgs{
				Term:         currentTerm,
				LeaderId:     cm.id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: cm.commitIndex,
			}
			cm.mu.Unlock()
			cm.dlog("领导者同步日志条目： AppendEntries to %v: ni=%d, logLen=%d, args=%+v", peerId, ni, len(cm.log), args)
			var reply AppendEntriesReply
			var err = cm.server.Call(peerId, "Raft.AppendEntries", args, &reply)
			if err != nil {
				fmt.Println("rpc 领导者请求错误： ", err)
				return
			}

			cm.mu.Lock()
			defer cm.mu.Unlock()
			// 当前节点任期 < 节点最新的任期，执行降级，本节点强制为跟随者
			if reply.Term > cm.currentTerm {
				cm.dlog("领导者节点任期已过期")
				cm.becomeFollower(reply.Term)
				return
			}

			// 是领导者 && 最新term，否则进行日志同步
			if !(cm.state == Leader && currentTerm == reply.Term) {
				return
			}

			if reply.Success {
				cm.nextIndex[peerId] = ni + len(entries)         //表明日志同步成功，更新index
				cm.matchIndex[peerId] = cm.nextIndex[peerId] - 1 //上一条日志的index

				var savedCommitIndex = cm.commitIndex //旧的已提交的commitIndex
				// 这里是为了优化程序，只检测最新的数据
				for i := cm.commitIndex + 1; i < len(cm.log); i++ {
					//倒叙搜索，如果任期相同
					if cm.log[i].Term == cm.currentTerm {
						var matchCount = 1
						for _, id := range cm.peerIds {
							if cm.matchIndex[id] >= i {
								matchCount++
							}
						}

						//得到一半以上的节点认可,更新commitIndex
						if matchCount*2 > len(cm.peerIds)+1 {
							cm.commitIndex = i
						}
					}
				}

				cm.dlog("领导者 （可忽略的日志）AppendEntries reply from %d success: nextIndex := %v, matchIndex := %v; commitIndex := %d",
					peerId, cm.nextIndex, cm.matchIndex, cm.commitIndex)
				if cm.commitIndex != savedCommitIndex {
					// 有新的数据需要同步给其他从服务器，leader -> follower
					// 向追随者发送 AE 来通知追随者
					cm.dlog("数据已被集群认可，领导者通知追随者更新 commitIndex = %d", cm.commitIndex)
					cm.newCommitReadyChan <- struct{}{}

					// 这里通知信号， 等待 100ms 通知其他服务器更新commitIndex
					// 相当于重调 cm.leaderSendAEs() 避免了多次调用
					cm.triggerAEChan <- struct{}{}
				}

				return
			}

			// follower拒绝了本次追加【AppendEntriesRPC】
			// 1.可能是日志不一致
			// 2.任期过时
			// 拒绝后默认再次同步 ConflictIndex 之后的日志条目，同样领导者也会重新设置该机器的 cm.nextIndex[peerId]
			cm.nextIndex[peerId] = reply.ConflictIndex
			if reply.ConflictTerm >= 0 {
				// follower节点在某个特定任期上存在冲突，leader在自己的日志中找到该任期对应的日志条目
				// 并且从该位置开始重新发送 AppendEntriesRPC
				var lastIndexOfTerm = -1
				for i := len(cm.log) - 1; i >= 0; i-- {
					if cm.log[i].Term == reply.ConflictTerm {
						lastIndexOfTerm = i
						break
					}
				}

				// 修改追随者服务器认为合法的日志索引
				// 这里的matchIndex未被更新
				if lastIndexOfTerm >= 0 {
					cm.nextIndex[peerId] = lastIndexOfTerm + 1
				}
			}
			cm.dlog("追随者拒绝领导者本次同步请求 AppendEntries reply from %d: nextIndex=%d", peerId, ni-1)
		}(peerId)
	}
}

// 等待同步信号，进行数据同步
// 同步的数据： log[applied+1:commitIndex+1]
func (cm *Raft) commitChanSender() {
	for range cm.newCommitReadyChan {
		cm.mu.Lock()
		var savedTerm = cm.currentTerm
		var savedApplied = cm.applied

		var entries []LogEntry
		if cm.commitIndex > cm.applied {
			//有新的已提交的日志条目需要被应用到状态机
			entries = cm.log[cm.applied+1 : cm.commitIndex+1]
			cm.applied = cm.commitIndex
		}
		cm.mu.Unlock()
		cm.dlog("commitChanSender entries=%v, savedApplied=%d", entries, savedApplied)

		for i, entry := range entries {

			// 数据在这里进行持久化操作，目前并未正常处理，所以先注释
			// cm.commitChan <- CommitEntry{}
			_ = CommitEntry{
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
