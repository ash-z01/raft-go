package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// Debug 开关
const DebugCM = 1

// 日志条目
type LogEntry struct {
	// 命令
	Command interface{}

	// 任期号
	Term int
}

// raft提交到 commit channel 的数据
// 每次提交条目 都会通知客户端 基于本次提交的命令 共识模块 已经达成了一致
// 且可以应用于 客户端状态机
type CommitEntry struct {
	// 正在提交的 客户端命令
	Command interface{}

	// 客户端已提交命令的 日志 索引
	Index int

	// 客户端已提交命令的 Raft 任期号
	Term int
}

type CMRole int

const (
	// 跟随者
	Follower CMRole = iota

	// 候选者
	Candidate

	// 领导者
	Leader

	// 一具尸体
	Dead
)

func (s CMRole) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	case Dead:
		return "Dead"
	default:
		panic("Unreachable")
	}
}

// 共识模块 "核心"
type ConsensusModule struct {
	// 互斥锁
	mu sync.Mutex

	// 当前 CM 的 Server ID
	id int

	// IDs of our peers in the cluster.
	// 集群中的其余成员 的 节点 ID
	peerIDs []int

	// 包含CM的server，用于和其它成员节点进行 RPC通信
	server *Server

	// 存储接口， 持久化 CM 的 state [需要持久化的状态]
	storage Storage

	// CM 用于报告 已提交日志条目 的 channel
	commitChan chan<- CommitEntry

	// go程使用的 内部通知 channel: 提交新的日志条目给log, 并通知这些条目 可能要被传递到 commitChan
	newCommitReadyChan chan struct{}

	// 同样是用于内部通知的 channle: 当[有趣的]变动发生时，向 followers 发送 AE
	triggerAEChan chan struct{}

	/*
		========================================
		所有服务节点上 都需要持久化 的 Raft-state.
		========================================
	*/

	// 当前的任期号
	currentTerm int

	// 给这个 节点[peerID] 投票
	votedFor int

	// 日志
	log []LogEntry

	/*
		========================================
		所有服务节点上 易变的 Raft-state.
		========================================
	*/

	// 最后一条已提交日志的索引
	commitIndex int

	// 最后一条应用到状态机中的日志索引
	lastApplied int

	// 当前节点的角色
	role CMRole

	// 选举重置事件 [时间]
	electionResetEvent time.Time

	/*
		========================================
		Leader 节点上 易变的 Raft-state.
		========================================
	*/

	// 下一次 同步到follower节点时的日志索引  map[peerID][logIndex]
	nextIndex map[int]int

	// follower 节点的最大日志索引
	matchIndex map[int]int
}

// 共识模块 构造函数
// 根据给定的ID, 服务节点 和 服务 和 存储, 创建CM.
// ready channel 给CM发信号，表示所有的节点都已连接，可以安全启动状态机
// commitCh 会被CM用来发送在Raft集群中 已提交的日志条目
func NewConsensusModule(id int, peerIDs []int, server *Server,
	storage Storage, ready <-chan interface{}, commitCh chan<- CommitEntry) *ConsensusModule {
	cm := new(ConsensusModule)
	cm.id = id
	cm.peerIDs = peerIDs
	cm.server = server
	cm.storage = storage
	cm.commitChan = commitCh
	cm.newCommitReadyChan = make(chan struct{}, 16)
	cm.triggerAEChan = make(chan struct{}, 1)
	cm.role = Follower
	cm.votedFor = -1
	cm.commitIndex = -1
	cm.lastApplied = -1
	cm.nextIndex = make(map[int]int)
	cm.matchIndex = make(map[int]int)

	if storage.HasData() {
		cm.restoreFromStorage(cm.storage)
	}

	go func() {
		// CM处于休眠状态，直到收到ready信号
		// 然后，开始 倒计时进入 领导人选举阶段
		<-ready
		cm.mu.Lock()
		cm.electionResetEvent = time.Now()
		cm.mu.Unlock()
		cm.runElectionTimer()
	}()

	go cm.commitChanSender()

	return cm
}

// 报告CM的状态
func (cm *ConsensusModule) Report() (id int, term int, lsLeader bool) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.id, cm.currentTerm, cm.role == Leader
}

// 向CM提交一条命令
// 客户端 读取构造函数中传递的 commitCh，以收到新提交条目的通知
//! return true 当前CM是Leader 提交被接收
//! return false 客户端需要寻找另外的CM去提交
func (cm *ConsensusModule) Submit(cmd interface{}) bool {
	cm.mu.Lock()
	// defer cm.mu.Unlock()

	cm.dlog("Submit received by %v: %v", cm.role, cmd)
	if cm.role == Leader {
		cm.log = append(cm.log, LogEntry{Command: cmd, Term: cm.currentTerm})
		cm.persistToStorage()
		cm.dlog("... log=%v", cm.log)
		cm.mu.Unlock()
		cm.triggerAEChan <- struct{}{}
		return true
	}
	cm.mu.Unlock()
	return false
}

// 停止CM
func (cm *ConsensusModule) Stop() {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.role = Dead
	cm.dlog("CM Stoped, becomes Dead")
	close(cm.newCommitReadyChan)
}

//  从CM的存储中恢复持久化state
//! 应该在构造函数中调用，先于并发的开始
func (cm *ConsensusModule) restoreFromStorage(storage Storage) {
	if termData, found := cm.storage.Get("currentTerm"); found {
		d := gob.NewDecoder(bytes.NewBuffer(termData))
		if err := d.Decode(&cm.currentTerm); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Fatal("currentTerm not found in storage")
	}

	if votedData, found := cm.storage.Get("votedFor"); found {
		d := gob.NewDecoder(bytes.NewBuffer(votedData))
		if err := d.Decode(&cm.votedFor); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Fatal("votedFor not found in storage")
	}

	if logData, found := cm.storage.Get("log"); found {
		d := gob.NewDecoder(bytes.NewBuffer(logData))
		if err := d.Decode(&cm.log); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Fatal("log not found in storage")
	}
}

//  持久化 state 到CM 的存储
//! 需要将CM上锁后处理
func (cm *ConsensusModule) persistToStorage() {
	var termData bytes.Buffer
	if err := gob.NewEncoder(&termData).Encode(cm.currentTerm); err != nil {
		log.Fatal(err)
	}
	cm.storage.Set("currentTerm", termData.Bytes())

	var votedData bytes.Buffer
	if err := gob.NewEncoder(&votedData).Encode(cm.votedFor); err != nil {
		log.Fatal(err)
	}
	cm.storage.Set("votedFor", votedData.Bytes())

	var logData bytes.Buffer
	if err := gob.NewEncoder(&logData).Encode(cm.log); err != nil {
		log.Fatal(err)
	}
	cm.storage.Set("log", logData.Bytes())
}

// dlog 记录debug info日志
//! DebugCM > 0
func (cm *ConsensusModule) dlog(format string, args ...interface{}) {
	if DebugCM > 0 {
		format = fmt.Sprintf("[%d] ", cm.id) + format
		log.Printf(format, args...)
	}
}

/*
	==============================
	RV: RequestVote[RPC]
	==============================
*/

// RV请求[RPC]
type RequestVoteArgs struct {
	// 请求者的任期号
	Term int

	// [候选者]请求者ID
	CandidateId int

	// 最后一条日志的索引
	LastLogIndex int

	// 最后一条日志的任期号
	LastLogTerm int
}

// RV响应[RPC]
type RequestVoteReply struct {
	// 响应者的任期号
	Term int

	// 是否同意 [获得投票?]
	VoteGranted bool
}

func (cm *ConsensusModule) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.role == Dead {
		return nil
	}
	lastLogIndex, lastLogTerm := cm.lastLogIndexAndTerm()
	cm.dlog("RequestVote: %+v [currentTerm=%d, votedFor=%d, log index/term=(%d, %d)]", args, cm.currentTerm, cm.votedFor, lastLogIndex, lastLogTerm)

	if args.Term > cm.currentTerm {
		cm.dlog("... term out of date in RequestVote")
		cm.becomeFollower(args.Term)
	}

	// TODO 请求节点的状态更新更全，就投票给它
	// @1 CM[Candidate]的任期号==请求的任期号
	// @2 CM未投票 || 投给了 请求的候选者
	// @3 请求的 最后一条日志的任期号 > CM的最后一条日志的任期号
	// @4 请求的 最后一条日志的任期号 == CM的最后一条日志任期号
	// @5 请求的 最后一条日志索引 >= CM的最后一条日志索引
	// *   1 && 2 && (3 || (4 && 5))
	if cm.currentTerm == args.Term &&
		(cm.votedFor == -1 || cm.votedFor == args.CandidateId) &&
		(args.LastLogTerm > lastLogTerm ||
			(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {
		reply.VoteGranted = true
		cm.votedFor = args.CandidateId
		cm.electionResetEvent = time.Now()
	} else {
		reply.VoteGranted = false
	}

	reply.Term = cm.currentTerm
	cm.persistToStorage()
	cm.dlog("... RequestVote reply: %+v", reply)

	return nil
}

/*
	==============================
	AE: AppendEntries[RPC]
	==============================
*/

// AE请求[RPC]
type AppendEntriesArgs struct {

	// 领导者的任期号
	Term int

	// 领导者ID，follower可以重定向客户端
	LeaderID int

	// 新日志条目之前的那条日志的索引
	PrevLogIndex int

	// 新日志条目之前的那条日志的任期号
	PrevLogTerm int

	// 需要存储的日志条目
	// * 心跳检测时为空
	Entries []LogEntry

	// 领导者的日志 已提交索引 commitIndex
	LeaderCommit int
}

// AE响应[RPC]
type AppendEntriesReply struct {
	// 跟随者的任期号
	Term int

	// * 返回true 说明跟随者 包含了 匹配 PrevLogIndex 和 PrevLogTerm 的日志条目
	Success bool

	// * Raft Paper:
	// * In practice, we doubt this optimization is necessary,
	// * since failures happen infrequently and it is unlikely that there will be many inconsistent entries.
	ConflictIndex int
	ConflictTerm  int
}

func (cm *ConsensusModule) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.role == Dead {
		return nil
	}
	cm.dlog("AppendEntries: %+v", args)

	if args.Term > cm.currentTerm {
		cm.dlog("... term out of date in AppendEntries")
		cm.becomeFollower(args.Term)
	}

	reply.Success = false
	if args.Term == cm.currentTerm {
		// important only one leader
		if cm.role != Follower {
			cm.becomeFollower(args.Term)
		}
		cm.electionResetEvent = time.Now()

		if args.PrevLogIndex == -1 ||
			(args.PrevLogIndex < len(cm.log) && args.PrevLogTerm == cm.log[args.PrevLogIndex].Term) {
			reply.Success = true

			logInsertIndex := args.PrevLogIndex + 1
			newEntriesIndex := 0

			for {
				if logInsertIndex >= len(cm.log) || newEntriesIndex >= len(args.Entries) {
					break
				}
				if cm.log[logInsertIndex].Term != args.Entries[newEntriesIndex].Term {
					break
				}
				logInsertIndex++
				newEntriesIndex++
			}

			if newEntriesIndex < len(args.Entries) {
				cm.dlog("... inserting entries %v from index %d", args.Entries[newEntriesIndex:], logInsertIndex)
				cm.log = append(cm.log[:logInsertIndex], args.Entries[newEntriesIndex:]...)
				cm.dlog("... log is now: %v", cm.log)
			}

			if args.LeaderCommit > cm.commitIndex {
				cm.commitIndex = intMin(args.LeaderCommit, len(cm.log)-1)
				cm.dlog("... setting commitIndex=%d", cm.commitIndex)
				cm.newCommitReadyChan <- struct{}{}
			}
		} else {
			// TODO 加速 不匹配/冲突 的日志索引和任期的 解决
		}
	}

	reply.Term = cm.currentTerm
	cm.dlog("AppendEntries reply: %+v", *reply)
	return nil
}

// 生成一个伪随机的选举超时时间
func (cm *ConsensusModule) electionTimeout() time.Duration {
	if len(os.Getenv("RAFT_FORCE_MORE_REELECTION")) > 0 && rand.Intn(3) == 0 {
		return time.Duration(150) * time.Millisecond
	} else {
		return time.Duration(150+rand.Intn(150)) * time.Millisecond
	}
}

// 选举超时触发函数，超时则发起新一轮的选举
func (cm *ConsensusModule) runElectionTimer() {
	timeoutDuration := cm.electionTimeout()
	cm.mu.Lock()
	termStarted := cm.currentTerm
	cm.mu.Unlock()
	cm.dlog("election timer started (%v), term=%d", timeoutDuration, termStarted)

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		<-ticker.C

		cm.mu.Lock()
		if cm.role != Candidate && cm.role != Follower {
			cm.dlog("in election timer state=%s, bailing out", cm.role)
			cm.mu.Unlock()
			return
		}

		if termStarted != cm.currentTerm {
			cm.dlog("in election timer term changed from %d to %d, bailing out", termStarted, cm.currentTerm)
			cm.mu.Unlock()
			return
		}

		if elapsed := time.Since(cm.electionResetEvent); elapsed >= timeoutDuration {
			cm.startElection()
			cm.mu.Unlock()
			return
		}
		cm.mu.Unlock()
	}
}

func (cm *ConsensusModule) lastLogIndexAndTerm() (int, int) {
	if len(cm.log) > 0 {
		lastIndex := len(cm.log) - 1
		return lastIndex, cm.log[lastIndex].Term
	}
	return -1, -1
}

// 发起选举
// ! 需要锁住CM
func (cm *ConsensusModule) startElection() {
	cm.role = Candidate
	cm.currentTerm += 1
	savedCurrentTerm := cm.currentTerm
	cm.electionResetEvent = time.Now()
	cm.votedFor = cm.id
	cm.dlog("becomes Candidate (currentTerm=%d); log=%v", savedCurrentTerm, cm.log)

	var votesReceived int32 = 1

	for _, peerID := range cm.peerIDs {
		go func(peerID int) {
			cm.mu.Lock()
			savedLastLogIndex, savedLastLogTerm := cm.lastLogIndexAndTerm()
			cm.mu.Unlock()
			args := RequestVoteArgs{
				Term:         savedCurrentTerm,
				CandidateId:  cm.id,
				LastLogIndex: savedLastLogIndex,
				LastLogTerm:  savedLastLogTerm,
			}

			cm.dlog("sending RequestVote to %d: %+v", peerID, args)
			var reply RequestVoteReply
			err := cm.server.Call(peerID, "ConsensusModule.RequestVote", args, &reply)
			if err != nil {
			} else {
				cm.mu.Lock()
				defer cm.mu.Unlock()
				cm.dlog("received RequestVoteReply %+v", reply)

				if cm.role != Candidate {
					cm.dlog("while waiting for reply, state=%v", cm.role)
					return
				}

				if reply.Term > savedCurrentTerm {
					cm.dlog("term out of date in RequestVoteReply")
					cm.becomeFollower(reply.Term)
					return
				}

				if reply.Term == savedCurrentTerm {
					if reply.VoteGranted {
						votes := int(atomic.AddInt32(&votesReceived, 1))
						if votes*2 > len(cm.peerIDs)+1 {
							cm.dlog("wins election with %d votes", votes)
							cm.startLeader()
							return
						}
					}
				}
			}
		}(peerID)
	}

	// Run another election timer, in case this election is not successful.
	go cm.runElectionTimer()
}

func (cm *ConsensusModule) becomeFollower(term int) {
	cm.dlog("becomes Follower with term=%d; log=%+v", term, cm.log)
	cm.role = Follower
	cm.currentTerm = term
	cm.votedFor = -1
	cm.electionResetEvent = time.Now()

	go cm.runElectionTimer()
}

func (cm *ConsensusModule) startLeader() {
	cm.role = Leader

	for _, peerID := range cm.peerIDs {
		cm.nextIndex[peerID] = len(cm.log)
		cm.matchIndex[peerID] = -1
	}
	cm.dlog("becomes leader; term=%d, nextIndex=%v, matchIndex=%v, log=%v", cm.currentTerm, cm.nextIndex, cm.matchIndex, cm.log)

	go func(heartbeatTimeout time.Duration) {

		cm.leaderSendAEs()

		// ticker := time.NewTicker(50 * time.Millisecond)
		t := time.NewTimer(heartbeatTimeout)
		defer t.Stop()

		for {
			doSend := false
			select {
			case <-t.C:
				doSend = true
				t.Stop()
				t.Reset(heartbeatTimeout)
			case _, ok := <-cm.triggerAEChan:
				if ok {
					doSend = true
				} else {
					return
				}

				if !t.Stop() {
					<-t.C
				}
				t.Reset(heartbeatTimeout)
			}

			if doSend {
				cm.mu.Lock()
				if cm.role != Leader {
					cm.mu.Unlock()
					return
				}
				cm.mu.Unlock()
				cm.leaderSendAEs()
			}
		}
	}(50 * time.Millisecond)
}

func (cm *ConsensusModule) leaderSendAEs() {
	cm.mu.Lock()
	savedCurrentTerm := cm.currentTerm
	cm.mu.Unlock()

	for _, peerID := range cm.peerIDs {
		go func(peerID int) {
			cm.mu.Lock()
			ni := cm.nextIndex[peerID]
			prevLogIndex := ni - 1
			prevLogTerm := -1
			if prevLogIndex >= 0 {
				prevLogTerm = cm.log[prevLogIndex].Term
			}
			entries := cm.log[ni:]
			args := AppendEntriesArgs{
				Term:         savedCurrentTerm,
				LeaderID:     cm.id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: cm.commitIndex,
			}
			cm.mu.Unlock()
			cm.dlog("sending AppendEntries to %v: ni=%d, args=%+v", peerID, ni, args)
			var reply AppendEntriesReply

			err := cm.server.Call(peerID, "ConsensusModule.AppendEntries", args, &reply)
			if err != nil {

			} else {
				cm.mu.Lock()
				defer cm.mu.Unlock()

				if reply.Term > savedCurrentTerm {
					cm.dlog("term out of date in heartbeat reply")
					cm.becomeFollower(reply.Term)
					return
				}

				if cm.role == Leader && savedCurrentTerm == reply.Term {
					if reply.Success {
						cm.nextIndex[peerID] = ni + len(entries)
						cm.matchIndex[peerID] = cm.nextIndex[peerID] - 1
						cm.dlog("AppendEntries reply from %d success: nextIndex := %v, matchIndex := %v", peerID, cm.nextIndex, cm.matchIndex)
						savedCommitIndex := cm.commitIndex
						for i := cm.commitIndex + 1; i < len(cm.log); i++ {
							if cm.log[i].Term == cm.currentTerm {
								matchCount := 1
								for _, peerID := range cm.peerIDs {
									if cm.matchIndex[peerID] >= i {
										matchCount++
									}
								}
								if matchCount*2 > len(cm.peerIDs)+1 {
									cm.commitIndex = i
								}
							}
						}
						if cm.commitIndex != savedCommitIndex {
							cm.dlog("leader sets commitIndex := %d", cm.commitIndex)
							cm.newCommitReadyChan <- struct{}{}
							cm.triggerAEChan <- struct{}{}
						}
					} else {
						// TODO 加速 不匹配/冲突 的日志索引和任期的 解决
						cm.nextIndex[peerID] = ni - 1
						cm.dlog("AppendEntries reply from %d not success: nextIndex := %d", peerID, ni-1)
					}
				}
			}
		}(peerID)
	}
}

func (cm *ConsensusModule) commitChanSender() {
	for range cm.newCommitReadyChan {
		cm.mu.Lock()
		savedTerm := cm.currentTerm
		savedLastApplied := cm.lastApplied
		var entries []LogEntry
		if cm.commitIndex > cm.lastApplied {
			entries = cm.log[cm.lastApplied+1 : cm.commitIndex+1]
			cm.lastApplied = cm.commitIndex
		}
		cm.mu.Unlock()
		cm.dlog("commitChanSender entries=%v, savedLastApplied=%d", entries, savedLastApplied)
		for i, entry := range entries {
			cm.commitChan <- CommitEntry{
				Command: entry.Command,
				Index:   savedLastApplied + i + 1,
				Term:    savedTerm,
			}
		}
	}
	cm.dlog("commitChanSender done")
}

func intMin(a, b int) int {
	if a < b {
		return a
	}
	return b
}
