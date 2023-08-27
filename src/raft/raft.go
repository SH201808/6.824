package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"log"
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

type State uint8

const (
	Leader State = iota
	Candidate
	Follower
)

var (
	disConnectTime       = 1500 * time.Millisecond
	AppendEntriesTimeOut = 6 * time.Second
	heartBeatTime        = 1 * time.Second
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Command interface{}
	Term    int
	Index   int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	applyCh     chan ApplyMsg
	state       State
	currentTerm int
	votedFor    int
	logs        []LogEntry
	LeaderTime  time.Time

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	rf.mu.Unlock()
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

type AppendEntriesArgs struct {
	ReqId int

	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	IsConnect bool
	Term      int
	Success   bool
	Server    int

	XTerm  int
	XIndex int
	XLen   int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reqId := rand.Intn(100000)
	if args.ReqId != 0 {
		reqId = args.ReqId
	}
	if len(args.Entries) != 0 {
		log.Printf("appendEntries, me: %v, args: %+v\n", rf.me, *args)
	}
	rf.mu.Lock()
	defer func() {
		if reply.Success {
			oldCommitIndex := rf.commitIndex
			log.Printf("reqId: %v, oldCommitIndex: %v\n", reqId, oldCommitIndex)
			if args.LeaderCommit > rf.commitIndex {
				log.Printf("min, reqId: %v, args.LeaderCommit: %v, lastIndex: %v", reqId, args.LeaderCommit, rf.logs[len(rf.logs)-1].Index)
				rf.commitIndex = min(args.LeaderCommit, rf.logs[len(rf.logs)-1].Index)
			}
			for _, entry := range rf.logs[oldCommitIndex+1 : rf.commitIndex+1] {
				log.Printf("reqId: %v, apply: %v\n", reqId, entry.Command)
				rf.applyCh <- ApplyMsg{Command: entry.Command, CommandIndex: entry.Index, CommandValid: true}
			}
		}
		rf.LeaderTime = time.Now()
		rf.mu.Unlock()
	}()

	if args.Term < rf.currentTerm && rf.state != Candidate {
		log.Printf("reqId: %v, rf.me: %v, args.Term: %v < rf.currentTerm:%v\n", reqId, rf.me, args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		return
	}
	if rf.state == Candidate || rf.state == Leader {
		log.Printf("reqId: %v, candidate or leader become follower, rf.me: %v\n", reqId, rf.me)
		rf.votedFor = args.LeaderId
		rf.state = Follower
		rf.currentTerm = args.Term
	}
	// 心跳包
	if len(args.Entries) == 0 {
		reply.Success = true
		log.Printf("reply heartBeat reqId: %v, me: %v\n", reqId, rf.me)
		return
	}
	logsLength := len(rf.logs)
	// 没有日志直接添加
	if logsLength == 0 {
		rf.logs = append(rf.logs, args.Entries...)
		log.Println("success")
		reply.Success = true
		return
	}
	// 日志比主节点少
	if logsLength < args.PrevLogIndex {
		reply.XLen = logsLength
		log.Println("less")
		return
	}
	// 日志比主节点多
	if logsLength > args.PrevLogIndex+1 {
		log.Printf("rf.me: %v reqId: %v, more than master\n", rf.me, reqId)
		rf.logs = rf.logs[:args.PrevLogIndex+1]
	}
	prevLogIndex := args.PrevLogIndex
	myPrevTerm := rf.logs[prevLogIndex].Term
	if rf.logs[prevLogIndex].Term == args.PrevLogTerm {
		rf.logs = append(rf.logs, args.Entries...)
		//log.Printf("success command: %v\n", args.Entries[0].Command)
		reply.Success = true
		return
	} else {
		reply.XTerm = myPrevTerm
		reply.XLen = logsLength
		log.Printf("reqId: %v, myterm: %v\n", reqId, myPrevTerm)
		for prevLogIndex = prevLogIndex - 1; prevLogIndex >= 1; prevLogIndex-- {
			if rf.logs[prevLogIndex].Term != myPrevTerm {
				log.Println("rf.logs[prevLogIndex].Term != myPrevTerm")
				reply.XIndex = prevLogIndex + 1
				return
			}
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, ch chan *AppendEntriesReply) {
	reply := &AppendEntriesReply{}
	if len(args.Entries) != 0 {
		log.Printf("reqId: %v, me: %v,sendAppendEntries: %v\n", args.ReqId, rf.me, server)
	} else {
		log.Printf("heartBeat reqId: %v, me: %v, server: %v, commitIndex: %v", args.ReqId, rf.me, server, args.LeaderCommit)
	}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	reply.Server = server
	if !ok {
		log.Printf("appendEntries not ok,server: %v, rf.me: %v, rf.state: %v, args: %+v\n", server, rf.me, rf.state, args)
	} else {
		reply.IsConnect = true
	}
	if len(args.Entries) != 0 {
		log.Printf("rf.me: %v, reqId: %v, reply: %+v\n", rf.me, args.ReqId, *reply)
	}
	ch <- reply
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	reqId := rand.Intn(100000)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Printf("reqId: %v, args: %+v,requestVote: %d\n", reqId, args, rf.me)
	if args.Term < rf.currentTerm {
		log.Printf("reqId: %v,me: %v, args.Term: %v, rf.currentTerm: %v\n", reqId, rf.me, args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if rf.votedFor != -1 && args.Term == rf.currentTerm {
		log.Printf("rf.votedFor: %v, args.CandidateId: %v\n", rf.votedFor, args.CandidateId)
		reply.VoteGranted = false
		return
	}
	// 如果两个日志的任期不同，则更高任期的日志较新；如果两个日志的任期相同，则索引位置大的日志较新。
	myLog := rf.logs[len(rf.logs)-1]
	if myLog.Term > args.LastLogTerm || (myLog.Term == args.LastLogTerm && myLog.Index > args.LastLogIndex) {
		log.Printf("reqId: %v,me is update: myLogTerm: %v, myLogIndex: %v\n", reqId, myLog.Term, myLog.Index)
		reply.VoteGranted = false
		return
	}
	// args.Term > rf.currentTerm，所以leader降为从节点
	if rf.state == Candidate || rf.state == Leader {
		if rf.state == Leader {
			log.Printf("rf.me: %v, leader come to follower when requestVote\n", rf.me)
		}
		rf.state = Follower
	}
	rf.votedFor = args.CandidateId
	rf.currentTerm = args.Term
	rf.LeaderTime = time.Now()

	reply.VoteGranted = true
	return
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, ch chan *RequestVoteReply) {
	reply := &RequestVoteReply{}
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		log.Printf("sendRequestVote not ok, server: %v, me: %v\n", server, rf.me)
	}
	ch <- reply
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	reqId := rand.Intn(1000000)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader := rf.state == Leader

	// Your code here (2B).
	if !isLeader {
		return index, rf.currentTerm, isLeader
	}

	log.Printf("leader: %v\n", rf.me)

	newLog := LogEntry{Command: command, Term: rf.currentTerm, Index: len(rf.logs)}
	log.Printf("command: %v\n", command)
	prevLogIndex, prevLogTerm := 0, 0
	currentTerm := rf.currentTerm
	successReply := make([]bool, len(rf.peers))

	ch := make(chan *AppendEntriesReply)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		nextIndex := rf.nextIndex[i]
		var entries []LogEntry
		if nextIndex != len(rf.logs) {
			entries = rf.logs[nextIndex:]
		}
		entries = append(entries, newLog)
		if len(rf.logs) > 1 {
			// 日志索引以 1 开始
			prevLogIndex = rf.logs[nextIndex-1].Index
			prevLogTerm = rf.logs[nextIndex-1].Term
		}

		args := AppendEntriesArgs{Term: currentTerm, LeaderId: rf.me, PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm, Entries: entries, LeaderCommit: rf.commitIndex}
		args.ReqId = reqId
		go rf.sendAppendEntries(i, &args, ch)
	}

	successCnt := 1
	receiveCnt := 1
	successLeast := len(rf.peers)

	for {
		select {
		case reply := <-ch:
			receiveCnt++
			// 连接在，但不成功，可能是主节点已经不是最新的主节点了 || 主从节点任期不匹配
			if !reply.Success && reply.IsConnect {
				if reply.XLen != 0 {
					newNextIndex := 0
					// 从节点日志数量少于主节点
					if reply.XTerm == 0 && reply.XIndex == 0 {
						newNextIndex = reply.XLen + 1
					} else {
						// 如果返回的冲突任期在主节点的日志中不存在，则 nextIndex = 从节点冲突任期的第一条日志索引
						// 存在，则 nextIndex = 主节点在该任期下的最后一条日志索引
						for i := len(rf.logs) - 1; i >= 0; i-- {
							if rf.logs[i].Term == reply.XTerm {
								newNextIndex = i + 1
								break
							}
						}
						if newNextIndex == 0 {
							newNextIndex = reply.XIndex
						}
					}
					rf.nextIndex[reply.Server] = newNextIndex

					continue
				}
				if reply.Term > rf.currentTerm {
					log.Printf("reqId: %v, reply.Term: %v, rf.currentTerm: %v\n", reqId, reply.Term, rf.currentTerm)
					rf.state = Candidate
					return index, currentTerm, isLeader
				}
			}
			if reply.Success {
				successReply[reply.Server] = true
				successCnt++
			}
			if !reply.IsConnect {
				//go rf.Retry(reply, ch)
				successLeast--
			}
			if receiveCnt == len(rf.peers) {
				goto done
			}
			//case <-time.After(AppendEntriesTimeOut):
			//	goto done
			//}
		}
	}
done:
	if successCnt*2 >= successLeast {
		rf.logs = append(rf.logs, newLog)

		index = len(rf.logs) - 1
		nextIndex := len(rf.logs)
		for i := 0; i < len(rf.peers); i++ {
			if successReply[i] {
				rf.nextIndex[i] = nextIndex
				rf.matchIndex[i] = nextIndex - 1
			}
		}
		log.Printf("nextIndex: %+v\n", rf.nextIndex)
		if successCnt*2 >= len(rf.peers) {
			oldCommitIndex := rf.commitIndex
			rf.commitIndex = len(rf.logs) - 1
			log.Printf("rf.me: %v, rf.state: %v,oldCommitIndex: %v, commitIndex: %v\n", rf.me, rf.state, oldCommitIndex, rf.commitIndex)
			for _, entry := range rf.logs[oldCommitIndex+1 : rf.commitIndex+1] {
				rf.applyCh <- ApplyMsg{Command: entry.Command, CommandValid: true, CommandIndex: entry.Index}
			}
		}
	}

	log.Printf("command: %v, receiveCnt: %v, successCnt: %v, successLeast: %v\n", command, receiveCnt, successCnt, successLeast)
	log.Printf("index: %v\n", index)
	return index, currentTerm, isLeader
}

func (rf *Raft) Retry(oldReply *AppendEntriesReply, oldCh chan *AppendEntriesReply) {
	rf.mu.Lock()
	nextIndex := rf.nextIndex[oldReply.Server]
	logLength := len(rf.logs)
	currentTerm := rf.currentTerm
	rf.mu.Unlock()
	ch := make(chan *AppendEntriesReply)
	for i := 2; nextIndex >= 0 && logLength-i >= 0; i++ {
		prevLogIndex := rf.logs[logLength-i].Index
		prevLogTerm := rf.logs[logLength-i].Term
		entries := []LogEntry{rf.logs[logLength-i]}
		args := AppendEntriesArgs{Term: currentTerm, LeaderId: rf.me, PrevLogTerm: prevLogTerm, PrevLogIndex: prevLogIndex, Entries: entries, LeaderCommit: prevLogIndex}
		go rf.sendAppendEntries(oldReply.Server, &args, ch)
	}
	for {
		select {
		case reply := <-ch:
			if reply.Success {
				oldCh <- &AppendEntriesReply{Success: true, Term: reply.Term}
			}
			if !reply.Success && !reply.IsConnect {
				oldCh <- reply
			}
		case <-time.After(5 * time.Second):
			log.Println("retry timeout")
			return
		}
	}
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.state == Leader {
			rf.mu.Unlock()
			continue
		}
		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		rf.mu.Lock()
		if rf.votedFor != -1 && rf.votedFor != rf.me {
			subTime := time.Now().Add(-disConnectTime)
			if !rf.LeaderTime.Before(subTime) {
				rf.mu.Unlock()
				continue
			}
			log.Printf("rf.me: %v, disconn\n, subTime: %v, leaderTime: %v\n", rf.me, subTime, rf.LeaderTime)
		}

		rf.state = Candidate
		rf.currentTerm++
		rf.votedFor = rf.me

		voteSum := 1
		ch := make(chan *RequestVoteReply, 5)
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			lastLog := rf.logs[len(rf.logs)-1]
			args := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIndex: lastLog.Index, LastLogTerm: lastLog.Term}
			// todo 如果收到信息，有节点的 term 高过自己，也变为 follwer
			go rf.sendRequestVote(i, &args, ch)
		}
		receiveCnt := 0
		for {
			select {
			case reply := <-ch:
				if reply.VoteGranted {
					voteSum++
				}
				receiveCnt++
				if receiveCnt == len(rf.peers)-1 {
					goto done
				}
			case <-time.After(500 * time.Millisecond):
				goto done
			}
		}
	done:
		// 票数过半是否需要不再发起 vote 请求
		if voteSum*2 > len(rf.peers) {
			rf.nextIndex = make([]int, len(rf.peers))
			rf.matchIndex = make([]int, len(rf.peers))
			for i := 0; i < len(rf.peers); i++ {
				rf.nextIndex[i] = 1
				rf.matchIndex[i] = 0
			}
			rf.state = Leader
			//rf.commitIndex++
		} else {
			rf.currentTerm--
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) HeartBeat() {
	reqId := rand.Intn(100000)
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			continue
		}
		commitIndex := rf.commitIndex
		rf.mu.Unlock()
		ch := make(chan *AppendEntriesReply, 5)

		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			args := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, LeaderCommit: commitIndex}
			args.ReqId = reqId
			go rf.sendAppendEntries(i, &args, ch)
		}
		receiveCnt := 0
		successSum := 1
		for {
			select {
			case reply := <-ch:
				if reply.Success {
					successSum++
				} else {
					if rf.currentTerm < reply.Term {
						rf.mu.Lock()
						rf.state = Follower
						rf.currentTerm = reply.Term
						rf.mu.Unlock()
						goto done
					}
				}
				//log.Printf("rf.me: %v, reqId: %v, reply: %+v\n", rf.me, reqId, *reply)

				receiveCnt++
				if receiveCnt == len(rf.peers)-1 {
					goto done
				}
			// 设置超时时间过长，导致阻塞在这里，后续 heartBeat，暂停, 之前设置，是因为可能死锁超时
			case <-time.After(500 * time.Millisecond):
				goto done
			}

		}
	done:
		if successSum*2 < len(rf.peers) {
			log.Printf("reqId: %v,rf.me: %v, heartBeatSuccessSum: %v, len(rf.peers): %v,leader to follower\n", reqId, rf.me, successSum, len(rf.peers))
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// Make the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	log.SetFlags(log.Llongfile | log.Lmicroseconds)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.logs = make([]LogEntry, 1)
	rf.applyCh = applyCh
	rf.state = Follower
	rf.votedFor = -1
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.HeartBeat()

	return rf
}
