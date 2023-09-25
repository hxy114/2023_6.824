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
	"6.5840/labgob"
	"bytes"
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

const (
	FOLLOWER   = 1
	CANDIDATER = 2
	LEADER     = 3
)

var incTerm int = 1
var muIncTerm sync.Mutex

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
type LogStruct struct {
	Command interface{}
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	Log           []LogStruct
	CurrentTerm   int
	VoteFor       int
	applyCh       chan ApplyMsg
	submitCond    *sync.Cond
	isAppendEntry bool
	startCount    []int
	startChan     []chan int
	appendCond    *sync.Cond
	state         int
	done          int
	commitIndex   int
	lastApplied   int
	nextIndex     []int
	matchIndex    []int
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	term = rf.CurrentTerm
	if rf.state == LEADER {
		isleader = true
	} else {
		isleader = false
	}
	// Your code here (2A).
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VoteFor)
	e.Encode(rf.Log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var voteFor int
	log := make([]LogStruct, 0, 1000)
	d.Decode(&term)
	d.Decode(&voteFor)
	d.Decode(&log)
	rf.CurrentTerm = term
	rf.VoteFor = voteFor
	rf.Log = log
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

type AppendEntriesArgs struct {
	Term         int
	Leader       int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogStruct
	LeaderCommit int
	// Your data here (2A, 2B).
}
type AppendEntriesReply struct {
	Term    int
	Success bool
	// Your data here (2A).
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
	// Your data here (2A, 2B).
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
	// Your data here (2A).
}

func (rf *Raft) checkRequestVote(args *RequestVoteArgs) bool {
	if rf.CurrentTerm > args.Term {
		return false
	}
	if rf.CurrentTerm < args.Term || (rf.VoteFor == -1 || rf.VoteFor == args.CandidateId) {
		if args.LastLogIndex >= 0 && len(rf.Log) > 0 {
			return args.LastLogTerm > rf.Log[len(rf.Log)-1].Term || (args.LastLogTerm == rf.Log[len(rf.Log)-1].Term && args.LastLogIndex >= (len(rf.Log)-1))
		} else if args.LastLogIndex < 0 && len(rf.Log) <= 0 {
			return true
		} else if args.LastLogIndex >= 0 && len(rf.Log) <= 0 {
			return true
		} else if args.LastLogIndex < 0 && len(rf.Log) > 0 {
			return false
		}
	}
	return false
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	if !rf.killed() {
		rf.mu.Lock()
		DPrintf("%d 进入投票阶段", rf.me)
		defer rf.mu.Unlock()

		if rf.checkRequestVote(args) {
			DPrintf("%d 投票给 %d", rf.me, args.CandidateId)
			rf.state = FOLLOWER
			rf.VoteFor = args.CandidateId
			rf.CurrentTerm = args.Term
			rf.appendCond.Broadcast()
			reply.VoteGranted = true
			reply.Term = args.Term
			rf.persist()
			return
		} else {
			DPrintf("%d 拒绝投票给 %d", rf.me, args.CandidateId)
			reply.VoteGranted = false
			reply.Term = rf.CurrentTerm
			return
		}
	}

	// Your code here (2A, 2B).
}
func (rf *Raft) checkAppendEntries(args *AppendEntriesArgs) bool {
	if rf.CurrentTerm > args.Term {
		return false
	}
	if len(rf.Log) == 0 && args.PrevLogIndex < 0 {
		return true
	}

	if (len(rf.Log)-1) < args.PrevLogIndex || (args.PrevLogIndex >= 0 && rf.Log[args.PrevLogIndex].Term != args.PrevLogTerm) {
		return false
	}
	return true

}
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if !rf.killed() {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if len(args.Entries) == 0 {
			if rf.CurrentTerm <= args.Term {
				DPrintf("%d 收到%d 的心跳 并且重置选举时间", rf.me, args.Leader)
				DPrintf("%d的len(log)是%d,log是 %v", rf.me, len(rf.Log), rf.Log)
				rf.state = FOLLOWER
				rf.CurrentTerm = args.Term
				rf.VoteFor = args.Leader
				rf.appendCond.Broadcast()
				rf.persist()

				if rf.checkAppendEntries(args) && rf.commitIndex < args.LeaderCommit {
					if args.LeaderCommit > args.PrevLogIndex {
						rf.commitIndex = args.PrevLogIndex
					} else {
						rf.commitIndex = args.LeaderCommit
					}

					rf.submitCond.Broadcast()
				}

				reply.Success = true
				reply.Term = args.Term
				rf.done = 1
				return
			} else {
				DPrintf("%d 收到%d 的心跳 并且不重置选举时间", rf.me, args.Leader)
				reply.Success = false
				reply.Term = rf.CurrentTerm
				return
			}
		} else {
			if rf.checkAppendEntries(args) {
				rf.Log = rf.Log[:args.PrevLogIndex+1]
				rf.Log = append(rf.Log, args.Entries...)

				DPrintf("%d 收到%d 的消息 并且添加成功,并且重置选举时间", rf.me, args.Leader)
				DPrintf("%d的log是 %v", rf.me, rf.Log)
				rf.state = FOLLOWER
				rf.appendCond.Broadcast()
				rf.CurrentTerm = args.Term
				rf.VoteFor = args.Leader
				if rf.commitIndex < args.LeaderCommit {
					if args.LeaderCommit > (len(rf.Log) - 1) {
						rf.commitIndex = len(rf.Log) - 1
					} else {
						rf.commitIndex = args.LeaderCommit
					}
					rf.submitCond.Broadcast()
				}
				reply.Success = true
				reply.Term = args.Term
				rf.done = 1
				return
			} else if rf.CurrentTerm <= args.Term {
				DPrintf("%d 收到%d 的消息 并且添加失败,并且重置选举时间", rf.me, args.Leader)
				rf.state = FOLLOWER
				rf.appendCond.Broadcast()
				rf.CurrentTerm = args.Term
				rf.VoteFor = args.Leader
				rf.persist()
				if rf.checkAppendEntries(args) && rf.commitIndex < args.LeaderCommit {
					if args.LeaderCommit > args.PrevLogIndex {
						rf.commitIndex = args.PrevLogIndex
					} else {
						rf.commitIndex = args.LeaderCommit
					}

					rf.submitCond.Broadcast()
				}
				reply.Success = false
				reply.Term = args.Term
				rf.done = 1
				return
			} else {
				DPrintf("%d 收到%d 的消息 并且不重置选举时间", rf.me, args.Leader)
				reply.Success = false
				reply.Term = rf.CurrentTerm
				return
			}

		}

	}

	// Your code here (2A, 2B).
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
func (rf *Raft) submit() {
	rf.mu.Lock()
	for {
		if rf.killed() {
			DPrintf("%d 被杀", rf.me)
			rf.mu.Unlock()
			break
		}

		for rf.commitIndex <= rf.lastApplied {

			rf.submitCond.Wait()
		}
		DPrintf("%d 的commitIndex:%d,lastApplied %d", rf.me, rf.commitIndex, rf.lastApplied)
		DPrintf("%d 提交给应用log，index:%d ,len(logs):%d", rf.me, rf.lastApplied+1, len(rf.Log))
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.Log[rf.lastApplied+1].Command,
			CommandIndex: rf.lastApplied + 2,

			// For 2D:
			SnapshotValid: false,
			Snapshot:      make([]byte, 0, 0),
			SnapshotTerm:  -1,
			SnapshotIndex: -1,
		}

		rf.applyCh <- applyMsg

		rf.lastApplied++

	}

}
func (rf *Raft) jisuanCommitIndex(N int) {

	if N <= rf.commitIndex {
		return
	}
	count := 0
	for i := 0; i < len(rf.peers); i++ {
		if rf.matchIndex[i] >= N {
			count++
		}
		if count > len(rf.peers)/2 && rf.Log[N].Term == rf.CurrentTerm {
			rf.commitIndex = N
			DPrintf("计算commitndex:%d", rf.commitIndex)
			rf.submitCond.Broadcast()
			return
		}

	}

}
func (rf *Raft) runAppendEntries() {
	rf.mu.Lock()
	lenPeers := len(rf.peers)
	me := rf.me
	term := rf.CurrentTerm
	rf.mu.Unlock()
	for i := 0; i < lenPeers; i++ {
		go func(i int) {
			if i != me {

				for {
					rf.mu.Lock()

					if rf.startCount[i] == 0 {
						rf.appendCond.Wait()
						if rf.CurrentTerm != term || rf.state != LEADER || rf.killed() {
							rf.mu.Unlock()
							return
						}

					}
					if rf.CurrentTerm != term || rf.state != LEADER || rf.killed() {
						rf.mu.Unlock()
						return
					}

					startIndex := <-rf.startChan[i]
					logs := rf.Log[:startIndex]
					nextIndex := rf.nextIndex[i]
					leaderCommit := rf.commitIndex
					rf.mu.Unlock()
					retry := 1
					for {
						rf.mu.Lock()
						if rf.CurrentTerm != term || rf.state != LEADER || rf.killed() {
							rf.mu.Unlock()
							return
						}
						rf.mu.Unlock()
						var args *AppendEntriesArgs
						DPrintf("%d 组织entries中,发送给%d,nextIndex:%d len(logs):%d", me, i, nextIndex, len(logs))
						if nextIndex <= 0 {
							args = &AppendEntriesArgs{Term: term,
								Leader:       me,
								PrevLogIndex: -1,
								PrevLogTerm:  -1,
								LeaderCommit: leaderCommit,
								Entries:      logs,
							}
						} else {
							args = &AppendEntriesArgs{Term: term,
								Leader:       me,
								PrevLogIndex: nextIndex - 1,
								PrevLogTerm:  logs[nextIndex-1].Term,
								LeaderCommit: leaderCommit,
								Entries:      logs[nextIndex:],
							}
						}

						reply := &AppendEntriesReply{}
						DPrintf("%d 发送AppendEntries给 %d,len(entries):%d，最后一条log是 %d\n", me, i, len(args.Entries), args.Entries[len(args.Entries)-1])
						ok := rf.sendAppendEntries(i, args, reply)
						if ok && reply.Success {
							DPrintf("%d 成功收到 %d添加log的消息 ，index:%d", me, i, nextIndex)
							rf.mu.Lock()
							rf.matchIndex[i] = startIndex - 1
							rf.nextIndex[i] = startIndex
							rf.startCount[i]--
							rf.jisuanCommitIndex(rf.matchIndex[i])
							rf.mu.Unlock()

							break
						} else if ok && !reply.Success {
							if reply.Term > term {
								DPrintf("%d 不是leader了，通过append %d index:%d返回得知", me, i, nextIndex)

								rf.mu.Lock()
								rf.CurrentTerm = reply.Term
								rf.VoteFor = -1
								rf.state = FOLLOWER
								rf.appendCond.Broadcast()
								rf.startCount[i]--
								rf.persist()
								rf.mu.Unlock()
								break
							} else {
								DPrintf("日志不统一")
								if nextIndex > 0 {
									nextIndex--
								}

							}
						} else {

							DPrintf("%d 失去联系 ，preindex:%d", i, nextIndex-1)
							rf.mu.Lock()
							if rf.CurrentTerm != term || rf.state != LEADER || rf.killed() {
								rf.mu.Unlock()
								return
							}
							if retry > 3 {
								rf.nextIndex[i] = nextIndex
								rf.startCount[i]--
								rf.mu.Unlock()

								break
							} else {
								retry++
								DPrintf("%d 失去联系 重试 %d", i, retry)
							}
							rf.mu.Unlock()

						}
					}

				}
			}
		}(i)

	}
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	DPrintf("%d 开始 start 消息  command:%v ", rf.me, command)
	index := -1
	term := -1
	isLeader := false

	rf.mu.Lock()
	if !rf.killed() && rf.state == LEADER {
		log := LogStruct{Command: command,
			Term: rf.CurrentTerm}
		rf.Log = append(rf.Log, log)

		rf.nextIndex[rf.me]++
		rf.matchIndex[rf.me]++
		index = len(rf.Log)
		for i := 0; i < len(rf.peers); i++ {
			rf.startChan[i] <- index
			rf.startCount[i]++
		}
		<-rf.startChan[rf.me]
		rf.appendCond.Broadcast()
		term = rf.CurrentTerm
		isLeader = true
		rf.persist()

	}

	rf.mu.Unlock()

	// Your code here (2B).

	return index, term, isLeader
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
	rf.mu.Lock()
	rf.appendCond.Broadcast()
	rf.mu.Unlock()
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
func (rf *Raft) KillOffElection() {
	rf.mu.Lock()
	rf.state = CANDIDATER
	muIncTerm.Lock()
	rf.CurrentTerm = incTerm
	incTerm++
	muIncTerm.Unlock()
	//rf.CurrentTerm += 1
	DPrintf("%d candidater term %d", rf.me, rf.CurrentTerm)
	term := rf.CurrentTerm
	rf.VoteFor = rf.me
	me := rf.me

	prevLogIndex := len(rf.Log) - 1
	var prevLogTerm int
	if len(rf.Log) == 0 {
		prevLogTerm = 0
	} else {

		prevLogTerm = rf.Log[prevLogIndex].Term

	}
	rf.persist()
	DPrintf("prevLogIndex is %d,prevLogTerm is %d\n", prevLogIndex, prevLogTerm)
	rf.mu.Unlock()
	var lock sync.Mutex
	cond := sync.NewCond(&lock)
	count := 1
	finish := 1

	for i := 0; i < len(rf.peers); i++ {
		go func(i int) {
			if i != me {
				args := &RequestVoteArgs{Term: term,
					CandidateId:  me,
					LastLogIndex: prevLogIndex,
					LastLogTerm:  prevLogTerm}
				reply := &RequestVoteReply{}
				DPrintf("%d 发送选举消息给 %d", me, i)
				ok := rf.sendRequestVote(i, args, reply)
				if ok && reply.VoteGranted {
					lock.Lock()
					count++
					finish++
					cond.Broadcast()
					lock.Unlock()

				} else if ok && !reply.VoteGranted {
					lock.Lock()
					rf.mu.Lock()
					if term == rf.CurrentTerm && rf.CurrentTerm < reply.Term {

						rf.CurrentTerm = reply.Term
						rf.VoteFor = -1
						rf.state = FOLLOWER
						rf.persist()
					}
					rf.mu.Unlock()
					finish++
					cond.Broadcast()
					lock.Unlock()

				} else {
					lock.Lock()
					finish++
					cond.Broadcast()
					lock.Unlock()
				}
			}
		}(i)
	}
	lock.Lock()
	for {
		rf.mu.Lock()
		if term != rf.CurrentTerm || rf.state != CANDIDATER {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		if count <= len(rf.peers)/2 && finish < len(rf.peers) {
			DPrintf("%d 获得票数%d,得到回复%d,len peers %d", me, count, finish, len(rf.peers))
			cond.Wait()

		} else {
			break
		}
	}
	DPrintf("%d 从投票中出来", me)
	rf.mu.Lock()
	if count > len(rf.peers)/2 && rf.CurrentTerm == term && rf.state == CANDIDATER {
		DPrintf("%d 成功当选领导,count:%d, lenpeers:%d ,currentTerm :%d ,term:%d state:%d", rf.me, count, len(rf.peers), rf.CurrentTerm, term, rf.state)
		rf.state = LEADER
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = len(rf.Log)
			rf.matchIndex[i] = -1
			if i == me {
				rf.matchIndex[i] = len(rf.Log) - 1
			}
			rf.startChan[i] = make(chan int, 1000)
			rf.startCount[i] = 0
		}

		rf.mu.Unlock()
		go rf.HeatBeat()
		go rf.runAppendEntries()

	} else {
		DPrintf("%d 失败当选领导,count:%d, lenpeers:%d ,currentTerm :%d ,term:%d state:%d", rf.me, count, len(rf.peers), rf.CurrentTerm, term, rf.state)
		rf.state = FOLLOWER

		rf.mu.Unlock()
	}
	lock.Unlock()

}
func (rf *Raft) HeatBeat() {
	for {
		if rf.killed() {
			DPrintf("%d 被杀", rf.me)
			break
		}
		rf.mu.Lock()
		if rf.state != LEADER {
			rf.mu.Unlock()
			break
		}
		me := rf.me
		term := rf.CurrentTerm
		leaderCommit := rf.commitIndex
		nextIndex := rf.nextIndex
		log := rf.Log

		lenPeers := len(rf.peers)
		rf.mu.Unlock()
		for i := 0; i < lenPeers; i++ {
			go func(i int) {
				if i != me {
					var args *AppendEntriesArgs
					if nextIndex[i] <= 0 {
						args = &AppendEntriesArgs{Term: term,
							Leader:       me,
							PrevLogIndex: -1,
							PrevLogTerm:  -1,
							LeaderCommit: leaderCommit,
						}

					} else {
						args = &AppendEntriesArgs{Term: term,
							Leader:       me,
							PrevLogIndex: nextIndex[i] - 1,
							PrevLogTerm:  log[nextIndex[i]-1].Term,
							LeaderCommit: leaderCommit,
						}
					}

					reply := &AppendEntriesReply{}
					DPrintf("%d 发送心跳给 %d", me, i)
					rf.sendAppendEntries(i, args, reply)
				}
			}(i)

		}
		time.Sleep(100 * time.Millisecond)

	}
}
func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.mu.Lock()

		if rf.state != LEADER && rf.done == 0 {
			DPrintf("%d 触发选举", rf.me)
			rf.done = 0
			rf.mu.Unlock()
			go rf.KillOffElection()

		} else {
			//DPrintf("%d 没有触发选举", rf.me)
			rf.done = 0
			rf.mu.Unlock()
		}

		// Your code here (2A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		//ms := 200 + (rand.Int63()%50)*(int64(rf.me)%7)

		ms := 200 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.applyCh = applyCh
	rf.VoteFor = -1
	rf.state = FOLLOWER
	rf.CurrentTerm = 0
	rf.dead = 0
	rf.done = 0
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.nextIndex = make([]int, len(peers), len(peers))
	rf.matchIndex = make([]int, len(peers), len(peers))
	rf.Log = make([]LogStruct, 0, 1000)
	rf.submitCond = sync.NewCond(&rf.mu)
	rf.isAppendEntry = false
	rf.appendCond = sync.NewCond(&rf.mu)

	rf.startChan = make([]chan int, len(rf.peers))
	rf.startCount = make([]int, len(rf.peers))

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	DPrintf("%d 初始化成功", me)
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.submit()

	return rf
}
