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
	myApplyCh     chan ApplyMsg
	submitCond    *sync.Cond
	isAppendEntry bool
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
	isStart           bool
	isStartCond       *sync.Cond
	lastIncludedIndex int
	lastIncludedTerm  int
	snapshot          []byte
	killChan          chan interface{}
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
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	e.Encode(rf.Log)

	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte, snapshot []byte) {
	DPrintf("%d正在恢复日志", rf.me)
	if data != nil || len(data) >= 1 { // bootstrap without any state?
		DPrintf("%d len(data):%d", rf.me, len(data))
		r := bytes.NewBuffer(data)
		d := labgob.NewDecoder(r)
		var term int
		var voteFor int
		log := make([]LogStruct, 0, 10000)
		var lastIncludedIndex int
		var lastIncludedTerm int
		d.Decode(&term)
		d.Decode(&voteFor)
		d.Decode(&lastIncludedIndex)
		d.Decode(&lastIncludedTerm)
		d.Decode(&log)
		rf.CurrentTerm = term
		rf.VoteFor = voteFor
		rf.lastIncludedTerm = lastIncludedTerm
		rf.lastIncludedIndex = lastIncludedIndex
		rf.commitIndex = lastIncludedIndex
		rf.Log = log
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

	if snapshot != nil || len(snapshot) >= 1 { // bootstrap without any state?
		DPrintf("%d len(snapshot):%d", rf.me, len(snapshot))
		rf.snapshot = clone(snapshot)
	}

}
func (rf *Raft) TrimLog(index int) {
	term := rf.Log[index-rf.lastIncludedIndex-1].Term
	rf.Log = rf.Log[index-rf.lastIncludedIndex:]
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = term

}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	DPrintf("%d 在进行快照,index:%d,lastIncludedIndex:%d,len(snapshot):%d", rf.me, index, rf.lastIncludedIndex, len(snapshot))
	if index > rf.lastIncludedIndex {
		rf.TrimLog(index)
		rf.snapshot = clone(snapshot)
		rf.persist()
	}
	DPrintf("%d 完成快照,index:%d,lastIncludedIndex:%d,len(rf.snapshot):%d", rf.me, index, rf.lastIncludedIndex, len(rf.snapshot))
	rf.mu.Unlock()

}
func (rf *Raft) GetPersistLogSize() int {
	//rf.mu.Lock()
	//DPrintf("获取到PersistLogSize的锁")
	//defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
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
	XTerm   int
	XIndex  int
	XLen    int
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
type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Snapshot          []byte
}
type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) checkRequestVote(args *RequestVoteArgs) bool {
	if rf.CurrentTerm > args.Term {
		return false
	}
	if rf.CurrentTerm < args.Term || (rf.VoteFor == -1 || rf.VoteFor == args.CandidateId) {
		if args.LastLogIndex >= 1 && rf.lastIncludedIndex+len(rf.Log) >= 1 { //两个都有
			if len(rf.Log) == 0 {
				return args.LastLogTerm > rf.lastIncludedTerm || (args.LastLogTerm == rf.lastIncludedTerm && args.LastLogIndex >= rf.lastIncludedIndex)
			} else {
				return args.LastLogTerm > rf.Log[len(rf.Log)-1].Term || (args.LastLogTerm == rf.Log[len(rf.Log)-1].Term && args.LastLogIndex >= (len(rf.Log)+rf.lastIncludedIndex))
			}

		} else if args.LastLogIndex == 0 && rf.lastIncludedIndex+len(rf.Log) == 0 { //两个都没有
			return true
		} else if args.LastLogIndex >= 1 && rf.lastIncludedIndex+len(rf.Log) == 0 { //args有，rf没有
			return true
		} else if args.LastLogIndex < 0 && len(rf.Log) > 0 { //args没有，rf有
			return false
		}
	}
	return false
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	if !rf.killed() {

		DPrintf("%d 进入投票阶段", rf.me)
		defer rf.mu.Unlock()

		if rf.checkRequestVote(args) {
			DPrintf("%d 投票给 %d", rf.me, args.CandidateId)
			rf.state = FOLLOWER
			rf.VoteFor = args.CandidateId
			rf.CurrentTerm = args.Term
			rf.done = 1
			rf.appendCond.Broadcast()
			reply.VoteGranted = true
			reply.Term = args.Term
			rf.persist()
			return
		} else {
			DPrintf("%d 拒绝投票给 %d", rf.me, args.CandidateId)
			if args.Term > rf.CurrentTerm {
				rf.state = FOLLOWER
				rf.VoteFor = -1
				rf.CurrentTerm = args.Term
				rf.appendCond.Broadcast()
				reply.VoteGranted = false
				rf.persist()
			}
			reply.VoteGranted = false
			reply.Term = rf.CurrentTerm
			return
		}
	} else {
		rf.mu.Unlock()
	}

	// Your code here (2A, 2B).
}
func (rf *Raft) checkAppendEntries(args *AppendEntriesArgs) bool {
	if rf.CurrentTerm > args.Term {
		return false
	}
	if len(rf.Log)+rf.lastIncludedIndex == 0 && args.PrevLogIndex == 0 { //rf没有,args的pre也没有
		return true
	}
	if len(rf.Log)+rf.lastIncludedIndex > 0 && args.PrevLogIndex == 0 {
		if args.PrevLogIndex <= rf.lastIncludedIndex { //
			if len(args.Entries) != 0 {
				args.Entries = args.Entries[rf.lastIncludedIndex-args.PrevLogIndex:]
				args.PrevLogIndex = rf.lastIncludedIndex
			}

		}
		return true
	}
	if len(rf.Log)+rf.lastIncludedIndex == 0 && args.PrevLogIndex > 0 {
		return false
	}

	if len(rf.Log)+rf.lastIncludedIndex > 0 && args.PrevLogIndex > 0 {
		if args.PrevLogIndex <= rf.lastIncludedIndex { //
			if len(args.Entries) != 0 {
				args.Entries = args.Entries[rf.lastIncludedIndex-args.PrevLogIndex:]
				args.PrevLogIndex = rf.lastIncludedIndex
			}
			return true
		} else {
			if args.PrevLogIndex > len(rf.Log)+rf.lastIncludedIndex {
				return false
			} else {
				if args.PrevLogTerm == rf.Log[args.PrevLogIndex-rf.lastIncludedIndex-1].Term {
					return true
				} else {
					return false
				}
			}
		}

	}
	return true

}
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {

	rf.mu.Lock()
	DPrintf("%d 收到 %d的的snapshot，args.LastIncludedIndex:%d,args.LastIncludedTerm:%d", rf.me, args.LeaderId, args.LastIncludedIndex, args.LastIncludedTerm)
	if !rf.killed() {
		defer rf.mu.Unlock()

		if rf.CurrentTerm <= args.Term {
			if args.LastIncludedIndex > rf.lastIncludedIndex {
				DPrintf("%d 收到%d 的snapshot 并且添加成功,并且重置选举时间", rf.me, args.LeaderId)
				rf.Log = rf.Log[:0]
				rf.snapshot = clone(args.Snapshot)
				rf.lastIncludedIndex = args.LastIncludedIndex
				rf.lastIncludedTerm = args.LastIncludedTerm
				rf.commitIndex = rf.lastIncludedIndex
				rf.submitCond.Broadcast()
				//rf.submitSnapshot(args.LastIncludedTerm, args.LastIncludedIndex)
			} else {
				DPrintf("%d 收到%d 过期的snapshot,并且重置选举时间", rf.me, args.LeaderId)
			}
			rf.state = FOLLOWER
			rf.appendCond.Broadcast()
			rf.CurrentTerm = args.Term
			rf.VoteFor = args.LeaderId
			rf.persist()
			reply.Term = args.Term
			rf.done = 1
			return
		} else {
			DPrintf("%d 收到%d 的snapshot 并且添加失败,并且不重置选举时间", rf.me, args.LeaderId)
			reply.Term = rf.CurrentTerm
			return
		}

	} else {
		rf.mu.Unlock()
	}

}
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	if !rf.killed() {
		defer rf.mu.Unlock()
		if len(args.Entries) == 0 {
			if rf.CurrentTerm <= args.Term {
				DPrintf("%d 收到%d 的心跳 并且重置选举时间", rf.me, args.Leader)
				//DPrintf("%d的len(log)是%d,log是 %v", rf.me, len(rf.Log), rf.Log)
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
				var diff int
				for diff = args.PrevLogIndex + 1; diff <= (args.PrevLogIndex + len(args.Entries)); diff++ {
					if diff > (len(rf.Log)+rf.lastIncludedIndex) || rf.Log[diff-rf.lastIncludedIndex-1].Term != args.Entries[diff-args.PrevLogIndex-1].Term {
						break
					}
				}
				//rf.Log = rf.Log[:args.PrevLogIndex-rf.lastIncludedIndex]
				//rf.Log = append(rf.Log, args.Entries...)
				if diff <= (args.PrevLogIndex + len(args.Entries)) {
					DPrintf("%d 收到%d 的消息 并且添加成功,并且重置选举时间", rf.me, args.Leader)
					rf.Log = rf.Log[:diff-rf.lastIncludedIndex-1]
					rf.Log = append(rf.Log, args.Entries[diff-args.PrevLogIndex-1:]...)
				} else {
					DPrintf("%d 收到%d 过期的消息 ,并且重置选举时间", rf.me, args.Leader)
				}

				//DPrintf("%d的log是 %v", rf.me, rf.Log)
				rf.state = FOLLOWER
				rf.appendCond.Broadcast()
				rf.CurrentTerm = args.Term
				rf.VoteFor = args.Leader
				rf.persist()
				if rf.commitIndex < args.LeaderCommit {
					if args.LeaderCommit > (rf.lastIncludedIndex + len(rf.Log)) {
						rf.commitIndex = rf.lastIncludedIndex + len(rf.Log)
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

				/*if rf.checkAppendEntries(args) && rf.commitIndex < args.LeaderCommit {
					if args.LeaderCommit > args.PrevLogIndex {
						rf.commitIndex = args.PrevLogIndex
					} else {
						rf.commitIndex = args.LeaderCommit
					}

					rf.submitCond.Broadcast()
				}*/
				reply.Success = false
				reply.Term = args.Term
				if (len(rf.Log) + rf.lastIncludedIndex) < args.PrevLogIndex {
					reply.XLen = len(rf.Log) + rf.lastIncludedIndex
					reply.XTerm = -1
					reply.XIndex = -1
				} else {

					reply.XLen = len(rf.Log) + rf.lastIncludedIndex
					reply.XTerm = rf.Log[args.PrevLogIndex-rf.lastIncludedIndex-1].Term
					for i := args.PrevLogIndex; i > rf.lastIncludedIndex; i-- {
						if rf.Log[i-rf.lastIncludedIndex-1].Term != reply.XTerm {
							reply.XIndex = i + 1
							break
						} else {
							reply.XIndex = i
						}
					}
					rf.Log = rf.Log[:args.PrevLogIndex-rf.lastIncludedIndex-1]
				}
				rf.persist()
				rf.done = 1
				return
			} else {
				DPrintf("%d 收到%d 的消息 并且不重置选举时间", rf.me, args.Leader)
				reply.Success = false
				reply.Term = rf.CurrentTerm
				return
			}

		}

	} else {
		rf.mu.Unlock()
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
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
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
	//rf.submitSnapshot(rf.lastIncludedTerm, rf.lastIncludedIndex)
	/*	go func() {
		for {
			select {
			case a := <-rf.myApplyCh:
				if rf.killed() {
					return
				}
				rf.applyCh <- a
			default:
				time.Sleep(1 * time.Millisecond)
				if rf.killed() {
					return
				}
			}
		}

	}()*/
	for {
		if rf.killed() {
			DPrintf("%d 被杀", rf.me)
			rf.mu.Unlock()
			return
		}

		for rf.commitIndex <= rf.lastApplied {

			rf.submitCond.Wait()
			if rf.killed() {
				DPrintf("%d 被杀", rf.me)
				rf.mu.Unlock()
				return
			}
		}
		if rf.lastApplied >= rf.lastIncludedIndex {
			DPrintf("%d 的commitIndex:%d,lastApplied %d", rf.me, rf.commitIndex, rf.lastApplied)
			DPrintf("%d 提交给应用log，index:%d ,len(logs):%d", rf.me, rf.lastApplied+1, len(rf.Log))
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.Log[rf.lastApplied-rf.lastIncludedIndex].Command,
				CommandIndex: rf.lastApplied + 1,

				// For 2D:
				SnapshotValid: false,
				Snapshot:      make([]byte, 0),
				SnapshotTerm:  -1,
				SnapshotIndex: -1,
			}
			DPrintf("%d 提交给应用log准备就绪，index:%d ,len(logs):%d", rf.me, rf.lastApplied+1, len(rf.Log))
			logLen := len(rf.Log)
			rf.mu.Unlock()
			select {
			case <-rf.killChan:
				return
			default:
				select {
				case <-rf.killChan:
					return
				case rf.applyCh <- applyMsg:
					rf.mu.Lock()
					DPrintf("%d 提交给应用log，index:%d ,len(logs):%d结束", rf.me, applyMsg.CommandIndex, logLen)
					rf.lastApplied++

				}
			}

			/*			select {
						case rf.applyCh <- applyMsg:
							DPrintf("%d 提交给应用log，index:%d ,len(logs):%d结束", rf.me, rf.lastApplied+1, len(rf.Log))
							rf.lastApplied++
							rf.mu.Unlock()
							time.Sleep(1 * time.Millisecond)
							rf.mu.Lock()
							if rf.killed() {
								DPrintf("%d 被杀", rf.me)
								rf.mu.Unlock()
								return
							}
						case <-time.After(50 * time.Millisecond):
							DPrintf("%d 提交给应用log，index:%d ,len(logs):%d应用繁忙拒绝", rf.me, rf.lastApplied+1, len(rf.Log))
							rf.mu.Unlock()
							time.Sleep(1 * time.Millisecond)
							rf.mu.Lock()
							if rf.killed() {
								DPrintf("%d 被杀", rf.me)
								rf.mu.Unlock()
								return
							}

						}*/
		} else {
			DPrintf("%d 正在提交快照", rf.me)
			if rf.snapshot != nil && len(rf.snapshot) > 0 {
				DPrintf("%d 的commitIndex:%d,lastApplied %d,len(rf.snapshot):%d", rf.me, rf.commitIndex, rf.lastApplied, len(rf.snapshot))
				DPrintf("%d 提交给应用snapshot，snapshotTerm:%d ,snapshotIndex:%d", rf.me, rf.lastIncludedTerm, rf.lastIncludedIndex)
				applyMsg := ApplyMsg{
					CommandValid: false,
					Command:      nil,
					CommandIndex: 0,

					// For 2D:
					SnapshotValid: true,
					SnapshotTerm:  rf.lastIncludedTerm,
					SnapshotIndex: rf.lastIncludedIndex,
				}
				applyMsg.Snapshot = clone(rf.snapshot)
				DPrintf("%d组织好submitSnapshot", rf.me)
				//rf.applyCh <- applyMsg
				lastIncludedIndex := rf.lastIncludedIndex
				rf.mu.Unlock()
				select {
				case <-rf.killChan:
					return
				default:
					select {
					case <-rf.killChan:
						return
					case rf.applyCh <- applyMsg:
						rf.mu.Lock()
						DPrintf("%d发送好submitSnapshot", rf.me)
						//rf.commitIndex = rf.lastIncludedIndex
						rf.lastApplied = lastIncludedIndex

					}
				}

				/*select {
				case rf.applyCh <- applyMsg:
					DPrintf("%d发送好submitSnapshot", rf.me)
					//rf.commitIndex = rf.lastIncludedIndex
					rf.lastApplied = rf.lastIncludedIndex
					rf.mu.Unlock()
					time.Sleep(1 * time.Millisecond)
					rf.mu.Lock()
					if rf.killed() {
						DPrintf("%d 被杀", rf.me)
						rf.mu.Unlock()
						return
					}
				case <-time.After(50 * time.Millisecond):
					DPrintf("%d 提交给应用snapshot，snapshotTerm:%d ,snapshotIndex:%d应用繁忙拒绝", rf.me, rf.lastIncludedTerm, rf.lastIncludedIndex)
					rf.mu.Unlock()
					time.Sleep(1 * time.Millisecond)
					rf.mu.Lock()
					if rf.killed() {
						DPrintf("%d 被杀", rf.me)
						rf.mu.Unlock()
						return
					}

				}*/
			}
			DPrintf("%d 结束提交快照", rf.me)

		}

	}

}
func (rf *Raft) submitSnapshot(snapshotTerm int, snapshotIndex int) {
	DPrintf("%d 正在提交快照", rf.me)
	if rf.snapshot != nil && len(rf.snapshot) > 0 {
		DPrintf("%d 的commitIndex:%d,lastApplied %d,len(rf.snapshot):%d", rf.me, rf.commitIndex, rf.lastApplied, len(rf.snapshot))
		DPrintf("%d 提交给应用snapshot，snapshotTerm:%d ,snapshotIndex:%d", rf.me, snapshotTerm, snapshotIndex)
		applyMsg := ApplyMsg{
			CommandValid: false,
			Command:      nil,
			CommandIndex: 0,

			// For 2D:
			SnapshotValid: true,
			SnapshotTerm:  snapshotTerm,
			SnapshotIndex: snapshotIndex,
		}
		applyMsg.Snapshot = clone(rf.snapshot)
		DPrintf("%d组织好submitSnapshot", rf.me)
		rf.applyCh <- applyMsg
		DPrintf("%d发送好submitSnapshot", rf.me)
		rf.commitIndex = snapshotIndex
		rf.lastApplied = snapshotIndex

	}
	DPrintf("%d 结束提交快照", rf.me)

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
		if count > len(rf.peers)/2 && rf.Log[N-rf.lastIncludedIndex-1].Term == rf.CurrentTerm {
			rf.commitIndex = N
			DPrintf("完成计算commitndex:%d", rf.commitIndex)
			rf.submitCond.Broadcast()
			return
		}

	}

}
func (rf *Raft) runAppendEntries(term int) {
	rf.mu.Lock()
	if rf.isStart == false {
		rf.isStartCond.Wait()
	}
	if rf.CurrentTerm != term || rf.state != LEADER || rf.killed() {
		rf.mu.Unlock()
		return
	}
	lenPeers := len(rf.peers)
	me := rf.me
	rf.mu.Unlock()
	for i := 0; i < lenPeers; i++ {
		go func(i int) {
			if i != me {

				for {
					rf.mu.Lock()
					if rf.CurrentTerm != term || rf.state != LEADER || rf.killed() {
						rf.mu.Unlock()
						return
					}

					if rf.matchIndex[i] == rf.matchIndex[me] {
						rf.appendCond.Wait()

					}
					if rf.CurrentTerm != term || rf.state != LEADER || rf.killed() {
						rf.mu.Unlock()
						return
					}

					logs := rf.Log
					nextIndex := rf.nextIndex[i]
					prevLogIndex := nextIndex - 1
					lastIncludedIndex := rf.lastIncludedIndex
					lastIncludedTerm := rf.lastIncludedTerm
					snapshot := clone(rf.snapshot)
					leaderCommit := rf.commitIndex
					rf.mu.Unlock()
					if nextIndex <= lastIncludedIndex {
						var args *InstallSnapshotArgs
						DPrintf("%d 组织snapshot中,发送给%d,lastIncludedIndex:%d,LastIncludedTerm:%d,len(snapshot):%d", me, i, lastIncludedIndex, lastIncludedTerm, len(snapshot))
						args = &InstallSnapshotArgs{
							Term:              term,
							LeaderId:          me,
							LastIncludedIndex: lastIncludedIndex,
							LastIncludedTerm:  lastIncludedTerm,
							Snapshot:          snapshot,
						}
						reply := &InstallSnapshotReply{}

						c1 := make(chan bool, 1)
						c2 := make(chan bool, 1)
						okMux := sync.Mutex{}
						ok := false
						go func() {
							ok1 := rf.sendInstallSnapshot(i, args, reply)
							okMux.Lock()
							ok = ok1
							okMux.Unlock()
							c1 <- true
						}()
						go func() {
							time.Sleep(100 * time.Millisecond)
							c2 <- true
						}()
						select {
						case <-c1:
							DPrintf("%d发给%d的snapshot消息返回", me, i)
						case <-c2:
							DPrintf("%d发给%d的snapshot消息丢失", me, i)
						}

						rf.mu.Lock()
						if rf.CurrentTerm == term {
							okMux.Lock()
							if ok && reply.Term <= term {
								DPrintf("%d 成功收到 %d添加snapshot的消息,并成功 ", me, i)
								rf.matchIndex[i] = lastIncludedIndex
								rf.nextIndex[i] = lastIncludedIndex + 1
								rf.jisuanCommitIndex(rf.matchIndex[i])
							} else if ok && reply.Term > term {
								DPrintf("%d 成功收到 %d添加snapshot的消息,但失败,%d不是leader ", me, i, me)
								rf.CurrentTerm = reply.Term
								rf.VoteFor = -1
								rf.state = FOLLOWER
								rf.appendCond.Broadcast()
								rf.persist()
							} else {
								DPrintf("%d 失去联系 ", i)
							}
							okMux.Unlock()

						}
						rf.mu.Unlock()
					} else {
						var args *AppendEntriesArgs

						var PrevLogTerm int
						if prevLogIndex == 0 {
							PrevLogTerm = 0
						} else {
							if prevLogIndex > lastIncludedIndex {
								PrevLogTerm = logs[prevLogIndex-lastIncludedIndex-1].Term
							} else {
								PrevLogTerm = lastIncludedTerm
							}
						}
						DPrintf("%d 组织entries中,发送给%d,nextIndex:%d prevLogIndex:%d,PrevLogTerm:%d", me, i, nextIndex, prevLogIndex, PrevLogTerm)
						args = &AppendEntriesArgs{Term: term,
							Leader:       me,
							PrevLogIndex: prevLogIndex,
							PrevLogTerm:  PrevLogTerm,
							LeaderCommit: leaderCommit,
							Entries:      logs[nextIndex-lastIncludedIndex-1:],
						}

						reply := &AppendEntriesReply{}
						DPrintf("%d 发送AppendEntries给 %d,len(entries):%d，最后一条log是 %d\n", me, i, len(args.Entries), args.Entries[len(args.Entries)-1])
						c1 := make(chan bool, 1)
						c2 := make(chan bool, 1)
						okMux := sync.Mutex{}
						ok := false
						go func() {
							ok1 := rf.sendAppendEntries(i, args, reply)
							okMux.Lock()
							ok = ok1
							okMux.Unlock()
							c1 <- true
						}()
						go func() {
							time.Sleep(100 * time.Millisecond)
							c2 <- true
						}()
						select {
						case <-c1:
							DPrintf("%d发给%d的append消息返回", me, i)
						case <-c2:
							DPrintf("%d发给%d的append消息丢失", me, i)
						}
						rf.mu.Lock()
						if rf.CurrentTerm == term {
							okMux.Lock()
							if ok && reply.Success {
								DPrintf("%d 成功收到 %d添加log的消息 ，index:%d", me, i, nextIndex)
								rf.matchIndex[i] = lastIncludedIndex + len(logs)
								rf.nextIndex[i] = lastIncludedIndex + len(logs) + 1
								rf.jisuanCommitIndex(rf.matchIndex[i])
							} else if ok && !reply.Success {
								if reply.Term > term {
									DPrintf("%d 不是leader了，通过append %d index:%d返回得知", me, i, nextIndex)
									rf.CurrentTerm = reply.Term
									rf.VoteFor = -1
									rf.state = FOLLOWER
									rf.appendCond.Broadcast()
									rf.persist()
								} else {
									DPrintf("日志不统一")
									if reply.XTerm > -1 {
										if reply.XIndex <= rf.lastIncludedIndex {
											rf.nextIndex[i] = reply.XIndex

										} else {
											if rf.Log[reply.XIndex-rf.lastIncludedIndex-1].Term != reply.XTerm {
												rf.nextIndex[i] = reply.XIndex
											} else {
												for j := reply.XIndex; j <= rf.lastIncludedIndex+len(rf.Log); j++ {
													if rf.Log[j-rf.lastIncludedIndex-1].Term != reply.XTerm {
														rf.nextIndex[i] = j - 1
														break
													} else {
														rf.nextIndex[i] = j
													}
												}
											}

										}

									} else {
										rf.nextIndex[i] = reply.XLen
									}
								}
							} else {
								DPrintf("%d 失去联系 ，preindex:%d", i, nextIndex-1)
							}
							okMux.Unlock()

						}
						rf.mu.Unlock()

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
		rf.isStart = true
		rf.isStartCond.Broadcast()
		rf.nextIndex[rf.me]++
		rf.matchIndex[rf.me]++
		index = rf.matchIndex[rf.me]
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
	DPrintf("%d 进入Kill", rf.me)
	rf.mu.Lock()
	atomic.StoreInt32(&rf.dead, 1)
	rf.appendCond.Broadcast()
	rf.submitCond.Broadcast()
	rf.killChan <- 1
	rf.isStartCond.Broadcast()
	DPrintf("%d 结束kill", rf.me)
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
	/*muIncTerm.Lock()
	rf.CurrentTerm = incTerm
	incTerm++
	muIncTerm.Unlock()*/
	rf.CurrentTerm += 1
	DPrintf("%d candidater term %d", rf.me, rf.CurrentTerm)
	term := rf.CurrentTerm
	rf.VoteFor = rf.me
	me := rf.me

	lastLogIndex := rf.lastIncludedIndex + len(rf.Log)
	var lastLogTerm int
	if lastLogIndex == 0 {
		lastLogTerm = 0
	} else {
		if len(rf.Log) != 0 {
			lastLogTerm = rf.Log[len(rf.Log)-1].Term
		} else {
			lastLogTerm = rf.lastIncludedTerm
		}
	}
	rf.persist()
	DPrintf("lastLogIndex is %d,lastLogTerm is %d\n", lastLogIndex, lastLogTerm)
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
					LastLogIndex: lastLogIndex,
					LastLogTerm:  lastLogTerm}
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
					if term == rf.CurrentTerm && rf.CurrentTerm < reply.Term && rf.state == CANDIDATER {

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
			rf.nextIndex[i] = rf.lastIncludedIndex + len(rf.Log) + 1
			rf.matchIndex[i] = 0
			if i == me {
				rf.matchIndex[i] = rf.lastIncludedIndex + len(rf.Log)
			}

			rf.isStart = false
		}

		rf.mu.Unlock()
		go rf.HeatBeat(term)
		go rf.runAppendEntries(term)

	} else {
		DPrintf("%d 失败当选领导,count:%d, lenpeers:%d ,currentTerm :%d ,term:%d state:%d", rf.me, count, len(rf.peers), rf.CurrentTerm, term, rf.state)
		rf.state = FOLLOWER

		rf.mu.Unlock()
	}
	lock.Unlock()

}
func (rf *Raft) HeatBeat(preTerm int) {
	for {
		rf.mu.Lock()
		if rf.killed() {
			DPrintf("%d 被杀", rf.me)
			rf.mu.Unlock()
			break
		}

		if preTerm != rf.CurrentTerm || rf.state != LEADER {
			rf.mu.Unlock()
			break
		}
		me := rf.me
		term := rf.CurrentTerm
		leaderCommit := rf.commitIndex

		prevLogIndex := make([]int, len(rf.peers), len(rf.peers))
		copy(prevLogIndex, rf.nextIndex)
		prevLogTerm := make([]int, len(rf.peers), len(rf.peers))

		for i := 0; i < len(rf.peers); i++ {
			prevLogIndex[i] = prevLogIndex[i] - 1
			if prevLogIndex[i] == 0 {
				prevLogTerm[i] = 0
			} else {
				if prevLogIndex[i] <= rf.lastIncludedIndex {
					prevLogTerm[i] = rf.lastIncludedTerm
					prevLogIndex[i] = rf.lastIncludedIndex
				} else {
					prevLogTerm[i] = rf.Log[prevLogIndex[i]-rf.lastIncludedIndex-1].Term
				}
			}
		}

		lenPeers := len(rf.peers)
		rf.mu.Unlock()
		for i := 0; i < lenPeers; i++ {
			go func(i int) {
				if i != me {
					//DPrintf("%d->%d heatbeat ", me, i)
					var args *AppendEntriesArgs
					args = &AppendEntriesArgs{Term: term,
						Leader:       me,
						PrevLogIndex: prevLogIndex[i],
						PrevLogTerm:  prevLogTerm[i],
						LeaderCommit: leaderCommit,
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
		ms := 200 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
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
	rf.myApplyCh = make(chan ApplyMsg, 10000)
	rf.killChan = make(chan interface{}, 1)
	rf.VoteFor = -1
	rf.state = FOLLOWER
	rf.CurrentTerm = 0
	rf.dead = 0
	rf.done = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers), len(peers))
	rf.matchIndex = make([]int, len(peers), len(peers))
	rf.Log = make([]LogStruct, 0, 1000)
	rf.submitCond = sync.NewCond(&rf.mu)
	rf.isAppendEntry = false
	rf.appendCond = sync.NewCond(&rf.mu)

	rf.isStart = false
	rf.isStartCond = sync.NewCond(&rf.mu)
	// Your initialization code here (2A, 2B, 2C).
	rf.snapshot = nil
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState(), persister.ReadSnapshot())

	DPrintf("%d 初始化成功", me)
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.submit()

	return rf
}
