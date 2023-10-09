package shardctrler

import (
	"6.5840/raft"
	"log"
	"math"
	"sort"
	"sync/atomic"
)
import "6.5840/labrpc"
import "sync"
import "6.5840/labgob"

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	dead         int32 // set by Kill()
	duplicateMap map[int64]Duplicate
	commitIndex  int
	commitCond   *sync.Cond
	killChan     chan interface{}
	isEmpty      bool

	configs []Config // indexed by config num
}

type Op struct {
	// Your data here.

	Op         int //0 query   1 join  2 leave 3 mvoe
	ClientId   int64
	Seq        int
	JoinServer map[int][]string
	LeaveGIDs  []int
	MoveShard  int
	MoveGID    int
	QueryNum   int
}
type Duplicate struct {
	ClientId int64
	Seq      int
	Op       int //0 query   1 join  2 leave 3 mvoe
	Value    Config
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}
func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.mu.Lock()
	defer sc.mu.Unlock()

	DPrintf("%d,收到join消息,clientid:%d,seq:%d", sc.me, args.ClientId, args.Seq)
	op := Op{
		Op:         1, //0 query   1 join  2 leave 3 mvoe
		ClientId:   args.ClientId,
		Seq:        args.Seq,
		JoinServer: args.Servers,
	}

	if sc.killed() {
		DPrintf("%d已经死了，但是还是收到了join消息", sc.me)
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return
	}
	duplicate, ok := sc.duplicateMap[args.ClientId]
	if ok {
		if duplicate.Seq == args.Seq {
			reply.WrongLeader = false
			reply.Err = OK
			return
		} else if duplicate.Seq > args.Seq {
			reply.WrongLeader = false
			reply.Err = ErrExpired
			return
		}

	}
	DPrintf("%d,把put消息传给join开始,seq:%d", sc.me, args.Seq)
	index, _, isLeader := sc.rf.Start(op)
	DPrintf("%d,把put消息传给join结束,seq:%d", sc.me, args.Seq)
	if isLeader {
		DPrintf("%d,发送join消息后发现自己还是leader,seq:%d", sc.me, args.Seq)
		for {
			if _, isLeader = sc.rf.GetState(); isLeader == true {
				if sc.commitIndex >= index {
					DPrintf("%d,提交了join消息seq:%d,client:%d", sc.me, args.Seq, args.ClientId)
					reply.Err = OK
					reply.WrongLeader = false
					return
				} else {
					DPrintf("%d,等待join消息提交,seq:%d,client:%d", sc.me, args.Seq, args.ClientId)
					sc.commitCond.Wait()
					if sc.killed() {
						reply.Err = ErrWrongLeader
						reply.WrongLeader = true
						return
					}
				}

			} else {
				reply.Err = ErrWrongLeader
				reply.WrongLeader = true
				return
			}

		}
	} else {
		DPrintf("%d,发送join消息后发现自己不是leader,seq:%d", sc.me, args.Seq)
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}

}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.mu.Lock()
	defer sc.mu.Unlock()

	DPrintf("%d,收到leave消息,clientid:%d,seq:%d", sc.me, args.ClientId, args.Seq)
	op := Op{
		Op:        2, //0 query   1 join  2 leave 3 mvoe
		ClientId:  args.ClientId,
		Seq:       args.Seq,
		LeaveGIDs: args.GIDs,
	}

	if sc.killed() {
		DPrintf("%d已经死了，但是还是收到了leave消息", sc.me)
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return
	}
	duplicate, ok := sc.duplicateMap[args.ClientId]
	if ok {
		if duplicate.Seq == args.Seq {
			reply.WrongLeader = false
			reply.Err = OK
			return
		} else if duplicate.Seq > args.Seq {
			reply.WrongLeader = false
			reply.Err = ErrExpired
			return
		}

	}
	DPrintf("%d,把put消息传给leave开始,seq:%d", sc.me, args.Seq)
	index, _, isLeader := sc.rf.Start(op)
	DPrintf("%d,把put消息传给leave结束,seq:%d", sc.me, args.Seq)
	if isLeader {
		DPrintf("%d,发送leave消息后发现自己还是leader,seq:%d", sc.me, args.Seq)
		for {
			if _, isLeader = sc.rf.GetState(); isLeader == true {
				if sc.commitIndex >= index {
					DPrintf("%d,提交了leave消息seq:%d,client:%d", sc.me, args.Seq, args.ClientId)
					reply.Err = OK
					reply.WrongLeader = false
					return
				} else {
					DPrintf("%d,等待leave消息提交,seq:%d,client:%d", sc.me, args.Seq, args.ClientId)
					sc.commitCond.Wait()
					if sc.killed() {
						reply.Err = ErrWrongLeader
						reply.WrongLeader = true
						return
					}
				}

			} else {
				reply.Err = ErrWrongLeader
				reply.WrongLeader = true
				return
			}

		}
	} else {
		DPrintf("%d,发送leave消息后发现自己不是leader,seq:%d", sc.me, args.Seq)
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.mu.Lock()
	defer sc.mu.Unlock()

	DPrintf("%d,收到move消息,clientid:%d,seq:%d", sc.me, args.ClientId, args.Seq)
	op := Op{
		Op:        3, //0 query   1 join  2 leave 3 mvoe
		ClientId:  args.ClientId,
		Seq:       args.Seq,
		MoveGID:   args.GID,
		MoveShard: args.Shard,
	}

	if sc.killed() {
		DPrintf("%d已经死了，但是还是收到了move消息", sc.me)
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return
	}
	duplicate, ok := sc.duplicateMap[args.ClientId]
	if ok {
		if duplicate.Seq == args.Seq {
			reply.WrongLeader = false
			reply.Err = OK
			return
		} else if duplicate.Seq > args.Seq {
			reply.WrongLeader = false
			reply.Err = ErrExpired
			return
		}

	}
	DPrintf("%d,把put消息传给move开始,seq:%d", sc.me, args.Seq)
	index, _, isLeader := sc.rf.Start(op)
	DPrintf("%d,把put消息传给move结束,seq:%d", sc.me, args.Seq)
	if isLeader {
		DPrintf("%d,发送move消息后发现自己还是leader,seq:%d", sc.me, args.Seq)
		for {
			if _, isLeader = sc.rf.GetState(); isLeader == true {
				if sc.commitIndex >= index {
					DPrintf("%d,提交了move消息seq:%d,client:%d", sc.me, args.Seq, args.ClientId)
					reply.Err = OK
					reply.WrongLeader = false
					return
				} else {
					DPrintf("%d,等待move消息提交,seq:%d,client:%d", sc.me, args.Seq, args.ClientId)
					sc.commitCond.Wait()
					if sc.killed() {
						reply.Err = ErrWrongLeader
						reply.WrongLeader = true
						return
					}
				}

			} else {
				reply.Err = ErrWrongLeader
				reply.WrongLeader = true
				return
			}

		}
	} else {
		DPrintf("%d,发送move消息后发现自己不是leader,seq:%d", sc.me, args.Seq)
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}

}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sc.mu.Lock()
	defer sc.mu.Unlock()
	DPrintf("%d,收到query消息,clientid:%d,seq:%d", sc.me, args.ClientId, args.Seq)
	op := Op{
		Op:       0, //0 query   1 join  2 leave 3 mvoe
		ClientId: args.ClientId,
		Seq:      args.Seq,
		QueryNum: args.Num,
	}

	if sc.killed() {
		DPrintf("%d已经死了，但是还是收到了get消息", sc.me)
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}
	duplicate, ok := sc.duplicateMap[args.ClientId]
	if ok {
		if duplicate.Seq == args.Seq {
			reply.Err = OK
			reply.Config = clone(duplicate.Value)
			reply.WrongLeader = false
			return
		} else if duplicate.Seq > args.Seq {
			reply.Err = ErrExpired
			reply.WrongLeader = false
			return
		}

	}
	DPrintf("%d,把query消息传给raft开始,seq:%d", sc.me, args.Seq)
	index, _, isLeader := sc.rf.Start(op)
	DPrintf("%d,把query消息传给raft结束,seq:%d", sc.me, args.Seq)
	if isLeader {
		DPrintf("%d,发送query消息后发现自己还是leader,seq:%d", sc.me, args.Seq)
		for {
			if _, isLeader = sc.rf.GetState(); isLeader == true {
				if sc.commitIndex >= index {
					DPrintf("%d,提交了query消息,seq:%d,client:%d", sc.me, args.Seq, args.ClientId)

					duplicate, _ := sc.duplicateMap[args.ClientId]
					//DPrintf("duplicate:%v", duplicate)
					if duplicate.Seq > op.Seq {
						reply.Err = ErrExpired
						reply.WrongLeader = false

					} else if duplicate.Seq == op.Seq {
						reply.Config = clone(duplicate.Value)
						reply.Err = OK
						reply.WrongLeader = false

					}

					return
				} else {
					DPrintf("%d,等待query消息提交,seq:%d,client:%d,", sc.me, args.Seq, args.ClientId)
					sc.commitCond.Wait()
					if sc.killed() {
						reply.Err = ErrWrongLeader
						reply.WrongLeader = true
						return
					}

				}

			} else {
				reply.Err = ErrWrongLeader
				reply.WrongLeader = true
				return
			}

		}
	} else {
		DPrintf("%d,发送query消息后发现自己不是leader,seq:%d", sc.me, args.Seq)
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}
}
func (sc *ShardCtrler) join(config *Config, op Op) {
	DPrintf("op.JoinServer:%v", op.JoinServer)
	for k, v := range op.JoinServer {
		DPrintf("k:%d,v:%v", k, v)
		s := make([]string, len(v))
		copy(s, v)
		DPrintf("s:%v", s)
		config.Groups[k] = s
	}
	DPrintf("config.Groups:%v", config.Groups)
	if sc.isEmpty {
		for i := 0; i < len(config.Shards); i++ {
			minGid, _ := getMinNumGid(*config)
			DPrintf("minGid:%d", minGid)
			config.Shards[i] = minGid
		}
		sc.isEmpty = false

	} else {
		for {
			maxGid, maxCount := getMaxNumGid(*config)
			DPrintf("maxGid:%d,maxCount:%d", maxGid, maxCount)
			minGid, minCount := getMinNumGid(*config)
			DPrintf("minGid:%d,minCount:%d", minGid, minCount)
			if maxCount-minCount <= 1 {
				return
			} else {
				for i := 0; i < len(config.Shards); i++ {
					if config.Shards[i] == maxGid {
						config.Shards[i] = minGid
						break
					}
				}
			}
		}

	}

}
func (sc *ShardCtrler) leave(config *Config, op Op) {
	for i := 0; i < len(op.LeaveGIDs); i++ {
		delete(config.Groups, op.LeaveGIDs[i])

	}
	indexs := make([]int, 0)
	for i := 0; i < len(config.Shards); i++ {
		for j := 0; j < len(op.LeaveGIDs); j++ {
			if config.Shards[i] == op.LeaveGIDs[j] {
				config.Shards[i] = 0
				indexs = append(indexs, i)
			}
		}
	}
	if len(config.Groups) == 0 {
		sc.isEmpty = true
		return
	} else {
		for _, value := range indexs {
			minGid, _ := getMinNumGid(*config)
			config.Shards[value] = minGid
		}
	}

}
func getMaxNumGid(config Config) (int, int) {
	gids := make([]int, 0, len(config.Groups))
	for k, _ := range config.Groups {
		gids = append(gids, k)
	}
	sort.Ints(gids)
	DPrintf("gids:%v", gids)
	countMap := make(map[int]int)
	for _, v := range config.Shards {
		countMap[v] = countMap[v] + 1
	}
	DPrintf("countMap:%v", countMap)
	ret := 0
	maxCount := 0
	for _, gid := range gids {
		if ret == 0 || maxCount < countMap[gid] {
			ret = gid
			maxCount = countMap[gid]
		}
	}
	return ret, maxCount

}

func getMinNumGid(config Config) (int, int) {
	gids := make([]int, 0, len(config.Groups))
	for k, _ := range config.Groups {
		gids = append(gids, k)
	}
	sort.Ints(gids)
	DPrintf("gids:%v", gids)
	countMap := make(map[int]int)
	for _, v := range config.Shards {
		countMap[v] = countMap[v] + 1
	}
	DPrintf("countMap:%v", countMap)
	ret := 0
	minCount := math.MaxInt
	for _, gid := range gids {
		if ret == 0 || minCount > countMap[gid] {
			ret = gid
			minCount = countMap[gid]
		}
	}
	return ret, minCount

}
func (sc *ShardCtrler) runApply() {
	for {
		select {
		case applyMsg := <-sc.applyCh:
			// 处理通道消息 m
			sc.mu.Lock()
			if sc.killed() {
				sc.mu.Unlock()
				return
			}

			if applyMsg.CommandValid {
				if (sc.commitIndex + 1) != applyMsg.CommandIndex {
					log.Fatalf("%d 提交错误,kv.commitIndex=%d,applyMsg.CommandIndex:%d", sc.me, sc.commitIndex, applyMsg.CommandIndex)
				}
				sc.commitIndex = applyMsg.CommandIndex
				op := applyMsg.Command.(Op)
				if op.Op == 1 {
					DPrintf("%d,收到raft的join提交消息，seq:%d,JoinServer:%v", sc.me, op.Seq, op.JoinServer)
					duplicate, ok := sc.duplicateMap[op.ClientId]
					if ok {
						if duplicate.Seq >= op.Seq {
							DPrintf("%d,收到raft的join提交消息，seq:%d,JoinServer:%v 是重复提交的", sc.me, op.Seq, op.JoinServer)
						} else if duplicate.Seq < op.Seq {
							config := clone(sc.configs[len(sc.configs)-1])
							config.Num = len(sc.configs)
							sc.join(&config, op)
							DPrintf("%d，要加入的config:%v", sc.me, config)
							sc.configs = append(sc.configs, config)
							d := Duplicate{
								Seq:      op.Seq,
								ClientId: op.ClientId,
								Op:       op.Op,
							}
							sc.duplicateMap[op.ClientId] = d

						}
					} else {
						config := clone(sc.configs[len(sc.configs)-1])
						config.Num = len(sc.configs)
						sc.join(&config, op)
						DPrintf("%d，要加入的config:%v", sc.me, config)
						sc.configs = append(sc.configs, config)
						d := Duplicate{
							Seq:      op.Seq,
							ClientId: op.ClientId,
							Op:       op.Op,
						}
						sc.duplicateMap[op.ClientId] = d

					}

				} else if op.Op == 2 {
					DPrintf("%d,收到raft的leave提交消息，seq:%d,LeaveGIDs:%v", sc.me, op.Seq, op.LeaveGIDs)
					duplicate, ok := sc.duplicateMap[op.ClientId]
					if ok {
						if duplicate.Seq >= op.Seq {
							DPrintf("%d,收到raft的leave提交消息，seq:%d,LeaveGIDs:%v 是重复提交的", sc.me, op.Seq, op.LeaveGIDs)
						} else if duplicate.Seq < op.Seq {
							config := clone(sc.configs[len(sc.configs)-1])
							config.Num = len(sc.configs)
							sc.leave(&config, op)
							sc.configs = append(sc.configs, config)
							d := Duplicate{
								Seq:      op.Seq,
								ClientId: op.ClientId,
								Op:       op.Op,
							}
							sc.duplicateMap[op.ClientId] = d

						}
					} else {
						config := clone(sc.configs[len(sc.configs)-1])
						config.Num = len(sc.configs)
						sc.leave(&config, op)
						sc.configs = append(sc.configs, config)
						d := Duplicate{
							Seq:      op.Seq,
							ClientId: op.ClientId,
							Op:       op.Op,
						}
						sc.duplicateMap[op.ClientId] = d

					}

				} else if op.Op == 3 {
					DPrintf("%d,收到raft的move提交消息，seq:%d,MoveGID:%d,MoveShard:%d", sc.me, op.Seq, op.MoveGID, op.MoveShard)
					duplicate, ok := sc.duplicateMap[op.ClientId]
					if ok {
						if duplicate.Seq >= op.Seq {
							DPrintf("%d,收到raft的move提交消息，seq:%d,MoveGID:%d,MoveShard:%d 是重复提交的", sc.me, op.Seq, op.MoveGID, op.MoveShard)
						} else if duplicate.Seq < op.Seq {
							config := clone(sc.configs[len(sc.configs)-1])
							config.Num = len(sc.configs)
							config.Shards[op.MoveShard] = op.MoveGID
							sc.configs = append(sc.configs, config)
							d := Duplicate{
								Seq:      op.Seq,
								ClientId: op.ClientId,
								Op:       op.Op,
							}
							sc.duplicateMap[op.ClientId] = d

						}
					} else {
						config := clone(sc.configs[len(sc.configs)-1])
						config.Num = len(sc.configs)
						config.Shards[op.MoveShard] = op.MoveGID
						sc.configs = append(sc.configs, config)
						d := Duplicate{
							Seq:      op.Seq,
							ClientId: op.ClientId,
							Op:       op.Op,
						}
						sc.duplicateMap[op.ClientId] = d

					}

				} else if op.Op == 0 {
					DPrintf("%d,收到raft的query提交消息，seq:%d,QueryNum:%d", sc.me, op.Seq, op.QueryNum)

					duplicate, ok := sc.duplicateMap[op.ClientId]
					if ok {
						if duplicate.Seq >= op.Seq {
							DPrintf("%d,收到raft的get提交消息，seq:%d,QueryNum:%d,是重复提交的", sc.me, op.Seq, op.QueryNum)
						} else if duplicate.Seq < op.Seq {
							var config Config
							if op.QueryNum == -1 || op.QueryNum >= len(sc.configs) {
								config = clone(sc.configs[len(sc.configs)-1])

							} else {
								config = clone(sc.configs[op.QueryNum])
							}
							d := Duplicate{
								Seq:      op.Seq,
								ClientId: op.ClientId,
								Op:       op.Op,
								Value:    config,
							}
							DPrintf("%d在apply get消息,seq:%d", sc.me, op.Seq)
							sc.duplicateMap[op.ClientId] = d

						}
					} else {
						var config Config
						if op.QueryNum == -1 || op.QueryNum >= len(sc.configs) {
							config = clone(sc.configs[len(sc.configs)-1])

						} else {
							config = clone(sc.configs[op.QueryNum])
						}
						d := Duplicate{
							Seq:      op.Seq,
							ClientId: op.ClientId,
							Op:       op.Op,
							Value:    config,
						}
						DPrintf("%d在apply get消息,seq:%d", sc.me, op.Seq)
						sc.duplicateMap[op.ClientId] = d

					}

				} else {
					log.Fatalf("无此操作")
				}
				sc.commitCond.Broadcast()

			} else if applyMsg.SnapshotValid {
				log.Fatalf("%d 不支持Snapshot", sc.me)

			}
			sc.mu.Unlock()
		case <-sc.killChan:
			return // 接收到 stopCh 信号时退出协程
		}
	}

}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	//sc.rf.Kill()
	// Your code here, if desired.
	//DPrintf("%d服务器进入killed", kv.me)
	sc.mu.Lock()
	DPrintf("%d服务器正在killed", sc.me)
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	sc.commitCond.Broadcast()
	//sc.generateSnapshotCond.Broadcast()
	//close(kv.applyCh)
	sc.killChan <- 1
	DPrintf("%d服务器结束killed", sc.me)
	sc.mu.Unlock()
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.commitIndex = 0
	sc.commitCond = sync.NewCond(&sc.mu)
	sc.killChan = make(chan interface{}, 1)
	sc.duplicateMap = make(map[int64]Duplicate)
	sc.isEmpty = true
	go sc.runApply()
	return sc
}
