package shardkv

import (
	"6.5840/labrpc"
	"6.5840/shardctrler"
	"bytes"
	"log"
	"sync/atomic"
	"time"
)
import "6.5840/raft"
import "sync"
import "6.5840/labgob"

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	CommandType CommandType
	Data        interface{}
}
type CommandType int
type GetData struct {
	Key      string
	ClientId int64
	Seq      int
}
type ChangeShardState struct {
	ShardId    int
	Version    int
	ShardState shardState
}
type PutAppendData struct {
	Key      string
	Value    string
	ClientId int64
	Seq      int
}
type InstallConfigData struct {
	Config shardctrler.Config
}
type DeleteShardData struct {
	Version int
	ShardId int
}
type InstallShardData struct {
	ConfigNum int
	ShardId   int
	ShardData []byte
}

const (
	GET = iota
	PUT
	APPEND
	INSTALLCONFIG
	DELETESHARD
	INSTALLSHARD
	ChangeShard
)

type Duplicate struct {
	ClientId     int64
	Seq          int
	Op           CommandType
	Value        string
	IsVaildValue bool
}
type shardState int

const (
	NOSERVER shardState = iota
	SERVER
	PULLING
	BEPULLING
)

type Shard struct {
	DuplicateMap map[int64]Duplicate
	KvMap        map[string]string
	State        shardState
	Version      int
}
type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	// Your definitions here.
	shards               map[int]Shard
	currentConfig        shardctrler.Config
	lastConfig           shardctrler.Config
	sm                   *shardctrler.Clerk
	shardStateCond       *sync.Cond
	commitIndex          int
	commitCond           *sync.Cond
	dead                 int32
	generateSnapshotCond *sync.Cond
	killChan             chan interface{}
	installConfigCond    *sync.Cond
	pullShardCond        *sync.Cond
	deleteShardCond      *sync.Cond
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}
func (kv *ShardKV) isInstallConfig() bool {
	for _, v := range kv.shards {
		if v.State == PULLING || v.State == BEPULLING {
			return false
		}
	}
	return true
}

func (kv *ShardKV) getConfig() {
	for {
		kv.mu.Lock()
		if kv.killed() {
			DPrintf("%d-%d已经死了，getConfig退出", kv.gid, kv.me)
			kv.mu.Unlock()
			return
		}
		num := kv.currentConfig.Num
		kv.mu.Unlock()
		config := kv.sm.Query(num + 1)
		kv.mu.Lock()
		if config.Num == (kv.currentConfig.Num + 1) /*&& kv.isInstallConfig()*/ {
			op := Op{
				CommandType: INSTALLCONFIG,
			}
			op.Data = InstallConfigData{Config: shardctrler.Clone(config)}
			DPrintf("%d-%d开始start installConfig,conifg:%v", kv.gid, kv.me, op.Data)
			kv.rf.Start(op)
			DPrintf("%d-%d结束start installConfig,conifg:%v", kv.gid, kv.me, op.Data)
		}
		kv.mu.Unlock()
		time.Sleep(50 * time.Millisecond)

	}
}
func (kv *ShardKV) checkeServerKey(key string) bool {
	shard := key2shard(key)
	if kv.currentConfig.Shards[shard] == kv.gid {
		return true
	}
	return false
}
func (kv *ShardKV) PullShard(args *PullShardArgs, reply *PullShardReply) {

}
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("%d-%d,收到get消息,key：%s,clientid:%d,seq:%d", kv.gid, kv.me, args.Key, args.ClientId, args.Seq)
	if kv.killed() {
		DPrintf("%d-%d已经死了，但是还是收到了get消息", kv.gid, kv.me)
		reply.Err = ErrWrongLeader
		return
	}
	if !kv.checkeServerKey(args.Key) {
		DPrintf("%d-%d 不负责key:%s,shard:%d", kv.gid, kv.me, args.Key, key2shard(args.Key))
		reply.Err = ErrWrongGroup
		return
	}
	shard := key2shard(args.Key)
	op := Op{
		CommandType: GET, // 0 => get, 1 => put, 2 => append
		Data: GetData{Seq: args.Seq,
			ClientId: args.ClientId,
			Key:      args.Key},
	}
	if kv.shards[shard].State != SERVER {
		reply.Err = ErrWaitShardTran
		return
	}
	if kv.killed() {
		DPrintf("%d-%d已经死了，但是还是收到了get消息", kv.gid, kv.me)
		reply.Err = ErrWrongLeader
		return
	}

	duplicate, ok := kv.shards[shard].DuplicateMap[args.ClientId]
	if ok {
		if duplicate.Seq == args.Seq {
			reply.Err = OK
			reply.Value = duplicate.Value
			return
		} else if duplicate.Seq > args.Seq {
			reply.Err = ErrExpired
			return
		}

	}
	DPrintf("%d-%d,把get消息传给raft开始,seq:%d", kv.gid, kv.me, args.Seq)
	index, _, isLeader := kv.rf.Start(op)
	DPrintf("%d-%d,把get消息传给raft结束,seq:%d", kv.gid, kv.me, args.Seq)
	if isLeader {
		DPrintf("%d-%d,发送get消息后发现自己还是leader,seq:%d", kv.gid, kv.me, args.Seq)
		for {
			if _, isLeader = kv.rf.GetState(); isLeader == true && kv.checkeServerKey(args.Key) {
				if kv.commitIndex >= index {
					DPrintf("%d-%d,提交了get消息,key:%s,seq:%d,client:%d", kv.gid, kv.me, args.Key, args.Seq, args.ClientId)

					duplicate, _ := kv.shards[shard].DuplicateMap[args.ClientId]
					//DPrintf("duplicate:%v", duplicate)
					if duplicate.Seq > op.Data.(GetData).Seq {
						reply.Err = ErrExpired

					} else if duplicate.Seq == op.Data.(GetData).Seq {
						if duplicate.IsVaildValue {
							reply.Value = duplicate.Value
							reply.Err = OK
						} else {
							reply.Err = ErrNoKey
						}
					}

					return
				} else {
					DPrintf("%d-%d,等待get消息提交,key:%s,seq:%d,client:%d,", kv.gid, kv.me, args.Key, args.Seq, args.ClientId)
					kv.commitCond.Wait()
					if kv.killed() {
						reply.Err = ErrWrongLeader
						return
					}

				}

			} else if isLeader == false {
				reply.Err = ErrWrongLeader
				return
			} else {
				reply.Err = ErrWrongGroup
				return
			}

		}
	} else {
		DPrintf("%d-%d,发送get消息后发现自己不是leader,seq:%d", kv.gid, kv.me, args.Seq)
		reply.Err = ErrWrongLeader
		return
	}

}
func (kv *ShardKV) ChangeShard(args *ChangeShardArgs, reply *ChangeShardReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("%d-%d,收到change shard消息,args:%v", kv.gid, kv.me, args)
	if kv.killed() {
		DPrintf("%d-%d已经死了，但是还是收到了get消息", kv.gid, kv.me)
		reply.Err = ErrWrongLeader
		return
	}
	if _, ok := kv.shards[args.ShardId]; !ok {
		reply.Err = OK
		return
	}
	if (kv.shards[args.ShardId].Version == args.ConfigNum && kv.shards[args.ShardId].State == args.ShardState) || kv.shards[args.ShardId].Version > args.ConfigNum {
		reply.Err = OK
		return
	}

	op := Op{
		CommandType: ChangeShard, // 0 => get, 1 => put, 2 => append
		Data: ChangeShardState{ShardId: args.ShardId,
			Version:    args.ConfigNum,
			ShardState: args.ShardState},
	}
	if kv.killed() {
		DPrintf("%d-%d已经死了，但是还是收到了get消息", kv.gid, kv.me)
		reply.Err = ErrWrongLeader
		return
	}

	DPrintf("%d-%d,把change shard消息传给raft开始,args:%v", kv.gid, kv.me, args)
	index, _, isLeader := kv.rf.Start(op)
	DPrintf("%d-%d,把change shard消息传给raft结束,args:%v", kv.gid, kv.me, args)
	if isLeader {
		DPrintf("%d-%d,发送change shard消息后发现自己还是leader,args:%v", kv.gid, kv.me, args)
		for {
			if _, isLeader = kv.rf.GetState(); isLeader == true {
				if kv.commitIndex >= index {
					DPrintf("%d-%d,提交了change shard消息,args:%v", kv.gid, kv.me, args)

					reply.Err = OK

					return
				} else {
					DPrintf("%d-%d,等待change shard消息提交,args:%v", kv.gid, kv.me, args)
					kv.commitCond.Wait()
					if kv.killed() {
						reply.Err = ErrWrongLeader
						return
					}

				}

			} else if isLeader == false {
				reply.Err = ErrWrongLeader
				return
			} else {
				reply.Err = ErrWrongGroup
				return
			}

		}
	} else {
		DPrintf("%d-%d,发送change shard消息后发现自己不是leader,args:%v", kv.gid, kv.me, args)
		reply.Err = ErrWrongLeader
		return
	}

}
func (kv *ShardKV) GetShard(args *PullShardArgs, reply *PullShardReply) {
	kv.mu.Lock()
	DPrintf("%d-%d收到GetShard消息,shardId:%d,configNum:%d", kv.gid, kv.me, args.ShardId, args.ConfigNum)
	kv.printShardStatus()
	_, isLeader := kv.rf.GetState()
	if isLeader {
		DPrintf("%d-%d收到GetShard消息,shardId:%d,configNum:%d,是leader", kv.gid, kv.me, args.ShardId, args.ConfigNum)
		if kv.shards[args.ShardId].Version != args.ConfigNum {
			DPrintf("%d-%d收到GetShard消息,shardId:%d,configNum:%d,kv.shards[args.ShardId].Version：%d,是leader但是版本不匹配", kv.gid, kv.me, args.ShardId, args.ConfigNum, kv.shards[args.ShardId].Version)
			reply.Err = ErrWaitInstallConfig
			kv.mu.Unlock()
			return
		}
		if kv.shards[args.ShardId].State == SERVER {
			DPrintf("%d-%d收到GetShard消息,shardId:%d,configNum:%d,是leader但是正在服务", kv.gid, kv.me, args.ShardId, args.ConfigNum)
			reply.Err = ErrWaitInstallConfig
			kv.mu.Unlock()
			return
		} else if kv.shards[args.ShardId].State == PULLING {
			DPrintf("%d-%d收到GetShard消息,shardId:%d,configNum:%d,是leader但是正在拉取", kv.gid, kv.me, args.ShardId, args.ConfigNum)
			reply.Err = ErrWaitInstallConfig
			kv.mu.Unlock()
			return
		}
		reply.Err = OK
		reply.ShardId = args.ShardId
		reply.ConfigNum = args.ConfigNum
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(kv.shards[args.ShardId])
		DPrintf("len(w.Bytes()):%d,%v", len(w.Bytes()), w.Bytes())
		reply.Data = w.Bytes()

	} else {
		DPrintf("%d-%d收到GetShard消息,shardId:%d,configNum:%d,不是leader", kv.gid, kv.me, args.ShardId, args.ConfigNum)
		reply.Err = ErrWrongLeader
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if args.Op == "Put" {
		DPrintf("%d-%d,收到put消息,key：%s,clientid:%d,seq:%d", kv.gid, kv.me, args.Key, args.ClientId, args.Seq)
		if kv.killed() {
			DPrintf("%d-%d已经死了，但是还是收到了Put消息", kv.gid, kv.me)
			reply.Err = ErrWrongLeader
			return
		}
		if !kv.checkeServerKey(args.Key) {
			DPrintf("%d-%d 不负责key:%s,shard:%d", kv.gid, kv.me, args.Key, key2shard(args.Key))
			reply.Err = ErrWrongGroup
			return
		}
		shard := key2shard(args.Key)
		op := Op{
			CommandType: PUT, // 0 => get, 1 => put, 2 => append
			Data: PutAppendData{Key: args.Key,
				Value:    args.Value,
				ClientId: args.ClientId,
				Seq:      args.Seq},
		}

		if kv.shards[shard].State != SERVER {
			reply.Err = ErrWaitShardTran
			return
		}

		if kv.killed() {
			DPrintf("%d-%d已经死了，但是还是收到了put消息", kv.gid, kv.me)
			reply.Err = ErrWrongLeader
			return
		}
		duplicate, ok := kv.shards[shard].DuplicateMap[args.ClientId]
		if ok {
			if duplicate.Seq == args.Seq {
				reply.Err = OK
				return
			} else if duplicate.Seq > args.Seq {
				reply.Err = ErrExpired
				return
			}

		}
		DPrintf("%d-%d,把put消息传给raft开始,seq:%d", kv.gid, kv.me, args.Seq)
		index, _, isLeader := kv.rf.Start(op)
		DPrintf("%d-%d,把put消息传给raft结束,seq:%d", kv.gid, kv.me, args.Seq)
		if isLeader {
			DPrintf("%d-%d,发送put消息后发现自己还是leader,seq:%d", kv.gid, kv.me, args.Seq)
			for {
				if _, isLeader = kv.rf.GetState(); isLeader == true && kv.checkeServerKey(args.Key) {
					if kv.commitIndex >= index {
						DPrintf("%d-%d,提交了put消息,key:%s,seq:%d,client:%d", kv.gid, kv.me, args.Key, args.Seq, args.ClientId)
						reply.Err = OK
						return
					} else {
						DPrintf("%d-%d,等待put消息提交,key:%s,seq:%d,client:%d", kv.gid, kv.me, args.Key, args.Seq, args.ClientId)
						kv.commitCond.Wait()
						if kv.killed() {
							reply.Err = ErrWrongLeader
							return
						}
					}

				} else if isLeader == false {
					reply.Err = ErrWrongLeader
					return
				} else {
					reply.Err = ErrWrongGroup
					return
				}

			}
		} else {
			DPrintf("%d-%d,发送put消息后发现自己不是leader,seq:%d", kv.gid, kv.me, args.Seq)
			reply.Err = ErrWrongLeader
			return
		}

	} else if args.Op == "Append" {
		DPrintf("%d-%d,收到append消息,key：%s,clientid:%d,seq:%d", kv.gid, kv.me, args.Key, args.ClientId, args.Seq)
		if kv.killed() {
			DPrintf("%d-%d已经死了，但是还是收到了Append消息", kv.gid, kv.me)
			reply.Err = ErrWrongLeader
			return
		}
		if !kv.checkeServerKey(args.Key) {
			DPrintf("%d-%d 不负责key:%s,shard:%d", kv.gid, kv.me, args.Key, key2shard(args.Key))
			reply.Err = ErrWrongGroup
			return
		}
		shard := key2shard(args.Key)
		op := Op{
			CommandType: APPEND, // 0 => get, 1 => put, 2 => append
			Data: PutAppendData{Key: args.Key,
				Value:    args.Value,
				ClientId: args.ClientId,
				Seq:      args.Seq},
		}
		if kv.shards[shard].State != SERVER {
			reply.Err = ErrWaitShardTran
			return
		}
		if kv.killed() {
			DPrintf("%d-%d已经死了，但是还是收到了append消息", kv.gid, kv.me)
			reply.Err = ErrWrongLeader
			return
		}
		duplicate, ok := kv.shards[shard].DuplicateMap[args.ClientId]
		if ok {
			if duplicate.Seq == args.Seq {
				reply.Err = OK
				return
			} else if duplicate.Seq > args.Seq {
				reply.Err = ErrExpired
				return
			}

		}
		DPrintf("%d-%d,把append消息传给raft开始,seq:%d", kv.gid, kv.me, args.Seq)
		index, _, isLeader := kv.rf.Start(op)
		DPrintf("%d-%d,把append消息传给raft结束,seq:%d", kv.gid, kv.me, args.Seq)
		if isLeader {
			for {
				if _, isLeader = kv.rf.GetState(); isLeader == true && kv.checkeServerKey(args.Key) {
					if kv.commitIndex >= index {
						DPrintf("%d-%d,提交了append消息,key:%s,value:%s,seq:%d", kv.gid, kv.me, args.Key, args.Value, args.Seq)
						reply.Err = OK
						return
					} else {
						DPrintf("%d-%d,等待append消息提交,seq:%d", kv.gid, kv.me, args.Seq)
						kv.commitCond.Wait()
						if kv.killed() {
							reply.Err = ErrWrongLeader
							return
						}
					}

				} else {
					reply.Err = ErrWrongLeader
					return
				}

			}
		} else if isLeader == false {
			DPrintf("%d-%d,发送append消息后发现自己不是leader,seq:%d", kv.gid, kv.me, args.Seq)
			reply.Err = ErrWrongLeader
			return
		} else {
			reply.Err = ErrWrongGroup
			return
		}
	}

}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	//kv.rf.Kill()
	// Your code here, if desired.
	//DPrintf("%d服务器进入killed", kv.me)
	kv.mu.Lock()
	DPrintf("%d-%d服务器正在killed", kv.gid, kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	kv.commitCond.Broadcast()
	kv.generateSnapshotCond.Broadcast()
	//close(kv.applyCh)
	kv.killChan <- 1
	DPrintf("%d-%d服务器结束killed", kv.gid, kv.me)
	kv.mu.Unlock()
	// Your code here, if desired.
}

func (kv *ShardKV) generateSnapshot(snapshot *[]byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.commitIndex)
	e.Encode(kv.shards)
	e.Encode(kv.lastConfig)
	e.Encode(kv.currentConfig)
	DPrintf("len(w.Bytes()):%d", len(w.Bytes()))
	*snapshot = w.Bytes()
	DPrintf("len(snapshot):%d", len(*snapshot))
}
func (kv *ShardKV) installSnapshot(snapshot []byte) {
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var commitIndex int
	shards := make(map[int]Shard)
	lastConfig := shardctrler.Config{
		Groups: make(map[int][]string),
	}
	currentConfig := shardctrler.Config{
		Groups: make(map[int][]string),
	}
	d.Decode(&commitIndex)
	d.Decode(&shards)
	d.Decode(&lastConfig)
	d.Decode(&currentConfig)
	kv.commitIndex = commitIndex
	kv.shards = shards
	kv.lastConfig = lastConfig
	kv.currentConfig = currentConfig

}
func (kv *ShardKV) runApply() {
	for {
		select {
		case applyMsg := <-kv.applyCh:
			// 处理通道消息 m
			kv.mu.Lock()
			if kv.killed() {
				kv.mu.Unlock()
				return
			}

			if applyMsg.CommandValid {
				if (kv.commitIndex + 1) != applyMsg.CommandIndex {
					log.Fatalf("%d-%d 提交错误,kv.commitIndex=%d,applyMsg.CommandIndex:%d", kv.gid, kv.me, kv.commitIndex, applyMsg.CommandIndex)
				}
				kv.commitIndex = applyMsg.CommandIndex
				op := applyMsg.Command.(Op)
				if op.CommandType == PUT {
					data := op.Data.(PutAppendData)
					shard := key2shard(data.Key)
					if kv.checkeServerKey(data.Key) {
						DPrintf("%d-%d,收到raft的put提交消息，seq:%d,key:%s,value:%s,shard:%d", kv.gid, kv.me, data.Seq, data.Key, data.Value, key2shard(data.Key))
						duplicate, ok := kv.shards[shard].DuplicateMap[data.ClientId]
						if ok {
							if duplicate.Seq >= data.Seq {
								DPrintf("%d-%d,收到raft的put提交消息，seq:%d,key:%s,value:%s,shard:%d,是重复提交的", kv.gid, kv.me, data.Seq, data.Key, data.Value, key2shard(data.Key))
							} else if duplicate.Seq < data.Seq {
								kv.shards[shard].KvMap[data.Key] = data.Value
								d := Duplicate{
									Seq:      data.Seq,
									ClientId: data.ClientId,
									Op:       op.CommandType,
								}
								kv.shards[shard].DuplicateMap[data.ClientId] = d

							}
						} else {
							kv.shards[shard].KvMap[data.Key] = data.Value
							d := Duplicate{
								Seq:      data.Seq,
								ClientId: data.ClientId,
								Op:       op.CommandType,
							}
							kv.shards[shard].DuplicateMap[data.ClientId] = d

						}

					} else {
						DPrintf("%d-%d,收到raft的put提交消息，seq:%d,key:%s,value:%s,但是不负责shard：%d", kv.gid, kv.me, data.Seq, data.Key, data.Value, shard)
					}
					if kv.maxraftstate > 0 {
						kv.generateSnapshotCond.Broadcast()
					}

				} else if op.CommandType == APPEND {
					data := op.Data.(PutAppendData)
					shard := key2shard(data.Key)
					if kv.checkeServerKey(data.Key) {
						DPrintf("%d-%d,收到raft的append提交消息，seq:%d,key:%s,value:%s,shard:%d", kv.gid, kv.me, data.Seq, data.Key, data.Value, key2shard(data.Key))
						duplicate, ok := kv.shards[shard].DuplicateMap[data.ClientId]
						if ok {
							if duplicate.Seq >= data.Seq {
								DPrintf("%d-%d,收到raft的append提交消息，seq:%d,key:%s,value:%s,shard:%d,是重复提交的", kv.gid, kv.me, data.Seq, data.Key, data.Value, key2shard(data.Key))
							} else if duplicate.Seq < data.Seq {
								kv.shards[shard].KvMap[data.Key] = kv.shards[shard].KvMap[data.Key] + data.Value
								d := Duplicate{
									Seq:      data.Seq,
									ClientId: data.ClientId,
									Op:       op.CommandType,
								}
								kv.shards[shard].DuplicateMap[data.ClientId] = d

							}
						} else {
							kv.shards[shard].KvMap[data.Key] = kv.shards[shard].KvMap[data.Key] + data.Value
							d := Duplicate{
								Seq:      data.Seq,
								ClientId: data.ClientId,
								Op:       op.CommandType,
							}
							kv.shards[shard].DuplicateMap[data.ClientId] = d

						}
					} else {
						DPrintf("%d-%d,收到raft的append提交消息，seq:%d,key:%s,value:%s,但是不负责shard:%d", kv.gid, kv.me, data.Seq, data.Key, data.Value, shard)

					}

					if kv.maxraftstate > 0 {
						kv.generateSnapshotCond.Broadcast()
					}

				} else if op.CommandType == GET {
					data := op.Data.(GetData)
					shard := key2shard(data.Key)
					if kv.checkeServerKey(data.Key) {
						DPrintf("%d-%d,收到raft的get提交消息，seq:%d,key:%s,shard:%d", kv.gid, kv.me, data.Seq, data.Key, key2shard(data.Key))

						duplicate, ok := kv.shards[shard].DuplicateMap[data.ClientId]
						if ok {
							if duplicate.Seq >= data.Seq {
								DPrintf("%d-%d,收到raft的get提交消息，seq:%d,key:%s,shard:%d,是重复提交的", kv.gid, kv.me, data.Seq, data.Key, key2shard(data.Key))
							} else if duplicate.Seq < data.Seq {
								val, ok := kv.shards[shard].KvMap[data.Key]
								if ok {
									d := Duplicate{
										Seq:          data.Seq,
										ClientId:     data.ClientId,
										Op:           op.CommandType,
										Value:        val,
										IsVaildValue: true,
									}
									DPrintf("%d-%d在apply get消息,seq:%d", kv.gid, kv.me, data.Seq)
									kv.shards[shard].DuplicateMap[data.ClientId] = d
								} else {
									d := Duplicate{
										Seq:          data.Seq,
										ClientId:     data.ClientId,
										Op:           op.CommandType,
										Value:        "",
										IsVaildValue: false,
									}
									DPrintf("%d-%d在apply get消息,,seq:%d", kv.gid, kv.me, data.Seq)
									kv.shards[shard].DuplicateMap[data.ClientId] = d

								}

							}
						} else {
							d := Duplicate{
								Seq:          data.Seq,
								ClientId:     data.ClientId,
								Op:           op.CommandType,
								Value:        kv.shards[shard].KvMap[data.Key],
								IsVaildValue: true,
							}
							kv.shards[shard].DuplicateMap[data.ClientId] = d

						}
					} else {
						DPrintf("%d-%d,收到raft的get提交消息，seq:%d,key:%s,是重复提交的,但是不负责shard:%d", kv.gid, kv.me, data.Seq, data.Key, shard)
					}
					if kv.maxraftstate > 0 {
						kv.generateSnapshotCond.Broadcast()
					}

				} else if op.CommandType == INSTALLCONFIG {
					data := op.Data.(InstallConfigData)

					for {
						if data.Config.Num <= kv.currentConfig.Num {
							DPrintf("%d-%d,收到了一个过期的config,kv.config.num:%d,data.config.num:%d", kv.gid, kv.me, kv.currentConfig.Num, data.Config.Num)
							break
						} else if data.Config.Num > kv.currentConfig.Num+1 {
							log.Fatalf("%d-%d,收到了一个错误的config,kv.config.num:%d,data.config.num:%d", kv.gid, kv.me, kv.currentConfig.Num, data.Config.Num)
						}
						DPrintf("%d-%d,收到了一个正确的的config,kv.config.num:%d,data.config.num:%d", kv.gid, kv.me, kv.currentConfig.Num, data.Config.Num)
						if kv.isInstallConfig() {
							DPrintf("%d-%d,正在安装config,kv.config.num:%d,data.config.num:%d", kv.gid, kv.me, kv.currentConfig.Num, data.Config.Num)
							for i := 0; i < shardctrler.NShards; i++ {
								if kv.currentConfig.Shards[i] == kv.gid && kv.currentConfig.Shards[i] != data.Config.Shards[i] {
									s := kv.shards[i]
									s.State = BEPULLING
									kv.shards[i] = s
								} else if kv.currentConfig.Shards[i] != kv.gid && data.Config.Shards[i] == kv.gid {
									if kv.currentConfig.Shards[i] == 0 {

										kv.shards[i] = Shard{
											State:        SERVER,
											DuplicateMap: make(map[int64]Duplicate),
											KvMap:        make(map[string]string),
											Version:      data.Config.Num,
										}
									} else {
										kv.shards[i] = Shard{
											State:        PULLING,
											DuplicateMap: make(map[int64]Duplicate),
											KvMap:        make(map[string]string),
											Version:      data.Config.Num,
										}
										kv.pullShardCond.Broadcast()

									}
								} else if kv.currentConfig.Shards[i] == kv.gid && kv.currentConfig.Shards[i] == data.Config.Shards[i] {
									s := kv.shards[i]
									s.Version = data.Config.Num
									kv.shards[i] = s
								}
							}
							kv.lastConfig = kv.currentConfig
							kv.currentConfig = shardctrler.Clone(data.Config)
							break
						} else {
							DPrintf("%d-%d,需要等待shard正确，需等待安装config,kv.config.num:%d,data.config.num:%d", kv.gid, kv.me, kv.currentConfig.Num, data.Config.Num)
							//kv.installConfigCond.Wait()
							break
						}
					}

					if kv.maxraftstate > 0 {
						kv.generateSnapshotCond.Broadcast()
					}

				} else if op.CommandType == DELETESHARD {
					data := op.Data.(DeleteShardData)
					if shard, ok := kv.shards[data.ShardId]; ok {
						if data.Version != shard.Version || kv.shards[data.ShardId].State != NOSERVER {
							DPrintf("%d-%d,delete错误版本的shard,kv.config.Num:%d,data.ConfigNum:%d，data.ShardId:%d", kv.gid, kv.me, kv.currentConfig.Num, data.Version, data.ShardId)
						} else {
							DPrintf("%d-%d,delete正确版本的shard,kv.config.Num:%d,data.ConfigNum:%d,state:%d,data.ShardId:%d", kv.gid, kv.me, kv.currentConfig.Num, data.Version, kv.shards[data.ShardId].State, data.ShardId)
							delete(kv.shards, data.ShardId)
							if kv.maxraftstate > 0 {
								kv.generateSnapshotCond.Broadcast()
							}
						}
					}

				} else if op.CommandType == INSTALLSHARD {
					data := op.Data.(InstallShardData)

					if data.ConfigNum != kv.currentConfig.Num || kv.shards[data.ShardId].State != PULLING {
						DPrintf("%d-%d,安装错误版本的shard,kv.config.Num:%d,data.ConfigNum:%d，data.ShardId:%d,%v", kv.gid, kv.me, kv.currentConfig.Num, data.ConfigNum, data.ShardId, data.ShardData)
					} else {
						DPrintf("%d-%d,安装正确版本的shard,kv.config.Num:%d,data.ConfigNum:%d,state:%d,data.ShardId:%d ,%v", kv.gid, kv.me, kv.currentConfig.Num, data.ConfigNum, kv.shards[data.ShardId].State, data.ShardId, data.ShardData)
						r := bytes.NewBuffer(data.ShardData)
						d := labgob.NewDecoder(r)
						shard := Shard{}
						d.Decode(&shard)
						shard.Version = data.ConfigNum
						shard.State = SERVER
						kv.shards[data.ShardId] = shard
						if kv.maxraftstate > 0 {
							kv.generateSnapshotCond.Broadcast()
						}
					}

				} else if op.CommandType == ChangeShard {
					data := op.Data.(ChangeShardState)

					if data.Version != kv.shards[data.ShardId].Version {
						DPrintf("%d-%d,change错误版本的shard,kv.config.Num:%d,data:%v", kv.gid, kv.me, kv.lastConfig.Num, data)
					} else {
						DPrintf("%d-%d,change正确版本的shard,kv.config.Num:%d,data:%v", kv.gid, kv.me, kv.lastConfig.Num, data)
						shard := kv.shards[data.ShardId]
						shard.State = data.ShardState
						kv.shards[data.ShardId] = shard
						if kv.maxraftstate > 0 {
							kv.generateSnapshotCond.Broadcast()
						}
						kv.deleteShardCond.Broadcast()
					}

				} else {
					log.Fatalf("无此操作")
				}
				kv.commitCond.Broadcast()

			} else if applyMsg.SnapshotValid {
				if applyMsg.SnapshotIndex > kv.commitIndex {
					kv.installSnapshot(applyMsg.Snapshot)
					kv.commitCond.Broadcast()
				}

			}
			kv.mu.Unlock()
		case <-kv.killChan:
			return // 接收到 stopCh 信号时退出协程
		}
	}

}
func (kv *ShardKV) tickerSnapshot() {
	for {
		kv.mu.Lock()
		if kv.killed() {
			kv.mu.Unlock()
			return
		}
		for kv.rf.GetPersistLogSize() < kv.maxraftstate {
			kv.generateSnapshotCond.Wait()
			if kv.killed() {
				kv.mu.Unlock()
				return
			}
		}

		DPrintf("%d-%d服务器,kv.commitIndex:%d,kv.rf.GetPersistLogSize():%d,kv.maxraftstate:%d", kv.gid, kv.me, kv.commitIndex, kv.rf.GetPersistLogSize(), kv.maxraftstate)

		var snapshot []byte
		kv.generateSnapshot(&snapshot)
		DPrintf("%d-%d安装快照，len(snapshot):%d开始", kv.gid, kv.me, len(snapshot))
		go func(commitIndex int) {
			kv.rf.Snapshot(commitIndex, snapshot)
		}(kv.commitIndex)
		DPrintf("%d-%d安装快照，len(snapshot):%d结束", kv.gid, kv.me, len(snapshot))
		kv.mu.Unlock()
		//time.Sleep(1 * time.Millisecond)

	}
}
func (kv *ShardKV) prepareStart() {
	/*kv.mu.Lock()
	for{
		_, isLeader :=kv.rf.GetState()
		if isLeader &&kv.currentConfig.Num!=-1{
			if kv.lastConfig.Num != -1 {
				for i:=0;i<shardctrler.NShards;i++{
					if kv.currentConfig
				}

			}else{
				break
			}
		}else{
			kv.mu.Unlock()
			time.Sleep(20*time.Millisecond)
		}

	}

	kv.mu.Unlock()*/
}
func (kv *ShardKV) pullShard() {
	for {
		kv.mu.Lock()
		if kv.killed() {
			kv.mu.Unlock()
			return
		}
		//kv.pullShardCond.Wait()
		if kv.killed() {
			kv.mu.Unlock()
			return
		}

		_, isLeader := kv.rf.GetState()
		wg := sync.WaitGroup{}
		if isLeader {
			for shardId, shard := range kv.shards {
				if shard.State == PULLING {
					gid := kv.lastConfig.Shards[shardId]
					servers := kv.lastConfig.Groups[gid]
					lastConfigNum := kv.lastConfig.Num
					go func(servers []string, shardId int, lastConfigNum int) {
						wg.Add(1)
						args := PullShardArgs{
							ShardId:   shardId,
							ConfigNum: lastConfigNum,
						}
						reply := PullShardReply{}
						for { //给其他服务器发送请求shard消息
							isInstall := false
							for i := 0; i < len(servers); i++ {
								c1 := make(chan bool, 1)
								srv := kv.make_end(servers[i])
								ok := false
								go func() {
									ok = srv.Call("ShardKV.GetShard", &args, &reply)
									c1 <- true
								}()
								select {
								case <-c1:
									DPrintf("%d-%d发送的pull消息成功,serverid:%d,shardId:%d,configNUm:%d", kv.gid, kv.me, i, shardId, lastConfigNum)

								case <-time.After(300 * time.Millisecond):
									DPrintf("%d-%d发送的pull消息失败,serverid:%d,shardId:%d,configNUm:%d", kv.gid, kv.me, i, shardId, lastConfigNum)
								}
								if ok && reply.Err == OK {
									DPrintf("len(reply.data):%d,%v", len(reply.Data), reply.Data)
									op := Op{
										CommandType: INSTALLSHARD, // 0 => get, 1 => put, 2 => append
										Data: InstallShardData{
											ConfigNum: lastConfigNum + 1,
											ShardId:   shardId,
											ShardData: reply.Data},
									}
									kv.rf.Start(op)
									go func(servers []string, shardId int, lastConfigNum int) {
										args := ChangeShardArgs{
											ShardId:    shardId,
											ConfigNum:  lastConfigNum,
											ShardState: NOSERVER,
										}
										reply := ChangeShardReply{}
										for { //给其他服务器发送请求shard消息
											isChange := false
											for i := 0; i < len(servers); i++ {
												c1 := make(chan bool, 1)
												srv := kv.make_end(servers[i])
												ok := false
												go func() {
													ok = srv.Call("ShardKV.ChangeShard", &args, &reply)
													c1 <- true
												}()
												select {
												case <-c1:
													DPrintf("%d-%d发送的change shard消息成功,serverid:%d,shardId:%d,configNUm:%d", kv.gid, kv.me, i, shardId, lastConfigNum)

												case <-time.After(300 * time.Millisecond):
													DPrintf("%d-%change shard,serverid:%d,shardId:%d,configNUm:%d", kv.gid, kv.me, i, shardId, lastConfigNum)
												}
												if ok && reply.Err == OK {
													isChange = true
													break
												}
											}
											if isChange {
												break
											}

										}

									}(servers, shardId, lastConfigNum)
									isInstall = true
									break
								}

							}
							if isInstall {
								break
							}

						}
						wg.Done()
					}(servers, shardId, lastConfigNum)
				}
			}

		}

		kv.mu.Unlock()
		//time.Sleep(1 * time.Millisecond)
		wg.Wait()
		time.Sleep(20 * time.Millisecond)

	}
}
func (kv *ShardKV) deleteShard() {
	for {
		kv.mu.Lock()
		if kv.killed() {
			kv.mu.Unlock()
			return
		}
		for i := 0; i < shardctrler.NShards; i++ {
			if shard, ok := kv.shards[i]; ok {
				if shard.State == NOSERVER {
					op := Op{
						CommandType: DELETESHARD, // 0 => get, 1 => put, 2 => append
						Data: DeleteShardData{
							Version: shard.Version,
							ShardId: i,
						},
					}
					kv.rf.Start(op)
				}
			}
		}
		kv.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
	}

}
func (kv *ShardKV) printShardStatus() {
	DPrintf("%d-%d的shard状态=========================================", kv.gid, kv.me)
	for i := 0; i < shardctrler.NShards; i++ {
		if shard, ok := kv.shards[i]; ok {
			DPrintf("%d-%d的shard状态:config:%d,shardid:%d,shardversion:%d,shardstatus:%d", kv.gid, kv.me, kv.currentConfig.Num, i, shard.Version, shard.State)
		}
	}
	DPrintf("%d-%d的shard状态结束=========================================", kv.gid, kv.me)

}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(PutAppendData{})
	labgob.Register(InstallConfigData{})
	labgob.Register(InstallShardData{})
	labgob.Register(GetData{})
	labgob.Register(DeleteShardData{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(Shard{})
	labgob.Register(ChangeShardState{})
	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	kv.lastConfig = shardctrler.Config{Num: -1}
	// Your initialization code here.
	kv.shards = make(map[int]Shard)
	kv.currentConfig = shardctrler.Config{Num: -1}
	kv.sm = shardctrler.MakeClerk(ctrlers)
	kv.shardStateCond = sync.NewCond(&kv.mu)
	kv.commitIndex = 0
	kv.commitCond = sync.NewCond(&kv.mu)
	kv.generateSnapshotCond = sync.NewCond(&kv.mu)
	kv.killChan = make(chan interface{}, 1)
	kv.installConfigCond = sync.NewCond(&kv.mu)
	kv.pullShardCond = sync.NewCond(&kv.mu)
	kv.deleteShardCond = sync.NewCond(&kv.mu)
	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	go kv.runApply()
	go kv.getConfig()
	go kv.pullShard()
	//go kv.prepareStart()
	//go kv.deleteShard()
	if kv.maxraftstate > 0 {
		go kv.tickerSnapshot()
	}
	return kv
}
