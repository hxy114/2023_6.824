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
	SERVER shardState = iota
	PULLING
	NOSERVER
)

type Shard struct {
	duplicateMap map[int64]Duplicate
	kvMap        map[string]string
	state        shardState
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
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}
func (kv *ShardKV) isInstallConfig() bool {
	for _, v := range kv.shards {
		if v.state == PULLING {
			return false
		}
	}
	return true
}

func (kv *ShardKV) getConfig() {
	for {
		kv.mu.Lock()
		if kv.killed() {
			DPrintf("%d已经死了，getConfig退出", kv.me)
			kv.mu.Unlock()
			return
		}
		num := kv.currentConfig.Num
		kv.mu.Unlock()
		config := kv.sm.Query(num + 1)
		kv.mu.Lock()
		if config.Num == (kv.currentConfig.Num+1) && kv.isInstallConfig() {
			op := Op{
				CommandType: INSTALLCONFIG,
			}
			op.Data = InstallConfigData{Config: shardctrler.Clone(config)}
			DPrintf("%d开始start installConfig,conifg:%v", kv.me, op.Data)
			kv.rf.Start(op)
			DPrintf("%d结束start installConfig,conifg:%v", kv.me, op.Data)
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
	DPrintf("%d,收到get消息,key：%s,clientid:%d,seq:%d", kv.me, args.Key, args.ClientId, args.Seq)
	if kv.killed() {
		DPrintf("%d已经死了，但是还是收到了get消息", kv.me)
		reply.Err = ErrWrongLeader
		return
	}
	if !kv.checkeServerKey(args.Key) {
		DPrintf("%d 不负责key:%s,shard:%d", kv.me, args.Key, key2shard(args.Key))
		reply.Err = ErrWrongGroup
	}
	shard := key2shard(args.Key)
	op := Op{
		CommandType: GET, // 0 => get, 1 => put, 2 => append
		Data: GetData{Seq: args.Seq,
			ClientId: args.ClientId,
			Key:      args.Key},
	}
	for {
		if kv.shards[shard].state != SERVER {
			kv.shardStateCond.Wait()
		} else {
			break
		}
	}
	if kv.killed() {
		DPrintf("%d已经死了，但是还是收到了get消息", kv.me)
		reply.Err = ErrWrongLeader
		return
	}

	duplicate, ok := kv.shards[shard].duplicateMap[args.ClientId]
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
	DPrintf("%d,把get消息传给raft开始,seq:%d", kv.me, args.Seq)
	index, _, isLeader := kv.rf.Start(op)
	DPrintf("%d,把get消息传给raft结束,seq:%d", kv.me, args.Seq)
	if isLeader {
		DPrintf("%d,发送get消息后发现自己还是leader,seq:%d", kv.me, args.Seq)
		for {
			if _, isLeader = kv.rf.GetState(); isLeader == true && kv.checkeServerKey(args.Key) {
				if kv.commitIndex >= index {
					DPrintf("%d,提交了get消息,key:%s,seq:%d,client:%d", kv.me, args.Key, args.Seq, args.ClientId)

					duplicate, _ := kv.shards[shard].duplicateMap[args.ClientId]
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
					DPrintf("%d,等待get消息提交,key:%s,seq:%d,client:%d,", kv.me, args.Key, args.Seq, args.ClientId)
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
		DPrintf("%d,发送get消息后发现自己不是leader,seq:%d", kv.me, args.Seq)
		reply.Err = ErrWrongLeader
		return
	}

}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if args.Op == "Put" {
		DPrintf("%d,收到put消息,key：%s,clientid:%d,seq:%d", kv.me, args.Key, args.ClientId, args.Seq)
		if kv.killed() {
			DPrintf("%d已经死了，但是还是收到了Put消息", kv.me)
			reply.Err = ErrWrongLeader
			return
		}
		if !kv.checkeServerKey(args.Key) {
			DPrintf("%d 不负责key:%s,shard:%d", kv.me, args.Key, key2shard(args.Key))
			reply.Err = ErrWrongGroup
		}
		shard := key2shard(args.Key)
		op := Op{
			CommandType: PUT, // 0 => get, 1 => put, 2 => append
			Data: PutAppendData{Key: args.Key,
				Value:    args.Value,
				ClientId: args.ClientId,
				Seq:      args.Seq},
		}
		for {
			if kv.shards[shard].state != SERVER {
				kv.shardStateCond.Wait()
			} else {
				break
			}
		}
		if kv.killed() {
			DPrintf("%d已经死了，但是还是收到了put消息", kv.me)
			reply.Err = ErrWrongLeader
			return
		}
		duplicate, ok := kv.shards[shard].duplicateMap[args.ClientId]
		if ok {
			if duplicate.Seq == args.Seq {
				reply.Err = OK
				return
			} else if duplicate.Seq > args.Seq {
				reply.Err = ErrExpired
				return
			}

		}
		DPrintf("%d,把put消息传给raft开始,seq:%d", kv.me, args.Seq)
		index, _, isLeader := kv.rf.Start(op)
		DPrintf("%d,把put消息传给raft结束,seq:%d", kv.me, args.Seq)
		if isLeader {
			DPrintf("%d,发送put消息后发现自己还是leader,seq:%d", kv.me, args.Seq)
			for {
				if _, isLeader = kv.rf.GetState(); isLeader == true && kv.checkeServerKey(args.Key) {
					if kv.commitIndex >= index {
						DPrintf("%d,提交了put消息,key:%s,seq:%d,client:%d", kv.me, args.Key, args.Seq, args.ClientId)
						reply.Err = OK
						return
					} else {
						DPrintf("%d,等待put消息提交,key:%s,seq:%d,client:%d", kv.me, args.Key, args.Seq, args.ClientId)
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
			DPrintf("%d,发送put消息后发现自己不是leader,seq:%d", kv.me, args.Seq)
			reply.Err = ErrWrongLeader
			return
		}

	} else if args.Op == "Append" {
		DPrintf("%d,收到append消息,key：%s,clientid:%d,seq:%d", kv.me, args.Key, args.ClientId, args.Seq)
		if kv.killed() {
			DPrintf("%d已经死了，但是还是收到了Append消息", kv.me)
			reply.Err = ErrWrongLeader
			return
		}
		if !kv.checkeServerKey(args.Key) {
			DPrintf("%d 不负责key:%s,shard:%d", kv.me, args.Key, key2shard(args.Key))
			reply.Err = ErrWrongGroup
		}
		shard := key2shard(args.Key)
		op := Op{
			CommandType: APPEND, // 0 => get, 1 => put, 2 => append
			Data: PutAppendData{Key: args.Key,
				Value:    args.Value,
				ClientId: args.ClientId,
				Seq:      args.Seq},
		}
		for {
			if kv.shards[shard].state != SERVER {
				kv.shardStateCond.Wait()
			} else {
				break
			}
		}
		if kv.killed() {
			DPrintf("%d已经死了，但是还是收到了append消息", kv.me)
			reply.Err = ErrWrongLeader
			return
		}
		duplicate, ok := kv.shards[shard].duplicateMap[args.ClientId]
		if ok {
			if duplicate.Seq == args.Seq {
				reply.Err = OK
				return
			} else if duplicate.Seq > args.Seq {
				reply.Err = ErrExpired
				return
			}

		}
		DPrintf("%d,把append消息传给raft开始,seq:%d", kv.me, args.Seq)
		index, _, isLeader := kv.rf.Start(op)
		DPrintf("%d,把append消息传给raft结束,seq:%d", kv.me, args.Seq)
		if isLeader {
			for {
				if _, isLeader = kv.rf.GetState(); isLeader == true && kv.checkeServerKey(args.Key) {
					if kv.commitIndex >= index {
						DPrintf("%d,提交了append消息,key:%s,value:%s,seq:%d", kv.me, args.Key, args.Value, args.Seq)
						reply.Err = OK
						return
					} else {
						DPrintf("%d,等待append消息提交,seq:%d", kv.me, args.Seq)
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
			DPrintf("%d,发送append消息后发现自己不是leader,seq:%d", kv.me, args.Seq)
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
	DPrintf("%d服务器正在killed", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	kv.commitCond.Broadcast()
	kv.generateSnapshotCond.Broadcast()
	//close(kv.applyCh)
	kv.killChan <- 1
	DPrintf("%d服务器结束killed", kv.me)
	kv.mu.Unlock()
	// Your code here, if desired.
}

func (kv *ShardKV) generateSnapshot(snapshot *[]byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.commitIndex)
	e.Encode(kv.shards)
	DPrintf("len(w.Bytes()):%d", len(w.Bytes()))
	*snapshot = w.Bytes()
	DPrintf("len(snapshot):%d", len(*snapshot))
}
func (kv *ShardKV) installSnapshot(snapshot []byte) {
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var commitIndex int
	shards := make(map[int]Shard)
	d.Decode(&commitIndex)
	d.Decode(&shards)
	kv.commitIndex = commitIndex
	kv.shards = shards

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
					log.Fatalf("%d 提交错误,kv.commitIndex=%d,applyMsg.CommandIndex:%d", kv.me, kv.commitIndex, applyMsg.CommandIndex)
				}
				kv.commitIndex = applyMsg.CommandIndex
				op := applyMsg.Command.(Op)
				if op.CommandType == PUT {
					data := op.Data.(PutAppendData)
					shard := key2shard(data.Key)
					if kv.checkeServerKey(data.Key) {
						DPrintf("%d,收到raft的put提交消息，seq:%d,key:%s,value:%s", kv.me, data.Seq, data.Key, data.Value)
						duplicate, ok := kv.shards[shard].duplicateMap[data.ClientId]
						if ok {
							if duplicate.Seq >= data.Seq {
								DPrintf("%d,收到raft的put提交消息，seq:%d,key:%s,value:%s,是重复提交的", kv.me, data.Seq, data.Key, data.Value)
							} else if duplicate.Seq < data.Seq {
								kv.shards[shard].kvMap[data.Key] = data.Value
								d := Duplicate{
									Seq:      data.Seq,
									ClientId: data.ClientId,
									Op:       op.CommandType,
								}
								kv.shards[shard].duplicateMap[data.ClientId] = d

							}
						} else {
							kv.shards[shard].kvMap[data.Key] = data.Value
							d := Duplicate{
								Seq:      data.Seq,
								ClientId: data.ClientId,
								Op:       op.CommandType,
							}
							kv.shards[shard].duplicateMap[data.ClientId] = d

						}

					} else {
						DPrintf("%d,收到raft的put提交消息，seq:%d,key:%s,value:%s,但是不负责shard：%d", kv.me, data.Seq, data.Key, data.Value, shard)
					}
					if kv.maxraftstate > 0 {
						kv.generateSnapshotCond.Broadcast()
					}

				} else if op.CommandType == APPEND {
					data := op.Data.(PutAppendData)
					shard := key2shard(data.Key)
					if kv.checkeServerKey(data.Key) {
						DPrintf("%d,收到raft的append提交消息，seq:%d,key:%s,value:%s", kv.me, data.Seq, data.Key, data.Value)
						duplicate, ok := kv.shards[shard].duplicateMap[data.ClientId]
						if ok {
							if duplicate.Seq >= data.Seq {
								DPrintf("%d,收到raft的append提交消息，seq:%d,key:%s,value:%s,是重复提交的", kv.me, data.Seq, data.Key, data.Value)
							} else if duplicate.Seq < data.Seq {
								kv.shards[shard].kvMap[data.Key] = kv.shards[shard].kvMap[data.Key] + data.Value
								d := Duplicate{
									Seq:      data.Seq,
									ClientId: data.ClientId,
									Op:       op.CommandType,
								}
								kv.shards[shard].duplicateMap[data.ClientId] = d

							}
						} else {
							kv.shards[shard].kvMap[data.Key] = kv.shards[shard].kvMap[data.Key] + data.Value
							d := Duplicate{
								Seq:      data.Seq,
								ClientId: data.ClientId,
								Op:       op.CommandType,
							}
							kv.shards[shard].duplicateMap[data.ClientId] = d

						}
					} else {
						DPrintf("%d,收到raft的append提交消息，seq:%d,key:%s,value:%s,但是不负责shard:%d", kv.me, data.Seq, data.Key, data.Value, shard)

					}

					if kv.maxraftstate > 0 {
						kv.generateSnapshotCond.Broadcast()
					}

				} else if op.CommandType == GET {
					data := op.Data.(GetData)
					shard := key2shard(data.Key)
					if kv.checkeServerKey(data.Key) {
						DPrintf("%d,收到raft的get提交消息，seq:%d,key:%s", kv.me, data.Seq, data.Key)

						duplicate, ok := kv.shards[shard].duplicateMap[data.ClientId]
						if ok {
							if duplicate.Seq >= data.Seq {
								DPrintf("%d,收到raft的get提交消息，seq:%d,key:%s,是重复提交的", kv.me, data.Seq, data.Key)
							} else if duplicate.Seq < data.Seq {
								val, ok := kv.shards[shard].kvMap[data.Key]
								if ok {
									d := Duplicate{
										Seq:          data.Seq,
										ClientId:     data.ClientId,
										Op:           op.CommandType,
										Value:        val,
										IsVaildValue: true,
									}
									DPrintf("%d在apply get消息,seq:%d", kv.me, data.Seq)
									kv.shards[shard].duplicateMap[data.ClientId] = d
								} else {
									d := Duplicate{
										Seq:          data.Seq,
										ClientId:     data.ClientId,
										Op:           op.CommandType,
										Value:        "",
										IsVaildValue: false,
									}
									DPrintf("%d在apply get消息,,seq:%d", kv.me, data.Seq)
									kv.shards[shard].duplicateMap[data.ClientId] = d

								}

							}
						} else {
							d := Duplicate{
								Seq:          data.Seq,
								ClientId:     data.ClientId,
								Op:           op.CommandType,
								Value:        kv.shards[shard].kvMap[data.Key],
								IsVaildValue: true,
							}
							kv.shards[shard].duplicateMap[data.ClientId] = d

						}
					} else {
						DPrintf("%d,收到raft的get提交消息，seq:%d,key:%s,是重复提交的,但是不负责shard:%d", kv.me, data.Seq, data.Key, shard)
					}
					if kv.maxraftstate > 0 {
						kv.generateSnapshotCond.Broadcast()
					}

				} else if op.CommandType == INSTALLCONFIG {
					data := op.Data.(InstallConfigData)

					for {
						if data.Config.Num <= kv.currentConfig.Num {
							DPrintf("%d,收到了一个过期的config,kv.config.num:%d,data.config.num:%d", kv.me, kv.currentConfig.Num, data.Config.Num)
							break
						} else if data.Config.Num > kv.currentConfig.Num+1 {
							log.Fatalf("%d,收到了一个错误的config,kv.config.num:%d,data.config.num:%d", kv.me, kv.currentConfig.Num, data.Config.Num)
						}
						DPrintf("%d,收到了一个正确的的config,kv.config.num:%d,data.config.num:%d", kv.me, kv.currentConfig.Num, data.Config.Num)
						if kv.isInstallConfig() {
							DPrintf("%d,正在安装config,kv.config.num:%d,data.config.num:%d", kv.me, kv.currentConfig.Num, data.Config.Num)
							for i := 0; i < shardctrler.NShards; i++ {
								if kv.currentConfig.Shards[i] == kv.gid && kv.currentConfig.Shards[i] != data.Config.Shards[i] {
									s := kv.shards[i]
									s.state = NOSERVER
									kv.shards[i] = s
								} else if kv.currentConfig.Shards[i] != kv.gid && data.Config.Shards[i] == kv.gid {
									if kv.currentConfig.Shards[i] == 0 {

										kv.shards[i] = Shard{
											state:        SERVER,
											duplicateMap: make(map[int64]Duplicate),
											kvMap:        make(map[string]string),
										}
									} else {
										kv.shards[i] = Shard{
											state:        PULLING,
											duplicateMap: make(map[int64]Duplicate),
											kvMap:        make(map[string]string),
										}
										kv.pullShardCond.Broadcast()

									}
								}
							}
							kv.lastConfig = kv.currentConfig
							kv.currentConfig = shardctrler.Clone(data.Config)
							break
						} else {
							DPrintf("%d,需要等待shard正确，需等待安装config,kv.config.num:%d,data.config.num:%d", kv.me, kv.currentConfig.Num, data.Config.Num)
							kv.installConfigCond.Wait()
						}
					}

					if kv.maxraftstate > 0 {
						kv.generateSnapshotCond.Broadcast()
					}

				} else if op.CommandType == DELETESHARD {

				} else if op.CommandType == INSTALLSHARD {
					data := op.Data.(InstallShardData)

					if data.ConfigNum != kv.currentConfig.Num {
						log.Fatalf("%d,安装错误版本的shard,kv.config.Num:%d,data.ConfigNum:%d", kv.me, kv.currentConfig.Num, data.ConfigNum)
					}

					if kv.maxraftstate > 0 {
						kv.generateSnapshotCond.Broadcast()
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
		for k, v := range kv.shards {
			if v.state == PULLING {
				gid := kv.lastConfig.Shards[k]
				go func(gid int) {
					for { //给其他服务器发送请求shard消息

					}
				}(gid)
			}
		}
		DPrintf("%d服务器,kv.commitIndex:%d,kv.rf.GetPersistLogSize():%d,kv.maxraftstate:%d", kv.me, kv.commitIndex, kv.rf.GetPersistLogSize(), kv.maxraftstate)

		var snapshot []byte
		kv.generateSnapshot(&snapshot)
		DPrintf("%d安装快照，len(snapshot):%d开始", kv.me, len(snapshot))
		go func(commitIndex int) {
			kv.rf.Snapshot(commitIndex, snapshot)
		}(kv.commitIndex)
		DPrintf("%d安装快照，len(snapshot):%d结束", kv.me, len(snapshot))
		kv.mu.Unlock()
		//time.Sleep(1 * time.Millisecond)

	}
}
func (kv *ShardKV) pullShard() {
	for {
		kv.mu.Lock()
		if kv.killed() {
			kv.mu.Unlock()
			return
		}
		for kv.isInstallConfig() {
			kv.pullShardCond.Wait()
			if kv.killed() {
				kv.mu.Unlock()
				return
			}
		}

		var snapshot []byte
		kv.generateSnapshot(&snapshot)
		DPrintf("%d安装快照，len(snapshot):%d开始", kv.me, len(snapshot))
		go func(commitIndex int) {
			kv.rf.Snapshot(commitIndex, snapshot)
		}(kv.commitIndex)
		DPrintf("%d安装快照，len(snapshot):%d结束", kv.me, len(snapshot))
		kv.mu.Unlock()
		//time.Sleep(1 * time.Millisecond)

	}
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
	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	kv.shards = make(map[int]Shard)
	kv.currentConfig = shardctrler.Config{}
	kv.sm = shardctrler.MakeClerk(ctrlers)
	kv.shardStateCond = sync.NewCond(&kv.mu)
	kv.commitIndex = 0
	kv.commitCond = sync.NewCond(&kv.mu)
	kv.generateSnapshotCond = sync.NewCond(&kv.mu)
	kv.killChan = make(chan interface{}, 1)
	kv.installConfigCond = sync.NewCond(&kv.mu)
	kv.pullShardCond = sync.NewCond(&kv.mu)
	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	go kv.runApply()
	go kv.getConfig()
	if kv.maxraftstate > 0 {
		go kv.tickerSnapshot()
	}
	return kv
}
