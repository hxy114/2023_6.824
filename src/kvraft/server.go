package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key      string
	Value    string
	Op       int //0get   1 put 2 append
	ClientId int64
	Seq      int
}
type Duplicate struct {
	ClientId     int64
	Seq          int
	Op           int
	Value        string
	IsVaildValue bool
}
type KVServer struct {
	mu          sync.Mutex
	me          int
	rf          *raft.Raft
	applyCh     chan raft.ApplyMsg
	dead        int32 // set by Kill()
	kvMap       map[string]string
	commitIndex int
	//commitTerm   int
	commitCond           *sync.Cond
	maxraftstate         int // snapshot if log grows this big
	maxCount             int
	duplicateMap         map[int64]Duplicate
	isTickerSnapshot     bool
	generateSnapshotCond *sync.Cond
	killChan             chan interface{}
	// Your definitions here.
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("%d,收到get消息,key：%s,clientid:%d,seq:%d", kv.me, args.Key, args.ClientId, args.Seq)
	op := Op{
		Op:       0, // 0 => get, 1 => put, 2 => append
		Key:      args.Key,
		ClientId: args.ClientId,
		Seq:      args.Seq,
	}

	if kv.killed() {
		DPrintf("%d已经死了，但是还是收到了get消息", kv.me)
		reply.Err = ErrWrongLeader
		return
	}
	duplicate, ok := kv.duplicateMap[args.ClientId]
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
			if _, isLeader = kv.rf.GetState(); isLeader == true {
				if kv.commitIndex >= index {
					DPrintf("%d,提交了get消息,key:%s,seq:%d,client:%d", kv.me, args.Key, args.Seq, args.ClientId)

					duplicate, _ := kv.duplicateMap[args.ClientId]
					//DPrintf("duplicate:%v", duplicate)
					if duplicate.Seq > op.Seq {
						reply.Err = ErrExpired

					} else if duplicate.Seq == op.Seq {
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

			} else {
				reply.Err = ErrWrongLeader
				return
			}

		}
	} else {
		DPrintf("%d,发送get消息后发现自己不是leader,seq:%d", kv.me, args.Seq)
		reply.Err = ErrWrongLeader
		return
	}

	// Your code here.
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if args.Op == "Put" {
		DPrintf("%d,收到put消息,key：%s,clientid:%d,seq:%d", kv.me, args.Key, args.ClientId, args.Seq)
		op := Op{
			Op:       1, // 0 => get, 1 => put, 2 => append
			Key:      args.Key,
			Value:    args.Value,
			ClientId: args.ClientId,
			Seq:      args.Seq,
		}

		if kv.killed() {
			DPrintf("%d已经死了，但是还是收到了put消息", kv.me)
			reply.Err = ErrWrongLeader
			return
		}
		duplicate, ok := kv.duplicateMap[args.ClientId]
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
				if _, isLeader = kv.rf.GetState(); isLeader == true {
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

				} else {
					reply.Err = ErrWrongLeader
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
		op := Op{
			Op:       2, // 0 => get, 1 => put, 2 => append
			Key:      args.Key,
			Value:    args.Value,
			ClientId: args.ClientId,
			Seq:      args.Seq,
		}
		if kv.killed() {
			DPrintf("%d已经死了，但是还是收到了append消息", kv.me)
			reply.Err = ErrWrongLeader
			return
		}
		duplicate, ok := kv.duplicateMap[args.ClientId]
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
				if _, isLeader = kv.rf.GetState(); isLeader == true {
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
		} else {
			DPrintf("%d,发送append消息后发现自己不是leader,seq:%d", kv.me, args.Seq)
			reply.Err = ErrWrongLeader
			return
		}
	}

	// Your code here.
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
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

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}
func (kv *KVServer) generateSnapshot(snapshot *[]byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.commitIndex)
	e.Encode(kv.duplicateMap)
	e.Encode(kv.kvMap)
	DPrintf("len(w.Bytes()):%d", len(w.Bytes()))
	*snapshot = w.Bytes()
	DPrintf("len(snapshot):%d", len(*snapshot))
}
func (kv *KVServer) installSnapshot(snapshot []byte) {
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var commitIndex int
	duplicateMap := make(map[int64]Duplicate)
	kvMap := make(map[string]string)
	d.Decode(&commitIndex)
	d.Decode(&duplicateMap)
	d.Decode(&kvMap)
	kv.commitIndex = commitIndex
	kv.duplicateMap = duplicateMap
	kv.kvMap = kvMap

}
func (kv *KVServer) runApply() {
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
				if op.Op == 1 {
					DPrintf("%d,收到raft的put提交消息，seq:%d,key:%s,value:%s", kv.me, op.Seq, op.Key, op.Value)
					duplicate, ok := kv.duplicateMap[op.ClientId]
					if ok {
						if duplicate.Seq >= op.Seq {
							DPrintf("%d,收到raft的put提交消息，seq:%d,key:%s,value:%s,是重复提交的", kv.me, op.Seq, op.Key, op.Value)
						} else if duplicate.Seq < op.Seq {
							kv.kvMap[op.Key] = op.Value
							d := Duplicate{
								Seq:      op.Seq,
								ClientId: op.ClientId,
								Op:       op.Op,
							}
							kv.duplicateMap[op.ClientId] = d

						}
					} else {
						kv.kvMap[op.Key] = op.Value
						d := Duplicate{
							Seq:      op.Seq,
							ClientId: op.ClientId,
							Op:       op.Op,
						}
						kv.duplicateMap[op.ClientId] = d

					}
					/*if kv.maxraftstate > 0 && kv.commitIndex%kv.maxCount == 0 {
						DPrintf("%d在获取logsize开始", kv.me)
						if kv.rf.GetPersistLogSize() > kv.maxraftstate {

							var snapshot []byte
							kv.generateSnapshot(&snapshot)
							DPrintf("%d安装快照，len(snapshot):%d开始", kv.me, len(snapshot))
							go func(commitIndex int) {
								kv.rf.Snapshot(kv.commitIndex, snapshot)
							}(kv.commitIndex)
							DPrintf("%d安装快照，len(snapshot):%d结束", kv.me, len(snapshot))
							kv.isTickerSnapshot = false
						}
						DPrintf("%d在获取logsize结束", kv.me)
					}*/
					if kv.maxraftstate > 0 {
						kv.generateSnapshotCond.Broadcast()
					}

				} else if op.Op == 2 {
					DPrintf("%d,收到raft的append提交消息，seq:%d,key:%s,value:%s", kv.me, op.Seq, op.Key, op.Value)
					duplicate, ok := kv.duplicateMap[op.ClientId]
					if ok {
						if duplicate.Seq >= op.Seq {
							DPrintf("%d,收到raft的append提交消息，seq:%d,key:%s,value:%s,是重复提交的", kv.me, op.Seq, op.Key, op.Value)
						} else if duplicate.Seq < op.Seq {
							kv.kvMap[op.Key] = kv.kvMap[op.Key] + op.Value
							d := Duplicate{
								Seq:      op.Seq,
								ClientId: op.ClientId,
								Op:       op.Op,
							}
							kv.duplicateMap[op.ClientId] = d

						}
					} else {
						kv.kvMap[op.Key] = kv.kvMap[op.Key] + op.Value
						d := Duplicate{
							Seq:      op.Seq,
							ClientId: op.ClientId,
							Op:       op.Op,
						}
						kv.duplicateMap[op.ClientId] = d

					}
					/*DPrintf("%d在获取logsize开始", kv.me)
					if kv.maxraftstate > 0 && kv.commitIndex%kv.maxCount == 0 {
						if kv.rf.GetPersistLogSize() > kv.maxraftstate {
							var snapshot []byte
							kv.generateSnapshot(&snapshot)
							DPrintf("%d安装快照，len(snapshot):%d开始", kv.me, len(snapshot))
							go func(commitIndex int) {
								kv.rf.Snapshot(kv.commitIndex, snapshot)
							}(kv.commitIndex)
							DPrintf("%d安装快照，len(snapshot):%d结束", kv.me, len(snapshot))
							kv.isTickerSnapshot = false
						}
					}
					DPrintf("%d在获取logsize结束", kv.me)*/
					if kv.maxraftstate > 0 {
						kv.generateSnapshotCond.Broadcast()
					}

				} else if op.Op == 0 {
					DPrintf("%d,收到raft的get提交消息，seq:%d,key:%s", kv.me, op.Seq, op.Key)

					duplicate, ok := kv.duplicateMap[op.ClientId]
					if ok {
						if duplicate.Seq >= op.Seq {
							DPrintf("%d,收到raft的get提交消息，seq:%d,key:%s,value:%s,是重复提交的", kv.me, op.Seq, op.Key, op.Value)
						} else if duplicate.Seq < op.Seq {
							val, ok := kv.kvMap[op.Key]
							if ok {
								d := Duplicate{
									Seq:          op.Seq,
									ClientId:     op.ClientId,
									Op:           op.Op,
									Value:        val,
									IsVaildValue: true,
								}
								DPrintf("%d在apply get消息,seq:%d", kv.me, op.Seq)
								kv.duplicateMap[op.ClientId] = d
							} else {
								d := Duplicate{
									Seq:          op.Seq,
									ClientId:     op.ClientId,
									Op:           op.Op,
									Value:        "",
									IsVaildValue: false,
								}
								DPrintf("%d在apply get消息,,seq:%d", kv.me, op.Seq)
								kv.duplicateMap[op.ClientId] = d

							}

						}
					} else {
						d := Duplicate{
							Seq:          op.Seq,
							ClientId:     op.ClientId,
							Op:           op.Op,
							Value:        kv.kvMap[op.Key],
							IsVaildValue: true,
						}
						kv.duplicateMap[op.ClientId] = d

					}
					DPrintf("%d在获取logsize开始", kv.me)
					/*if kv.maxraftstate > 0 && kv.commitIndex%kv.maxCount == 0 {
						if kv.rf.GetPersistLogSize() > kv.maxraftstate {
							var snapshot []byte
							kv.generateSnapshot(&snapshot)
							DPrintf("%d安装快照，len(snapshot):%d开始", kv.me, len(snapshot))
							go func(commitIndex int) {
								kv.rf.Snapshot(kv.commitIndex, snapshot)
							}(kv.commitIndex)
							DPrintf("%d安装快照，len(snapshot):%d结束", kv.me, len(snapshot))
							kv.isTickerSnapshot = false
						}
					}
					DPrintf("%d在获取logsize结束", kv.me)*/
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
	/*}
	for applyMsg := range kv.applyCh {


	}*/
}
func (kv *KVServer) timeTickerSnapshot() {
	/*for kv.killed() == false {
		time.Sleep(750 * time.Millisecond)
		kv.mu.Lock()

		if kv.isTickerSnapshot {
			DPrintf("%d 时间到触发snapshot", kv.me)
			var snapshot []byte
			kv.generateSnapshot(&snapshot)
			DPrintf("%d安装快照，len(snapshot):%d开始", kv.me, len(snapshot))
			go func(commitIndex int) {
				kv.rf.Snapshot(kv.commitIndex, snapshot)
			}(kv.commitIndex)

			DPrintf("%d安装快照，len(snapshot):%d结束", kv.me, len(snapshot))
			kv.mu.Unlock()

		} else {
			//DPrintf("%d 没有触发选举", rf.me)
			kv.isTickerSnapshot = true
			kv.mu.Unlock()
		}

		// Your code here (2A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		//ms := 200 + (rand.Int63()%50)*(int64(rf.me)%7)

	}*/
}
func (kv *KVServer) tickerSnapshot() {
	/*for kv.killed() == false {
		time.Sleep(750 * time.Millisecond)
		kv.mu.Lock()

		if kv.isTickerSnapshot {
			DPrintf("%d 时间到触发snapshot", kv.me)
			var snapshot []byte
			kv.generateSnapshot(&snapshot)
			DPrintf("%d安装快照，len(snapshot):%d开始", kv.me, len(snapshot))
			go func(commitIndex int) {
				kv.rf.Snapshot(kv.commitIndex, snapshot)
			}(kv.commitIndex)

			DPrintf("%d安装快照，len(snapshot):%d结束", kv.me, len(snapshot))
			kv.mu.Unlock()

		} else {
			//DPrintf("%d 没有触发选举", rf.me)
			kv.isTickerSnapshot = true
			kv.mu.Unlock()
		}

		// Your code here (2A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		//ms := 200 + (rand.Int63()%50)*(int64(rf.me)%7)

	}*/

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

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.maxCount = kv.maxraftstate / 10
	// You may need initialization code here.
	kv.isTickerSnapshot = true
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.killChan = make(chan interface{}, 1)
	kv.kvMap = make(map[string]string)
	kv.duplicateMap = make(map[int64]Duplicate)
	kv.commitIndex = 0
	kv.commitCond = sync.NewCond(&kv.mu)
	kv.generateSnapshotCond = sync.NewCond(&kv.mu)
	// You may need initialization code here.
	go kv.runApply()
	if kv.maxraftstate > 0 {
		go kv.tickerSnapshot()
	}

	return kv
}
