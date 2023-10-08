package kvraft

import (
	"6.5840/labrpc"
	"sync"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	seq     int
	mu      sync.Mutex
	id      int64
	//leaderId int
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.id = nrand()
	//ck.leaderId = -1
	ck.seq = 0
	// You'll have to add code here.
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	args := &GetArgs{
		ClientId: ck.id,
		Seq:      ck.seq,
		Key:      key}
	me := ck.id
	ck.seq++
	ck.mu.Unlock()
	ret := ""
	isVaildRet := false
	retMu := sync.Mutex{}
	retCond := sync.NewCond(&retMu)

	for i := 0; i < len(ck.servers); i++ {
		go func(i int) {
			for {
				DPrintf("%d,发送请求get给%d,seq:%d,key:%s", me, i, args.Seq, args.Key)
				reply := &GetReply{}
				c1 := make(chan bool, 1)
				c2 := make(chan bool, 1)
				ok := false
				go func() {
					ok = ck.servers[i].Call("KVServer.Get", args, reply)
					c1 <- true
				}()
				go func() {
					time.Sleep(1 * time.Second)
					c2 <- true
				}()
				select {
				case <-c1:
					DPrintf("%d发给%d的get消息返回,seq:%d", me, i, args.Seq)
				case <-c2:
					DPrintf("%d发给%d的get消息丢失,seq:%d", me, i, args.Seq)
				}

				if ok && reply.Err == OK {
					DPrintf("%d成功添加get请求,seq:%d,key:%s", i, args.Seq, args.Key)
					retMu.Lock()
					ret = reply.Value
					isVaildRet = true
					retCond.Broadcast()
					retMu.Unlock()
					return
				} else if ok && reply.Err == ErrNoKey {
					DPrintf("%d成功添加get请求,seq:%d,key:%s,但是无此key的value", i, args.Seq, args.Key)
					retMu.Lock()
					ret = ""
					isVaildRet = true
					retCond.Broadcast()
					retMu.Unlock()
					return
				} else if ok && reply.Err == ErrExpired {
					DPrintf("%d成功添加get请求,seq:%d,key:%s,但是消息过期了", i, args.Seq, args.Key)
					retMu.Lock()
					isVaildRet = true
					retCond.Broadcast()
					retMu.Unlock()
					return
				} else if ok && reply.Err == ErrWrongLeader {
					DPrintf("%d不是leader,seq:%d,key:%s", i, args.Seq, args.Key)
					retMu.Lock()
					if isVaildRet == true {
						retCond.Broadcast()
						retMu.Unlock()
						return
					} else {
						retMu.Unlock()
						time.Sleep(300 * time.Millisecond)
						retMu.Lock()
						if isVaildRet == true {
							retCond.Broadcast()
							retMu.Unlock()
							return
						}
						retMu.Unlock()
					}

				} else {
					DPrintf("%d在发送get消息时候,%d服务器失去联系,seq:%d", me, i, args.Seq)
					retMu.Lock()
					if isVaildRet == true {
						retCond.Broadcast()
						retMu.Unlock()
						return
					}
					retMu.Unlock()
				}
			}
		}(i)

	}
	retMu.Lock()
	defer retMu.Unlock()
	if isVaildRet == false {
		retCond.Wait()
	}

	// You will have to modify this function.
	return ret
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.mu.Lock()
	args := &PutAppendArgs{
		ClientId: ck.id,
		Seq:      ck.seq,
		Key:      key,
		Value:    value,
		Op:       op,
	}
	ck.seq++
	me := ck.id
	ck.mu.Unlock()
	isVaildRet := false
	retMu := sync.Mutex{}
	retCond := sync.NewCond(&retMu)

	for i := 0; i < len(ck.servers); i++ {
		go func(i int) {
			for {
				DPrintf("发送请求PutAppend给%d,key:%s,value:%s", i, args.Key, args.Value)

				reply := &PutAppendReply{}
				c1 := make(chan bool, 1)
				c2 := make(chan bool, 1)
				ok := false
				go func() {
					ok = ck.servers[i].Call("KVServer.PutAppend", args, reply)
					c1 <- true
				}()
				go func() {
					time.Sleep(2 * time.Second)
					c2 <- true
				}()
				select {
				case <-c1:
					DPrintf("%d发给%d的PutAppend消息返回,seq:%d", me, i, args.Seq)
				case <-c2:
					DPrintf("%d发给%d的PutAppend消息丢失,seq:%d", me, i, args.Seq)
				}

				if ok && reply.Err == OK {
					DPrintf("%d成功添加putAppend请求,key:%s,value:%s", i, args.Key, args.Value)
					retMu.Lock()
					isVaildRet = true
					retCond.Broadcast()
					retMu.Unlock()
					return
				} else if ok && reply.Err == ErrExpired {
					DPrintf("%d成功添加get请求,seq:%d,key:%s,但是消息过期了", i, args.Seq, args.Key)
					retMu.Lock()
					isVaildRet = true
					retCond.Broadcast()
					retMu.Unlock()
					return
				} else if ok && reply.Err == ErrWrongLeader {
					DPrintf("%d不是leader,seq:%d,key:%s", i, args.Seq, args.Key)
					retMu.Lock()
					if isVaildRet == true {
						retCond.Broadcast()
						retMu.Unlock()
						return
					} else {
						retMu.Unlock()
						time.Sleep(10 * time.Millisecond)
						retMu.Lock()
						if isVaildRet == true {
							retCond.Broadcast()
							retMu.Unlock()
							return
						}
						retMu.Unlock()
					}

				} else {
					DPrintf("%d在发送PutAppend消息时候,%d服务器失去联系,seq:%d", me, i, args.Seq)
					retMu.Lock()
					if isVaildRet == true {
						retCond.Broadcast()
						retMu.Unlock()
						return
					}
					retMu.Unlock()
				}
			}
		}(i)
	}

	retMu.Lock()
	defer retMu.Unlock()
	if isVaildRet == false {
		retCond.Wait()
	}

	return
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
