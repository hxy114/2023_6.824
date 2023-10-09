package shardctrler

//
// Shardctrler clerk.
//

import (
	"6.5840/labrpc"
	"sync"
)
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	seq int
	mu  sync.Mutex
	id  int64
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
	// Your code here.
	ck.id = nrand()
	ck.seq = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	args.Seq = ck.seq
	ck.seq++
	args.ClientId = ck.id
	// Your code here.
	args.Num = num
	for {
		// try each known server.
		for i, srv := range ck.servers {
			var reply QueryReply
			DPrintf("%d,发送请求query给%d,seq:%d,Num:%d", ck.id, i, args.Seq, args.Num)
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && reply.Err == OK {
				DPrintf("%d,发送请求query给%d,seq:%d,Num:%d 成功", ck.id, i, args.Seq, args.Num)
				return reply.Config
			}
			DPrintf("%d,发送请求query给%d,seq:%d,Num:%d 失败", ck.id, i, args.Seq, args.Num)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Seq = ck.seq
	ck.seq++
	args.ClientId = ck.id
	args.Servers = servers

	for {
		// try each known server.
		for i, srv := range ck.servers {
			var reply JoinReply
			DPrintf("%d,发送请求join给%d,seq:%d,Servers:%v", ck.id, i, args.Seq, args.Servers)
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && reply.Err == OK {
				DPrintf("%d,发送请求join给%d,seq:%d,Servers:%v 成功", ck.id, i, args.Seq, args.Servers)
				return
			}
			DPrintf("%d,发送请求join给%d,seq:%d,Servers:%v 失败", ck.id, i, args.Seq, args.Servers)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.Seq = ck.seq
	ck.seq++
	args.ClientId = ck.id
	args.GIDs = gids

	for {
		// try each known server.
		for i, srv := range ck.servers {
			var reply LeaveReply
			DPrintf("%d,发送请求leave给%d,seq:%d,GIDs:%v", ck.id, i, args.Seq, args.GIDs)
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.Err == OK {
				DPrintf("%d,发送请求leave给%d,seq:%d,GIDs:%v 成功", ck.id, i, args.Seq, args.GIDs)
				return
			}
			DPrintf("%d,发送请求leave给%d,seq:%d,GIDs:%v 失败", ck.id, i, args.Seq, args.GIDs)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Seq = ck.seq
	ck.seq++
	args.ClientId = ck.id
	args.Shard = shard
	args.GID = gid

	for {
		// try each known server.
		for i, srv := range ck.servers {
			var reply MoveReply
			DPrintf("%d,发送请求leave给%d,seq:%d,GIDs:%d,Shard:%d", ck.id, i, args.Seq, args.GID, args.Shard)
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && reply.Err == OK {
				DPrintf("%d,发送请求leave给%d,seq:%d,GIDs:%d,Shard:%d 成功", ck.id, i, args.Seq, args.GID, args.Shard)
				return
			}
			DPrintf("%d,发送请求leave给%d,seq:%d,GIDs:%d,Shard:%d 失败", ck.id, i, args.Seq, args.GID, args.Shard)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
