package shardkv

import "log"

// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	OK                   = "OK"
	ErrNoKey             = "ErrNoKey"
	ErrWrongGroup        = "ErrWrongGroup"
	ErrWrongLeader       = "ErrWrongLeader"
	ErrWaitShardTran     = "ErrWaitShardTran"
	ErrExpired           = "ErrExpired"
	ErrWaitInstallConfig = "ErrWaitInstallConfig"
	ErrWrongShardVersion = "ErrWrongShardVersion"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64
	Seq      int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key      string
	ClientId int64
	Seq      int
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}
type PullShardArgs struct {
	ShardId   int
	ConfigNum int
}
type PullShardReply struct {
	ShardId   int
	ConfigNum int
	Err       Err
	Data      []byte
}
type ChangeShardArgs struct {
	ShardId    int
	ConfigNum  int
	ShardState shardState
}
type ChangeShardReply struct {
	//ShardId   int
	//ConfigNum int
	Err Err
}
