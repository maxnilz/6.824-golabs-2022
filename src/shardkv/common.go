package shardkv

import (
	"6.824/labgob"
	"bytes"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
	ErrConflict    = "ErrConflict"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"

	ShardId int
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId  int64
	RequestId int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string

	ShardId int
	// You'll have to add definitions here.
	ClientId  int64
	RequestId int64
}

type GetReply struct {
	Err   Err
	Value string
}

type InitMigrationArgs struct {
	MigrationReq MigrationReq

	ClientId  int64
	RequestId int64
}

type InitMigrationReply struct {
	Err Err
}

type MigrateArgs struct {
	DB DB

	ClientId  int64 // src group id as client id
	RequestId int64 // config version/num as request id
}

type MigrateReply struct {
	Err Err
}

func mustDecode(data []byte, e interface{}) {
	r := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(r)
	err := decoder.Decode(e)
	if err != nil {
		panic(err)
	}
}

func mustEncode(e interface{}) []byte {
	w := bytes.Buffer{}
	encoder := labgob.NewEncoder(&w)
	err := encoder.Encode(e)
	if err != nil {
		panic(err)
	}
	return w.Bytes()
}
