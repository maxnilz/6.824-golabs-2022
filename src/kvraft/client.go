package kvraft

import (
	"6.824/labrpc"
	"crypto/rand"
	"math/big"
	"sync/atomic"
	"time"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId  int32
	clientId  int64
	requestId int64
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
	// You'll have to add code here.
	ck.leaderId = 0
	ck.clientId = nrand()
	ck.requestId = 0
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

	// You will have to modify this function.
	n := int32(len(ck.servers))
	requestId := atomic.AddInt64(&ck.requestId, 1)

	args := GetArgs{
		Key:       key,
		ClientId:  ck.clientId,
		RequestId: requestId,
	}
	i := atomic.LoadInt32(&ck.leaderId)
	for ; ; i = (i + 1) % n {
		reply := GetReply{}
		ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader {
			continue
		}
		atomic.StoreInt32(&ck.leaderId, i)
		if reply.Err != OK {
			if reply.Err == ErrNoKey {
				return ""
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}
		return reply.Value
	}
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
	// You will have to modify this function.
	n := int32(len(ck.servers))
	requestId := atomic.AddInt64(&ck.requestId, 1)
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientId:  ck.clientId,
		RequestId: requestId,
	}
	i := atomic.LoadInt32(&ck.leaderId)
	for ; ; i = (i + 1) % n {
		reply := PutAppendReply{}
		ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
		if !ok || reply.Err != OK {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		atomic.StoreInt32(&ck.leaderId, i)
		return
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
