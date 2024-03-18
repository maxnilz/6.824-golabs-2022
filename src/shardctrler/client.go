package shardctrler

//
// Shardctrler clerk.
//

import (
	"6.824/labrpc"
	"crypto/rand"
	"math/big"
	"sync/atomic"
	"time"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
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
	// Your code here.
	ck.leaderId = 0
	ck.clientId = nrand()
	ck.requestId = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num

	n := int32(len(ck.servers))
	requestId := atomic.AddInt64(&ck.requestId, 1)

	args.ClientId = ck.clientId
	args.RequestId = requestId
	i := atomic.LoadInt32(&ck.leaderId)
	for ; ; i = (i + 1) % n {
		srv := ck.servers[i]
		var reply QueryReply
		ok := srv.Call("ShardCtrler.Query", args, &reply)
		if !ok || reply.WrongLeader || reply.Err != OK {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		atomic.StoreInt32(&ck.leaderId, i)
		return reply.Config
	}
}

func (ck *Clerk) QueryAll(num int) QueryReply {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num

	n := int32(len(ck.servers))
	requestId := atomic.AddInt64(&ck.requestId, 1)

	args.ClientId = ck.clientId
	args.RequestId = requestId
	i := atomic.LoadInt32(&ck.leaderId)
	for ; ; i = (i + 1) % n {
		srv := ck.servers[i]
		var reply QueryReply
		ok := srv.Call("ShardCtrler.Query", args, &reply)
		if !ok || reply.WrongLeader || reply.Err != OK {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		atomic.StoreInt32(&ck.leaderId, i)
		return reply
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers

	n := int32(len(ck.servers))
	requestId := atomic.AddInt64(&ck.requestId, 1)

	args.ClientId = ck.clientId
	args.RequestId = requestId

	i := atomic.LoadInt32(&ck.leaderId)
	for ; ; i = (i + 1) % n {
		srv := ck.servers[i]
		var reply JoinReply
		ok := srv.Call("ShardCtrler.Join", args, &reply)
		if !ok || reply.WrongLeader || reply.Err != OK {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		atomic.StoreInt32(&ck.leaderId, i)
		return
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids

	n := int32(len(ck.servers))
	requestId := atomic.AddInt64(&ck.requestId, 1)

	args.ClientId = ck.clientId
	args.RequestId = requestId

	i := atomic.LoadInt32(&ck.leaderId)
	for ; ; i = (i + 1) % n {
		srv := ck.servers[i]
		var reply LeaveReply
		ok := srv.Call("ShardCtrler.Leave", args, &reply)
		if !ok || reply.WrongLeader || reply.Err != OK {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		atomic.StoreInt32(&ck.leaderId, i)
		return
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid

	n := int32(len(ck.servers))
	requestId := atomic.AddInt64(&ck.requestId, 1)

	args.ClientId = ck.clientId
	args.RequestId = requestId

	i := atomic.LoadInt32(&ck.leaderId)
	for ; ; i = (i + 1) % n {
		srv := ck.servers[i]
		var reply MoveReply
		ok := srv.Call("ShardCtrler.Move", args, &reply)
		if !ok || reply.WrongLeader || reply.Err != OK {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		atomic.StoreInt32(&ck.leaderId, i)
		return
	}
}

func (ck *Clerk) LockQueue(num, shard, group int) {
	args := &LockQueueArgs{}
	args.Num = num
	args.Shard = shard
	args.Group = group

	n := int32(len(ck.servers))
	requestId := atomic.AddInt64(&ck.requestId, 1)

	args.ClientId = ck.clientId
	args.RequestId = requestId

	i := atomic.LoadInt32(&ck.leaderId)
	for ; ; i = (i + 1) % n {
		srv := ck.servers[i]
		var reply LockQueueReply
		ok := srv.Call("ShardCtrler.LockQueue", args, &reply)
		if !ok || reply.WrongLeader || reply.Err != OK {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		atomic.StoreInt32(&ck.leaderId, i)
		return
	}
}

func (ck *Clerk) PopQueue(num, shard, group int) {
	args := &PopQueueArgs{}
	args.Num = num
	args.Shard = shard
	args.Group = group

	n := int32(len(ck.servers))
	requestId := atomic.AddInt64(&ck.requestId, 1)

	args.ClientId = ck.clientId
	args.RequestId = requestId

	i := atomic.LoadInt32(&ck.leaderId)
	for ; ; i = (i + 1) % n {
		srv := ck.servers[i]
		var reply PopQueueReply
		ok := srv.Call("ShardCtrler.PopQueue", args, &reply)
		if !ok || reply.WrongLeader || reply.Err != OK {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		atomic.StoreInt32(&ck.leaderId, i)
		return
	}
}
