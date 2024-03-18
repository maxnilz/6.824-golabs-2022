package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"fmt"
	"io"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	opGet    = "Get"
	opPut    = "Put"
	opAppend = "Append"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	// Type of operation, one of (Get, Put, Append)
	Type  string
	Key   string
	Value string

	// ClientId & RequestId together uniquely identify
	// a client operation. RequestId of a given client
	// is increases monotonically.
	ClientId  int64
	RequestId int64
}

type OpRes struct {
	Err Err

	Type  string
	Key   string
	Value string

	ClientId  int64
	RequestId int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	db map[string]string
	// lastApplied map the last applied RequestId to ClientId.
	lastApplied map[int64]int64

	// channels for notifying the client when the index is
	// applied to db from raft.
	results map[int]chan *OpRes

	// logger
	logger *log.Logger

	lastAppliedIndex  int
	lastSnapshotTerm  int
	lastSnapshotIndex int

	hasSnapshotPending bool
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Type:      opGet,
		Key:       args.Key,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	res, ok := kv.waitOpApplied(op)
	if !ok {
		reply.Err = ErrWrongLeader
		return
	}
	if res.Err != OK {
		reply.Err = res.Err
		return
	}
	reply.Err = OK
	reply.Value = res.Value
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Type:      args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	res, ok := kv.waitOpApplied(op)
	if !ok {
		reply.Err = ErrWrongLeader
		return
	}
	if res.Err != OK {
		reply.Err = res.Err
		return
	}
	reply.Err = OK
}

func (kv *KVServer) waitOpApplied(op Op) (*OpRes, bool) {
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return nil, false
	}

	kv.logger.Printf("%s %s at index %d, %d/%d", op.Type, op.Key, index, op.ClientId, op.RequestId)

	kv.mu.Lock()
	ch, ok := kv.results[index]
	if !ok {
		ch = make(chan *OpRes, 1)
		kv.results[index] = ch
	}
	kv.mu.Unlock()

	select {
	case res := <-ch:
		if res.ClientId != op.ClientId || res.RequestId != op.RequestId {

			kv.logger.Printf("%s %s at index %d confict with applied op: %s %s, %d/%d(%d/%d)",
				op.Type, op.Key, index, res.Type, res.Key, op.ClientId, op.RequestId, res.ClientId, res.RequestId)

			kv.mu.Lock()
			delete(kv.results, index)
			kv.mu.Unlock()

			res.Err = ErrConflict
			return res, true
		}

		kv.logger.Printf("%s %s at index %d ok, %d/%d", op.Type, op.Key, index, op.ClientId, op.RequestId)

		kv.mu.Lock()
		delete(kv.results, index)
		kv.mu.Unlock()

		return res, true

	case <-time.After(3 * time.Second):

		kv.logger.Printf("%s %s at index %d timeout, %d/%d", op.Type, op.Key, index, op.ClientId, op.RequestId)

		kv.mu.Lock()
		delete(kv.results, index)
		kv.mu.Unlock()

		res := OpRes{
			Err:       ErrTimeout,
			Key:       op.Key,
			ClientId:  op.ClientId,
			RequestId: op.RequestId,
		}
		return &res, true
	}
}

func (kv *KVServer) applier() {
	for msg := range kv.applyCh {
		if msg.CommandValid {
			kv.applyCommand(msg)
		}
		if msg.SnapshotValid {
			kv.applySnapshot(msg)
		}
		if msg.SnapshotAck {
			kv.ackSnapshot()
		}
	}
}

func (kv *KVServer) applyCommand(msg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	index := msg.CommandIndex
	op := msg.Command.(Op)
	clientId, requestId := op.ClientId, op.RequestId

	kv.lastAppliedIndex = index

	res := OpRes{
		Err:       OK,
		Type:      op.Type,
		Key:       op.Key,
		ClientId:  clientId,
		RequestId: requestId,
	}

	switch op.Type {
	case opGet:
		// Get will always get the latest version
		kv.logger.Printf("applier, apply %s %s at index %d", op.Type, op.Key, index)

		value, ok := kv.db[op.Key]
		if !ok {
			res.Err = ErrNoKey
			break
		}
		res.Value = value

	case opPut, opAppend:
		lastAppliedReqId := kv.lastApplied[clientId]
		// duplicate detection,
		// Put & Append should execute only if
		// the request is not applied yet before.
		// For example: the Clerk sends a request to
		// a kvserver leader in one term, times out
		// waiting for a reply, and re-sends the request
		// to a new leader in another term.
		if requestId <= lastAppliedReqId {
			kv.logger.Printf("applier, stale %s %s at index %d, requestId: %d, lastAppliedReqId: %d, client: %d",
				op.Type, op.Key, index, requestId, lastAppliedReqId, clientId)
			break // stale request, ignoring
		}

		kv.logger.Printf("applier, apply %s %s at index %d", op.Type, op.Key, index)

		switch op.Type {
		case opPut:
			kv.db[op.Key] = op.Value
		case opAppend:
			kv.db[op.Key] += op.Value
		}
		kv.lastApplied[clientId] = requestId
		res.Value = kv.db[op.Key]

	}

	if ch, ok := kv.results[index]; ok {
		kv.logger.Printf("applier, send res at index: %d", index)
		ch <- &res
		kv.logger.Printf("applier, sent res at index: %d", index)
	}
	kv.checkRaftState(index)
}

func (kv *KVServer) checkRaftState(index int) bool {
	if kv.maxraftstate == -1 {
		return false // no snapshot required
	}
	sz := kv.rf.GetStateSize()
	if sz < kv.maxraftstate {
		return false // not reach the threshold
	}

	if kv.hasSnapshotPending {
		return false // existing ongoing snapshot
	}

	kv.logger.Printf("start snapshot at index %d, raftsize: %d(%d)", index, sz, kv.maxraftstate)

	w := bytes.Buffer{}
	a := labgob.NewEncoder(&w)
	a.Encode(kv.db)
	a.Encode(kv.lastApplied)
	kv.rf.Snapshot(index, w.Bytes())

	kv.hasSnapshotPending = true
	kv.lastSnapshotIndex = index

	return true
}

func (kv *KVServer) applySnapshot(msg raft.ApplyMsg) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.lastAppliedIndex > 0 && msg.SnapshotIndex <= kv.lastAppliedIndex {

		kv.logger.Printf("applier, ignoreing snapshot, index: %d, applied: %v",
			msg.SnapshotIndex, kv.lastAppliedIndex)

		return false
	}

	kv.lastSnapshotTerm = msg.SnapshotTerm
	kv.lastSnapshotIndex = msg.SnapshotIndex
	kv.lastAppliedIndex = msg.SnapshotIndex

	r := bytes.NewBuffer(msg.Snapshot)
	a := labgob.NewDecoder(r)
	a.Decode(&kv.db)
	a.Decode(&kv.lastApplied)

	kv.logger.Printf("applier, snapshot, cur: %d %d, last: %d %d %d",
		msg.SnapshotTerm, msg.SnapshotIndex, kv.lastSnapshotTerm, kv.lastSnapshotIndex, kv.lastAppliedIndex)

	return true
}

func (kv *KVServer) ackSnapshot() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.logger.Printf("ack snapshot at index %d", kv.lastSnapshotIndex)

	kv.hasSnapshotPending = false
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
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
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

	// You may need initialization code here.
	kv.db = make(map[string]string)
	kv.lastApplied = make(map[int64]int64)

	kv.results = make(map[int]chan *OpRes)
	kv.logger = log.New(
		io.Discard,
		fmt.Sprintf("kvserver/%d ", me),
		log.LstdFlags|log.Lmicroseconds,
	)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.applier()

	return kv
}
