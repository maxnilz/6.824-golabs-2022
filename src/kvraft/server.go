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
	waitersCh map[int]chan *Op

	// logger
	logger *log.Logger

	index int

	lastAppliedIndex  int
	lastSnapshotTerm  int
	lastSnapshotIndex int

	hasSnapshotPending bool
}

const (
	getOp    = "Get"
	putOp    = "Put"
	appendOp = "Append"
)

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Type:      getOp,
		Key:       args.Key,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	appliedOp, ok := kv.waitOpApplied(op)
	if !ok {
		reply.Err = ErrWrongLeader
		return
	}
	reply.Err = OK
	reply.Value = appliedOp.Value
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
	if _, ok := kv.waitOpApplied(op); !ok {
		reply.Err = ErrWrongLeader
		return
	}
	reply.Err = OK
	return
}

func (kv *KVServer) waitOpApplied(op Op) (*Op, bool) {
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return nil, false
	}

	kv.logger.Printf("client: %d, reqId: %d, %s key: %s, index: %d",
		op.ClientId, op.RequestId, op.Type, op.Key, index)

	kv.mu.Lock()
	kv.index = index
	ch, ok := kv.waitersCh[index]
	if !ok {
		ch = make(chan *Op, 1)
		kv.waitersCh[index] = ch
	}
	kv.mu.Unlock()

	select {
	case appliedOp := <-ch:
		if appliedOp.ClientId != op.ClientId || appliedOp.RequestId != op.RequestId {

			kv.logger.Printf("client: %d, reqId: %d, %s key: %s, index: %d, confict",
				op.ClientId, op.RequestId, op.Type, op.Key, index)

			kv.mu.Lock()
			delete(kv.waitersCh, index)
			kv.mu.Unlock()

			return nil, false
		}

		kv.logger.Printf("client: %d, reqId: %d, %s key: %s, index: %d, ok",
			op.ClientId, op.RequestId, op.Type, op.Key, index)

		kv.mu.Lock()
		delete(kv.waitersCh, index)
		kv.mu.Unlock()

		return appliedOp, true

	case <-time.After(3 * time.Second):

		kv.logger.Printf("client: %d, reqId: %d, %s key: %s, index: %d, timeout",
			op.ClientId, op.RequestId, op.Type, op.Key, index)

		kv.mu.Lock()
		delete(kv.waitersCh, index)
		kv.mu.Unlock()

		return nil, false
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

	op := msg.Command.(Op)
	index := msg.CommandIndex

	kv.lastAppliedIndex = index

	kv.logger.Printf("applier, index: %d, key: %s", index, op.Key)

	switch op.Type {
	case getOp:
		// Get will always get the latest version
		op.Value = kv.db[op.Key]
	case putOp, appendOp:
		clientId, requestId := op.ClientId, op.RequestId
		lastAppliedReqId := kv.lastApplied[clientId]
		// duplicate detection,
		// Put & Append should execute only if
		// the request is not applied yet before.
		if requestId > lastAppliedReqId {
			switch op.Type {
			case putOp:
				kv.db[op.Key] = op.Value
			case appendOp:
				kv.db[op.Key] += op.Value
			}
			kv.lastApplied[clientId] = requestId
		}

		if requestId <= lastAppliedReqId {
			kv.logger.Printf("applier, client: %d, reqId: %d, %s key: %s, index: %d, duplication found, %d, %d",
				op.ClientId, op.RequestId, op.Type, op.Key, index, requestId, lastAppliedReqId)
		}
	}

	ch, ok := kv.waitersCh[index]
	if ok {
		kv.logger.Printf("applier, sending op at index: %d", index)
		ch <- &op
		kv.logger.Printf("applier, sent op at index: %d", index)
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

	kv.logger.Printf("start snapshot at index: %d, raftsize: %d(%d), hasSnapshotPending: %v",
		index, sz, kv.maxraftstate, kv.hasSnapshotPending)

	if kv.hasSnapshotPending {
		return false // existing ongoing snapshot
	}

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

	applicable := kv.isSnapshotApplicable(msg)

	kv.logger.Printf("applier, snapshot, cur: %d %d, last: %d %d %d, applicable: %v",
		msg.SnapshotTerm, msg.SnapshotIndex, kv.lastSnapshotTerm, kv.lastSnapshotIndex, kv.lastAppliedIndex, applicable)

	if !applicable {
		return false // snapshot is not applicable
	}

	kv.lastSnapshotTerm = msg.SnapshotTerm
	kv.lastSnapshotIndex = msg.SnapshotIndex
	kv.lastAppliedIndex = msg.SnapshotIndex

	r := bytes.NewBuffer(msg.Snapshot)
	a := labgob.NewDecoder(r)
	a.Decode(&kv.db)
	a.Decode(&kv.lastApplied)

	return true
}

func (kv *KVServer) isSnapshotApplicable(msg raft.ApplyMsg) bool {
	if kv.lastSnapshotTerm < msg.SnapshotTerm {
		return true
	}
	if kv.lastSnapshotTerm == msg.SnapshotTerm && kv.lastAppliedIndex < msg.SnapshotIndex {
		return true
	}
	return false
}

func (kv *KVServer) ackSnapshot() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.logger.Printf("ack snapshot at index: %d", kv.lastSnapshotIndex)

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

	kv.waitersCh = make(map[int]chan *Op)
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
