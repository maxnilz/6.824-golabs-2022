package shardkv

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
	"bytes"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	opGet           = "Get"
	opPut           = "Put"
	opAppend        = "Append"
	opInitMigration = "InitMigration"
	opMigrate       = "Migrate"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	// Type of operation, one of (Get, Put, Append, Migrate)
	Type  string
	Key   string
	Value string

	ShardId int

	// ClientId & RequestId together uniquely identify
	// a client operation. RequestId of a given client
	// is increases monotonically.
	ClientId  int64
	RequestId int64
}

func (op Op) String() string {
	switch op.Type {
	case opGet, opInitMigration, opMigrate:
		return fmt.Sprintf("{%s %s %d/%d/%d}", op.Type, op.Key, op.ShardId, op.ClientId, op.RequestId)
	default:
		return fmt.Sprintf("{%s %s:%s %d/%d/%d}", op.Type, op.Key, op.Value, op.ShardId, op.ClientId, op.RequestId)
	}
}

type OpRes struct {
	Err Err

	Type  string
	Key   string
	Value string

	ClientId  int64
	RequestId int64
}

type DB struct {
	ShardId int
	Store   map[string]string
	// lastApplied map the last applied RequestId to ClientId.
	LastApplied map[int64]int64
	// last config num/version
	LastNum int
	// last ongoing migration request for the given shard
	MigrationReq *MigrationReq
	// last ongoing pop head request for the given shard
	PopHeadReq *PopHeadReq
}

func (db DB) clone() DB {
	out := DB{
		ShardId:     db.ShardId,
		Store:       make(map[string]string),
		LastApplied: make(map[int64]int64),
	}
	for k, v := range db.Store {
		out.Store[k] = v
	}
	for k, v := range db.LastApplied {
		out.LastApplied[k] = v
	}
	return out
}

func (db DB) String() string {
	sb := strings.Builder{}
	a, b := 0, 0
	if db.MigrationReq != nil {
		a = 1
	}
	if db.PopHeadReq != nil {
		b = 1
	}
	sb.WriteString(fmt.Sprintf("{%d-%d,%d%d", db.ShardId, len(db.Store), a, b))
	for k, v := range db.Store {
		sb.WriteString(fmt.Sprintf(", %s:%s", k, v))
	}
	sb.WriteString("}")
	return sb.String()
}

type Shard struct {
	DB

	mu sync.Mutex
}

type MigrationReq struct {
	Num       int
	Shard     int
	Group     int
	PrevGroup int
	// Config.Groups[m.Group]
	Servers []string
}

type PopHeadReq struct {
	Num   int
	Shard int
	Group int
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
	shards map[int]*Shard

	mck *shardctrler.Clerk

	// channels for notifying the client when the index is
	// applied to db from raft. index --> result channel
	results map[int]chan *OpRes

	// logger
	logger *log.Logger

	lastAppliedIndex  int
	lastSnapshotTerm  int
	lastSnapshotIndex int

	hasSnapshotPending bool
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	shardId := args.ShardId
	if ok := kv.checkShard(shardId); !ok {
		reply.Err = ErrWrongGroup
		return
	}

	op := Op{
		Type:      opGet,
		Key:       args.Key,
		ShardId:   shardId,
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

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	shardId := args.ShardId
	if ok := kv.checkShard(shardId); !ok {
		reply.Err = ErrWrongGroup
		return
	}

	op := Op{
		Type:      args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ShardId:   shardId,
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

func (kv *ShardKV) InitMigration(args *InitMigrationArgs, reply *InitMigrationReply) {
	m := args.MigrationReq
	op := Op{
		Type:      opInitMigration,
		Key:       strconv.Itoa(m.Shard),
		Value:     string(mustEncode(m)),
		ShardId:   m.Shard,
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

func (kv *ShardKV) Migrate(args *MigrateArgs, reply *MigrateReply) {
	db := args.DB
	op := Op{
		Type:      opMigrate,
		Key:       strconv.Itoa(db.ShardId),
		Value:     string(mustEncode(db)),
		ShardId:   db.ShardId,
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

func (kv *ShardKV) waitOpApplied(op Op) (*OpRes, bool) {
	kv.logger.Printf("op %v starting", op)

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		kv.logger.Printf("op %v at wrong leader", op)
		return nil, false
	}

	kv.logger.Printf("op %v at index %d", op, index)

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

			kv.logger.Printf("op %v at index %d confict with applied op: {%s %s %d/%d}",
				op, index, res.Type, res.Key, res.ClientId, res.RequestId)

			kv.mu.Lock()
			delete(kv.results, index)
			kv.mu.Unlock()

			res.Err = ErrConflict
			return res, true
		}

		kv.logger.Printf("op %v at index %d ok", op, index)

		kv.mu.Lock()
		delete(kv.results, index)
		kv.mu.Unlock()

		return res, true

	case <-time.After(3 * time.Second):

		kv.logger.Printf("op %v at index %d timeout", op, index)

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

func (kv *ShardKV) applier() {
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

func (kv *ShardKV) applyCommand(msg raft.ApplyMsg) {
	index := msg.CommandIndex
	op := msg.Command.(Op)
	shardId, clientId, requestId := op.ShardId, op.ClientId, op.RequestId

	shard, ok := kv.shards[shardId]
	if !ok {
		db := DB{
			ShardId:     shardId,
			Store:       make(map[string]string),
			LastApplied: make(map[int64]int64),
		}
		shard = &Shard{DB: db}
		kv.shards[shardId] = shard
	}
	kv.lastAppliedIndex = index

	res := OpRes{
		Err:       OK,
		Type:      op.Type,
		Key:       op.Key,
		ClientId:  clientId,
		RequestId: requestId,
	}

	shard.mu.Lock()
	switch op.Type {
	case opGet:
		// Get will always get the latest version
		kv.logger.Printf("applier, apply %v at index %d", op, index)
		value, ok := shard.Store[op.Key]
		if !ok {
			res.Err = ErrNoKey
			break
		}
		res.Value = value

	case opPut, opAppend, opInitMigration, opMigrate:
		lastAppliedReqId := shard.LastApplied[clientId]
		// duplicate detection,
		// Put & Append should execute only if
		// the request is not applied yet before.
		if requestId <= lastAppliedReqId {
			kv.logger.Printf("applier, ignored duplicated %v at index %d, lastAppliedReqId: %d", op, index, lastAppliedReqId)
			break // stale request, ignoring
		}

		switch op.Type {
		case opPut:
			shard.Store[op.Key] = op.Value
			kv.logger.Printf("applier, apply %v at index %d", op, index)

		case opAppend:
			shard.Store[op.Key] += op.Value
			kv.logger.Printf("applier, apply %v at index %d, value: %v", op, index, shard.Store[op.Key])

		case opInitMigration:
			a := MigrationReq{}
			mustDecode([]byte(op.Value), &a)
			shard.DB.LastNum = a.Num
			shard.DB.MigrationReq = &a

			go kv.sendMigrationReq(shard)

			kv.logger.Printf("migration, apply %v at index %d, db: %v", op, index, shard.DB)

		case opMigrate:
			num := int(requestId)
			db := DB{}
			mustDecode([]byte(op.Value), &db)
			db.LastNum = num
			db.PopHeadReq = &PopHeadReq{Num: num, Shard: shardId, Group: kv.gid}
			shard.DB = db

			go kv.popQueueHead(shard)

			kv.logger.Printf("migration, apply %v at index %d, db: %v", op, index, db)

		}
		shard.LastApplied[clientId] = requestId
		res.Value = shard.Store[op.Key]
	}
	shard.mu.Unlock()

	kv.mu.Lock()
	if ch, ok := kv.results[index]; ok {
		ch <- &res
	}
	kv.mu.Unlock()

	kv.checkRaftState(index)
}

func (kv *ShardKV) checkRaftState(index int) bool {
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

	w := bytes.Buffer{}
	a := labgob.NewEncoder(&w)

	n := len(kv.shards)
	sb := strings.Builder{}
	a.Encode(n)
	for i := 0; i < shardctrler.NShards; i++ {
		shard, ok := kv.shards[i]
		if !ok {
			continue
		}
		shard.mu.Lock()
		a.Encode(shard.DB)
		sb.WriteString(shard.DB.String())
		if i < shardctrler.NShards-1 {
			sb.WriteString(",")
		}
		shard.mu.Unlock()
	}
	kv.rf.Snapshot(index, w.Bytes())

	kv.hasSnapshotPending = true
	kv.lastSnapshotIndex = index

	kv.logger.Printf("start snapshot at index %d, raftsize: %d(%d), %d, dbs: %s",
		index, sz, kv.maxraftstate, w.Len(), sb.String())

	return true
}

func (kv *ShardKV) applySnapshot(msg raft.ApplyMsg) bool {
	if kv.lastAppliedIndex > 0 && msg.SnapshotIndex <= kv.lastAppliedIndex {

		kv.logger.Printf("applier, ignoreing snapshot, index: %d, applied: %v",
			msg.SnapshotIndex, kv.lastAppliedIndex)

		return false
	}

	r := bytes.NewBuffer(msg.Snapshot)
	a := labgob.NewDecoder(r)

	var n int
	sb := strings.Builder{}
	a.Decode(&n)
	for i := 0; i < n; i++ {
		var db DB
		a.Decode(&db)

		sb.WriteString(db.String())
		if i < n-1 {
			sb.WriteString(",")
		}

		shard := &Shard{DB: db}

		go kv.sendMigrationReq(shard)
		go kv.popQueueHead(shard)

		kv.shards[db.ShardId] = shard
	}

	kv.logger.Printf("applier, snapshot, cur: %d %d, last: %d %d %d, len:%d, dbs: %s",
		msg.SnapshotTerm, msg.SnapshotIndex,
		kv.lastSnapshotTerm, kv.lastSnapshotIndex, kv.lastAppliedIndex,
		len(msg.Snapshot), sb.String())

	kv.lastSnapshotTerm = msg.SnapshotTerm
	kv.lastSnapshotIndex = msg.SnapshotIndex
	kv.lastAppliedIndex = msg.SnapshotIndex

	return true
}

func (kv *ShardKV) ackSnapshot() {
	kv.logger.Printf("ack snapshot at index %d", kv.lastSnapshotIndex)

	kv.hasSnapshotPending = false
}

func (kv *ShardKV) checkShard(shard int) bool {
	res := kv.mck.QueryAll(-1)
	cfg, q := res.Config, res.Queues[shard]

	kv.logger.Printf("check shard %d at group %d, q: %v, version: %d", shard, kv.gid, q, cfg.Num)

	if len(q) != 0 {
		return false
	}
	if cfg.Shards[shard] != kv.gid {
		return false
	}
	return true
}

func (kv *ShardKV) watchConfig() {
	for {
		res := kv.mck.QueryAll(-1)
		qs := res.Queues
		for shardId := 0; shardId < shardctrler.NShards; shardId++ {
			q := qs[shardId]
			if len(q) == 0 || q[0].PrevGroup != kv.gid {
				// no migration need or the source
				// group of the head ele is not the
				// source group for the given shard.
				continue
			}
			if q[0].Locked {
				continue // already in-progress
			}

			m := q[0]
			if ok := kv.sendInitMigrationReq(m); ok {
				kv.mck.LockQueue(m.Num, m.Shard, m.Group)
			}
		}
		time.Sleep(time.Millisecond * 100)
	}
}

func (kv *ShardKV) sendInitMigrationReq(m shardctrler.Migration) bool {
	if kv.gid == m.Group {
		kv.mck.PopQueue(m.Num, m.Shard, m.Group)
		kv.logger.Printf("migration, shard %d already in group %d", m.Shard, kv.gid)
		return true
	}

	clientId, requestId := int64(kv.gid), int64(m.Num)
	req := MigrationReq{
		Num:       m.Num,
		Shard:     m.Shard,
		Group:     m.Group,
		PrevGroup: m.PrevGroup,
		Servers:   m.Config.Groups[m.Group],
	}
	args := InitMigrationArgs{
		MigrationReq: req,
		ClientId:     clientId,
		RequestId:    requestId,
	}
	servers := m.PreConfig.Groups[kv.gid]
	serverId, n := 0, len(servers)
	for ; ; serverId = (serverId + 1) % n {

		kv.logger.Printf("migration, send InitMigration RPC to server %d-%d, shard %d from %d to %d, %d/%d",
			kv.gid, serverId, m.Shard, m.PrevGroup, m.Group, clientId, requestId)

		var reply InitMigrationReply
		srv := kv.make_end(servers[serverId])
		ok := srv.Call("ShardKV.InitMigration", &args, &reply)
		if ok && reply.Err == OK {
			break
		}

		time.Sleep(time.Millisecond * 100)
	}

	kv.logger.Printf("migration, init migration, shard %d, from %d to %d, %d/%d",
		m.Shard, m.PrevGroup, m.Group, clientId, requestId)

	return true
}

func (kv *ShardKV) sendMigrationReq(shard *Shard) bool {
	shard.mu.Lock()
	m := shard.DB.MigrationReq
	if m == nil {
		shard.mu.Unlock()
		return false
	}
	db := shard.DB.clone()
	clientId, requestId := int64(kv.gid), int64(m.Num)
	shard.mu.Unlock()

	args := MigrateArgs{
		DB:        db,
		ClientId:  clientId,
		RequestId: requestId,
	}
	servers := m.Servers
	serverId, n := 0, len(servers)
	for ; ; serverId = (serverId + 1) % n {

		kv.logger.Printf("migration, send Migrate RPC to server %d-%d, shard %d from %d to %d, %d/%d",
			m.Group, serverId, m.Shard, m.PrevGroup, m.Group, clientId, requestId)

		var reply MigrateReply
		srv := kv.make_end(servers[serverId])
		ok := srv.Call("ShardKV.Migrate", &args, &reply)
		if ok && reply.Err == OK {
			break
		}

		time.Sleep(time.Millisecond * 100)
	}

	kv.logger.Printf("migration, send migration, shard %d, from %d to %d, %d/%d",
		m.Shard, m.PrevGroup, m.Group, clientId, requestId)

	// Check if the current config num/version
	// is still same with the config num of the
	// migration request after the RPC calls.
	// Because during the RPC calls above, the
	// shard could be migrated to another group
	// then migrated back if the migration got
	// called multiple times. An example events
	// could be:
	//  1. At epoch 1, the shard send the migration
	//    request out to target server.
	//  2. Then the snapshot is triggered before the
	//    success migration response returns, in this
	//    case the snapshot still carry the pending
	//    migration request.
	//  3. The server got restarted, and the shard data
	//    got reconstruct from snapshot, then trigger
	//    another migration request at epoch 2(same as
	//    the one in step #1)
	//  4. At the moment, the shard get migrated back
	//    From another group with new store and regained
	//    the shard ownership.
	// In this case, the migration request should being
	// considered as out-of-date, and shouldn't change
	// the shard state.
	shard.mu.Lock()
	kv.logger.Printf("migration, clean store, shard %d %d %d", m.Shard, shard.DB.LastNum, m.Num)
	if shard.DB.LastNum == m.Num {
		shard.DB.MigrationReq = nil
		shard.DB.Store = make(map[string]string)
	}
	shard.mu.Unlock()

	return true
}

func (kv *ShardKV) popQueueHead(shard *Shard) bool {
	shard.mu.Lock()
	req := shard.DB.PopHeadReq
	if req == nil {
		kv.logger.Printf("migration, pop queue head, empty req")
		shard.mu.Unlock()
		return false
	}
	num, shardId, gid := req.Num, req.Shard, req.Group
	shard.mu.Unlock()

	kv.logger.Printf("migration, pop queue head started, shard %d, [%d,%d]", shardId, num, gid)

	kv.mck.PopQueue(num, shardId, gid)

	kv.logger.Printf("migration, pop queue head ended, shard %d, [%d,%d]", shardId, num, gid)

	shard.mu.Lock()
	kv.logger.Printf("migration, pop queue head clean req, shard %d, %d %d", shardId, shard.DB.LastNum, num)
	if shard.DB.LastNum == num {
		shard.DB.PopHeadReq = nil
	}
	shard.mu.Unlock()

	return true
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
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

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.GroupMake(servers, gid, me, persister, kv.applyCh)

	epoch := time.Now().Unix()
	kv.shards = make(map[int]*Shard)
	kv.results = make(map[int]chan *OpRes)
	kv.logger = log.New(
		os.Stdout,
		fmt.Sprintf("kvserver/%d-%d %d ", gid, me, epoch),
		log.LstdFlags|log.Lmicroseconds,
	)
	go kv.applier()
	go kv.watchConfig()

	kv.logger.Printf("start kvserver: %d-%d", gid, me)

	return kv
}
