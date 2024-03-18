package shardctrler

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"fmt"
	"io"
	"log"
	"sort"
	"strings"
	"sync"
	"time"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num
	// migration queue for each shard
	queues [NShards][]Migration

	// lastApplied map the last applied RequestId to ClientId.
	lastApplied map[int64]int64

	maxraftstate int // snapshot if log grows this big

	// channels for notifying the client when the index is
	// applied to db from raft.
	results map[int]chan *OpRes

	logger *log.Logger

	lastAppliedIndex  int
	lastSnapshotTerm  int
	lastSnapshotIndex int

	hasSnapshotPending bool
}

type Migration struct {
	Num       int
	Shard     int
	Group     int
	PrevGroup int
	Locked    bool

	// Config at Num
	Config Config
	// Config at Num-1
	PreConfig Config
}

func (m Migration) String() string {
	locked := 0
	if m.Locked {
		locked = 1
	}
	return fmt.Sprintf("%d,%d,%d,%d", m.Num, m.PrevGroup, m.Group, locked)
}

const (
	opJoin      = "Join"
	opLeave     = "Leave"
	opMove      = "Move"
	opQuery     = "Query"
	opLockQueue = "LockQueue"
	opPopQueue  = "PopQueue"
)

type Op struct {
	// Your data here.
	// Type of operation, one of (Join, Leave, Move, Query, LockQueue, PopQueue)
	Type string
	Cmd  []byte

	// ClientId & RequestId together uniquely identify
	// a client operation. RequestId of a given client
	// is increases monotonically.
	ClientId  int64
	RequestId int64
}

func (op Op) String() string {
	sb := strings.Builder{}
	switch op.Type {
	case opLockQueue:
		a := LockQueueArgs{}
		mustDecode(op.Cmd, &a)
		sb.WriteString(fmt.Sprintf("{c:%s %d %d %d,%d/%d}", op.Type, a.Shard, a.Num, a.Group, op.ClientId, op.RequestId))
	case opPopQueue:
		a := PopQueueArgs{}
		mustDecode(op.Cmd, &a)
		sb.WriteString(fmt.Sprintf("{c:%s %d %d %d,%d/%d}", op.Type, a.Shard, a.Num, a.Group, op.ClientId, op.RequestId))
	case opJoin:
		a := JoinArgs{}
		mustDecode(op.Cmd, &a)
		sb.WriteString(fmt.Sprintf("{c:%s %v,%d/%d}", op.Type, a.Servers, op.ClientId, op.RequestId))
	case opLeave:
		a := LeaveArgs{}
		mustDecode(op.Cmd, &a)
		sb.WriteString(fmt.Sprintf("{c:%s %v,%d/%d}", op.Type, a.GIDs, op.ClientId, op.RequestId))
	case opMove:
		a := MoveArgs{}
		mustDecode(op.Cmd, &a)
		sb.WriteString(fmt.Sprintf("{c:%s %d %d,%d/%d}", op.Type, a.Shard, a.GID, op.ClientId, op.RequestId))
	default:
		sb.WriteString(fmt.Sprintf("{c:%s,%d/%d}", op.Type, op.ClientId, op.RequestId))
	}
	return sb.String()
}

type OpRes struct {
	Err Err

	Config Config
	Queues [NShards][]Migration

	ClientId  int64
	RequestId int64
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{
		Type:      opJoin,
		Cmd:       mustEncode(args),
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	res, ok := sc.waitOpApplied(op)
	if !ok {
		reply.WrongLeader = true
		return
	}
	if res.Err != OK {
		reply.Err = res.Err
		return
	}
	reply.Err = OK
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{
		Type:      opLeave,
		Cmd:       mustEncode(args),
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	res, ok := sc.waitOpApplied(op)
	if !ok {
		reply.WrongLeader = true
		return
	}
	if res.Err != OK {
		reply.Err = res.Err
		return
	}
	reply.Err = OK

}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{
		Type:      opMove,
		Cmd:       mustEncode(args),
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	res, ok := sc.waitOpApplied(op)
	if !ok {
		reply.WrongLeader = true
		return
	}
	if res.Err != OK {
		reply.Err = res.Err
		return
	}
	reply.Err = OK
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{
		Type:      opQuery,
		Cmd:       mustEncode(args),
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	res, ok := sc.waitOpApplied(op)
	if !ok {
		reply.WrongLeader = true
		return
	}
	if res.Err != OK {
		reply.Err = res.Err
		return
	}
	reply.Err = OK
	reply.Config = res.Config
	reply.Queues = res.Queues
}

func (sc *ShardCtrler) LockQueue(args *LockQueueArgs, reply *LockQueueReply) {
	op := Op{
		Type:      opLockQueue,
		Cmd:       mustEncode(args),
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	res, ok := sc.waitOpApplied(op)
	if !ok {
		reply.WrongLeader = true
		return
	}
	if res.Err != OK {
		reply.Err = res.Err
		return
	}
	reply.Err = OK
}

func (sc *ShardCtrler) PopQueue(args *PopQueueArgs, reply *PopQueueReply) {
	// Your code here.
	op := Op{
		Type:      opPopQueue,
		Cmd:       mustEncode(args),
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	res, ok := sc.waitOpApplied(op)
	if !ok {
		reply.WrongLeader = true
		return
	}
	if res.Err != OK {
		reply.Err = res.Err
		return
	}
	reply.Err = OK
}

func (sc *ShardCtrler) waitOpApplied(op Op) (*OpRes, bool) {
	sc.logger.Printf("op %v starting", op)

	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		sc.logger.Printf("op %v at wrong leader", op)
		return nil, false
	}

	sc.logger.Printf("op %v at index %d", op, index)

	sc.mu.Lock()
	ch, ok := sc.results[index]
	if !ok {
		ch = make(chan *OpRes, 1)
		sc.results[index] = ch
	}
	sc.mu.Unlock()

	select {
	case res := <-ch:
		if res.ClientId != op.ClientId || res.RequestId != op.RequestId {
			sc.logger.Printf("op %v at index %d conflict", op, index)

			sc.mu.Lock()
			delete(sc.results, index)
			sc.mu.Unlock()

			res.Err = ErrConflict
			return res, true
		}

		sc.logger.Printf("op %v at index %d ok", op, index)

		sc.mu.Lock()
		delete(sc.results, index)
		sc.mu.Unlock()

		return res, true

	case <-time.After(3 * time.Second):

		sc.logger.Printf("op %v at index %d timeout", op, index)

		sc.mu.Lock()
		delete(sc.results, index)
		sc.mu.Unlock()

		res := OpRes{
			Err:       ErrTimeout,
			ClientId:  op.ClientId,
			RequestId: op.RequestId,
		}
		return &res, true
	}
}

func (sc *ShardCtrler) applier() {
	for msg := range sc.applyCh {
		if msg.CommandValid {
			sc.applyCommand(msg)
		}
		if msg.SnapshotValid {
			sc.applySnapshot(msg)
		}
		if msg.SnapshotAck {
			sc.ackSnapshot()
		}
	}
}

func (sc *ShardCtrler) applyCommand(msg raft.ApplyMsg) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	index := msg.CommandIndex
	op := msg.Command.(Op)
	clientId, requestId := op.ClientId, op.RequestId

	sc.lastAppliedIndex = index

	res := OpRes{
		Err:       OK,
		ClientId:  clientId,
		RequestId: requestId,
	}

	switch op.Type {
	case opJoin, opLeave, opMove:
		lastAppliedReqId := sc.lastApplied[clientId]
		// duplicate detection,
		// Join, Leave and Move should execute only if
		// the request is not applied yet before.
		if requestId <= lastAppliedReqId {
			sc.logger.Printf("applier, %v at index %d, duplicated request ignored", op, index)
			break // stale request, ignoring
		}

		switch op.Type {
		case opJoin:
			a := JoinArgs{}
			mustDecode(op.Cmd, &a)
			cur, next := sc.nextcfg()
			var gids []int
			for gid, servers := range a.Servers {
				gids = append(gids, gid)
				next.Groups[gid] = append(next.Groups[gid], servers...)
			}

			next.Shards = rebalance(next.Groups)
			sc.configs = append(sc.configs, next)
			sc.updateMigrationQueues(cur, next)

			res.Config = next
			res.Queues = sc.queues

			sc.logger.Printf("applier, %v at index %d, gids:%v, queue:%v, version:%d",
				op, index, gids, res.Queues, res.Config.Num)

		case opLeave:
			a := LeaveArgs{}
			mustDecode(op.Cmd, &a)
			cur, next := sc.nextcfg()
			for _, gid := range a.GIDs {
				delete(next.Groups, gid)
			}
			next.Shards = rebalance(next.Groups)
			sc.configs = append(sc.configs, next)
			sc.updateMigrationQueues(cur, next)

			res.Config = next
			res.Queues = sc.queues

			sc.logger.Printf("applier, %v at index %d, queue:%v, version:%d",
				op, index, res.Queues, res.Config.Num)

		case opMove:
			a := MoveArgs{}
			mustDecode(op.Cmd, &a)
			cur, next := sc.nextcfg()
			next.Shards[a.Shard] = a.GID
			sc.configs = append(sc.configs, next)
			sc.updateMigrationQueues(cur, next)

			res.Config = next
			res.Queues = sc.queues

			sc.logger.Printf("applier, %v at index %d, queue:%v, version:%d",
				op, index, res.Queues, res.Config.Num)

		}
		sc.lastApplied[clientId] = requestId

	case opLockQueue:
		a := LockQueueArgs{}
		mustDecode(op.Cmd, &a)
		shardId, num, gid := a.Shard, a.Num, a.Group

		q := sc.queues[shardId]
		if len(q) != 0 && q[0].Num == num && q[0].Group == gid {
			q[0].Locked = true
		}
		sc.logger.Printf("applier, %v at index %d, %v", op, index, q)

	case opPopQueue:
		a := PopQueueArgs{}
		mustDecode(op.Cmd, &a)
		shardId, num, gid := a.Shard, a.Num, a.Group

		ok := false
		q := sc.queues[shardId]
		if len(q) != 0 && q[0].Num == num && q[0].Group == gid {
			ok = true
			sc.queues[shardId] = sc.queues[shardId][1:]
		}
		sc.logger.Printf("applier, %v at index %d, %v, %v->%v", op, index, ok, q, sc.queues[shardId])

	case opQuery:
		a := QueryArgs{}
		mustDecode(op.Cmd, &a)

		n := len(sc.configs)
		cur := sc.configs[n-1]
		if a.Num >= 0 && a.Num < n {
			cur = sc.configs[a.Num]
		}
		res.Config = cur
		for i := 0; i < NShards; i++ {
			res.Queues[i] = make([]Migration, len(sc.queues[i]))
			copy(res.Queues[i], sc.queues[i])
		}
	}

	if ch, ok := sc.results[index]; ok {
		ch <- &res
	}
	sc.checkRaftState(index)
}

func (sc *ShardCtrler) nextcfg() (Config, Config) {
	n := len(sc.configs)
	cur := sc.configs[n-1]

	next := clonecfg(cur)
	next.Num += 1

	return cur, next
}

func (sc *ShardCtrler) updateMigrationQueues(a, b Config) {
	for i := 0; i < NShards; i++ {
		from, to := a.Shards[i], b.Shards[i]
		if from == 0 || from == to {
			continue
		}
		m := Migration{
			Num:       b.Num,
			Shard:     i,
			Group:     to,
			PrevGroup: from,
			Config:    clonecfg(b),
			PreConfig: clonecfg(a),
		}
		sc.queues[i] = append(sc.queues[i], m)
	}
}

func clonecfg(cfg Config) Config {
	out := Config{
		Num:    cfg.Num,
		Groups: make(map[int][]string),
	}
	for shard, group := range cfg.Shards {
		out.Shards[shard] = group
	}
	for gid, group := range cfg.Groups {
		g := make([]string, len(group))
		copy(g, group)
		out.Groups[gid] = g
	}
	return out
}

// rebalance divide the shards as evenly as possible
// among the full set of groups, and should move as
// few shards as possible to achieve that goal.
//
// the idea here is like consistent hashing.
//
// since one shard can be assigned to exact one group.
// we sort all the group by ids first, then try to make
// a distribution to represent the number of shards that
// would be assigned to the given gid secondly. once the
// distribution is made, we do the actual assignment by
// following the consistent hashing schema(clockwise).
//
// For the distribution, we first try to divide the shards
// to group evenly, then distribute the remaining backward-wise.
// the reason for chose the backward(from tail to head)
// direction is because it will require less shard movement
// compared to forward-wise(from head to tail).
//
// example1: given three group g1, g2, g3, and 10 shards, the
// distribution would be [3, 3, 4], i.e.,:
//
// g1 --> shard(6, 7, 8, 9)
// g2 --> shard(0, 1, 2)
// g3 --> shard(3, 4, 5)
//
// if we have 4 group, g1, g2, g3, g4, and 10 shards, the dist
// would be [2, 2, 3, 3], i.e.,:
//
// g1 --> shard(7, 8, 9)
// g2 --> shard(0, 1)
// g3 --> shard(2, 3)
// g4 --> shard(4, 5, 6)
//
// there would be 4 shards movement from example1 to example2.
func rebalance(groups map[int][]string) [NShards]int {
	var shards [NShards]int

	if len(groups) == 0 {
		return shards
	}

	ngrp := len(groups)
	gids := make([]int, 0, ngrp)
	for gid := range groups {
		gids = append(gids, gid)
	}

	sort.SliceStable(gids, func(i, j int) bool {
		return gids[i] < gids[j]
	})
	n, m := NShards/ngrp, NShards%ngrp
	size := ngrp
	if size > NShards {
		size = NShards
	}
	dists := make([]int, ngrp)
	for j := 0; j < size; j++ {
		dists[j] = n
	}
	for j := size - 1; j >= 0 && m > 0; j-- {
		dists[j]++
		m--
	}

	ind := 0
	for i := 0; i < ngrp; i++ {
		// do the assignment
		// clockwise.
		gid := gids[(i+1)%size]
		dist := dists[i]
		if dist == 0 {
			break
		}
		for j := 0; j < dist; j++ {
			shards[ind] = gid
			ind++
		}
	}
	return shards
}

func (sc *ShardCtrler) checkRaftState(index int) bool {
	if sc.maxraftstate == -1 {
		return false // no snapshot required
	}

	sz := sc.rf.GetStateSize()
	if sz < sc.maxraftstate {
		return false // not reach the threshold
	}

	if sc.hasSnapshotPending {
		return false // existing ongoing snapshot
	}

	w := bytes.Buffer{}
	a := labgob.NewEncoder(&w)
	a.Encode(sc.configs)
	a.Encode(sc.queues)
	a.Encode(sc.lastApplied)
	sc.rf.Snapshot(index, w.Bytes())

	sc.hasSnapshotPending = true
	sc.lastSnapshotIndex = index

	return true
}

func (sc *ShardCtrler) applySnapshot(msg raft.ApplyMsg) bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if sc.lastAppliedIndex > 0 && msg.SnapshotIndex <= sc.lastAppliedIndex {

		sc.logger.Printf("applier, ignoreing snapshot, index: %d, applied: %v",
			msg.SnapshotIndex, sc.lastAppliedIndex)

		return false
	}

	sc.lastSnapshotTerm = msg.SnapshotTerm
	sc.lastSnapshotIndex = msg.SnapshotIndex
	sc.lastAppliedIndex = msg.SnapshotIndex

	r := bytes.NewBuffer(msg.Snapshot)
	a := labgob.NewDecoder(r)
	a.Decode(&sc.configs)
	a.Decode(&sc.queues)
	a.Decode(&sc.lastApplied)

	return true
}

func (sc *ShardCtrler) ackSnapshot() {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	sc.hasSnapshotPending = false
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.lastApplied = make(map[int64]int64)

	sc.maxraftstate = 1000
	sc.results = make(map[int]chan *OpRes)
	sc.logger = log.New(
		io.Discard,
		fmt.Sprintf("ctrl/%d ", me),
		log.LstdFlags|log.Lmicroseconds,
	)

	go sc.applier()

	return sc
}
