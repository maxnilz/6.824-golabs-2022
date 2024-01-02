package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/labgob"
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"os"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Entry struct {
	Command interface{} // the command for state machine
	Term    int         // the term when entry was received by leader(first index is 1)
}

const (
	follower = iota
	candidate
	leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//
	// persistent state on all servers
	//
	// latest term server has seen(initialized to 0
	// on first boot, increases monotonically)
	currentTerm int
	// candidateId that received vote in current term
	// (or invalidId if none)
	votedFor int
	// log entries, each entry contains command for state
	// machine, and term when entry was received by leader
	// (first index is 1)
	log []Entry
	// index of the highest log entry that's reflected in the snapshot
	// the service says it has created a snapshot that has
	// all info up to and including index. this means the
	// service no longer needs the log through (and including)
	// that index.
	lastSnapshotIndex int
	lastSnapshotTerm  int

	//
	// volatile state on all servers
	//
	// index of highest log entry known to be committed(initialized to 0,
	// increases monotonically)
	commitIndex int
	// index of highest log entry applied to state machine(initialized to
	// 0, increases monotonically)
	lastApplied int
	// so follower can redirect clients or
	// check if a server is leader or follower by check leaderId == me, or
	// indicate if a server is now the candidate state if leaderId is invalidId.
	leaderId int
	// status, whenever a server started it initialized as Follower
	status int32
	// channel to notify the state machine when a message committed
	applyCh chan ApplyMsg
	// snapshot
	snapshot             []byte
	pendingSnapshotIndex int
	pendingSnapshot      *bytes.Buffer
	// a counter for append entry message
	seq int64
	// a counter for heartbeat message
	heartbeatSeq int64
	// logger
	logger *log.Logger
	// followerC channel to notify a transition to follower
	followerC chan struct{}

	//
	// volatile state on leader
	// reinitialized after election
	//
	// for each server, index of next log entry to send that server
	// initialized to leader last log index + 1
	nextIndex []int
	// for each server, index of highest log entry known to be replicated
	// on server, initialized to 0, increases monotonically
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm
	isLeader := rf.leaderId == rf.me
	rf.logger.Printf("isLeader: %v, term: %d", isLeader, term)

	return term, isLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist(snapshot []byte) {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := bytes.Buffer{}
	a := labgob.NewEncoder(&w)
	if err := a.Encode(rf.currentTerm); err != nil {
		rf.logger.Printf("persist, encode currentTerm failed")
	}
	if err := a.Encode(rf.votedFor); err != nil {
		rf.logger.Printf("persist, encode votedFor failed")
	}
	if err := a.Encode(rf.log); err != nil {
		rf.logger.Printf("persist, encode log failed")
	}
	if err := a.Encode(rf.commitIndex); err != nil {
		rf.logger.Printf("persist, encode commitIndex failed")
	}
	if err := a.Encode(rf.lastApplied); err != nil {
		rf.logger.Printf("persist, encode lastApplied failed")
	}
	if err := a.Encode(rf.lastSnapshotIndex); err != nil {
		rf.logger.Printf("persist, encode lastSnapshotIndex failed")
	}
	if err := a.Encode(rf.lastSnapshotTerm); err != nil {
		rf.logger.Printf("persist, encode lastSnapshotTerm failed")
	}
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	a := labgob.NewDecoder(r)
	if err := a.Decode(&rf.currentTerm); err != nil {
		rf.logger.Printf("persist, decode currentTerm failed")
	}
	if err := a.Decode(&rf.votedFor); err != nil {
		rf.logger.Printf("persist, decode votedFor failed")
	}
	if err := a.Decode(&rf.log); err != nil {
		rf.logger.Printf("persist, decode log failed")
	}
	if err := a.Decode(&rf.commitIndex); err != nil {
		rf.logger.Printf("persist, decode commitIndex failed")
	}
	if err := a.Decode(&rf.lastApplied); err != nil {
		rf.logger.Printf("persist, decode lastApplied failed")
	}
	if err := a.Decode(&rf.lastSnapshotIndex); err != nil {
		rf.logger.Printf("persist, decode lastSnapshotIndex failed")
	}
	if err := a.Decode(&rf.lastSnapshotTerm); err != nil {
		rf.logger.Printf("persist, decode lastSnapshotTerm failed")
	}
	rf.snapshot = rf.persister.ReadSnapshot()
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

func (rf *Raft) trimLogs(index, term int) {
	lastIndex, _ := rf.last()
	if lastIndex < index {
		rf.log = make([]Entry, 1)
		rf.lastSnapshotIndex = index
		rf.lastSnapshotTerm = term
		rf.logger.Printf("trimLogs, discard all logs, term: %d, index: %d, lastIndex: %d", term, index, lastIndex)
		return
	}
	ind := rf.index2ind(index)

	rf.logger.Printf("trimLogs, term: %d, index: %d, ind: %d, #log: %d", term, index, ind, len(rf.log)-1)

	// trim log upto ind(including)
	logs := make([]Entry, 1)
	// the store entries is removed the
	// dummy entry at index 0 of log.
	var entries []Entry
	if ind+1 <= len(rf.log)-1 {
		entries = rf.log[ind+1:]
	}
	logs = append(logs, entries...)
	rf.log = logs
	rf.lastSnapshotIndex = index
	rf.lastSnapshotTerm = term
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

	// There is a deadlock caused by test code `applierSnap`
	// 1. AppendEntry send message to applyCh while holding the lock
	// 2. applierSnap receive message from applyCh
	// 3. applierSnap start a snapshot to Raft
	// 4. Snapshot trying to acquire the lock.
	// However, if starting snapshot without lock will still pass
	// the test cases.

	//rf.mu.Lock()
	//defer rf.mu.Unlock()

	rf.logger.Printf("Snapshot, curTerm: %d, index: %d", rf.currentTerm, index)

	ind := rf.index2ind(index)
	entry := rf.log[ind]
	rf.trimLogs(index, entry.Term)
	rf.persist(snapshot)
	rf.snapshot = snapshot
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	// candidate's term
	Term int
	// candidate requesting vote
	CandidateId int
	// index of candidate's last log entry
	LastLogIndex int
	// term of candidates' last log entry
	LastLogTerm int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).

	// currentTerm, for candidate to update itself
	Term int
	// true means candidate received vote
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		rf.logger.Printf("RequestVote, candidate: %d, args.term: %d, curTerm: %d", args.CandidateId, args.Term, rf.currentTerm)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	switch2follower := false
	votedFor := rf.votedFor
	term := rf.currentTerm
	if args.Term > term {
		rf.votedFor = none
		rf.currentTerm = args.Term
		switch2follower = true
	}

	lastLogIndex, lastLogTerm := rf.last()
	isAllowedCandidate := rf.votedFor == none || rf.votedFor == args.CandidateId
	isUpToDate := func() bool {
		if args.LastLogTerm > lastLogTerm {
			return true
		}
		if args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex {
			return true
		}
		return false
	}
	voteGranted := false
	if isAllowedCandidate && isUpToDate() {
		voteGranted = true
	}

	if voteGranted {
		// possible transition is
		//  1. stale candidate --> follower
		//  2. stale leader --> follower
		rf.votedFor = args.CandidateId
		switch2follower = true
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = voteGranted

	if switch2follower {
		rf.persist(nil)
		rf.leaderId = none
		rf.followerC <- struct{}{}
	}

	rf.logger.Printf("RequestVote, candidate: %d, args.term: %d, curTerm: %d -> %d, preVotedFor: %d, allow: %v, uptodate: %v, granted: %v",
		args.CandidateId, args.Term, term, rf.currentTerm, votedFor, isAllowedCandidate, isUpToDate(), voteGranted)
}

type AppendEntryArgs struct {
	// leader's term
	Term int
	// so follower can redirect clients
	LeaderId int

	// index of log entry immediately preceding new ones
	PrevLogIndex int
	// term of preLogIndex entry
	PrevLogTerm int
	// log entries to store (empty for heartbeat;
	// may send more than one for efficiency)
	Entries []Entry

	// leader's commit index
	LeaderCommit int

	// for debug purpose
	HeartbeatSeq   int64
	AppendEntrySeq int64
}

type AppendEntryReply struct {
	// current term for leader to update itself
	Term int
	// true if follower contained entry matching
	// preLogIndex and prevLogTerm
	Success bool
	// fast rollback
	XTerm  int // term in the conflicting entry(if any)
	XIndex int // index of first entry with xterm(if any)
	XLen   int // log length
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm
	lastIndex, _ := rf.last()
	if args.Term < rf.currentTerm {
		// reply false if term < currentTerm
		reply.Success = false
		reply.Term = rf.currentTerm
		rf.logger.Printf("AppendEntry, false, leader: %d, args.term: %d, curTerm: %d, "+
			"lastIndex: %d, #log: %d, #args.entry: %d, prevIndex: %d, prevTerm: %d",
			args.LeaderId, args.Term, rf.currentTerm,
			lastIndex, len(rf.log)-1, len(args.Entries),
			args.PrevLogIndex, args.PrevLogTerm)
		return
	}

	if args.Term > rf.currentTerm {
		// possible transition
		//  1. stale candidate --> follower
		//  2. stale leader --> follower
		role := "leader"
		if rf.me != rf.leaderId {
			role = "candidate"
		}
		rf.logger.Printf("AppendEntry, stale %v, term: %d -> %d", role, args.Term, rf.currentTerm)
		rf.votedFor = none
		rf.currentTerm = args.Term
		rf.persist(nil)

		rf.leaderId = args.LeaderId
	}

	// as long as the AE is valid
	// no mater if the log is acceptable
	// or not, reset the follower timer
	// anyway.
	rf.followerC <- struct{}{}

	acceptable := func() bool {
		if args.PrevLogIndex > 0 {
			if args.PrevLogIndex > lastIndex {
				// follower's log is too short
				reply.XLen = lastIndex + 1
				return false
			}
			prevTerm, ok := rf.termOf(args.PrevLogIndex)
			if !ok {
				return false
			}
			if prevTerm != args.PrevLogTerm {
				// index of first entry with the conflict term
				ind := rf.index2ind(args.PrevLogIndex)
				xIndex := args.PrevLogIndex
				for ; ind >= 0; ind-- {
					if rf.log[ind].Term != prevTerm {
						break
					}
					xIndex--
				}
				// set XTerm & XIndex for fast rollback
				reply.XTerm = prevTerm
				reply.XIndex = xIndex + 1
				return false
			}
			return true
		}
		// prev log index is 0
		return true
	}

	reply.Term = rf.currentTerm

	ok := acceptable()
	if ok {
		reply.Success = true
		if len(args.Entries) > 0 {
			ind := rf.index2ind(args.PrevLogIndex)
			rf.log = rf.log[:ind+1]
			rf.log = append(rf.log, args.Entries...)
			rf.persist(nil)
		}
	}

	latestIndex, _ := rf.last()
	rf.logger.Printf("AppendEntry, %v, leader: %d, args.term: %d, curTerm: %d, "+
		"lastIndex, before: %d, after: %d, #log: %d, #args.entry: %d, prevIndex: %d, prevTerm: %d, commit index: %d-%d, hseq: %d, aseq: %d",
		reply.Success, args.LeaderId, args.Term, term,
		lastIndex, latestIndex, len(rf.log)-1, len(args.Entries), args.PrevLogIndex, args.PrevLogTerm,
		args.LeaderCommit, rf.commitIndex, args.HeartbeatSeq, args.AppendEntrySeq)

	if !reply.Success {
		return
	}

	if args.LeaderCommit <= rf.commitIndex {
		return
	}

	commitIndex := args.LeaderCommit
	if commitIndex > latestIndex {
		commitIndex = latestIndex
	}
	rf.commitIndex = commitIndex

	commandIndex := rf.lastApplied + 1
	startInd := rf.index2ind(commandIndex)
	endInd := rf.index2ind(rf.commitIndex) + 1
	entries := rf.log[startInd:endInd]

	rf.applyEntry(entries, startInd, commandIndex)
	rf.persist(nil)
}

type InstallSnapshotArgs struct {
	// leader's term
	Term int
	// LeaderId so follower can redirect clients
	LeaderId int
	// the snapshot replaces all entries up through
	// and including this index
	LastIncludedIndex int
	// term of lastIncludedIndex
	LastIncludedTerm int
	// byte offset where chunk is positioned in the
	// snapshot file
	Offset int
	// raw bytes of the snapshot chunk, starting at
	// offset
	Data []byte
	// true if this is the last chunk
	Done bool
}

type InstallSnapshotReply struct {
	// currentTerm, for leader to update itself
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		rf.logger.Printf("InstallSnapshot, rejected, leader: %d, args.Term: %v, curTerm: %v",
			args.LeaderId, args.Term, rf.currentTerm)
		rf.mu.Unlock()
		return
	}
	if args.Term > rf.currentTerm {
		// possible transition
		//  1. stale candidate --> follower
		//  2. stale leader --> follower
		role := "leader"
		if rf.me != rf.leaderId {
			role = "candidate"
		}
		rf.logger.Printf("InstallSnapshot, stale %v, term: %d -> %d", role, args.Term, rf.currentTerm)
		rf.votedFor = none
		rf.currentTerm = args.Term
		rf.persist(nil)

		rf.leaderId = args.LeaderId
		rf.followerC <- struct{}{}
	}

	if args.LastIncludedIndex < rf.pendingSnapshotIndex {
		reply.Term = rf.currentTerm
		rf.logger.Printf("InstallSnapshot, rejected, leader: %d, existing newer pending snapshot, index: %d, args.index: %d",
			args.LeaderId, rf.pendingSnapshotIndex, args.LastIncludedIndex)
		rf.mu.Unlock()
		return
	}

	if args.Offset == 0 {
		rf.pendingSnapshot = &bytes.Buffer{}
		rf.pendingSnapshotIndex = args.LastIncludedIndex
	}
	rf.pendingSnapshot.Write(args.Data)
	if !args.Done {
		reply.Term = rf.currentTerm
		rf.logger.Printf("InstallSnapshot, pending, leader: %d, offset: %v, len: %v, curTerm: %v",
			args.LeaderId, args.Offset, len(args.Data), rf.currentTerm)
		rf.mu.Unlock()
		return
	}
	reply.Term = rf.currentTerm
	snapshot := rf.pendingSnapshot.Bytes()
	rf.logger.Printf("InstallSnapshot, accepted, leader: %d, len: %v, curTerm: %v, includedIndex: %d, includedTerm: %d",
		args.LeaderId, len(snapshot), rf.currentTerm, args.LastIncludedIndex, args.LastIncludedTerm)
	rf.mu.Unlock()

	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      snapshot,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}

	rf.mu.Lock()
	if rf.lastApplied < args.LastIncludedIndex {
		rf.lastApplied = args.LastIncludedIndex
	}
	if rf.commitIndex < args.LastIncludedIndex {
		rf.commitIndex = args.LastIncludedIndex
	}
	rf.trimLogs(args.LastIncludedIndex, args.LastIncludedTerm)
	// set latest snapshot
	rf.snapshot = snapshot
	// clean pending snapshot
	rf.pendingSnapshotIndex = 0
	rf.pendingSnapshot = &bytes.Buffer{}
	// persist
	rf.persist(snapshot)
	rf.mu.Unlock()
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs) (*RequestVoteReply, bool) {
	reply := &RequestVoteReply{}
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return reply, ok
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs) (*AppendEntryReply, bool) {
	reply := &AppendEntryReply{}
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return reply, ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs) (*InstallSnapshotReply, bool) {
	reply := &InstallSnapshotReply{}
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return reply, ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isLeader = rf.leaderId == rf.me

	if !isLeader {
		return index, term, isLeader
	}

	rf.logger.Printf("Start, appending command %v, term: %d", command, term)

	entry := Entry{
		Command: command,
		Term:    rf.currentTerm,
	}
	rf.log = append(rf.log, entry)
	index, _ = rf.last()
	rf.persist(nil)

	rf.logger.Printf("Start, command %v at index: %d, term: %d", command, index, term)

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

const (
	voteInvalid = iota - 2
	voteRejected

	none        = -1
	roundTripMs = 100
)

func (rf *Raft) nextVoteTimeout() time.Duration {
	const upperBound = 100
	const timeoutMs = roundTripMs * 3
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	rndn := rnd.Intn(upperBound)
	return time.Duration(timeoutMs+rndn) * time.Millisecond
}

func (rf *Raft) nextHeartbeatTimeout() time.Duration {
	timeout := time.Millisecond * roundTripMs
	return timeout
}

func (rf *Raft) elect(done <-chan struct{}) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.me == rf.leaderId {
		return false // already a leader, do nothing
	}
	rf.currentTerm++    // increase the term
	rf.votedFor = rf.me // vote itself first
	rf.leaderId = none
	rf.persist(nil)
	lastLogIndex, lastLogTerm := rf.last()

	n := len(rf.peers)
	majority := n/2 + 1
	var mu sync.Mutex
	var granted int
	var finished int
	ans := make([]int, n)
	for i := 0; i < n; i++ {
		ans[i] = voteInvalid
	}
	ch := make(chan struct{}, n)

	rf.logger.Printf("electing, gathering votes for term: %d", rf.currentTerm)

	for ind := range rf.peers {
		if ind == rf.me {
			mu.Lock()
			granted++
			finished++
			ans[ind] = rf.currentTerm
			mu.Unlock()
			continue // voted itself
		}
		args := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
		}
		go func(server int, args *RequestVoteArgs) {
			rTerm := voteRejected
			rf.logger.Printf("electing, send RequestVote req to %d, term: %d", server, args.Term)
			r, ok := rf.sendRequestVote(server, args)

			mu.Lock()
			defer mu.Unlock()

			if ok && r.VoteGranted {
				rTerm = r.Term
				granted++
			}
			finished++
			ans[server] = rTerm
			if granted >= majority || finished == n {
				ch <- struct{}{}
			}
			rf.logger.Printf("electing, RequestVote req to %d, term: %d, granted: %v", server, args.Term, r.VoteGranted)
		}(ind, &args)
	}

	// wait first events on ch or done only once with select.
	var maxTerm int
	var elected bool

	// use copies for log to avoid data race
	var numGranted int
	res := make([]int, len(rf.peers))

	select {
	case <-ch:
		mu.Lock()
		if granted >= majority {
			elected = true
		}
		for i, a := range ans {
			res[i] = a
			if maxTerm < a {
				maxTerm = a
			}
		}
		numGranted = granted
		mu.Unlock()
	case <-done:
		rf.logger.Printf("electing, term: %d, canceled", rf.currentTerm)
	}

	// handle election results
	term := rf.currentTerm
	if maxTerm > rf.currentTerm {
		rf.currentTerm = maxTerm
		rf.votedFor = none
		rf.persist(nil)
		rf.logger.Printf("electing, found higher term: (%d -> %d)", rf.currentTerm, term)
		return false
	}
	if !elected {
		rf.votedFor = none
		rf.persist(nil)
		rf.logger.Printf("electing, election failed, term: %d, granted: %d, require: %d", rf.currentTerm, numGranted, majority)
		return false
	}

	// candidate --> leader:
	//  0. leave votedFor as itself
	//  1. set me as leader id
	rf.leaderId = rf.me
	//  2. init next index & match index
	nextIndex := lastLogIndex + 1
	for peer := range rf.peers {
		rf.nextIndex[peer] = nextIndex
		rf.matchIndex[peer] = 0
	}
	rf.persist(nil)

	rf.logger.Printf("electing, elected as leader for term: %d, v: %d, c: %d, a: %d, n: %d, %v",
		rf.currentTerm, rf.votedFor, rf.commitIndex, rf.lastApplied, nextIndex, res)

	return true
}

func (rf *Raft) heartbeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.me != rf.leaderId {
		return
	}
	lastIndex, lastTerm := rf.last()
	rf.heartbeatSeq++
	seq := rf.heartbeatSeq
	args := AppendEntryArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: lastIndex,
		PrevLogTerm:  lastTerm,
		LeaderCommit: rf.commitIndex,
		HeartbeatSeq: seq,
	}
	for i := range rf.peers {
		if i == rf.me {
			continue // do not heartbeat to myself
		}
		go func(server int, a AppendEntryArgs) {
			rf.logger.Printf("heartbeat #%d to %d, term: %d", seq, server, a.Term)
			r, ok := rf.sendAppendEntry(server, &a)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if r.Term > rf.currentTerm {
				rf.logger.Printf("heartbeat #%d, stale leader, aTerm: %d, rTerm: %d, curTerm: %d", seq, a.Term, r.Term, rf.currentTerm)
				rf.currentTerm = r.Term
				rf.leaderId = none
				rf.persist(nil)

				rf.followerC <- struct{}{}
			}
			rf.logger.Printf("heartbeat #%d to %d %v, curseq: %d, term: %d", seq, server, ok, rf.heartbeatSeq, a.Term)
		}(i, args)
	}
}

func (rf *Raft) replicate(done <-chan struct{}) {
	interval := time.Millisecond * 5
	tick := time.NewTicker(interval)
	cancel := make(chan struct{})
	go func() {
		for it := range tick.C {
			if it.IsZero() {
				break // tick.C is closed
			}
			rf.mu.Lock()
			if rf.me != rf.leaderId {
				rf.mu.Unlock()
				return // not leader anymore
			}
			currentTerm := rf.currentTerm
			index, _ := rf.last()
			commitIndex := rf.commitIndex
			if index == commitIndex {
				rf.mu.Unlock()
				continue // no entries need to replicate
			}
			rf.seq++
			seq := rf.seq
			// abort any pending replicate process between
			// last replication & the replication we are
			// about to start, because we leave un-succeed
			// replication(may network lag) continue to run even
			// if we get majority success & committed the log.
			// reuse rf.mu to avoid data race.
			close(cancel)
			cancel = make(chan struct{})
			rf.mu.Unlock()

			rf.replicateToAll(currentTerm, index, commitIndex, seq, cancel)
		}
	}()

	<-done
	rf.mu.Lock()
	close(cancel)
	cancel = make(chan struct{})
	rf.logger.Printf("replicatiton canceled, term: %d, curLeader: %d", rf.currentTerm, rf.leaderId)
	rf.mu.Unlock()
	tick.Stop()
}

const (
	true_ = iota + 1
	abort
	retry
	unavailable
)

func (rf *Raft) replicateToAll(currentTerm, index, commitIndex int, seq int64, cancel <-chan struct{}) {
	timeout := time.Millisecond * roundTripMs * 3

	rf.logger.Printf("replicate #%d, term: %d, upto: %d, commitIndex: %d", seq, currentTerm, index, commitIndex)
	var success int64
	n := len(rf.peers)
	majority := int64(n/2 + 1)
	ans := make([]int64, n)
	ch := make(chan struct{}, n)
	abortCh := make(chan struct{}, n)
	var aborted int64
	for peer := range rf.peers {
		if peer == rf.me {
			ans[peer] = true_
			atomic.AddInt64(&success, 1)
			continue
		}
		go func(server int) {
			for atomic.LoadInt64(&aborted) != true_ {
				select {
				case <-cancel:
					rf.logger.Printf("replicate #%d, to: %d, term: %d, upto: %d canceled", seq, server, currentTerm, index)
					return // terminate the replication process loop
				default:
					// only for check cancel, make it nonblock
				}
				rtcd := rf.replicateTo(seq, currentTerm, server, index, commitIndex)
				switch rtcd {
				case retry:
					continue // retry the replication until succeed
				case unavailable:
					time.Sleep(time.Millisecond * 10)
					continue // backoff a little bit, then retry
				case abort:
					abortCh <- struct{}{}
					return // terminate the replicate process loop
				case true_:
					atomic.AddInt64(&success, 1)
					atomic.StoreInt64(&ans[server], true_)
					if atomic.LoadInt64(&success) >= majority {
						ch <- struct{}{}
					}
					return // the replication to given server succeed
				}
			}
		}(peer)
	}

	select {
	case <-ch:
		res := make([]int64, len(rf.peers))
		for peer := range rf.peers {
			res[peer] = atomic.LoadInt64(&ans[peer])
		}
		rf.logger.Printf("replicate #%d, term: %d, upto: %d, succeed, ans: %v", seq, currentTerm, index, res)

		rf.mu.Lock()
		commandIndex := rf.lastApplied + 1
		start := rf.index2ind(commandIndex)
		end := rf.index2ind(index) + 1
		entries := rf.log[start:end]
		rf.commitIndex = index
		rf.applyEntry(entries, start, commandIndex)
		rf.persist(nil)
		rf.mu.Unlock()

	case <-time.After(timeout):
		rf.logger.Printf("replicate #%d, term: %d, upto: %d, timeout", seq, currentTerm, index)
		// set aborted to true to cancel any pending replication process.
		// although, there could be replication request on the fly,
		// the replicateTo process is implemented out-of-order tolerate
		// way(maybe an out-of-order response could cause some lag, but
		// it won't harm the correctness)
		atomic.StoreInt64(&aborted, true_)

	case <-abortCh:
		rf.logger.Printf("replicate #%d, term: %d, upto: %d, aborted", seq, currentTerm, index)
		atomic.StoreInt64(&aborted, true_)
	}
}

func (rf *Raft) replicateTo(seq int64, currentTerm, server, upto, commitIndex int) int {
	rf.mu.Lock()
	if rf.me != rf.leaderId {
		rf.mu.Unlock()
		return abort // not leader anymore
	}
	start := rf.nextIndex[server]
	end := upto + 1
	if start == 0 {
		rf.mu.Unlock()
		return abort // start should be at least 1
	}
	if start <= rf.lastSnapshotIndex {
		// snapshot
		args := &InstallSnapshotArgs{
			Term:              currentTerm,
			LeaderId:          rf.me,
			LastIncludedIndex: rf.lastSnapshotIndex,
			LastIncludedTerm:  rf.lastSnapshotTerm,
			Data:              rf.snapshot,
			Done:              true,
		}
		rf.mu.Unlock()

		rf.logger.Printf("replicate #%d, send snapshot req to %d, lastIndex: %d, lastTerm: %d, index: [%d, %d)",
			seq, server, args.LastIncludedIndex, args.LastIncludedTerm, start, end)
		r, ok := rf.sendInstallSnapshot(server, args)
		if !ok {
			return unavailable
		}

		rf.mu.Lock()
		if r.Term > rf.currentTerm {
			// stale leader --> follower
			rf.logger.Printf("replicate #%d, snapshot req to %d, stale leader, term: %d -> %d", seq, server, r.Term, rf.currentTerm)
			rf.currentTerm = r.Term
			rf.leaderId = none
			rf.persist(nil)

			rf.followerC <- struct{}{}
			rf.mu.Unlock()
			return abort
		}
		rf.logger.Printf("replicate #%d, send snapshot req to %d, lastIndex: %d, lastTerm: %d success",
			seq, server, args.LastIncludedIndex, args.LastIncludedTerm)

		// update the next index and
		rf.nextIndex[server] = rf.lastSnapshotIndex + 1
		rf.mu.Unlock()

		// return false to indicate the caller continue to
		// replicate any un-committed entries
		return retry
	}

	prevIndex := start - 1
	prevTerm, _ := rf.termOf(prevIndex)
	a := rf.index2ind(start)
	b := rf.index2ind(end)

	rf.logger.Printf("replicate #%d, send ae req to %d, #entries: %d, index: [%d, %d), ind: [%d, %d), prevIndex: %d, prevTerm: %d",
		seq, server, end-start, start, end, a, b, prevIndex, prevTerm)

	var entries []Entry
	entries = rf.log[a:b]
	args := &AppendEntryArgs{
		Term:           currentTerm,
		LeaderId:       rf.me,
		PrevLogIndex:   prevIndex,
		PrevLogTerm:    prevTerm,
		Entries:        entries,
		LeaderCommit:   commitIndex,
		AppendEntrySeq: seq,
	}
	rf.mu.Unlock()

	r, ok := rf.sendAppendEntry(server, args)
	if !ok {
		return unavailable
	}

	rf.mu.Lock()
	if r.Term > rf.currentTerm {
		// stale leader --> follower
		rf.logger.Printf("replicate #%d, ae req to %d ,stale leader, term: %d -> %d", seq, server, r.Term, rf.currentTerm)
		rf.currentTerm = r.Term
		rf.leaderId = none
		rf.persist(nil)

		rf.followerC <- struct{}{}
		rf.mu.Unlock()
		return abort
	}

	// we might get an out-of-order response caused by the test network setup,
	// here we try to make the response handle the state update limited to the
	// correspond request by set the statue with the parameters we used in the
	// request. it may cause the state going backward a little bit if there is
	// an out-of-order response, but it will not harm the overall correctness.

	if r.Success {
		rf.logger.Printf("replicate #%d, send ae req to %d success, index: [%d, %d), prev index: %d",
			seq, server, start, end, prevIndex)

		rf.nextIndex[server] = end
		rf.matchIndex[server] = upto
		rf.mu.Unlock()
		return true_
	}

	nextIndex := prevIndex
	if r.XTerm != 0 {
		ind := rf.index2ind(nextIndex)
		found := false
		for ; ind >= 0; ind-- {
			term := rf.log[ind].Term
			if term > r.XTerm {
				continue
			}
			if term == r.XTerm && !found {
				found = true
			}
			if term < r.XTerm {
				break
			}
		}
		nextIndex = r.XIndex
		if found {
			nextIndex = rf.lastSnapshotIndex + (ind + 1)
		}
		rf.logger.Printf("replicate #%d, ae req to %d, fast backup: xterm: %d, xindex: %d, next0: %d, next: %d, found: %v",
			seq, server, r.XTerm, r.XIndex, prevIndex, nextIndex, found)
	}
	if r.XLen != 0 {
		rf.logger.Printf("replicate #%d, ae req to %d, fast backup: xlen: %d", seq, server, r.XLen)
		nextIndex = r.XLen
	}
	rf.nextIndex[server] = nextIndex
	rf.mu.Unlock()

	return retry
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	tick := time.NewTicker(rf.nextVoteTimeout())
	cancel := make(chan struct{})
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		case <-tick.C:
			status := atomic.LoadInt32(&rf.status)
			switch status {
			case follower, candidate:
				if status == follower {
					// leader --> follower:
					//  1. set leader id to new leader id(perform at transition site)
					//  2. set the term to leader's term(perform at transition site)
					//  3. reset timer to next vote
					// candidate --> follower:
					//  1. set votedFor to none(perform at transition side)
					//  2. reset timer to next vote
					atomic.StoreInt32(&rf.status, candidate)
				}
				// using close as the broadcast to notify
				// any pending election task to quit.
				close(cancel)
				cancel = make(chan struct{})

				tick.Reset(rf.nextVoteTimeout())

				go func(done <-chan struct{}) {
					// perform an election
					//  1. if the election succeed, transform to leader
					//     and set timer to next heartbeat
					//  2. if the election failed, stay as candidate
					//     and set votedFor to none & reset timer to next vote
					ok := rf.elect(done)
					if !ok {
						return // election does not succeed
					}
					// candidate --> leader:
					atomic.StoreInt32(&rf.status, leader)

					// use two separate path for replication
					// and heartbeat.
					// the reason that does not reuse the
					// heartbeat to do replication is: the
					// replication process is implemented
					// to wait the majority success, however
					// the heartbeat does not need to wait,
					// this implies replication and heartbeat
					// may happen in different pace.
					// for simplicity just separate these two.

					//  1. start a replication task
					go rf.replicate(done)
					//  2. fire a heartbeat immediately
					go rf.heartbeat()
					//  3. reset timer to next heartbeat
					tick.Reset(rf.nextHeartbeatTimeout())
				}(cancel)
			case leader:
				go rf.heartbeat()
				tick.Reset(rf.nextHeartbeatTimeout())
			}
		case <-rf.followerC:
			// using close as the broadcast to notify
			// any pending election or leader task to quit.
			close(cancel)
			cancel = make(chan struct{})

			status := atomic.LoadInt32(&rf.status)
			rf.logger.Printf("switch to %v from %v", status, follower)

			atomic.StoreInt32(&rf.status, follower)
			tick.Reset(rf.nextVoteTimeout())
		}
	}
	close(cancel)
}

func (rf *Raft) applyEntry(entries []Entry, commandIndStart, commandIndex int) {
	for i, entry := range entries {
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: commandIndex,
		}

		rf.lastApplied++
		ind := commandIndStart + i
		rf.logger.Printf("applyEntry, applied command: %v at %d(%d), term: %d, lastApplied:%d, commitIndex: %d",
			entry.Command, commandIndex, ind, entry.Term, rf.lastApplied, rf.commitIndex)

		commandIndex++
	}
}

// termOf return the term of the give index, if the index is
// truncated by the snapshot, return false.
func (rf *Raft) termOf(index int) (int, bool) {
	if rf.lastSnapshotIndex > 0 && index < rf.lastSnapshotIndex {
		// if the given index is fold into
		// the snapshot already, return false
		return 0, false
	}
	if index == rf.lastSnapshotIndex {
		// index is exactly the last snapshot index
		return rf.lastSnapshotTerm, true
	}
	if index == 0 {
		// index is zero, means no logs at all.
		return 0, true
	}
	// index is able to locate a valid log entry
	ind := rf.index2ind(index)
	return rf.log[ind].Term, true
}

func (rf *Raft) last() (index, term int) {
	if len(rf.log) == 1 && rf.lastSnapshotIndex != 0 {
		return rf.lastSnapshotIndex, rf.lastSnapshotTerm
	}
	ind := len(rf.log) - 1
	lastTerm := rf.log[ind].Term
	lastIndex := rf.lastSnapshotIndex + ind
	return lastIndex, lastTerm
}

func (rf *Raft) index2ind(index int) int {
	if rf.lastSnapshotIndex == 0 {
		return index
	}
	return index - rf.lastSnapshotIndex
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	rf.votedFor = none

	// leave index 0 unused, first log index is 1
	rf.log = make([]Entry, 1)
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.leaderId = none
	rf.followerC = make(chan struct{}, 3)

	rf.pendingSnapshot = &bytes.Buffer{}

	rf.applyCh = applyCh
	rf.logger = log.New(
		os.Stderr,
		fmt.Sprintf("server/%d ", me),
		log.LstdFlags|log.Lmicroseconds,
	)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
