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
	Follower = iota
	Candidate
	Leader
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

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
	term := rf.currentTerm
	if args.Term > term {
		rf.votedFor = none
		rf.currentTerm = args.Term

		rf.leaderId = none
		rf.followerC <- struct{}{}
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

		rf.leaderId = none
		rf.followerC <- struct{}{}
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = voteGranted

	rf.persist(nil)

	rf.logger.Printf("RequestVote, candidate: %d, args.term: %d, curTerm: %d -> %d, votedFor: %d, allow: %v, uptodate: %v, granted: %v",
		args.CandidateId, args.Term, term, rf.currentTerm, rf.votedFor, isAllowedCandidate, isUpToDate(), voteGranted)
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
}

type AppendEntryReply struct {
	// current term for leader to update itself
	Term int
	// true if follower contained entry matching
	// preLogIndex and prevLogTerm
	Success bool
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()

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
		rf.logger.Printf("AppendEntry, stale %v, term: %d -> %d", role, args.Term, rf.currentTerm)
		rf.votedFor = none
		rf.currentTerm = args.Term
		rf.persist(nil)

		rf.leaderId = args.LeaderId
	}

	reply.Term = rf.currentTerm

	acceptable := func() bool {
		if args.PrevLogIndex > 0 {
			if args.PrevLogIndex > lastIndex {
				return false
			}
			prevTerm, ok := rf.termOf(args.PrevLogIndex)
			if !ok {
				return false
			}
			if prevTerm != args.PrevLogTerm {
				return false
			}
			return true
		}
		// prev log index is 0
		return true
	}

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

	rf.logger.Printf("AppendEntry, %v, leader: %d, args.term: %d, curTerm: %d, "+
		"lastIndex: %d, #log: %d, #args.entry: %d, prevIndex: %d, prevTerm: %d, commit index: %d-%d",
		reply.Success, args.LeaderId, args.Term, term,
		lastIndex, len(rf.log)-1, len(args.Entries), args.PrevLogIndex, args.PrevLogTerm,
		args.LeaderCommit, rf.commitIndex)

	// as long as the AE is valid
	// no mater if the log is acceptable
	// or not, reset the follower timer
	// anyway.
	rf.followerC <- struct{}{}

	if !reply.Success {
		rf.mu.Unlock()
		return
	}

	if args.LeaderCommit <= rf.commitIndex {
		rf.mu.Unlock()
		return
	}

	commitIndex := args.LeaderCommit
	if commitIndex > lastIndex {
		commitIndex = lastIndex
	}
	rf.commitIndex = commitIndex

	commandIndex := rf.lastApplied + 1
	startInd := rf.index2ind(commandIndex)
	endInd := rf.index2ind(rf.commitIndex) + 1
	entries := rf.log[startInd:endInd]

	// unlock before apply entry
	// to avoid deadlock when trying to
	// send an applyMsg to state machine
	// while holding the lock, and the
	// state machine start a snapshot
	// while receiving a applyMsg to acquire
	// the lock
	rf.mu.Unlock()

	rf.applyEntry(entries, startInd, commandIndex)
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
	term = rf.currentTerm
	isLeader = rf.leaderId == rf.me
	rf.mu.Unlock()

	if !isLeader {
		return index, term, isLeader
	}

	rf.mu.Lock()
	entry := Entry{
		Command: command,
		Term:    rf.currentTerm,
	}
	rf.log = append(rf.log, entry)
	index, _ = rf.last()
	rf.persist(nil)
	rf.mu.Unlock()

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

func (rf *Raft) elect(cancel <-chan struct{}) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.me == rf.leaderId {
		return false // already a leader, do nothing
	}
	rf.currentTerm++    // increase the term
	rf.votedFor = rf.me // vote itself first
	rf.leaderId = none
	rf.persist(nil)

	votes := make([]int32, len(rf.peers))
	for i := range votes {
		votes[i] = voteInvalid
	}
	lastLogIndex, lastLogTerm := rf.last()

	rf.logger.Printf("electing, gathering votes for term: %d", rf.currentTerm)

	for ind := range rf.peers {
		if ind == rf.me { // voted itself
			votes[ind] = int32(rf.currentTerm)
			continue
		}
		args := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
		}
		go func(server int, args *RequestVoteArgs) {
			rf.logger.Printf("electing, send RequestVote req to %d, term: %d", server, args.Term)
			r, ok := rf.sendRequestVote(server, args)
			ans := voteRejected
			if ok && r.VoteGranted {
				ans = r.Term
			}
			rf.logger.Printf("electing, RequestVote req to %d, term: %d, granted: %v", server, args.Term, r.VoteGranted)
			atomic.StoreInt32(&votes[server], int32(ans))
		}(ind, &args)
	}
	var granted int
	var finished int
	var maxTerm int
	elected := false
	majority := len(votes)/2 + 1
	for !rf.killed() {
		granted = 0
		finished = 0
		maxTerm = rf.currentTerm
		for i := 0; i < len(votes); i++ {
			peerTerm := int(atomic.LoadInt32(&votes[i]))
			if peerTerm > rf.currentTerm {
				maxTerm = peerTerm
			}
			if peerTerm >= 0 {
				granted++
			}
			if peerTerm >= 0 || peerTerm == voteRejected {
				finished++
			}
		}
		if maxTerm > rf.currentTerm {
			break
		}
		if granted >= majority {
			elected = true
			break
		}
		if finished == len(votes) {
			break
		}
		select {
		case <-cancel:
			rf.logger.Printf("electing, term: %d, canceled", rf.currentTerm)
			goto end
		default:
			time.Sleep(time.Millisecond * 3)
		}
	}
end:
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
		rf.logger.Printf("electing, election failed, term: %d, granted: %d, require: %d", rf.currentTerm, granted, majority)
		return false
	}

	// candidate --> leader:
	//  1. set me as leader id
	rf.leaderId = rf.me
	//  2. set votedFor to none
	rf.votedFor = none
	//  3. init next index & match index
	nextIndex := lastLogIndex + 1
	for peer := range rf.peers {
		rf.nextIndex[peer] = nextIndex
		rf.matchIndex[peer] = 0
	}
	rf.persist(nil)

	rf.logger.Printf("electing, elected as leader for term: %d, c: %d, a: %d, n: %d", rf.currentTerm, rf.commitIndex, rf.lastApplied, nextIndex)

	return true
}

func (rf *Raft) heartbeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.me != rf.leaderId {
		return
	}
	lastIndex, lastTerm := rf.last()
	args := AppendEntryArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: lastIndex,
		PrevLogTerm:  lastTerm,
		LeaderCommit: rf.commitIndex,
	}
	rf.heartbeatSeq++
	seq := rf.heartbeatSeq
	for i := range rf.peers {
		if i == rf.me {
			continue // do not heartbeat to myself
		}
		go func(server int, a AppendEntryArgs) {
			rf.logger.Printf("heartbeat #%d to %d", seq, server)
			r, ok := rf.sendAppendEntry(server, &a)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if r.Term > rf.currentTerm {
				rf.logger.Printf("heartbeat #%d, stale leader, term: %d -> %d", seq, r.Term, rf.currentTerm)
				rf.followerC <- struct{}{}
			}
			rf.logger.Printf("heartbeat #%d to %d %v, seq: %d, %d", seq, server, ok, seq, rf.heartbeatSeq)
		}(i, args)
	}
}

func (rf *Raft) replicate(done <-chan struct{}) {
	cancel := make(chan struct{})
	for !rf.killed() {
		select {
		case <-time.After(time.Millisecond):
			rf.mu.Lock()
			if rf.me != rf.leaderId {
				rf.mu.Unlock()
				return
			}
			index, _ := rf.last()
			currentTerm := rf.currentTerm
			commitIndex := rf.commitIndex
			rf.seq++
			seq := rf.seq
			rf.mu.Unlock()

			if index == commitIndex {
				continue // no new log entries need to replicate
			}

			// before start a new replication
			// cancel all existing pending replication.
			close(cancel)
			cancel = make(chan struct{})

			rf.logger.Printf("replicate #%d, term: %d, upto: index %d, commitIndex: %d", seq, currentTerm, index, commitIndex)

			ok := rf.replicateWithWait(seq, currentTerm, index, commitIndex, cancel)
			if !ok {
				return
			}

			rf.mu.Lock()
			commandIndex := rf.lastApplied + 1
			start := rf.index2ind(commandIndex)
			end := rf.index2ind(index) + 1
			entries := rf.log[start:end]
			rf.commitIndex = index
			rf.mu.Unlock()

			rf.applyEntry(entries, start, commandIndex)
		case <-done:
			close(cancel)
			return
		}
	}
}

// replicateWithWait logs that up to index to all peers, return
// true immediately if majority replicated, and if there is
// any un-responded or failed replicate request, they will
// still continue to retry until the cancel channel is closed.
// i.e, it returns true if log replicated to majority peers,
// and false if the cancel channel is closed.
func (rf *Raft) replicateWithWait(seq int64, currentTerm, index, commitIndex int, cancel <-chan struct{}) bool {
	const true_ = 1
	var success int64
	ans := make([]int64, len(rf.peers))
	for peer := range rf.peers {
		if peer == rf.me {
			ans[peer] = true_
			atomic.AddInt64(&success, 1)
			continue
		}
		go func(server int) {
			ch := make(chan bool)
			go func() {
				ok := rf.replicateTo(seq, currentTerm, server, index, commitIndex, cancel)
				ch <- ok
			}()
			select {
			case ok := <-ch:
				if ok {
					atomic.StoreInt64(&ans[server], true_)
					atomic.AddInt64(&success, 1)
				}
			case <-cancel:
				return
			}
		}(peer)
	}

	ch := make(chan bool)
	majority := len(rf.peers)/2 + 1
	res := make([]int64, len(rf.peers))

	go func() {
		for !rf.killed() {
			select {
			case <-time.After(time.Millisecond):
				if atomic.LoadInt64(&success) >= int64(majority) {
					for peer := range rf.peers {
						res[peer] = atomic.LoadInt64(&ans[peer])
					}
					ch <- true
					return
				}
			case <-cancel:
				ch <- false
				return
			}
		}
	}()

	ok := <-ch
	n := atomic.LoadInt64(&success)
	rf.logger.Printf("replicate, term: %d upto: %d, success: %d, ans: %v, ok: %v", currentTerm, index, n, res, ok)
	return ok
}

func (rf *Raft) replicateTo(seq int64, currentTerm, server, index, commitIndex int, cancel <-chan struct{}) bool {
	startIndex := index
	endIndex := index + 1
	numSnapshotRetry, numAERetry := 0, 0
	for !rf.killed() {
		rf.mu.Lock()
		if rf.me != rf.leaderId {
			rf.mu.Unlock()
			return false
		}

		nextIndex := rf.nextIndex[server]
		if startIndex > nextIndex {
			startIndex = nextIndex
		}
		prevLogIndex := startIndex - 1
		if nextIndex == commitIndex+1 {
			// if nextIndex is equal to commitIndex+1
			// it means, we've replicated all the entries
			// up to commitIndex, no entries need to replicate
			// so, we make a heartbeat here. by heartbeat, it
			// means the prevLogIndex should be the commitIndex
			prevLogIndex = commitIndex
		}
		prevLogTerm, ok := rf.termOf(prevLogIndex)
		if !ok { // install snapshot
			args := &InstallSnapshotArgs{
				Term:              currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.lastSnapshotIndex,
				LastIncludedTerm:  rf.lastSnapshotTerm,
				Data:              rf.snapshot,
				Done:              true,
			}
			rf.mu.Unlock()

			rf.logger.Printf("replicate #%d, send snapshot req to %d, lastIndex: %d, lastTerm: %d, start: %d, end: %d, next: %d, cnt: %d",
				seq, server, args.LastIncludedIndex, args.LastIncludedTerm, startIndex, endIndex, nextIndex, numSnapshotRetry)
			r, ok := rf.sendInstallSnapshot(server, args)
			if !ok {
				numSnapshotRetry++
				continue // continue to retry
			}

			rf.mu.Lock()
			if seq < rf.seq {
				// out-of-order response, it means there must a new
				// replication process is currently running. Abort
				// here immediately so that this out-of-order response
				// will update any non-local state unexpectedly.
				rf.logger.Printf("relicate #%d, out-of-order snapshot response, current seq: %d", seq, rf.seq)
				rf.mu.Unlock()
				return false
			}
			if r.Term > rf.currentTerm {
				// stale leader --> follower
				rf.logger.Printf("replicate #%d, stale leader, term: %d -> %d", seq, r.Term, rf.currentTerm)
				rf.currentTerm = r.Term
				rf.leaderId = none
				rf.persist(nil)

				rf.followerC <- struct{}{}
				rf.mu.Unlock()
				return false
			}
			rf.logger.Printf("replicate #%d, send snapshot req to %d, lastIndex: %d, lastTerm: %d success",
				seq, server, args.LastIncludedIndex, args.LastIncludedTerm)

			// update the next index and
			startIndex = rf.lastSnapshotIndex + 1
			rf.nextIndex[server] = endIndex
			// continue the loop for replication if existing
			// any un-committed entries
			rf.mu.Unlock()
			continue
		}
		// regular append entry or heartbeat
		var entries []Entry
		if prevLogIndex < startIndex {
			start := rf.index2ind(startIndex)
			end := rf.index2ind(endIndex)
			entries = rf.log[start:end]
		}
		args := &AppendEntryArgs{
			Term:         currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: commitIndex,
		}
		rf.mu.Unlock()

		rf.logger.Printf("replicate #%d, send ae req to %d, #entries: %d, index: [%d, %d), prevIndex: %d, prevTerm: %d, cnt: %d",
			seq, server, len(entries), startIndex, endIndex, prevLogIndex, prevLogTerm, numAERetry)
		r, ok := rf.sendAppendEntry(server, args)
		if !ok {
			numAERetry++
			continue // continue to retry
		}

		rf.mu.Lock()
		if seq < rf.seq {
			// out-of-order response, it means there must a new
			// replication process is currently running. Abort
			// here immediately so that this out-of-order response
			// will update any non-local state unexpectedly.
			rf.logger.Printf("relicate #%d, out-of-order ae response, current seq: %d", seq, rf.seq)
			rf.mu.Unlock()
			return false
		}
		if r.Term > rf.currentTerm {
			// stale leader --> follower
			rf.logger.Printf("replicate #%d, stale leader, term: %d -> %d", seq, r.Term, rf.currentTerm)
			rf.currentTerm = r.Term
			rf.leaderId = none
			rf.persist(nil)

			rf.followerC <- struct{}{}
			rf.mu.Unlock()
			return false
		}
		if r.Success {
			rf.logger.Printf("replicate #%d, send ae req to %d success, index: [%d, %d), prev index: %d",
				seq, server, startIndex, endIndex, prevLogIndex)

			rf.nextIndex[server] = endIndex
			rf.matchIndex[server] = endIndex - 1
			rf.mu.Unlock()
			return true
		}

		if rf.nextIndex[server] > 1 {
			// make sure the minimum nextIndex is at least 1 after the decrement
			rf.nextIndex[server]--
		}
		rf.mu.Unlock()

		select {
		case <-cancel:
			return false
		default:
			// noop but make it nonblocking
		}
	}
	return false
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
			case Follower, Candidate:
				if status == Follower {
					// leader --> follower:
					//  1. set leader id to new leader id(perform at transition site)
					//  2. set the term to leader's term(perform at transition site)
					//  3. reset timer to next vote
					// candidate --> follower:
					//  1. set votedFor to none(perform at transition side)
					//  2. reset timer to next vote
					atomic.StoreInt32(&rf.status, Candidate)
				}
				// using close as the broadcast to notify
				// any pending election task to quit.
				close(cancel)
				cancel = make(chan struct{})

				// perform an election
				//  1. if the election succeed, transform to leader
				//     and set timer to next heartbeat
				//  2. if the election failed, stay as candidate
				//     and set votedFor to none & reset timer to next vote
				ok := rf.elect(cancel)
				if !ok {
					tick.Reset(rf.nextVoteTimeout())
					continue
				}
				// candidate --> leader:
				atomic.StoreInt32(&rf.status, Leader)
				// use two separate path for replication
				// and heartbeat.
				// the reason that does not reuse the
				// heartbeat to do replication is, the
				// replication process is implemented
				// to wait the majority success, however
				// the heartbeat does not need to wait,
				// this implies replication and heartbeat
				// may happen in different pace.
				// for simplicity just separate these two.
				//
				//  1. start a replication task
				go rf.replicate(cancel)
				//  2. reset timer to next heartbeat
				tick.Reset(time.Millisecond)
			case Leader:
				go rf.heartbeat()
				tick.Reset(rf.nextHeartbeatTimeout())
			}
		case <-rf.followerC:
			// using close as the broadcast to notify
			// any pending election or leader task to quit.
			close(cancel)
			cancel = make(chan struct{})

			atomic.StoreInt32(&rf.status, Follower)
			tick.Reset(rf.nextVoteTimeout())
		}
	}
}

func (rf *Raft) applyEntry(entries []Entry, commandIndStart, commandIndex int) {
	for i, entry := range entries {
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: commandIndex,
		}

		rf.mu.Lock()
		rf.lastApplied++
		ind := commandIndStart + i
		rf.logger.Printf("applyEntry, applied command: %v at %d(%d), term: %d, lastApplied:%d, commitIndex: %d",
			entry.Command, commandIndex, ind, entry.Term, rf.lastApplied, rf.commitIndex)
		rf.mu.Unlock()

		commandIndex++
	}
	rf.mu.Lock()
	rf.persist(nil)
	rf.mu.Unlock()
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
