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
	// so follower can redirect clients or
	// check if a server is leader or follower by check leaderId == me, or
	// indicate if a server is now the candidate state if leaderId is invalidId.
	leaderId int

	applyCh chan ApplyMsg

	// notify any leader task to exit via close this channel
	doneCh chan struct{}

	// notify any candidate state server to become follower by send a signal to this channel
	followerCh chan struct{}

	heartbeats int64

	logger *log.Logger
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
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	ind := rf.index2ind(index)
	entry := rf.log[ind]
	rf.lastSnapshotIndex = index
	rf.lastSnapshotTerm = entry.Term

	rf.logger.Printf("snapshot, term: %d, index:%d, ind:%d, #log:%d", entry.Term, index, ind, len(rf.log)-1)

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
	rf.persist(snapshot)
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
		rf.declareAsFollower(invalidId, args.Term, fmt.Sprintf("rv req, candidate: %d", args.CandidateId), false)
	}

	lastLogIndex, lastLogTerm := rf.last()
	isAllowedCandidate := rf.votedFor == invalidId || rf.votedFor == args.CandidateId
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
		rf.votedFor = args.CandidateId
		rf.followerCh <- struct{}{}
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = voteGranted

	rf.persist(nil)
	rf.logger.Printf("RequestVote, candidate: %d, args.term: %d, curTerm: %d -> %d, votedFor: %d, allow: %v, uptodate: %v, granted: %v",
		args.CandidateId, args.Term, term, rf.currentTerm, rf.votedFor, isAllowedCandidate, isUpToDate(), voteGranted)
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
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

	lastIndex, _ := rf.last()
	if args.Term < rf.currentTerm {
		// reply false if term < currentTerm
		reply.Success = false
		reply.Term = rf.currentTerm
		rf.logger.Printf("AppendEntry, false, leader: %d, args.term: %d, curTerm: %d, "+
			"lastIndex: %d, #log: %d, #args.entry: %d, prev: %d-%d",
			args.LeaderId, args.Term, rf.currentTerm,
			lastIndex, len(rf.log)-1, len(args.Entries),
			args.PrevLogIndex, args.PrevLogTerm)
		rf.mu.Unlock()
		return
	}

	acceptable := func() bool {
		if args.PrevLogIndex != 0 {
			if args.PrevLogIndex > lastIndex {
				return false
			}
			entry, ok := rf.get(args.PrevLogIndex)
			if !ok {
				return false
			}
			if entry.Term != args.PrevLogTerm {
				return false
			}
		}
		return true
	}

	reply.Success = false
	if acceptable() {
		reply.Success = true
		ind := rf.index2ind(args.PrevLogIndex)
		rf.log = rf.log[:ind+1]
		rf.log = append(rf.log, args.Entries...)
	}

	rf.declareAsFollower(args.LeaderId, args.Term, "ae req", false)
	rf.persist(nil)

	reply.Term = rf.currentTerm

	rf.logger.Printf("AppendEntry, %v, leader: %d, args.term: %d, curTerm: %d, "+
		"lastIndex: %d, #log:%d, #args.entry: %d, prev: %d-%d, commit index: %d-%d",
		reply.Success, args.LeaderId, args.Term, rf.currentTerm,
		lastIndex, len(rf.log)-1, len(args.Entries), args.PrevLogIndex, args.PrevLogTerm,
		args.LeaderCommit, rf.commitIndex)

	if !reply.Success || args.LeaderCommit <= rf.commitIndex {
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

	rf.mu.Lock()
	rf.persist(nil)
	rf.mu.Unlock()
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
	invalidVote = iota - 2
	rejectedVote

	invalidId   = -1
	roundTripMs = 100
)

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	const upperBound = 100
	const timeoutMs = roundTripMs * 3
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
		rndn := rnd.Intn(upperBound)
		timeout := time.Duration(timeoutMs+rndn) * time.Millisecond
		timer := time.NewTimer(timeout)
		cancel := make(chan struct{})
		select {
		case val := <-timer.C:
			if val.IsZero() {
				continue // timer ch is closed somewhere else
			}
			ok := rf.elect(cancel)
			if !ok {
				// election have not granted by peers, so waiting next timer
				continue
			}
			// election succeed, become to leader and send empty AE to peers
			rf.declareLeadership()
		case <-rf.followerCh:
			close(cancel)
			timer.Stop()
			rf.logger.Printf("electing, reset election timer")
		}
	}
}

func (rf *Raft) elect(cancel chan struct{}) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.me == rf.leaderId {
		return false // already a leader, do nothing
	}
	rf.currentTerm++    // increase the term
	rf.votedFor = rf.me // vote itself first
	rf.leaderId = invalidId
	rf.persist(nil)

	votes := make([]int32, len(rf.peers))
	for i := range votes {
		votes[i] = invalidVote
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
			ch := make(chan int32)
			// fire a request vote request, and set up a timer
			// if the timer timeout(e.g., network drop the packet
			// forever), we consider the vote as rejected.
			timeout := time.Millisecond * roundTripMs * 3
			go func() {
				ans := rejectedVote
				rf.logger.Printf("electing, send RequestVote req to %d, term: %d", server, args.Term)
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(server, args, &reply)
				if ok && reply.VoteGranted {
					ans = reply.Term
				}
				ch <- int32(ans)
			}()
			select {
			case ans := <-ch:
				atomic.StoreInt32(&votes[server], ans)
				rf.logger.Printf("electing, send RequestVote to %d, term: %d, get %d", server, args.Term, ans)
			case <-time.After(timeout):
				atomic.StoreInt32(&votes[server], rejectedVote)
				rf.logger.Printf("electing, send RequestVote req to %d, term: %d timeout", server, args.Term)
			case <-cancel:
				atomic.StoreInt32(&votes[server], rejectedVote)
				rf.logger.Printf("electing, send RequestVote req to %d, term: %d canceled", server, args.Term)
				return
			}
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
			if peerTerm >= 0 || peerTerm == rejectedVote {
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
		time.Sleep(time.Millisecond * 3)
	}
	if !elected {
		rf.votedFor = invalidId
	}
	term := rf.currentTerm
	if maxTerm > rf.currentTerm {
		rf.currentTerm = maxTerm
	}
	rf.persist(nil)
	rf.logger.Printf("electing, majority: %d, yes: %d, finished: %d, term: (%d -> %d), success:%v",
		majority, granted, finished, term, maxTerm, elected)

	return elected
}

func (rf *Raft) declareLeadership() {
	rf.mu.Lock()
	lastIndex, _ := rf.last()
	nextIndex := lastIndex + 1
	rf.logger.Printf("electing, elected as leader for term: %d, "+
		"commit index: %d, last applied: %d, next index: %d",
		rf.currentTerm, rf.commitIndex, rf.lastApplied, nextIndex)
	for peer := range rf.peers {
		rf.nextIndex[peer] = nextIndex
		rf.matchIndex[peer] = 0
	}
	rf.leaderId = rf.me
	rf.doneCh = make(chan struct{})
	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) { // heartbeat processor
			for !rf.killed() {
				timeout := time.Millisecond * roundTripMs
				timer := time.NewTimer(timeout)
				select {
				case val := <-timer.C:
					if val.IsZero() {
						continue // timer ch is closed somewhere else
					}
					// use a separate goroutine to send
					// the heartbeat so that the heartbeat
					// would always fire no matter if an
					// individual send get blocked(caused by slow
					// network, etc.) or not.
					//
					// An example is: assume s0, s1, s2
					// T1 s2 elected as leader
					// T2 s2 got disconnected
					// T3 s0 elected as leader
					// T4 s0 send heart to s2 in g1, and get blocked
					// T5 s2 get connected again
					// T6 s0 send heart to s2 in a g2, s0 will get resp
					//    and s2 can get chance to update it's state.
					go rf.heartbeatTo(server)
				case <-rf.doneCh:
					timer.Stop()
					rf.logger.Printf("yielding, terminate heartbeat for %d", server)
					return // convert to follower, terminate heartbeat processor
				}
			}
		}(i)
	}

	go rf.replicate()
}

func (rf *Raft) heartbeatTo(server int) {
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	commitIndex := rf.commitIndex
	index, term := rf.last()
	rf.heartbeats++
	seq := rf.heartbeats
	rf.mu.Unlock()

	args := AppendEntryArgs{
		LeaderId:     rf.me,
		Term:         currentTerm,
		PrevLogIndex: index,
		PrevLogTerm:  term,
		LeaderCommit: commitIndex,
	}
	reply := AppendEntryReply{}

	rf.logger.Printf("send heartbeat req %d to %d, term: %d", seq, server, currentTerm)
	ok := rf.sendAppendEntry(server, &args, &reply)
	rf.logger.Printf("send heartbeat %d to %d, term: %d, pterm: %d, ok: %v",
		seq, server, currentTerm, reply.Term, ok)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		// step down on the leadership
		rf.declareAsFollower(invalidId, reply.Term, "heartbeat resp", true)
	}
}

func (rf *Raft) replicate() {
	var prev int
	cancel := make(chan struct{})
	for !rf.killed() {
		rf.mu.Lock()
		if rf.me != rf.leaderId {
			close(cancel)
			rf.mu.Unlock()
			break // not leader anymore
		}
		index, _ := rf.last()
		commitIndex := rf.commitIndex
		currentTerm := rf.currentTerm
		rf.mu.Unlock()
		if index == prev || index <= commitIndex {
			time.Sleep(time.Millisecond)
			continue
		}

		rf.logger.Printf("replicate, === index %d -> %d ===", prev, index)

		// notify any pending replicate task to quit
		// and start a new replicate with the new index
		close(cancel)
		cancel = make(chan struct{})

		ok := rf.replicateToAll(currentTerm, index, cancel)
		if !ok {
			continue
		}

		rf.mu.Lock()
		commandIndex := rf.lastApplied + 1
		start := rf.index2ind(commandIndex)
		end := rf.index2ind(index) + 1
		entries := rf.log[start:end]
		rf.commitIndex = index
		rf.mu.Unlock()

		rf.applyEntry(entries, start, commandIndex)

		rf.mu.Lock()
		rf.persist(nil)
		rf.mu.Unlock()

		prev = index
	}
}

// replicateToAll logs that up to index to all peers, return
// true immediately if majority replicated, and if there is
// any un-responded or failed replicate request, they will
// still continue to retry until the cancel channel is closed.
// i.e, this replicateAll will guarantee it will always return
// true, otherwise the cancel channel is closed.
func (rf *Raft) replicateToAll(currentTerm, index int, cancel chan struct{}) bool {
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
				ok := rf.replicateTo(currentTerm, server, index, cancel)
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
			case <-rf.doneCh:
				return
			}
		}(peer)
	}

	done := make(chan bool)
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
					done <- true
					return
				}
			case <-cancel:
				done <- false
				return
			case <-rf.doneCh:
				done <- false
				return
			}
		}
	}()

	ok := <-done
	n := atomic.LoadInt64(&success)
	rf.logger.Printf("replicate, term: %d upto: %d, success: %d, ans: %v, ok: %v", currentTerm, index, n, res, ok)
	return ok
}

func (rf *Raft) replicateTo(currentTerm, server, index int, cancel chan struct{}) bool {
	startIndex := index
	endIndex := index + 1
	timeout := time.Millisecond * roundTripMs
	for !rf.killed() {
		rf.mu.Lock()
		nextIndex := rf.nextIndex[server]
		if startIndex > nextIndex {
			startIndex = nextIndex
		}
		commitIndex := rf.commitIndex
		start := rf.index2ind(startIndex)
		end := rf.index2ind(endIndex)
		entries := rf.log[start:end]
		prevLogIndex := startIndex - 1
		prevEntry, ok := rf.get(prevLogIndex)
		if !ok {
			panic(fmt.Sprintf("should send snapshot here: %v, %v, %v, %v", server, prevLogIndex, rf.lastSnapshotIndex, rf.lastSnapshotTerm))
		}
		prevLogTerm := prevEntry.Term
		rf.mu.Unlock()
		args := &AppendEntryArgs{
			Term:         currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: commitIndex,
		}

		timer := time.NewTimer(timeout)
		ch := make(chan *AppendEntryReply)

		go func() {
			reply := &AppendEntryReply{}
			rf.logger.Printf("replicate, send ae req to %d, index: %d-%d(%d), prev: %d-%d",
				server, startIndex, endIndex, len(entries), prevLogIndex, prevLogTerm)
			rf.sendAppendEntry(server, args, reply)
			ch <- reply
		}()
		select {
		case reply := <-ch:
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.declareAsFollower(invalidId, reply.Term, "ae resp", true)
				rf.mu.Unlock()
				return false
			}
			if reply.Success {
				rf.logger.Printf("replicate, send ae req to %d, index: %d-%d(%d), prev: %d-%d success",
					server, startIndex, endIndex, len(entries), prevLogIndex, prevLogTerm)
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
		case <-timer.C:
			rf.logger.Printf("replicate, send ae req to %d, index: %d-%d(%d) timeout", server, startIndex, endIndex, len(entries))
		case <-cancel:
			timer.Stop()
			rf.logger.Printf("replicate, cancel ae req to %d", server)
			return false
		case <-rf.doneCh:
			timer.Stop()
			rf.logger.Printf("replicate, terminate ae req to %d", server)
			return false
		}
	}
	return false
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
}

func (rf *Raft) get(index int) (Entry, bool) {
	if rf.lastSnapshotIndex > 0 && index <= rf.lastSnapshotIndex {
		return Entry{}, false
	}
	ind := rf.index2ind(index)
	return rf.log[ind], true
}

func (rf *Raft) set(index int, entry Entry) {
	ind := rf.index2ind(index)
	rf.log[ind] = entry
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

// declare myself as follower by stop any leader tasks.
// if I'm already a follower, reset any candidate task.
func (rf *Raft) declareAsFollower(leaderId, pterm int, reason string, persist bool) bool {
	term := rf.currentTerm
	if pterm < term {
		return false
	}

	if pterm > rf.currentTerm {
		rf.currentTerm = pterm
		rf.votedFor = invalidId
		if persist {
			rf.persist(nil)
		}
	}

	if rf.me != rf.leaderId {
		// notify any candidate task to terminate
		rf.followerCh <- struct{}{}
		return false
	}
	rf.leaderId = leaderId
	rf.logger.Printf("yielding, converting to follower, curTerm: %d -> %d, leader: %d, %v", term, pterm, leaderId, reason)

	// notify any leader task to terminate
	close(rf.doneCh)

	return true
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

	rf.votedFor = invalidId

	// leave index 0 unused, first log index is 1
	rf.log = make([]Entry, 1)
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.leaderId = invalidId
	rf.followerCh = make(chan struct{}, 3)

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
