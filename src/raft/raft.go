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
	// (or -1 if none)
	votedFor int
	// log entries, each entry contains command for state
	// machine, and term when entry was received by leader
	// (first index is 1)
	log []Entry

	//
	// volatile state on all servers
	//
	// index of highest log entry known to be committed(initialized to 0,
	// increases monotonically)
	commitIndex int
	// index of highest log entry applied to state machine(initialized to
	// 0, increases monotonically)
	lastApplied int
	// last heartbeat server has got from leader(piggyback in the append entry)
	lastHeartbeat int

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
	// indicate if a server is now the candidate state if leaderId is -1.
	leaderId int

	resetTimerCh chan struct{}
	heartbeatsCh []chan struct{}
	heartbeats   int64

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
	rf.logger.Printf("isLeader: %v, term: %d\n", isLeader, term)

	return term, isLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
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

	term := rf.currentTerm
	if args.Term < rf.currentTerm {
		rf.logger.Printf("RequestVote, candidate: %d, args.term: %d, curTerm: %d", args.CandidateId, args.Term, term)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.foundHigherTerm(-1, args.Term, fmt.Sprintf("rv req, candidate: %d", args.CandidateId))
	}

	lastLogIndex, lastLogTerm := rf.last()
	isAllowedCandidate := rf.votedFor == -1 || rf.votedFor == args.CandidateId
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
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		rf.resetTimerCh <- struct{}{}
	}
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
	defer rf.mu.Unlock()

	rf.logger.Printf("AppendEntry, leader: %d, args.term: %d, curTerm: %d", args.LeaderId, args.Term, rf.currentTerm)
	if args.Term > rf.currentTerm {
		rf.foundHigherTerm(args.LeaderId, args.Term, "ae req")
	}
	reply.Success = true
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
	}
	rf.resetTimerCh <- struct{}{}
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

	roundTripMs = 100
)

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	const timeoutMs = 3 * roundTripMs
	const upperBound = 100
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
		rndn := rnd.Intn(upperBound)
		timeout := time.Duration(timeoutMs+rndn) * time.Millisecond
		timer := time.NewTimer(timeout)
		select {
		case val := <-timer.C:
			if val.IsZero() {
				continue // timer ch is closed somewhere else
			}
			ok := rf.elect()
			if !ok {
				// elect have not granted by peers, so waiting next timer
				continue
			}
			// election succeed, become to leader and send empty AE to peers
			rf.setLeader()
		case <-rf.resetTimerCh:
			rf.logger.Printf("electing, reset timer")
			timer.Stop()
		}
	}
}

func (rf *Raft) elect() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.me == rf.leaderId {
		return false // already a leader, do nothing
	}
	rf.currentTerm++ // increase the term
	rf.leaderId = -1

	votes := make([]int32, len(rf.peers))
	for i := range votes {
		votes[i] = invalidVote
	}
	lastLogIndex, lastLogTerm := rf.last()

	rf.logger.Printf("electing, gathering votes for term: %d", rf.currentTerm)

	for ind := range rf.peers {
		if ind == rf.me { // vote itself first
			rf.votedFor = rf.me
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
			// fire a request vote request, and set up a timer
			// if the timer timeout(e.g., network drop the packet forever),
			// we consider the vote as rejected.
			timeoutMs := 3 * roundTripMs
			done := make(chan int32)
			go func() {
				ans := rejectedVote
				rf.logger.Printf("electing, send RequestVote req to %d, term: %d", server, args.Term)
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(server, args, &reply)
				if ok && reply.VoteGranted {
					ans = reply.Term
				}
				done <- int32(ans)
			}()
			select {
			case ans := <-done:
				atomic.StoreInt32(&votes[server], ans)
				rf.logger.Printf("electing, send RequestVote to %d, term: %d, get %d", server, args.Term, ans)
			case <-time.After(time.Duration(timeoutMs) * time.Millisecond):
				atomic.StoreInt32(&votes[server], rejectedVote)
				rf.logger.Printf("electing, send RequestVote req to %d, term: %d timeout", server, args.Term)
			}
		}(ind, &args)
	}
	majority := len(votes)/2 + 1
	elected := false
	granted := 0
	done := 0
	anyHigherTerm := false
	for {
		granted = 0
		done = 0
		anyHigherTerm = false
		for i := 0; i < len(votes); i++ {
			peerTerm := atomic.LoadInt32(&votes[i])
			if peerTerm > int32(rf.currentTerm) {
				anyHigherTerm = true
				break
			}
			if peerTerm >= 0 {
				granted++
			}
			if peerTerm >= 0 || peerTerm == rejectedVote {
				done++
			}
		}
		if anyHigherTerm {
			elected = false
			break
		}
		if granted >= majority {
			elected = true
			break
		}
		if done == len(votes) {
			break
		}
	}
	if !elected {
		rf.votedFor = -1
	}
	rf.logger.Printf("electing, majority: %d, yes: %d, done: %d, anyHigherTerm: %v, success:%v\n",
		majority, granted, done, anyHigherTerm, elected)

	return elected
}

func (rf *Raft) sendHeartbeat(server int) {
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	commitIndex := rf.commitIndex
	lastLogIndex, lastLogTerm := rf.last()
	rf.heartbeats++
	seq := rf.heartbeats
	rf.mu.Unlock()

	args := AppendEntryArgs{
		LeaderId:     rf.me,
		Term:         currentTerm,
		PrevLogIndex: lastLogIndex,
		PrevLogTerm:  lastLogTerm,
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
		rf.foundHigherTerm(-1, reply.Term, "heartbeat resp")
	}
}

func (rf *Raft) setLeader() {
	rf.mu.Lock()
	rf.logger.Printf("electing, elected as leader for term: %d", rf.currentTerm)
	rf.leaderId = rf.me
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
					// T6 s0 send heart to s2 in a g2, will get resp & s2 can get chance to update it's state.
					go rf.sendHeartbeat(server)
				case <-rf.heartbeatsCh[server]:
					timer.Stop()
					rf.logger.Printf("yielding, received terminating heartbeat processor for %d", server)
					return // convert to follower, no need for heartbeat processor
				}
			}
		}(i)
	}
}

func (rf *Raft) last() (index, term int) {
	if len(rf.log) == 0 {
		return
	}
	index = len(rf.log) - 1
	term = rf.log[index].Term
	return
}

func (rf *Raft) foundHigherTerm(leaderId, pterm int, reason string) bool {
	if pterm < rf.currentTerm {
		return false
	}
	rf.votedFor = -1

	term := rf.currentTerm
	rf.currentTerm = pterm

	if rf.me != rf.leaderId {
		return false
	}
	rf.leaderId = leaderId
	rf.logger.Printf("yielding, converting to follower, curTerm: %d -> %d, leader: %d, %v", term, pterm, leaderId, reason)
	for i := range rf.heartbeatsCh {
		if i == rf.me {
			continue
		}
		rf.heartbeatsCh[i] <- struct{}{}
	}
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

	rf.leaderId = -1
	rf.votedFor = -1
	rf.resetTimerCh = make(chan struct{})
	rf.heartbeatsCh = make([]chan struct{}, len(peers))
	for i := 0; i < len(peers); i++ {
		rf.heartbeatsCh[i] = make(chan struct{})
	}
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
