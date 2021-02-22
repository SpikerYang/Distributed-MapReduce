package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"log"
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

// import "bytes"
// import "../labgob"

const ElectionTimeOut = 500
const HeartbeatInterval = 200 * time.Millisecond
//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor  int
	leader    int
	log 	  []LogEntry
	heartbeatChan chan int
	voteChan  chan int
	electionTimeOut int
}
type LogEntry struct {
	Command string
	Term 	int
	Index   int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()

	term = rf.currentTerm
	isleader = rf.leader==rf.me
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
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


//
// restore previously persisted state.
//
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




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}
type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term int
	Success bool
}
//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.leader = -1
		rf.votedFor = -1
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId && candidateLogIsUpToDate(rf, args) {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.heartbeat()
		log.Printf("%d votes for %d, term: %d", rf.me, rf.votedFor, rf.currentTerm)
	}
}
func candidateLogIsUpToDate(rf *Raft, args * RequestVoteArgs) bool {
	if len(rf.log) == 0 {
		return args.LastLogIndex == 0
	}
	lastLog := rf.log[len(rf.log)]
	return lastLog.Term == args.LastLogTerm && len(rf.log) == args.LastLogIndex
}
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//TODO
	rf.mu.Lock()
	if args.Term >= rf.currentTerm {
		rf.updateState(args.Term, args.LeaderId)
		rf.heartbeat()
	} else {
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()
}
func (rf *Raft) isLeader() bool {
	return rf.leader == rf.me
}
func (rf *Raft) heartbeat() {
	rf.heartbeatChan <- 1
}
func (rf *Raft) sendHeartbeats() {
	for ; !rf.killed(); {
		rf.mu.Lock()
		if rf.isLeader() {
			for server, _ := range rf.peers {
				go rf.sendHeartbeat(server, rf.currentTerm)
			}
		}
		rf.mu.Unlock()
		time.Sleep(HeartbeatInterval)
	}
}
func (rf *Raft) sendHeartbeat(server int, term int) bool {
	args := AppendEntriesArgs{
		Term:         term,
		LeaderId:     rf.me,
	}
	reply := AppendEntriesReply{}
	ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
	//log.Printf("%d send heartbeat to %d", rf.me, server)
	//TODO: check term
	return ok
}
func (rf *Raft) updateState(term int, leader int) {
	rf.currentTerm = term
	rf.leader = leader
	rf.votedFor = -1
}
func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.initElection()
	go rf.heartbeat()
	log.Printf("Election start, Candidate: %d, term: %d", rf.me, rf.currentTerm)
	term := rf.currentTerm
	rf.mu.Unlock()
	for server, _ := range rf.peers {
		if server != rf.me {
			go rf.voteAndCount(server, term)
		}
	}
	count := 1
	for {
		if count > len(rf.peers)/2 {
			rf.beLeader(term)
			return
		}
		select {
			case v := <-rf.voteChan:
				if v == term {
					count += 1
					log.Printf("%d get vote, term %d", rf.me, term)
				}
			case <-time.After(time.Duration(rf.electionTimeOut) * time.Millisecond):
				//log.Printf("Election fail, candidate: %d, term: %d", rf.me, term)
				return
		}
	}
}
func (rf *Raft) initElection() {
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.leader = -1
}
func (rf *Raft) voteAndCount(server int, term int) {
	args := RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log),
	}
	if len(rf.log) > 0 {
		args.LastLogTerm = rf.log[len(rf.log) - 1].Term
	}
	reply := RequestVoteReply{}
	rf.sendRequestVote(server, &args, &reply)
	if reply.VoteGranted {
		rf.voteChan <- term
	}
}

func (rf *Raft) beLeader(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm == term && rf.leader == -1 {
		rf.leader = rf.me
		log.Printf("%d becomes leader, term: %d", rf.me,  rf.currentTerm)
		go rf.sendHeartbeats()
	}
}
//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}


//
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
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}


//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.leader = -1
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.electionTimeOut = ElectionTimeOut + rand.Intn(300)
	rf.heartbeatChan = make(chan int)
	rf.voteChan = make(chan int)
	go checkHeartbeat(rf)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	log.Printf("init %d", rf.me)
	return rf
}
func checkHeartbeat(rf *Raft) {
	for ; !rf.killed(); {
		select {
		case <-rf.heartbeatChan:
		case <-time.After(time.Duration(rf.electionTimeOut) * time.Millisecond):
			go rf.startElection()
		}
	}
}
