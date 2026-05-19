package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	//	"bytes"
	"math/rand"
	"sync"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

var ProgramStart = time.Now()

type Role string

const (
	LEADER    Role = "leader"
	FOLLOWER  Role = "follower"
	CANDIDATE Role = "candidate"
)

type LogValue struct {
	Term int
	//interface which implements zero methods, empty interface, esssentially a template
	Item interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	lastRpcContact  time.Time
	allowedDuration time.Duration
	currentTerm     int
	//which index you voted for in peers array
	votedFor    int
	currentRole Role
	//not sure what log stores yet
	log []LogValue
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (3A).
	//
	if rf.currentRole == LEADER {
		isleader = true
	}

	term = rf.currentTerm
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	//term for a lagging candidate to update itself
	Term        int
	VoteGranted bool
}

// append entries rpc struct
type AppendEntriesRequest struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogValue
	LeaderCommit int
}

//append entries rpc response struct

type AppendEntriesResponse struct {
	Term    int
	Success bool
}

// rpc handler
func (rf *Raft) SendAppendEntries(server int, args *AppendEntriesRequest, reply *AppendEntriesResponse) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// rpc handler helper
func (rf *Raft) AppendEntries(args *AppendEntriesRequest, reply *AppendEntriesResponse) {
	//now for vote requests
	rf.mu.Lock()
	defer rf.mu.Unlock()

	curLogTerm := rf.currentTerm
	DPrintf(dLog2, "S%d <- S%d AE T%d prevIdx=%d prevTerm=%d nEntries=%d", rf.me, args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries))
	//dont append entry from stale leader
	if curLogTerm > args.Term {

		DPrintf(dDrop, "S%d rejected AE from S%d: stale term", rf.me, args.LeaderId)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	//update your term to new leaders term and clear voted for
	if curLogTerm < args.Term {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	//demote your role
	if rf.currentRole == LEADER || rf.currentRole == CANDIDATE {
		DPrintf(dTerm, "S%d stepping down (AE reply T%d > my T%d)", rf.me, reply.Term, rf.currentTerm)
		rf.currentRole = FOLLOWER
	}

	//reset leader election time out
	rf.lastRpcContact = time.Now()
	durationInt := rand.Intn(800-400) + 400
	rf.allowedDuration = time.Duration(durationInt) * time.Millisecond

	//successful append entries and inform leader of followers term
	reply.Success = true
	reply.Term = rf.currentTerm

}

func (rf *Raft) AppendEntriesRoutine() {
	//send rpc every 10 milliseconds
	for {
		sleepTime := time.Duration(100)
		//only send rpc if leader
		rf.mu.Lock()
		if rf.currentRole == LEADER {
			//lock only if leader
			//right now just sending empty heartbeats

			dummyEntries := []LogValue{}
			appendEntriesReq := AppendEntriesRequest{Term: rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: -1,
				Entries:      dummyEntries,
				LeaderCommit: -1,
			}

			rf.mu.Unlock()
			for ind, _ := range rf.peers {
				//send an append entries request to each peer
				if ind == rf.me {
					continue
				}
				appendEntriesResp := AppendEntriesResponse{}

				//send rpc with args to peer
				go func(serverId int, args AppendEntriesRequest, resp AppendEntriesResponse) {
					//dont lock before rpc call
					rf.SendAppendEntries(serverId, &args, &resp)
					rf.mu.Lock()
					//only case we have to not worry about success

					if rf.currentRole != LEADER {
						rf.mu.Unlock()
						return
					}

					if resp.Term > rf.currentTerm {
						rf.currentRole = FOLLOWER
						rf.currentTerm = resp.Term
						rf.votedFor = -1
						rf.mu.Unlock()
						return
					}

					rf.mu.Unlock()
				}(ind, appendEntriesReq, appendEntriesResp)

			}
		} else {
			//unlock if not leader
			rf.mu.Unlock()
		}
		time.Sleep(sleepTime * time.Millisecond)
	}
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	curLogTerm := 0
	reply.Term = rf.currentTerm
	DPrintf(dVote, "S%d <- S%d RV req T%d lastIdx=%d lastTerm=%d", rf.me, args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm)
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		DPrintf(dDrop, "S%d denied vote to S%d: stale term (their T%d < my T%d)", rf.me, args.CandidateId, args.Term, rf.currentTerm)
		return
	} else if args.Term > rf.currentTerm {
		rf.currentRole = FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		reply.VoteGranted = false
		DPrintf(dDrop, "S%d denied vote to S%d: already voted for S%d", rf.me, args.CandidateId, rf.votedFor)
		return
	}

	if len(rf.log) > 0 {
		curLogTerm = rf.log[len(rf.log)-1].Term
	}

	//term check here
	if args.LastLogTerm < curLogTerm {
		reply.VoteGranted = false
		DPrintf(dDrop, "S%d denied vote to S%d: log not up-to-date", rf.me, args.CandidateId)
		return
	}

	//index check here
	if args.LastLogTerm == curLogTerm && args.LastLogIndex < len(rf.log)-1 {
		//length of log is wrong
		DPrintf(dDrop, "S%d denied vote to S%d: log not up-to-date", rf.me, args.CandidateId)
		reply.VoteGranted = false
		return
	}
	//now we know log is at least as up to date
	//we can grant vote here

	reply.VoteGranted = true
	DPrintf(dVote, "S%d granted vote to S%d at T%d", rf.me, args.CandidateId, rf.currentTerm)
	rf.votedFor = args.CandidateId
	rf.lastRpcContact = time.Now()
	durationInt := rand.Intn(800-400) + 400
	//reset random duration to be between 300 and 500 milliseconds, likely have to change it but forgot what values mit specified
	rf.allowedDuration = time.Duration(durationInt) * time.Millisecond
	rf.currentRole = FOLLOWER
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
	//ok so this is blocking
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

func (rf *Raft) ticker() {
	DPrintf(dInfo, "S%d ticker started", rf.me)
	for true {
		// Your code here (3A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		//lock to proect shared state
		rf.mu.Lock()
		elapsedTime := time.Since(rf.lastRpcContact)

		if rf.currentRole != LEADER && elapsedTime > rf.allowedDuration {
			//then send list of append entries rpcs to peers
			// vote for yourself
			rf.votedFor = rf.me
			rf.currentTerm += 1
			rf.currentRole = CANDIDATE
			//reset allowed duration for current term
			durationInt := rand.Intn(800-400) + 400
			rf.allowedDuration = time.Millisecond * time.Duration(durationInt)
			DPrintf(dTimer, "S%d election timeout, becoming candidate at T%d", rf.me, rf.currentTerm)
			lastTerm := 0
			if len(rf.log) > 0 {
				lastTerm = rf.log[len(rf.log)-1].Term
			}
			curVoteReq := RequestVoteArgs{Term: rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: len(rf.log) - 1,
				LastLogTerm:  lastTerm,
			}

			//send the rpcs non blocking, never hold lock before sending an rpc
			numVotes := 1
			curTerm := rf.currentTerm
			//unlock before rpc call
			rf.mu.Unlock()
			for ind, _ := range rf.peers {

				if ind == rf.me {
					continue
				}

				curVoteReply := RequestVoteReply{}
				go func(peerNum int, voteReply RequestVoteReply) {
					//do this synchronously
					rf.sendRequestVote(peerNum, &curVoteReq, &voteReply)
					rf.mu.Lock()
					if voteReply.Term > rf.currentTerm {
						DPrintf(dTerm, "S%d stepping down (RV reply T%d > my T%d)", rf.me, voteReply.Term, rf.currentTerm)
						rf.currentRole = FOLLOWER
						rf.currentTerm = voteReply.Term
						rf.votedFor = -1
						rf.mu.Unlock()
						return
					}

					//check if world has changed
					if rf.currentTerm != curTerm || rf.currentRole != CANDIDATE {
						DPrintf(dInfo, "S%d stepped down world has changed and no longer leader", rf.me)
						rf.mu.Unlock()
						return
					}

					//if everything cool then check if you got vote
					if voteReply.VoteGranted {
						DPrintf(dVote, "S%d got RV reply from S%d: granted=%v term=%d", rf.me, peerNum, voteReply.VoteGranted, voteReply.Term)
						numVotes += 1
					}

					if (numVotes) >= (len(rf.peers)/2)+1 {
						//you have majority at this point become leader
						DPrintf(dLeader, "S%d became leader at T%d with %d votes", rf.me, rf.currentTerm, numVotes)
						rf.currentRole = LEADER
						rf.mu.Unlock()
						return
					}
					//lock when exiting a function like so
					rf.mu.Unlock()
				}(ind, curVoteReply)

			}
		} else {
			rf.mu.Unlock()
		}
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
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
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.lastRpcContact = time.Now()
	durationInt := rand.Intn(800-400) + 400
	rf.allowedDuration = time.Duration(durationInt) * time.Millisecond
	rf.currentTerm = 0
	rf.currentRole = FOLLOWER
	rf.votedFor = -1
	rf.log = make([]LogValue, 0)
	DPrintf(dInfo, "S%d started at T%d", rf.me, rf.currentTerm)
	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.AppendEntriesRoutine()

	return rf
}
