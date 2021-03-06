package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server
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
	"math/rand"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const (
	Follower = iota
	Candidate
	Leader
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
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
	status      int //current status of Raft
	currentTerm int //current term
	voteFor     int

	nonLeaderCond     *sync.Cond
	lastHeartbeatTime int64
	electionTimeout   int

	electionTimeoutChan chan bool
	heartbeatPeriodChan chan bool

	commitIndex int
	lastApplied int

	heartbeatInterval int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (2A).

	term = rf.currentTerm
	isleader = rf.status == Leader
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
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
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
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []int
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

//state machine protocol.
func (rf *Raft) convertToCandidate() {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	rf.status = Candidate
	rf.currentTerm++
	rf.voteFor = rf.me
	rf.nonLeaderCond.Signal()
}

func (rf *Raft) convertToFollower(newTerm int) {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	rf.status = Follower
	rf.currentTerm = newTerm
	rf.voteFor = -1;
	rf.nonLeaderCond.Signal()
}

func (rf *Raft) convertToLeader() {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	rf.status = Leader
}

func (rf *Raft) getRandElecTime() int {
	rand.Seed(time.Now().UnixNano())
	electionTimeout := rand.Intn(150) + 150
	return electionTimeout
}

func (rf *Raft) resetElecTime() {
	rf.lastHeartbeatTime = time.Now().UnixNano()
	rf.electionTimeout = rf.getRandElecTime()
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	DPrintf("[RequestVote]: Id %d Term %d State %s  \t || \t Handle RequestVote",
		rf.me, rf.currentTerm, state2name(rf.status))

	defer func() {
		DPrintf("[RequestVote]:RaftNode[%d] Return RequestVote, CandidatesId[%d] VoteGranted[%v] ",
			rf.me, args.CandidateId, reply.VoteGranted)
	}()

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}

	if rf.voteFor == -1 || rf.voteFor == args.CandidateId {
		rf.voteFor = args.CandidateId
		reply.VoteGranted = true
	}
	//rf.persist()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("[AppendEntries]: Id %d Term %d State %s \t || \t election timeout , start an eletion\n",
		rf.me, rf.currentTerm, state2name(rf.status))

	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
// term. the third return value is true if this server believes it is
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func state2name(status int) string {
	switch status {
	case Candidate:
		return "Candidate"
	case Follower:
		return "Follower"
	case Leader:
		return "Leader"
	}
	return ""
}

func (rf *Raft) getLastLogInfo() (int, int) {
	index := -1
	term := -1
	return index, term
}

func (rf *Raft) getprevLogInfo() (int, int) {
	index := -1
	term := -1
	return index, term
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		select {
		case <-rf.electionTimeoutChan:
			rf.mu.Lock()
			DPrintf("[ticker]: Id %d Term %d State %s \t || \t election timeout , start an eletion\n",
				rf.me, rf.currentTerm, state2name(rf.status))
			rf.mu.Unlock()

			go rf.startElection()
		case <-rf.heartbeatPeriodChan:
			rf.mu.Lock()
			DPrintf("[ticker]: Id %d Term %d State %s \t || \t election timeout , start an eletion\n",
				rf.me, rf.currentTerm, state2name(rf.status))
			rf.mu.Unlock()

			go rf.broadcastHeartbeat()
		}
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.convertToCandidate()
	nVotes := 1
	rf.resetElecTime()
	DPrintf("[startElection]: Id %d Term %d state %s \n", rf.me, rf.currentTerm, state2name(rf.status))
	rf.mu.Unlock()
	go func(nVotes *int, rf *Raft) {
		winVotes := len(rf.peers)/2 + 1
		var wg sync.WaitGroup

		for i, _ := range rf.peers {
			if i == rf.me {
				continue
			}
			rf.mu.Lock()
			//lastLogIndex, LastLogTerm := rf.getLastLogInfo()
			args := RequestVoteArgs{
				Term:        rf.currentTerm,
				CandidateId: rf.me,
			}
			var reply RequestVoteReply
			rf.mu.Unlock()
			wg.Add(1)
			go func(server int, rf *Raft, args *RequestVoteArgs, reply *RequestVoteReply) {
				defer wg.Done()
				rf.mu.Lock()
				DPrintf("[startElection]: Id %d Term %d state %s \t || \t"+
					"start send request vote to %d \n", rf.me, rf.currentTerm, state2name(rf.status), server)
				rf.mu.Unlock()
				ok := rf.sendRequestVote(server, args, reply)
				if ok == false {
					return
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.VoteGranted == false {
					if reply.Term > rf.currentTerm {
						DPrintf("[startElection]: Id %d Term %d state %s \t || \t"+
							"peer term:%d \n", rf.me, rf.currentTerm, state2name(rf.status), reply.Term)
						rf.convertToFollower(reply.Term)
					}
				} else {
					*nVotes += 1
					DPrintf("[startElection]: Id %d Term %d state %s \t || \t"+
						"get election votes:%d for %d / %d \n", rf.me, rf.currentTerm, state2name(rf.status), *nVotes, server, winVotes)
					//_, isLeader := rf.GetState()
					//if isLeader {
					//	return
					//}
					if rf.status == Candidate && *nVotes >= winVotes {
						DPrintf("[startElection]: Id %d Term %d state %s \t || \t"+
							"win election votes:%d \n", rf.me, rf.currentTerm, state2name(rf.status), *nVotes)
						rf.convertToLeader()
						// send heartbeat immediately to all server
						DPrintf("start send heartbeat\n")
						go rf.broadcastHeartbeat()
					}
				}
			}(i, rf, &args, &reply)
		}
		wg.Wait()
	}(&nVotes, rf)

}

func (rf *Raft) broadcastHeartbeat() {
	for rf.killed() == false {
		//rf.mu.Lock()
		_, isLeader := rf.GetState()
		//rf.mu.Unlock()
		if !isLeader {
			return
		}

		for i, _ := range rf.peers {
			if i == rf.me {
				continue
			}

			rf.mu.Lock()
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				LeaderCommit: rf.commitIndex,
			}
			var reply AppendEntriesReply
			rf.mu.Unlock()

			go func(index int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
				ok := rf.sendAppendEntries(index, args, reply)
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if ok == false {
					return
				} else {
					if reply.Term > rf.currentTerm {
						DPrintf("[broadcastHeartbeat]: Id %d Term %d state %s \t || \t"+
							"peer term:%d \n", rf.me, rf.currentTerm, state2name(rf.status), reply.Term)
						rf.convertToFollower(reply.Term)
					} else if reply.Term == rf.currentTerm && reply.Success == false {

					} else {

					}
				}
			}(i, &args, &reply)
		}

		time.Sleep(time.Duration(rf.heartbeatInterval) * time.Millisecond)
	}
}

func (rf *Raft) electionTimeoutTick() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		//rf.mu.Lock()
		_, isLeader := rf.GetState()
		//rf.mu.Unlock()

		if isLeader {
			rf.nonLeaderCond.L.Lock()
			rf.nonLeaderCond.Wait()
			rf.nonLeaderCond.L.Unlock()
		} else {
			rf.mu.Lock()
			elapseTime := time.Now().UnixNano() - rf.lastHeartbeatTime
			if elapseTime/int64(time.Millisecond) > int64(rf.electionTimeout) {
				DPrintf("[electionTimeoutTick]: Id %d Term %d State %s\t || \ttimeout,"+
					"convert to candidate\n", rf.me, rf.currentTerm, state2name(rf.status))
				DPrintf("[electionTimeoutTick] : %d %d\n", elapseTime/int64(time.Millisecond), int64(rf.electionTimeout))
				rf.electionTimeoutChan <- true
			}

			rf.mu.Unlock()
			time.Sleep(time.Millisecond * 10)
		}
	}
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

	rf.mu = sync.Mutex{}
	// Your initialization code here (2A, 2B, 2C).
	rf.nonLeaderCond = sync.NewCond(&rf.mu)
	rf.currentTerm = 0
	rf.electionTimeoutChan = make(chan bool)
	rf.heartbeatPeriodChan = make(chan bool)
	rf.heartbeatInterval = 120
	rf.lastHeartbeatTime = time.Now().UnixNano()
	rf.electionTimeout = 300

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.electionTimeoutTick()
	DPrintf("Raftnode[%d] running", me)
	return rf
}
