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
	"bytes"
	"fmt"
	"labgob"
	"math/rand"
	"strings"
	"sync"
	"time"
)
import "labrpc"

// import "bytes"
// import "labgob"

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

type LogEntry struct {
	Index   int
	Command interface{}
	Term    int
}

const (
	LEADER = 1 + iota
	FOLLOWER
	CANDIDATE
)

const HeartBeatDuration = 140

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm    int
	votedFor       int
	log            []*LogEntry
	state          int
	commitIndex    int
	lastApplied    int
	appendCh       chan *AppendEntriesArgs
	voteCh         chan *RequestVoteArgs
	applyCh        chan ApplyMsg
	heartBeatTimer *time.Timer
	electionTimer  *time.Timer

	nextIndex  []int
	matchIndex []int
}

// used for test
func (rf *Raft) printLog() string {
	sb := strings.Builder{}
	for _, v := range rf.log {
		s := fmt.Sprintf("index:%d,term:%d,command:%v ", v.Index, v.Term, v.Command)
		sb.WriteString(s)
	}
	sb.WriteString("\n")
	return sb.String()
}

func printEntryLog(entries []*LogEntry) string {
	sb := strings.Builder{}
	for _, v := range entries {
		s := fmt.Sprintf("index:%d,term:%d,command:%v ", v.Index, v.Term, v.Command)
		sb.WriteString(s)
	}
	return sb.String()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == LEADER
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor int
	var log []*LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
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

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PreLogTerm   int
	Entries      []*LogEntry
	LeaderCommit int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
	VoterId     int
}

type AppendEntriesReply struct {
	Term      int
	Success   bool
	LastIndex int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	var requestTerm = args.Term
	reply.VoterId = rf.me
	if requestTerm < rf.currentTerm || (requestTerm == rf.currentTerm &&
		rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		reply.Term = rf.currentTerm
		return
	}
	if requestTerm > rf.currentTerm {
		// transfer to follower
		//DPrintf("server %d origin term is %d,update to %d", rf.me, rf.currentTerm, requestTerm)
		rf.currentTerm = requestTerm
		rf.votedFor = -1
		rf.state = FOLLOWER
	}
	reply.Term = rf.currentTerm
	rf.votedFor = args.CandidateId
	lastEntry := rf.log[len(rf.log)-1]
	if args.LastLogTerm < lastEntry.Term {
		return
	}
	if args.LastLogTerm == lastEntry.Term && args.LastLogIndex < lastEntry.Index {
		return
	}
	reply.VoteGranted = true
	//DPrintf("server %d term %d vote for server %d term %d", rf.me, rf.currentTerm, args.CandidateId, args.Term)
}

func (rf *Raft) AppendEntry(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	// a heart beat
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
	}
	reply.Term = rf.currentTerm

	lastLog := rf.log[len(rf.log)-1]
	lastIdx, lastIdxTerm := lastLog.Index, lastLog.Term
	if args.PrevLogIndex > 0 {
		if args.PrevLogIndex >= len(rf.log) {
			reply.LastIndex = len(rf.log) - 1
			return
		}
		var prevLogTerm int
		if lastIdx == args.PrevLogIndex {
			prevLogTerm = lastIdxTerm
		} else {
			prevLogTerm = rf.log[args.PrevLogIndex].Term
		}
		if prevLogTerm != args.PreLogTerm {
			return
		}
	}
	if len(args.Entries) > 0 {
		var logArr []*LogEntry
		for idx, entry := range args.Entries {
			if entry.Index > lastIdx {
				logArr = args.Entries[idx:]
				break
			}
			if entry.Term != rf.log[entry.Index].Term {
				rf.log = rf.log[:entry.Index]
				logArr = args.Entries[idx:]
				break
			}
		}
		for _, v := range logArr {
			rf.log = append(rf.log, v)
		}
	}
	if args.LeaderCommit > rf.commitIndex {
		lastIdx = rf.log[len(rf.log)-1].Index
		rf.sendCommitIndexApplyMsg(min(lastIdx, args.LeaderCommit))
	}
	//if len(args.Entries) > 0 {
	//	DPrintf("server %d log:%v", rf.me, rf.printLog())
	//}
	reply.Success = true
	go func() {
		rf.appendCh <- args
	}()
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
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
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
	if rf.getState() != LEADER {
		return index, term, false
	}
	rf.mu.Lock()
	term = rf.currentTerm
	index = len(rf.log)
	entry := &LogEntry{
		Command: command,
		Term:    term,
		Index:   index,
	}
	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1
	rf.log = append(rf.log, entry)
	rf.persist()
	rf.mu.Unlock()
	rf.broadcastHeartBeat()
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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
	rf.state = FOLLOWER
	rf.log = make([]*LogEntry, 1)
	rf.log[0] = &LogEntry{}
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.appendCh = make(chan *AppendEntriesArgs)
	rf.voteCh = make(chan *RequestVoteArgs)
	rf.applyCh = applyCh
	rf.electionTimer = time.NewTimer(electionRandDuration())
	rf.heartBeatTimer = time.NewTimer(heartBeatRandDuration())

	rf.readPersist(persister.ReadRaftState())

	rf.nextIndex = make([]int, len(rf.peers))
	for i, _ := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.log)
	}
	rf.matchIndex = make([]int, len(rf.peers))
	// Your initialization code here (2A, 2B, 2C).
	//initialize from state persisted before a crash
	go rf.handler()

	return rf
}

// heart beat time out 150ms~250ms
func heartBeatRandDuration() time.Duration {
	var x = rand.Intn(100)
	return time.Millisecond * (time.Duration(x) + 150)
}

// election time out 300ms~400ms
func electionRandDuration() time.Duration {
	var x = rand.Intn(100)
	return time.Millisecond * (time.Duration(x) + 300)
}

func (rf *Raft) handler() {
	for {
		switch rf.getState() {
		case FOLLOWER:
			rf.runFollower()
		case LEADER:
			rf.runLeader()
		case CANDIDATE:
			rf.runCandidate()
		}
	}
}

func (rf *Raft) runFollower() {
	rf.heartBeatTimer.Reset(heartBeatRandDuration())
	for rf.getState() == FOLLOWER {
		select {
		// time out,start a new election
		case <-rf.heartBeatTimer.C:
			//DPrintf("server %d cannot find leader, cur term is %d,voted for server %d", rf.me, rf.currentTerm, rf.votedFor)
			rf.convertTo(CANDIDATE)
			return
		// received msg from leader
		case <-rf.appendCh:
			return
		}
	}
}

func (rf *Raft) runLeader() {
	timer := time.NewTimer(HeartBeatDuration * time.Millisecond)
	for rf.getState() == LEADER {
		select {
		// send heartbeat timely
		case <-timer.C:
			rf.broadcastHeartBeat()
			return
		case <-rf.appendCh:
		}
	}
}

func (rf *Raft) runCandidate() {
	ch := rf.startElection()
	var votesGranted = 0
	var votedsNeeded = len(rf.peers) / 2
	for rf.getState() == CANDIDATE {
		select {
		case <-rf.electionTimer.C:
			// election timeout,just return and re-candidate
			return
		case vote := <-ch:
			// vote logic to become leader or follower
			if vote.Term > rf.getCurrentTerm() {
				rf.mu.Lock()
				rf.state = FOLLOWER
				rf.votedFor = -1
				rf.currentTerm = vote.Term
				rf.persist()
				rf.mu.Unlock()
				return
			}
			if vote.VoteGranted {
				//DPrintf("server %d term %d receive server %d' term %d vote",
				//	rf.me, rf.currentTerm, vote.VoterId,  vote.Term)
				votesGranted++
			}
			if votesGranted >= votedsNeeded {
				// double check current server is candidate
				if rf.getState() == CANDIDATE {
					//DPrintf("server %d term %d become leader", rf.me, rf.currentTerm)
					rf.convertTo(LEADER)
					rf.broadcastHeartBeat()
				}
				return
			}
		case <-rf.appendCh:
		}
	}

}

func (rf *Raft) startElection() chan *RequestVoteReply {
	defer rf.mu.Unlock()
	defer rf.persist()
	rf.mu.Lock()
	rf.currentTerm = rf.currentTerm + 1
	rf.votedFor = rf.me
	rf.electionTimer.Reset(electionRandDuration())
	peerLength := len(rf.peers)
	request := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	// unblocked channel to save the vote reply
	ch := make(chan *RequestVoteReply, peerLength)
	for i, _ := range rf.peers {
		if i != rf.me {
			go func(server int, req RequestVoteArgs) {
				if rf.getState() != CANDIDATE {
					return
				}
				reply := RequestVoteReply{}
				if rf.sendRequestVote(server, &req, &reply) {
					ch <- &reply
				}
			}(i, request)
		}
	}
	return ch
}

func (rf *Raft) broadcastHeartBeat() {
	for i, _ := range rf.peers {
		if i != rf.me {
			rf.mu.Lock()
			if rf.state != LEADER {
				rf.mu.Unlock()
				return
			}
			prevLogIndex := rf.nextIndex[i] - 1
			requestArgs := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PreLogTerm:   rf.log[prevLogIndex].Term,
				Entries:      rf.log[prevLogIndex+1:],
				LeaderCommit: rf.commitIndex,
			}
			rf.mu.Unlock()
			go func(server int, args *AppendEntriesArgs) {
				reply := &AppendEntriesReply{}
				if rf.getState() != LEADER {
					return
				}
				if rf.sendAppendEntries(server, args, reply) {
					rf.mu.Lock()
					//DPrintf("leader %d send a rpc to server %d:prevIndex:%d,prevTerm:%d,leaderCommit:%d",
					//	rf.me, server, args.PrevLogIndex, args.PreLogTerm, args.LeaderCommit)
					if reply.Success {
						//DPrintf("server %d successful append log:%v", server, printEntryLog(args.Entries))
						rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
						rf.nextIndex[server] = rf.matchIndex[server] + 1
						// search all the server's match index,if half of them come to agreement,
						// update the Leader's commit index
						for i := len(rf.log) - 1; i > rf.commitIndex; i-- {
							vote := 0
							for _, v := range rf.matchIndex {
								if v >= i {
									vote++
								}
							}
							if vote > len(rf.peers)/2 {
								//DPrintf("leader %d update commit index %d", rf.me, i)
								rf.sendCommitIndexApplyMsg(i)
								break
							}
						}
					} else {
						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
							rf.state = FOLLOWER
							rf.votedFor = -1
							rf.persist()
						} else if reply.LastIndex > 0 {
							// current next index too large
							rf.nextIndex[server] = reply.LastIndex + 1
						} else {
							// prev log index term not match
							rf.nextIndex[server] = max(rf.nextIndex[server]-1, 1)
						}
					}
					rf.mu.Unlock()
				}
			}(i, requestArgs)
		}
	}
}

func (rf *Raft) getState() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state
}

func (rf *Raft) setCurrentTerm(term int) {
	rf.mu.Lock()
	rf.currentTerm = term
	rf.mu.Unlock()
}

func (rf *Raft) getCurrentTerm() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm
}

func (rf *Raft) convertTo(s int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if s == rf.state {
		return
	}
	rf.state = s
	switch s {
	case FOLLOWER:
		rf.votedFor = -1
	}
}

// update the apply msg to commit index
func (rf *Raft) sendCommitIndexApplyMsg(index int) {
	rf.commitIndex = index
	if rf.lastApplied < rf.commitIndex {
		appendEntries := append([]*LogEntry{}, rf.log[rf.lastApplied+1:rf.commitIndex+1]...)
		go func(startIdx int, entries []*LogEntry) {
			for idx, entry := range entries {
				msg := ApplyMsg{
					Command:      entry.Command,
					CommandValid: true,
					CommandIndex: startIdx + idx,
				}
				rf.applyCh <- msg
				rf.mu.Lock()
				if rf.lastApplied < msg.CommandIndex {
					rf.lastApplied = msg.CommandIndex
				}
				rf.mu.Unlock()
			}
		}(rf.lastApplied+1, appendEntries)
	}
}

func min(x int, y int) int {
	if x < y {
		return x
	} else {
		return y
	}
}

func max(x int, y int) int {
	if x < y {
		return y
	} else {
		return x
	}
}
