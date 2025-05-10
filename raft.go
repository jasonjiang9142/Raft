// package raft

// import (
// 	"cs351/labrpc"

// 	"log"

// 	"math/rand"

// 	"sync"

// 	"sync/atomic"

// 	"time"
// )

// // ---------------------------------------------------------- Structs ----------------------------------------------------------

// type ApplyMsg struct {
// 	CommandValid bool

// 	Command interface{}

// 	CommandIndex int
// }

// type LogEntry struct {
// 	Term int // term when entry was recieved by leader

// 	Commands interface{} // command for state machine

// }

// // single raft peer

// type Raft struct {
// 	mu sync.Mutex // Lock to protect shared access to this peer's state

// 	peers []*labrpc.ClientEnd // RPC end points of all peers

// 	me int // This peer's index into peers[], current id for the node

// 	dead int32 // Set by Kill()

// 	// Persistent state

// 	currentTerm int // latest term server seen, start at 0

// 	votedFor int // candidateID that recieve vote in current term

// 	log []LogEntry // log entries containg state machine commands

// 	// Volatile state on all server

// 	commitIndex int // index of highest commited log entries, starts at 0

// 	lastApplied int // index of highest log entries applied to machine, starts at 0

// 	// Volatile state on leaders

// 	nextIndex []int // next log entry index to send to each follower

// 	matchIndex []int // highest log entry index known to replicated

// 	// new input for leaders

// 	state string // between follower, leader, candidate

// 	timeout time.Time // time when the node timeouts

// 	lastHeartbeatTime time.Time // when the leader sent last Heartbeat (only Leader has)

// 	leaderLoopRunning bool
// }

// type RequestVoteArgs struct {

// 	// Your data here (3A, 3B).

// 	Term int // Candidate's term

// 	CandidateID int // Candidate that is Requesting Vote

// 	LastLogIndex int // Candidate's latest log entry index

// 	LastLogTerm int // Candidate's latest log term

// }

// type RequestVoteReply struct {
// 	Term int // Current term of the peer responding to the request

// 	VoteGranted bool // Whether the follower will vote for candidate

// }

// // Append Entry RPC arguments structure.

// type AppendEntriesArgs struct {
// 	Term int // Leader's term

// 	LeaderID int // Leader that is Requesting Vote

// 	PrevLogIndex int // Leader's's latest log entry index

// 	PrevLogTerm int // Leader's's latest log term

// 	Entries []LogEntry // Log entries to store (empty for heartbeat; may send more than one for efficiency)

// 	LeaderCommit int // Leader's commit index

// }

// // Append Entry RPC reply structure.

// type AppendEntriesReply struct {
// 	Term int // Current term of the peer responding to the request

// 	Success bool // True if follower contained entry matchine prevLogIndex and prevLogTerm

// 	ConflictIndex int

// 	ConflictTerm int
// }

// // ---------------------------------------------------------- Helper Functions ----------------------------------------------------------

// func (rf *Raft) resetElectionTimeout() {

// 	// random_time := time.Duration(400+rand.Intn(200)) * time.Millisecond

// 	random_time := time.Duration(500+rand.Intn(500)) * time.Millisecond

// 	rf.timeout = time.Now().Add(random_time)

// 	// log.Printf("[Node %d] [resetElectionTimeout] Reset election timeout to %v", rf.me, rf.timeout)

// }

// // Return currentTerm and whether this server believes it is the leader.

// func (rf *Raft) GetState() (int, bool) {

// 	var isleader bool

// 	rf.mu.Lock()

// 	defer rf.mu.Unlock()

// 	term := rf.currentTerm

// 	if rf.state == "Leader" {

// 		isleader = true

// 	} else {

// 		isleader = false

// 	}

// 	return term, isleader

// }

// func init() {

// 	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

// }

// func min(a, b int) int {

// 	if a < b {

// 		return a

// 	}

// 	return b

// }

// func max(a, b int) int {

// 	if a > b {

// 		return a

// 	}

// 	return b

// }

// // ---------------------------------------------------------- Election Functions ----------------------------------------------------------

// // Example RequestVote RPC handler.

// func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

// 	rf.mu.Lock()

// 	defer rf.mu.Unlock()

// 	reply.Term = rf.currentTerm

// 	reply.VoteGranted = false

// 	// intialize candidate info

// 	candidateTerm := args.Term

// 	candidateID := args.CandidateID

// 	candidateLastLogIndex := args.LastLogIndex

// 	candidateLastLogTerm := args.LastLogTerm

// 	if args.Term < rf.currentTerm {

// 		return

// 	}

// 	// raft rule: peer steps down if it learns of higher term

// 	if candidateTerm > rf.currentTerm {

// 		rf.currentTerm = candidateTerm

// 		rf.votedFor = -1

// 		rf.state = "Follower"

// 	}

// 	// set current term for follower

// 	currentTerm := 0

// 	lastLogIndex := len(rf.log) - 1

// 	if lastLogIndex >= 0 {

// 		lastLog := rf.log[lastLogIndex]

// 		currentTerm = lastLog.Term

// 	}

// 	if (rf.votedFor == -1 || rf.votedFor == candidateID) &&

// 		((candidateLastLogTerm > currentTerm) || (candidateLastLogTerm == currentTerm && candidateLastLogIndex >= lastLogIndex)) {

// 		reply.VoteGranted = true

// 		reply.Term = rf.currentTerm

// 		rf.votedFor = candidateID

// 		rf.resetElectionTimeout()

// 	}

// }

// // Example code to send a RequestVote RPC to a server.

// func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {

// 	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

// 	return ok

// }

// // ---------------------------------------------------------- Log Functions ----------------------------------------------------------

// // check to see if we could update the commit index for the leader

// func (rf *Raft) updateCommitIndex() {

// 	if rf.state != "Leader" {

// 		return

// 	}

// 	// iterate through all uncommited index of the leader

// 	for n := rf.commitIndex + 1; n < len(rf.log); n++ {

// 		votes := 1 // votes for itself

// 		for i := range rf.peers {

// 			if i != rf.me {

// 				// if the log is replicated up to leader's index and the terms for peer and leader are the same

// 				if rf.matchIndex[i] >= n && rf.log[n].Term == rf.currentTerm {

// 					votes++

// 				}

// 			}

// 		}

// 		majority := len(rf.peers)/2 + 1

// 		if votes >= majority {

// 			rf.commitIndex = n

// 			log.Printf("[Node %d] [updateCommitIndex] Updated commitIndex to %d: reached majority with %d votes", rf.me, rf.commitIndex, votes)

// 		}

// 	}

// }

// // The service using Raft (e.g. a k/v server) wants to start agreement on the next command to be appended to Raft's log.

// // If this server isn't the leader, returns false. 			(done)

// // Otherwise start the agreement and return immediately.	(done)

// // There is no guarantee that this command will ever be committed to the Raft log, since the leader may fail or lose an election.

// // Even if the Raft instance has been killed, this function should return gracefully.

// // The first return value is the index that the command will appear at if it's ever committed.

// // The second return value is the current term.

// // The third return value is true if this server believes it is the leader.

// // Start agreement on a new log entry

// func (rf *Raft) Start(command interface{}) (int, int, bool) {

// 	rf.mu.Lock()

// 	if rf.state != "Leader" {

// 		term := rf.currentTerm

// 		rf.mu.Unlock()

// 		return -1, term, false

// 	}

// 	term := rf.currentTerm

// 	isLeader := true

// 	index := len(rf.log)

// 	new_entry := LogEntry{Term: term, Commands: command}

// 	log.Printf("%+v", new_entry)

// 	rf.log = append(rf.log, new_entry)

// 	// Update leader's nextIndex for itself

// 	rf.nextIndex[rf.me] = index + 1

// 	rf.matchIndex[rf.me] = index

// 	rf.mu.Unlock()

// 	log.Printf("[Node %d] [Start] Leader %d: Appended command at index %d, term %d", rf.me, rf.me, index, term)
// 	log.Printf("[Node %d] [Start] New Log for Leader %d: length: %d, Values: %v", rf.me, rf.me, len(rf.log)-1, rf.log)

// 	// for i := range rf.peers {

// 	// 	if i != rf.me {

// 	// 		go func(server int) {

// 	// 			for {

// 	// 				rf.mu.Lock()

// 	// 				if rf.state != "Leader" || rf.currentTerm != term {

// 	// 					rf.mu.Unlock()

// 	// 					return

// 	// 				}

// 	// 				//log.Printf("[Start] Server is %d", server)

// 	// 				// find out what the previous log term is to see what to send operation

// 	// 				nextIndex := rf.nextIndex[server]

// 	// 				prevLogIndex := nextIndex - 1

// 	// 				var prevLogTerm int

// 	// 				if prevLogIndex >= 0 && prevLogIndex < len(rf.log) {
// 	// 					prevLogTerm = rf.log[prevLogIndex].Term

// 	// 				} else {
// 	// 					prevLogTerm = -1

// 	// 				}

// 	// 				entries := make([]LogEntry, len(rf.log[nextIndex:]))

// 	// 				copy(entries, rf.log[nextIndex:])

// 	// 				args := AppendEntriesArgs{
// 	// 					Term:         term,
// 	// 					LeaderID:     rf.me,
// 	// 					PrevLogIndex: prevLogIndex,
// 	// 					PrevLogTerm:  prevLogTerm,
// 	// 					Entries:      entries,
// 	// 					LeaderCommit: rf.commitIndex,
// 	// 				}

// 	// 				rf.mu.Unlock()

// 	// 				reply := AppendEntriesReply{}

// 	// 				ok := rf.sendAppendEntries(server, &args, &reply)

// 	// 				if !ok {
// 	// 					time.Sleep(50 * time.Millisecond)
// 	// 					continue

// 	// 				}

// 	// 				rf.mu.Lock()

// 	// 				// raft rule: peer steps down if it learns of higher term

// 	// 				if reply.Term > rf.currentTerm {
// 	// 					rf.currentTerm = reply.Term
// 	// 					rf.votedFor = -1
// 	// 					rf.state = "Follower"
// 	// 					rf.log = rf.log[:rf.commitIndex+1]

// 	// 					log.Printf("[Node %d] [Start] Stepping down: higher term %d detected", rf.me, reply.Term)
// 	// 					rf.mu.Unlock()
// 	// 					return

// 	// 				}

// 	// 				if rf.state != "Leader" || rf.currentTerm != term {

// 	// 					// stop retrying if stepped down

// 	// 					rf.mu.Unlock()

// 	// 					return

// 	// 				}

// 	// 				if reply.Success {

// 	// 					// Update nextIndex and matchIndex on success

// 	// 					rf.nextIndex[server] = nextIndex + len(entries)
// 	// 					rf.matchIndex[server] = rf.nextIndex[server] - 1
// 	// 					log.Printf("[Node %d] [Start] Successfully replicated to Node %d at index %d", rf.me, server, reply.Term)
// 	// 					rf.updateCommitIndex()
// 	// 					log.Printf("[Node %d] [Start] Successfully (no)/updated Commit Index for Node %d to %d", rf.me, server, rf.commitIndex)
// 	// 					rf.mu.Unlock()

// 	// 					return

// 	// 				} else {
// 	// 					log.Printf("[Node %d] [Start] AppendEntry is not successful at conflict index %d for node %d", rf.me, reply.ConflictIndex, server)

// 	// 					if reply.ConflictTerm != 0 {
// 	// 						idx := rf.nextIndex[server] - 1
// 	// 						// Walk backwards until we find an entry with a different term.
// 	// 						for idx >= 0 && rf.log[idx].Term != reply.ConflictTerm {
// 	// 							idx--
// 	// 						}

// 	// 						rf.nextIndex[server] = idx + 1

// 	// 					} else if rf.nextIndex[server] > 1 {
// 	// 						// Fallback: decrement one index at a time.
// 	// 						rf.nextIndex[server] = max(1, rf.nextIndex[server]-1)

// 	// 					}

// 	// 					log.Printf("[Node %d] [Start] Changed Conflict index for Node %d to %d", rf.me, server, rf.nextIndex[server])

// 	// 					rf.mu.Unlock()

// 	// 					time.Sleep(50 * time.Millisecond)

// 	// 				}

// 	// 			}

// 	// 		}(i)

// 	// 	}

// 	// }

// 	return index, term, isLeader

// }

// // Your code can use killed() to check whether Kill() has been called. The use of atomic avoids the need for a lock.

// func (rf *Raft) Kill() {

// 	atomic.StoreInt32(&rf.dead, 1)

// }

// func (rf *Raft) killed() bool {

// 	z := atomic.LoadInt32(&rf.dead)

// 	return z == 1

// }

// // ---------------------------------------------------------- Append Entry Functions ----------------------------------------------------------

// func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

// 	// lock the process

// 	rf.mu.Lock()

// 	defer rf.mu.Unlock()

// 	// get the values of the rf

// 	reply.Term = rf.currentTerm

// 	reply.Success = false

// 	// return false if leader term is less than current term of the follower

// 	if args.Term < rf.currentTerm {

// 		return

// 	}

// 	// reset the election timeout since we heard from the leader

// 	rf.resetElectionTimeout()

// 	rf.state = "Follower"

// 	// raft rule: peer steps down if it learns of higher term

// 	if args.Term > rf.currentTerm {

// 		rf.currentTerm = args.Term

// 		rf.votedFor = -1

// 	}

// 	// check log consistency

// 	if args.PrevLogIndex >= len(rf.log) {
// 		reply.ConflictIndex = len(rf.log)
// 		return

// 	}

// 	if args.PrevLogIndex >= 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {

// 		conflictTerm := rf.log[args.PrevLogIndex].Term
// 		reply.ConflictTerm = conflictTerm

// 		// find the first index of conflictTerm

// 		i := args.PrevLogIndex
// 		for i > 0 && rf.log[i-1].Term == conflictTerm {
// 			i--
// 		}

// 		reply.ConflictIndex = i

// 		return

// 	}

// 	log.Printf("[Node %d] [Start] New Log for Node %d: length: %d, Values: %v", rf.me, rf.me, len(rf.log)-1, rf.log)

// 	//Append new entries

// 	logIndex := args.PrevLogIndex + 1

// 	for i, entry := range args.Entries {

// 		if logIndex+i >= len(rf.log) {
// 			rf.log = append(rf.log, args.Entries[i:]...)
// 			break

// 		} else if rf.log[logIndex+i].Term != entry.Term {
// 			rf.log = rf.log[:logIndex+i]
// 			rf.log = append(rf.log, args.Entries[i:]...)
// 			break

// 		}

// 	}

// 	if len(args.Entries) > 0 {
// 		log.Printf("[Node %d] [AppendEntries] Appended new entries to log: %v", rf.me, args.Entries)
// 		log.Printf("[Node %d] [AppendEntries] New Log for Node %d: length: %d, Values: %v", rf.me, rf.me, len(rf.log)-1, rf.log)
// 	}

// 	// Rule5: If args.LeaderCommit > commitIndex, set commitIndex = min(args.LeaderCommit, index of last new entry)

// 	if args.LeaderCommit > rf.commitIndex {
// 		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
// 	}

// 	reply.Success = true

// }

// func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {

// 	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

// 	return ok

// }

// // ---------------------------------------------------------- HeartBeat Functions ----------------------------------------------------------

// func (rf *Raft) ticker() {

// 	// log.Printf("[Node %d][Ticker] Ticker started", rf.me)

// 	for !rf.killed() {

// 		rf.mu.Lock()

// 		current_state := rf.state

// 		timeout := rf.timeout

// 		rf.mu.Unlock()

// 		current_time := time.Now()

// 		if current_state != "Leader" && current_time.After(timeout) {

// 			rf.startElection()

// 			rf.mu.Lock()

// 			rf.resetElectionTimeout()

// 			rf.mu.Unlock()

// 		} else if current_state == "Leader" {

// 			rf.mu.Lock()

// 			if !rf.leaderLoopRunning {

// 				go rf.leaderLoop()

// 			}

// 			rf.mu.Unlock()

// 		}

// 	}

// }

// func (rf *Raft) leaderLoop() {

// 	rf.mu.Lock()

// 	if rf.leaderLoopRunning {

// 		rf.mu.Unlock()

// 		return

// 	}

// 	rf.leaderLoopRunning = true

// 	rf.mu.Unlock()

// 	defer func() {

// 		rf.mu.Lock()

// 		rf.leaderLoopRunning = false

// 		rf.mu.Unlock()

// 	}()

// 	for {

// 		rf.mu.Lock()

// 		if rf.state != "Leader" || rf.killed() {

// 			rf.mu.Unlock()

// 			break

// 		}

// 		rf.sendHeartbeats()

// 		sleepDuration := 100 * time.Millisecond

// 		rf.mu.Unlock()

// 		time.Sleep(sleepDuration)

// 	}

// }

// func (rf *Raft) sendHeartbeats() {

// 	if rf.state != "Leader" {
// 		return

// 	}

// 	rf.lastHeartbeatTime = time.Now()

// 	// set current term for follower

// 	currentTerm := rf.currentTerm

// 	// iterate through all of its peers

// 	for i := range rf.peers {

// 		if i != rf.me {

// 			prevLogIndex := rf.nextIndex[i] - 1

// 			prevLogTerm := 0

// 			if prevLogIndex >= 0 {

// 				prevLogTerm = rf.log[prevLogIndex].Term

// 			}

// 			args34 := &AppendEntriesArgs{

// 				Term: currentTerm,

// 				LeaderID: rf.me,

// 				PrevLogIndex: prevLogIndex,

// 				PrevLogTerm: prevLogTerm,

// 				Entries: []LogEntry{},

// 				LeaderCommit: rf.commitIndex,
// 			}

// 			go func(server int, args *AppendEntriesArgs) {

// 				reply := AppendEntriesReply{}

// 				ok := rf.sendAppendEntries(server, args, &reply)

// 				if !ok {

// 					return

// 				}

// 				rf.mu.Lock()

// 				defer rf.mu.Unlock()

// 				// raft rule: peer steps down if it learns of higher term

// 				if reply.Term > rf.currentTerm {

// 					rf.currentTerm = reply.Term

// 					rf.votedFor = -1

// 					rf.state = "Follower"

// 					rf.log = rf.log[:rf.commitIndex+1]

// 					// log.Printf("[Node %d] [sendHeartbeats] Stepping down: higher term %d detected", rf.me, reply.Term)

// 				} else if reply.Success {

// 					// Update nextIndex and matchIndex on success

// 					rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)

// 					rf.nextIndex[server] = rf.matchIndex[server] + 1

// 					// log.Printf("[Node %d] [sendHeartbeats] Successfully replicated to Node %d at index %d", rf.me, server, reply.Term)

// 					rf.updateCommitIndex()

// 					// log.Printf("[Node %d] [sendHeartbeats] Successfully updated Commit Index to %d", rf.me, rf.commitIndex)

// 				} else {

// 					// Log Mismatch

// 					rf.nextIndex[server] = max(1, rf.nextIndex[server]-1)

// 					// log.Printf("[Node %d] [sendHeartbeats] Replication failed to Node %d, adjusting nextIndex to %d", rf.me, server, rf.nextIndex[server])

// 				}

// 			}(i, args34)

// 		}

// 	}

// }

// // ---------------------------------------------------------- Election Functions ----------------------------------------------------------

// func (rf *Raft) startElection() {

// 	rf.mu.Lock()

// 	if rf.state != "Follower" && rf.state != "Candidate" {

// 		// log.Printf("[Node %d] [startElection] Not starting election - current state: %s", rf.me, rf.state)

// 		rf.mu.Unlock()

// 		return

// 	}

// 	rf.state = "Candidate"

// 	rf.currentTerm += 1

// 	rf.votedFor = rf.me

// 	current_term := rf.currentTerm

// 	// log.Printf("[Node %d] [startElection] Starting election for Term %d", rf.me, rf.currentTerm)

// 	LastLogTerm := 0

// 	lastLogIndex := len(rf.log) - 1

// 	if lastLogIndex >= 0 {

// 		lastLog := rf.log[lastLogIndex]

// 		LastLogTerm = lastLog.Term

// 	}

// 	// create the arguments an

// 	args := &RequestVoteArgs{

// 		Term: current_term,

// 		CandidateID: rf.me,

// 		LastLogIndex: lastLogIndex,

// 		LastLogTerm: LastLogTerm,
// 	}

// 	channel := make(chan bool, len(rf.peers))

// 	// send requst vote request to all peers except this one

// 	for i := 0; i < len(rf.peers); i++ {

// 		if i != rf.me {

// 			go func(i int) {

// 				reply := &RequestVoteReply{}

// 				ok := rf.sendRequestVote(i, args, reply)

// 				if ok && reply.Term == current_term && reply.VoteGranted {

// 					channel <- true

// 				} else {

// 					channel <- false

// 				}

// 			}(i)

// 		}

// 	}

// 	rf.mu.Unlock()

// 	votes_needed := len(rf.peers)/2 + 1

// 	votes := 1

// 	// log.Printf("[Node %d] [startElection] Waiting for votes... (Need %d)", rf.me, votes_needed)

// 	for i := 0; i < len(rf.peers)-1; i += 1 {

// 		grant_vote := <-channel

// 		if grant_vote {

// 			votes++

// 			// log.Printf("[Node %d] [startElection] Received vote. Current total: %d", rf.me, votes)

// 			if votes >= votes_needed {

// 				// log.Printf("[Node %d] [startElection] Received majority Votes as %s, current term: %d", rf.me, rf.state, rf.currentTerm)

// 				rf.mu.Lock()

// 				if rf.state == "Candidate" && rf.currentTerm == current_term {

// 					rf.state = "Leader"

// 					rf.lastHeartbeatTime = time.Now()

// 					log.Printf("[Node %d] [startElection] Won election for Term %d! Becoming Leader.", rf.me, current_term)

// 					// Initialize leader state

// 					for i := range rf.nextIndex {

// 						rf.nextIndex[i] = len(rf.log)

// 						rf.matchIndex[i] = 0

// 					}

// 					// send inital heartbeat once candidate becomes leader

// 					go rf.leaderLoop()

// 				}

// 				rf.mu.Unlock()

// 				break

// 			}

// 		}

// 	}

// }

// // The service or tester wants to create a Raft server.

// // The ports of all the Raft servers (including this one) are in peers[].

// // This server's port is peers[me].

// // All the servers' peers[] arrays have the same order.

// // applyCh is a channel on which the tester or service expects Raft to send ApplyMsg messages.

// // Make() must return quickly, so it should  goroutines for any long-running work.

// // ApplyMsg

// //   Each time a new entry is committed to the log, each Raft peer

// //   should send an ApplyMsg to the service (or tester) in the same server.

// // Create a new Raft server.

// func Make(peers []*labrpc.ClientEnd, me int, applyCh chan ApplyMsg) *Raft {

// 	rf := &Raft{}

// 	rf.peers = peers

// 	rf.me = me

// 	rf.mu = sync.Mutex{}

// 	rf.dead = 0

// 	rf.currentTerm = 0

// 	rf.votedFor = -1

// 	rf.log = []LogEntry{{Term: 0, Commands: nil}}

// 	rf.commitIndex = 0

// 	rf.lastApplied = 0

// 	rf.nextIndex = make([]int, len(peers))

// 	rf.matchIndex = make([]int, len(peers))

// 	rf.state = "Follower"

// 	rf.resetElectionTimeout()

// 	//  ticker goroutine to  elections.

// 	go func() {

// 		rf.ticker()

// 	}()

// 	go func() {

// 		for !rf.killed() {

// 			rf.mu.Lock()

// 			for rf.lastApplied < rf.commitIndex {

// 				rf.lastApplied += 1

// 				log_info := rf.log[rf.lastApplied]

// 				msg := ApplyMsg{

// 					CommandValid: true,

// 					Command: log_info.Commands,

// 					CommandIndex: rf.lastApplied,
// 				}

// 				rf.mu.Unlock()

// 				applyCh <- msg

// 				rf.mu.Lock()

// 			}

// 			rf.mu.Unlock()

// 			time.Sleep(10 * time.Millisecond)

// 		}

// 	}()

// 	return rf

// }

// ----------------------------------------------------------- End of File ----------------------------------------------------------

package raft

import (
	"cs351/labrpc"

	"log"

	"math/rand"

	"sync"

	"sync/atomic"

	"time"
)

// ---------------------------------------------------------- Structs ----------------------------------------------------------

type ApplyMsg struct {
	CommandValid bool

	Command interface{}

	CommandIndex int
}

type LogEntry struct {
	Term int // term when entry was recieved by leader

	Commands interface{} // command for state machine

}

// single raft peer

type Raft struct {
	mu sync.Mutex // Lock to protect shared access to this peer's state

	peers []*labrpc.ClientEnd // RPC end points of all peers

	me int // This peer's index into peers[], current id for the node

	dead int32 // Set by Kill()

	// Persistent state

	currentTerm int // latest term server seen, start at 0

	votedFor int // candidateID that recieve vote in current term

	log []LogEntry // log entries containg state machine commands

	// Volatile state on all server

	commitIndex int // index of highest commited log entries, starts at 0

	lastApplied int // index of highest log entries applied to machine, starts at 0

	// Volatile state on leaders

	nextIndex []int // next log entry index to send to each follower

	matchIndex []int // highest log entry index known to replicated

	// new input for leaders

	state string // between follower, leader, candidate

	timeout time.Time // time when the node timeouts

	lastHeartbeatTime time.Time // when the leader sent last Heartbeat (only Leader has)

	leaderLoopRunning bool
}

type RequestVoteArgs struct {

	// Your data here (3A, 3B).

	Term int // Candidate's term

	CandidateID int // Candidate that is Requesting Vote

	LastLogIndex int // Candidate's latest log entry index

	LastLogTerm int // Candidate's latest log term

}

type RequestVoteReply struct {
	Term int // Current term of the peer responding to the request

	VoteGranted bool // Whether the follower will vote for candidate

}

// Append Entry RPC arguments structure.

type AppendEntriesArgs struct {
	Term int // Leader's term

	LeaderID int // Leader that is Requesting Vote

	PrevLogIndex int // Leader's's latest log entry index

	PrevLogTerm int // Leader's's latest log term

	Entries []LogEntry // Log entries to store (empty for heartbeat; may send more than one for efficiency)

	LeaderCommit int // Leader's commit index

}

// Append Entry RPC reply structure.

type AppendEntriesReply struct {
	Term int // Current term of the peer responding to the request

	Success bool // True if follower contained entry matchine prevLogIndex and prevLogTerm

	ConflictIndex int

	ConflictTerm int
}

// ---------------------------------------------------------- Helper Functions ----------------------------------------------------------

func (rf *Raft) resetElectionTimeout() {

	// random_time := time.Duration(400+rand.Intn(200)) * time.Millisecond

	random_time := time.Duration(500+rand.Intn(500)) * time.Millisecond

	rf.timeout = time.Now().Add(random_time)

	// log.Printf("[Node %d] [resetElectionTimeout] Reset election timeout to %v", rf.me, rf.timeout)

}

// Return currentTerm and whether this server believes it is the leader.

func (rf *Raft) GetState() (int, bool) {

	var isleader bool

	rf.mu.Lock()

	defer rf.mu.Unlock()

	term := rf.currentTerm

	if rf.state == "Leader" {

		isleader = true

	} else {

		isleader = false

	}

	return term, isleader

}

func init() {

	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

}

func min(a, b int) int {

	if a < b {

		return a

	}

	return b

}

func max(a, b int) int {

	if a > b {

		return a

	}

	return b

}

// ---------------------------------------------------------- Election Functions ----------------------------------------------------------

// Example RequestVote RPC handler.

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	rf.mu.Lock()

	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	reply.VoteGranted = false

	// intialize candidate info

	candidateTerm := args.Term

	candidateID := args.CandidateID

	candidateLastLogIndex := args.LastLogIndex

	candidateLastLogTerm := args.LastLogTerm

	if args.Term < rf.currentTerm {

		return

	}

	// raft rule: peer steps down if it learns of higher term

	if candidateTerm > rf.currentTerm {

		rf.currentTerm = candidateTerm

		rf.votedFor = -1

		rf.state = "Follower"

	}

	// set current term for follower

	currentTerm := 0

	lastLogIndex := len(rf.log) - 1

	if lastLogIndex >= 0 {

		lastLog := rf.log[lastLogIndex]

		currentTerm = lastLog.Term

	}

	if (rf.votedFor == -1 || rf.votedFor == candidateID) &&

		((candidateLastLogTerm > currentTerm) || (candidateLastLogTerm == currentTerm && candidateLastLogIndex >= lastLogIndex)) {

		reply.VoteGranted = true

		reply.Term = rf.currentTerm

		rf.votedFor = candidateID

		rf.resetElectionTimeout()

	}

}

// Example code to send a RequestVote RPC to a server.

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	return ok

}

// ---------------------------------------------------------- Log Functions ----------------------------------------------------------

// check to see if we could update the commit index for the leader

func (rf *Raft) updateCommitIndex() {

	if rf.state != "Leader" {

		return

	}

	// iterate through all uncommited index of the leader

	for n := rf.commitIndex + 1; n < len(rf.log); n++ {

		votes := 1 // votes for itself

		for i := range rf.peers {

			if i != rf.me {

				// if the log is replicated up to leader's index and the terms for peer and leader are the same

				if rf.matchIndex[i] >= n && rf.log[n].Term == rf.currentTerm {

					votes++

				}

			}

		}

		majority := len(rf.peers)/2 + 1

		if votes >= majority {

			rf.commitIndex = n

			log.Printf("[Node %d] [updateCommitIndex] Updated commitIndex to %d: reached majority with %d votes", rf.me, rf.commitIndex, votes)

		}

	}

}

// The service using Raft (e.g. a k/v server) wants to start agreement on the next command to be appended to Raft's log.

// If this server isn't the leader, returns false. 			(done)

// Otherwise start the agreement and return immediately.	(done)

// There is no guarantee that this command will ever be committed to the Raft log, since the leader may fail or lose an election.

// Even if the Raft instance has been killed, this function should return gracefully.

// The first return value is the index that the command will appear at if it's ever committed.

// The second return value is the current term.

// The third return value is true if this server believes it is the leader.

// Start agreement on a new log entry

func (rf *Raft) Start(command interface{}) (int, int, bool) {

	rf.mu.Lock()

	if rf.state != "Leader" {

		term := rf.currentTerm

		rf.mu.Unlock()

		return -1, term, false

	}

	term := rf.currentTerm

	isLeader := true

	index := len(rf.log)

	new_entry := LogEntry{Term: term, Commands: command}

	log.Printf("%+v", new_entry)

	rf.log = append(rf.log, new_entry)

	// Update leader's nextIndex for itself

	rf.nextIndex[rf.me] = index + 1

	rf.matchIndex[rf.me] = index

	rf.mu.Unlock()

	log.Printf("[Node %d] [Start] Leader %d: Appended command at index %d, term %d", rf.me, rf.me, index, term)
	log.Printf("[Node %d] [Start] New Log for Leader %d: length: %d, Values: %v", rf.me, rf.me, len(rf.log)-1, rf.log)

	for i := range rf.peers {

		if i != rf.me {

			go func(server int) {

				for {

					rf.mu.Lock()

					if rf.state != "Leader" || rf.currentTerm != term {

						rf.mu.Unlock()

						return

					}

					//log.Printf("[Start] Server is %d", server)

					// find out what the previous log term is to see what to send operation

					nextIndex := rf.nextIndex[server]

					prevLogIndex := nextIndex - 1

					var prevLogTerm int

					if prevLogIndex >= 0 && prevLogIndex < len(rf.log) {
						prevLogTerm = rf.log[prevLogIndex].Term

					} else {
						prevLogTerm = -1

					}

					entries := make([]LogEntry, len(rf.log[nextIndex:]))

					copy(entries, rf.log[nextIndex:])

					args := AppendEntriesArgs{
						Term:         term,
						LeaderID:     rf.me,
						PrevLogIndex: prevLogIndex,
						PrevLogTerm:  prevLogTerm,
						Entries:      entries,
						LeaderCommit: rf.commitIndex,
					}

					rf.mu.Unlock()

					reply := AppendEntriesReply{}

					ok := rf.sendAppendEntries(server, &args, &reply)

					if !ok {
						time.Sleep(50 * time.Millisecond)
						continue

					}

					rf.mu.Lock()

					// raft rule: peer steps down if it learns of higher term

					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.state = "Follower"
						rf.log = rf.log[:rf.commitIndex+1]

						log.Printf("[Node %d] [Start] Stepping down: higher term %d detected", rf.me, reply.Term)
						rf.mu.Unlock()
						return

					}

					if rf.state != "Leader" || rf.currentTerm != term {

						// stop retrying if stepped down

						rf.mu.Unlock()

						return

					}

					if reply.Success {

						// Update nextIndex and matchIndex on success

						rf.nextIndex[server] = nextIndex + len(entries)
						rf.matchIndex[server] = rf.nextIndex[server] - 1
						log.Printf("[Node %d] [Start] Successfully replicated to Node %d at index %d", rf.me, server, reply.Term)
						rf.updateCommitIndex()
						log.Printf("[Node %d] [Start] Successfully (no)/updated Commit Index for Node %d to %d", rf.me, server, rf.commitIndex)
						rf.mu.Unlock()

						return

					} else {
						log.Printf("[Node %d] [Start] AppendEntry is not successful at conflict index %d for node %d", rf.me, reply.ConflictIndex, server)

						if reply.ConflictTerm != 0 {
							idx := rf.nextIndex[server] - 1
							// Walk backwards until we find an entry with a different term.
							for idx >= 0 && rf.log[idx].Term != reply.ConflictTerm {
								idx--
							}

							rf.nextIndex[server] = idx + 1

						} else if rf.nextIndex[server] > 1 {
							// Fallback: decrement one index at a time.
							rf.nextIndex[server] = max(1, rf.nextIndex[server]-1)

						}

						log.Printf("[Node %d] [Start] Changed Conflict index for Node %d to %d", rf.me, server, rf.nextIndex[server])

						rf.mu.Unlock()

						time.Sleep(50 * time.Millisecond)

					}

				}

			}(i)

		}

	}

	return index, term, isLeader

}

// Your code can use killed() to check whether Kill() has been called. The use of atomic avoids the need for a lock.

func (rf *Raft) Kill() {

	atomic.StoreInt32(&rf.dead, 1)

}

func (rf *Raft) killed() bool {

	z := atomic.LoadInt32(&rf.dead)

	return z == 1

}

// ---------------------------------------------------------- Append Entry Functions ----------------------------------------------------------

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	// lock the process

	rf.mu.Lock()

	defer rf.mu.Unlock()

	// get the values of the rf

	reply.Term = rf.currentTerm

	reply.Success = false

	// return false if leader term is less than current term of the follower

	if args.Term < rf.currentTerm {

		return

	}

	// reset the election timeout since we heard from the leader

	rf.resetElectionTimeout()

	rf.state = "Follower"

	// raft rule: peer steps down if it learns of higher term

	if args.Term > rf.currentTerm {

		rf.currentTerm = args.Term

		rf.votedFor = -1

	}

	// check log consistency

	if args.PrevLogIndex >= len(rf.log) {
		reply.ConflictIndex = len(rf.log)
		return

	}

	if args.PrevLogIndex >= 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {

		conflictTerm := rf.log[args.PrevLogIndex].Term
		reply.ConflictTerm = conflictTerm

		// find the first index of conflictTerm

		i := args.PrevLogIndex
		for i > 0 && rf.log[i-1].Term == conflictTerm {
			i--
		}

		reply.ConflictIndex = i

		return

	}

	log.Printf("[Node %d] [Start] New Log for Node %d: length: %d, Values: %v", rf.me, rf.me, len(rf.log)-1, rf.log)

	//Append new entries

	logIndex := args.PrevLogIndex + 1

	for i, entry := range args.Entries {

		if logIndex+i >= len(rf.log) {
			rf.log = append(rf.log, args.Entries[i:]...)
			break

		} else if rf.log[logIndex+i].Term != entry.Term {
			rf.log = rf.log[:logIndex+i]
			rf.log = append(rf.log, args.Entries[i:]...)
			break

		}

	}

	if len(args.Entries) > 0 {
		log.Printf("[Node %d] [AppendEntries] Appended new entries to log: %v", rf.me, args.Entries)
		log.Printf("[Node %d] [AppendEntries] New Log for Node %d: length: %d, Values: %v", rf.me, rf.me, len(rf.log)-1, rf.log)
	}

	// Rule5: If args.LeaderCommit > commitIndex, set commitIndex = min(args.LeaderCommit, index of last new entry)

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
	}

	reply.Success = true

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	return ok

}

// ---------------------------------------------------------- HeartBeat Functions ----------------------------------------------------------

func (rf *Raft) ticker() {

	// log.Printf("[Node %d][Ticker] Ticker started", rf.me)

	for !rf.killed() {

		rf.mu.Lock()

		current_state := rf.state

		timeout := rf.timeout

		rf.mu.Unlock()

		current_time := time.Now()

		if current_state != "Leader" && current_time.After(timeout) {

			rf.startElection()

			rf.mu.Lock()

			rf.resetElectionTimeout()

			rf.mu.Unlock()

		} else if current_state == "Leader" {

			rf.mu.Lock()

			if !rf.leaderLoopRunning {

				go rf.leaderLoop()

			}

			rf.mu.Unlock()

		}

	}

}

func (rf *Raft) leaderLoop() {

	rf.mu.Lock()

	if rf.leaderLoopRunning {

		rf.mu.Unlock()

		return

	}

	rf.leaderLoopRunning = true

	rf.mu.Unlock()

	defer func() {

		rf.mu.Lock()

		rf.leaderLoopRunning = false

		rf.mu.Unlock()

	}()

	for {

		rf.mu.Lock()

		if rf.state != "Leader" || rf.killed() {

			rf.mu.Unlock()

			break

		}

		rf.sendHeartbeats()

		sleepDuration := 100 * time.Millisecond

		rf.mu.Unlock()

		time.Sleep(sleepDuration)

	}

}

func (rf *Raft) sendHeartbeats() {

	if rf.state != "Leader" {
		return

	}

	rf.lastHeartbeatTime = time.Now()

	// set current term for follower

	currentTerm := rf.currentTerm

	// iterate through all of its peers

	for i := range rf.peers {

		if i != rf.me {

			prevLogIndex := rf.nextIndex[i] - 1

			prevLogTerm := 0

			if prevLogIndex >= 0 {

				prevLogTerm = rf.log[prevLogIndex].Term

			}

			args34 := &AppendEntriesArgs{

				Term: currentTerm,

				LeaderID: rf.me,

				PrevLogIndex: prevLogIndex,

				PrevLogTerm: prevLogTerm,

				Entries: []LogEntry{},

				LeaderCommit: rf.commitIndex,
			}

			go func(server int, args *AppendEntriesArgs) {

				reply := AppendEntriesReply{}

				ok := rf.sendAppendEntries(server, args, &reply)

				if !ok {

					return

				}

				rf.mu.Lock()

				defer rf.mu.Unlock()

				// raft rule: peer steps down if it learns of higher term

				if reply.Term > rf.currentTerm {

					rf.currentTerm = reply.Term

					rf.votedFor = -1

					rf.state = "Follower"

					rf.log = rf.log[:rf.commitIndex+1]

					// log.Printf("[Node %d] [sendHeartbeats] Stepping down: higher term %d detected", rf.me, reply.Term)

				} else if reply.Success {

					// Update nextIndex and matchIndex on success

					rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)

					rf.nextIndex[server] = rf.matchIndex[server] + 1

					// log.Printf("[Node %d] [sendHeartbeats] Successfully replicated to Node %d at index %d", rf.me, server, reply.Term)

					rf.updateCommitIndex()

					// log.Printf("[Node %d] [sendHeartbeats] Successfully updated Commit Index to %d", rf.me, rf.commitIndex)

				} else {

					// Log Mismatch

					rf.nextIndex[server] = max(1, rf.nextIndex[server]-1)

					// log.Printf("[Node %d] [sendHeartbeats] Replication failed to Node %d, adjusting nextIndex to %d", rf.me, server, rf.nextIndex[server])

				}

			}(i, args34)

		}

	}

}

// ---------------------------------------------------------- Election Functions ----------------------------------------------------------

func (rf *Raft) startElection() {

	rf.mu.Lock()

	if rf.state != "Follower" && rf.state != "Candidate" {

		// log.Printf("[Node %d] [startElection] Not starting election - current state: %s", rf.me, rf.state)

		rf.mu.Unlock()

		return

	}

	rf.state = "Candidate"

	rf.currentTerm += 1

	rf.votedFor = rf.me

	current_term := rf.currentTerm

	// log.Printf("[Node %d] [startElection] Starting election for Term %d", rf.me, rf.currentTerm)

	LastLogTerm := 0

	lastLogIndex := len(rf.log) - 1

	if lastLogIndex >= 0 {

		lastLog := rf.log[lastLogIndex]

		LastLogTerm = lastLog.Term

	}

	// create the arguments an

	args := &RequestVoteArgs{

		Term: current_term,

		CandidateID: rf.me,

		LastLogIndex: lastLogIndex,

		LastLogTerm: LastLogTerm,
	}

	channel := make(chan bool, len(rf.peers))

	// send requst vote request to all peers except this one

	for i := 0; i < len(rf.peers); i++ {

		if i != rf.me {

			go func(i int) {

				reply := &RequestVoteReply{}

				ok := rf.sendRequestVote(i, args, reply)

				if ok && reply.Term == current_term && reply.VoteGranted {

					channel <- true

				} else {

					channel <- false

				}

			}(i)

		}

	}

	rf.mu.Unlock()

	votes_needed := len(rf.peers)/2 + 1

	votes := 1

	// log.Printf("[Node %d] [startElection] Waiting for votes... (Need %d)", rf.me, votes_needed)

	for i := 0; i < len(rf.peers)-1; i += 1 {

		grant_vote := <-channel

		if grant_vote {

			votes++

			// log.Printf("[Node %d] [startElection] Received vote. Current total: %d", rf.me, votes)

			if votes >= votes_needed {

				// log.Printf("[Node %d] [startElection] Received majority Votes as %s, current term: %d", rf.me, rf.state, rf.currentTerm)

				rf.mu.Lock()

				if rf.state == "Candidate" && rf.currentTerm == current_term {

					rf.state = "Leader"

					rf.lastHeartbeatTime = time.Now()

					log.Printf("[Node %d] [startElection] Won election for Term %d! Becoming Leader.", rf.me, current_term)

					// Initialize leader state

					for i := range rf.nextIndex {

						rf.nextIndex[i] = len(rf.log)

						rf.matchIndex[i] = 0

					}

					// send inital heartbeat once candidate becomes leader

					go rf.leaderLoop()

				}

				rf.mu.Unlock()

				break

			}

		}

	}

}

// The service or tester wants to create a Raft server.

// The ports of all the Raft servers (including this one) are in peers[].

// This server's port is peers[me].

// All the servers' peers[] arrays have the same order.

// applyCh is a channel on which the tester or service expects Raft to send ApplyMsg messages.

// Make() must return quickly, so it should  goroutines for any long-running work.

// ApplyMsg

//   Each time a new entry is committed to the log, each Raft peer

//   should send an ApplyMsg to the service (or tester) in the same server.

// Create a new Raft server.

func Make(peers []*labrpc.ClientEnd, me int, applyCh chan ApplyMsg) *Raft {

	rf := &Raft{}

	rf.peers = peers

	rf.me = me

	rf.mu = sync.Mutex{}

	rf.dead = 0

	rf.currentTerm = 0

	rf.votedFor = -1

	rf.log = []LogEntry{{Term: 0, Commands: nil}}

	rf.commitIndex = 0

	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))

	rf.matchIndex = make([]int, len(peers))

	rf.state = "Follower"

	rf.resetElectionTimeout()

	//  ticker goroutine to  elections.

	go func() {

		rf.ticker()

	}()

	go func() {

		for !rf.killed() {

			rf.mu.Lock()

			for rf.lastApplied < rf.commitIndex {

				rf.lastApplied += 1

				log_info := rf.log[rf.lastApplied]

				msg := ApplyMsg{

					CommandValid: true,

					Command: log_info.Commands,

					CommandIndex: rf.lastApplied,
				}

				rf.mu.Unlock()

				applyCh <- msg

				rf.mu.Lock()

			}

			rf.mu.Unlock()

			time.Sleep(10 * time.Millisecond)

		}

	}()

	return rf

}
