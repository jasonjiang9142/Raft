
# Raft Consensus Algorithm


# Overview

This project implements the Raft consensus algorithm, a replicated state machine protocol designed for fault-tolerant distributed systems. Raft ensures that a set of replica servers maintain identical logs of client requests, allowing the service to continue operating despite server failures (crashes or network issues). The implementation supports leader election, log replication, and fault tolerance as long as a majority of servers remain operational and can communicate.

This project follows the design outlined in the **extended Raft paper**, focusing on Figure 2, and implements most of the protocol's core components. It does not include persistent state saving, cluster membership changes (Section 6), or log snapshotting.

The implementation is split into two parts:
- **Part A:** Leader election and heartbeats.
- **Part 3B:** Log replication and agreement.

## Features

- **Fault Tolerance:** Continues operation with a majority of servers alive and communicating.
- **Leader Election:** Elects a single leader and handles re-elections after failures.
- **Log Replication:** Ensures all replicas maintain consistent logs of client commands.
- **Concurrency:** Uses Go goroutines and channels for concurrent operation, with proper synchronization.
- **Robust Testing:** Passes a comprehensive test suite for leader election (3A) and log replication (3B).


---

## Protocol Description

Raft organizes client requests into a replicated log, ensuring all live replicas execute the same sequence of commands in the same order. Key components include:

### **1. Leader Election:**

Servers are in one of three states: `leader`, `follower`, or `candidate`.
`Followers` periodically time out and become `candidates`, initiating elections by sending RequestVote RPCs.
A `candidate` becomes `leader` if it receives votes from a majority of peers.
`Leaders` send periodic `AppendEntries` RPCs (heartbeats) to maintain authority and prevent new elections.


### **2. Log Replication:**

Clients submit commands to the leader via `Start()`, which appends them to the leader's log.
The leader sends `AppendEntries` RPCs to replicate log entries to followers.
Entries are committed once a majority of servers have them, and the leader notifies the service via an `ApplyMsg`.
The `leader` handles log inconsistencies by backing up followers' `nextIndex` when entries conflict.


### **3. Fault Tolerance:**

Raft operates as long as a majority of servers are alive and can communicate.
If a leader fails, a new election occurs within five seconds (tested by the suite).
Followers with outdated logs are brought up to date by the leader.


---

## Code Structure

| File            | Description                                      |
|-----------------|--------------------------------------------------|
| `raft/raft.go`| Contains the Raft peer implementation, including leader election, log replication, and RPC handling.  |
| `raft/test_test.go:`     | Includes the test suite for validating the implementation.
 

---

## Interface

The Raft implementation provides the following interface, used by the tester and service:

- `Make`: Initializes a Raft peer with a list of peer network identifiers, its index, and a channel for committed entries.
- `Start`: Submits a new command to the log, returning immediately without waiting for replication.
- `GetState`: Returns the current term and leadership status.
- `ApplyMsg`: Sent to the service for each committed log entry.


```bash 
// Create a new Raft server instance
rf := Make(peers []*labrpc.ClientEnd, me int, applyCh chan ApplyMsg) *Raft

// Start agreement on a new log entry
rf.Start(command interface{}) (index int, term int, isLeader bool)

// Get the current term and whether this peer is the leader
rf.GetState() (term int, isLeader bool)

// Structure for committed log entries sent to the service
type ApplyMsg struct {
    CommandValid bool
    Command      interface{}
    CommandIndex int
}

```

---

## RPC Interface

Raft peers communicate via two RPCs:

- `RequestVote`: Used during elections to gather votes from peers.
- `AppendEntries`: Used for heartbeats and log replication.


---

## Implementation Details


**Part A: Leader Election**

- **State Management:** Tracks server state (`leader`, `follower`, `candidate`), current term, and voted-for candidate.
- **Election Logic:** A goroutine periodically checks for election timeouts, triggering `RequestVote` RPCs if no leader heartbeats are received.
- **Heartbeats:** Leaders send `AppendEntries` RPCs (empty for heartbeats) at least 10 times per second to reset follower election timeouts.
- **Timeouts:** Randomized election timeouts (e.g., 300-600ms) prevent split votes and ensure elections complete within 5 seconds.
- **Concurrency:** Uses goroutines with `time.Sleep` loops for periodic tasks, avoiding `time.Timer` or `time.Ticker`.

**Part B: Log Replication**

- **Log Management:** Stores log entries with `index`, `term`, and `command`. Implements the election restriction (Section 5.4.1).
- **Replication:** `Leaders` send `AppendEntries` RPCs to replicate entries, handling conflicts by backing up `nextIndex`.
- **Commitment:** Commits entries when a majority of servers have them, sending `ApplyMsg` to the service.
- **Optimization:** Backs up nextIndex by multiple entries at a time to handle large log discrepancies efficiently (per the paper, pages 7-8).
- **Concurrency:** Uses condition variables or short `time.Sleep `calls in loops to avoid excessive CPU usage.


---

## Testing

The project includes a test suite in `raft/test_test.go`, covering:
- **Part 3A Tests**: Initial election, election after network failure, and leader stability.
- **Part 3B Tests**: Basic agreement, agreement despite failures, concurrent Start() calls, leader rejoin, and log consistency.


Example test output:
```bash
$ go test -run 3A -race
Test (3A): initial election ...
  ... Passed --   4.0  3   32    9170    0
Test (3A): election after network failure ...
  ... Passed --   6.1  3   70   13895    0
PASS
ok      raft    10.187s

$ go test -run 3B -race
Test (3B): basic agreement ...
  ... Passed --   1.6  3   18    5158    3
...
PASS
ok      raft    189.840s

```

To run individual test

```bash
go test -race -run 3A
go test -race -run TestInitialElection3A

```




## Setup and Installation

Clone the project

**Prerequisites:**

- Go (version 1.16 or later)
- Git

**Clone the Repository:**
```bash
git clone https://github.com/jasonjiang9142/raft.git
cd raft
```

**Run Tests:**

```bash
  go test -run 3A -race
  go test -run 3B -race
```


## Limitations

- Does not implement persistent state storage, cluster membership changes, or log snapshotting.
- Assumes the labrpc package for RPC communication, with simulated network failures.
- Performance is tuned to pass tests within time limits (600s total, 120s per test).
## Contributing

Contributions are welcome! Please submit a pull request or open an issue for bugs, improvements, or feature requests.

## License


This project is licensed under the 
[MIT](https://choosealicense.com/licenses/mit/) License.

## Acknowledgments
This project was completed as part of a coursework assignment for **CS351: Distributed Systems** at **Boston University**.



## References 

[Raft Extended Paper](https://raft.github.io/raft.pdf)


