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
    "sync"
    "labrpc"
    "math/rand"
    "time"
)
// import "bytes"
// import "labgob"

const (
    follower  = "follower"
    candidate = "candidate"
    leader    = "leader"
    dead      = "dead"
)

const (
    cmdExit         = "exit"
    cmdShutdown     = "shutdown"
    cmdFoundLeader  = "foundLeader"
)

const heartBeatInterval = 125 * time.Millisecond  // 125 ms


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

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
    // persistent state
    currentTerm int             // latest term server has seen
    votedFor    int             // candidateId that recevied vote in current term
    log         []interface{}   // log entries(first index is 1)

    // Volatile state
    commitIndex int
    lastApplied int

    // Volatile state on leaders
    nextIndex   []int
    matchIndex  []int

    heartBeatInterval   int64   // millisecond
    flagChan            chan string
    t                   *time.Timer
    elecTimeFunc        func(rfp *Raft, timeSigChan chan string)
    state               string

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
    isLeader := false
    DPrintf("GetState: peer %d, term %d, state %s",
            rf.me, rf.currentTerm, rf.state)
    if rf.state == leader {
        isLeader = true
    }
       return rf.currentTerm, isLeader
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

func (rf *Raft) genElectionTimeout() time.Duration {
    //Election timeout should be 1s ~ 1.25s
    max := int64(time.Second) + 1 * rf.heartBeatInterval
    min := int64(time.Second)
    rand.Seed(time.Now().UTC().UnixNano())
    return time.Duration(rand.Int63n(max - min) + min)
}


type AppendEntriesArgs struct {
    Term            int     // leader's term
    LeaderID        int     // so follower can redirect clients
    PrevLogIndex    int     // index of log entry immediately preceding new ones
    PrevLogTerm     int     //term of PrevLogIndex entry
    Entries         []interface{}   // log entries to store (empty for heartbeat)
                                    // may send more than one for efficiency)
    LeaderCommit    int     // Leader's commitIndex
}

type AppendEntriesReply struct {
    Term    int     // currentTerm, for leader to update itself
    Success bool    // true if follower contained entry matching PrevLogIndex and PrevLogTerm
    Me      int     // which peer returns it
    Rtn     bool    // Determine whether this is a RPC return
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs,
                              reply *AppendEntriesReply) {
    DPrintf("peer %d recvd AppendEntries from %d\n", rf.me, args.LeaderID)
    if rf.currentTerm > args.Term {
        reply.Term = rf.currentTerm
        return
    }

    rf.mu.Lock()
    // TODO: which value should be set
    reply.Term = rf.currentTerm
    if rf.currentTerm < args.Term {
        rf.votedFor = -1
        // Change itself state to follower
        rf.flagChan <- cmdFoundLeader
    }
    rf.currentTerm = args.Term
    rf.mu.Unlock()

    // Reset timeout
    rf.t.Reset(rf.genElectionTimeout())
    return
}

func (rf *Raft) sendAppendEntries(server int,
                                  args *AppendEntriesArgs,
                                  reply *AppendEntriesReply) bool {
    ch := make(chan bool, 1)
    go func(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, ch chan bool) {
       ch <- rf.peers[server].Call("Raft.AppendEntries", args, reply)
    }(server, args, reply, ch)
    select {
    case ok := <- ch:
        return ok
    case <- time.After(time.Duration(rf.heartBeatInterval)):
        return false
    }
}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
    Term            int     // candidate's term
    CandidateId     int     // candidate requesting vote
    LastLogIndex    int     // index of candidate's last log entry
    LastLogTerm     int     // term of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
    Term        int     // currentTerm, for candidate to update itself
    VoteGranted bool    // true means candidate received vote
    Me          int     // which peer returns it
    Rtn         bool    // Determine whether this is a RPC return
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
    reply.Term = rf.currentTerm
    reply.VoteGranted = false
    reply.Me = rf.me
    DPrintf("peer %d recvd RequestVote for %d\n", rf.me, args.CandidateId)

    if args.Term == rf.currentTerm {
        //TODO: log should be checked
        if rf.votedFor == -1 {
            reply.VoteGranted = true
            rf.mu.Lock()
            rf.votedFor = args.CandidateId
            rf.mu.Unlock()
            DPrintf("peer %d voted for %d\n", rf.me, args.CandidateId)
        }
    } else if args.Term > rf.currentTerm {
            reply.VoteGranted = true
            rf.mu.Lock()
            rf.votedFor = args.CandidateId
            rf.currentTerm = args.Term
            rf.mu.Unlock()
            rf.flagChan <- cmdFoundLeader
            DPrintf("peer %d voted for %d\n", rf.me, args.CandidateId)
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
    ch := make(chan bool, 1)
    go func(server int, args *RequestVoteArgs, reply *RequestVoteReply, ch chan bool) {
        ch <-  rf.peers[server].Call("Raft.RequestVote", args, reply)
    }(server, args, reply, ch)

    select {
    case ok := <- ch:
        return ok
    case <- time.After(time.Duration(rf.heartBeatInterval)):
        return false
    }
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
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
    DPrintf("clients killing peer %d\n", rf.me)
    rf.flagChan <- cmdShutdown
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
    rf.heartBeatInterval = int64(heartBeatInterval)
    rf.votedFor = -1
    rf.t = time.NewTimer(rf.genElectionTimeout())
    rf.flagChan = make(chan string, 1)

    // Timeout goroutine
    rf.elecTimeFunc = func(rfp *Raft, timeSigChan chan string) {
        DPrintf("peer %d election timer started\n", rfp.me)
        //for {
            select {
            case <- rf.t.C:
                DPrintf("peer %d election timeout\n", rfp.me)
                //Remind peer to exit
                rfp.flagChan <- cmdExit
                rf.t.Stop()
                DPrintf("peer %d election timer goroutine exited\n", rfp.me)
                return
            case <- timeSigChan:
                DPrintf("timeout goroutine was asked to exit\n")
                return
            }
        //}
    }

	// Your initialization code here (2A, 2B, 2C).
    DPrintf("Create peer %d\n", me)
    go func(rfp *Raft) {
        cmdChan := make(chan string, 1)

        // Initial state should be follower
        cmdChan <- follower

        go func(rf *Raft, cmdChan chan string) {
            for {
                select {
                case cmd := <- cmdChan:
                    switch cmd {
                    case follower:
                        rf.follower(cmdChan)
                    case candidate:
                        rf.candidate(cmdChan)
                    case leader:
                        rf.leader(cmdChan)
                    case dead:
                        DPrintf("peer %d exited\n", rf.me)
                        return
                    default:
                        DPrintf("received unknown cmd", cmd)
                    }
                }
            }
        }(rf, cmdChan)
    }(rf)


	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}

func (rf *Raft) leader(cmdChan chan string) {
    subCmdChan := make([]chan string, len(rf.peers))
    sigChan := make(chan string)
    rf.state = leader
    DPrintf("peer %d changed to leader\n", rf.me)

    doAppend := func(signalChan chan string, subCmdChan chan string,
                     i int, rfp *Raft) {
                    t := time.NewTimer(time.Duration(rfp.heartBeatInterval))
                    for {
                        select {
                        case <- subCmdChan:
                            DPrintf("Recvd leader %d append req\n", rfp.me)
                        default:
                            DPrintf("hb %d -> %d\n", rfp.me, i)
                        }

                        //TODO: Need fill related data in 2B
                        args := AppendEntriesArgs{}
                        args.Term = rfp.currentTerm
                        args.LeaderID = rf.me
                        reply := AppendEntriesReply{}
                        rfp.sendAppendEntries(i, &args, &reply)

                        //If replied term is larger then itself. It should change itself to follower
                        if reply.Term > rf.currentTerm {
                            //Stop heart beat timer
                            t.Stop()
                            DPrintf("leader %d found server %s with hight term\n", rf.me, i)
                            rf.flagChan <- cmdFoundLeader
                        }

                        select {
                        case <- t.C:
                            DPrintf("heartBeat timeout,%d -> %d\n",
                                    rfp.me, i)
                            t.Reset(time.Duration(rfp.heartBeatInterval))
                        }

                        select {
                        case <- signalChan:
                            DPrintf("Leader %d request hb routine to exit\n",
                                    rfp.me)
                            return
                        default:
                        }
                    }
                }
    for i := 0; i < len(rf.peers); i ++ {
        if i != rf.me {
            subCmdChan[i] = make(chan string, 1)
            go doAppend(sigChan, subCmdChan[i], i, rf)
        }
    }

    select {
    case cmd := <- rf.flagChan:
        DPrintf("leader %d recvd %s\n", rf.me, cmd)
        switch cmd {
        case cmdFoundLeader:
            //Tell sub routines to exit
            close(sigChan)
            cmdChan <- follower
            return
        case cmdShutdown:
            close(sigChan)
            cmdChan <- dead
            return
        }
    }
}

func (rf *Raft) follower(cmdChan chan string) {
    //timeSigChan := make(chan string, 1)
    DPrintf("peer %d enter follower state\n", rf.me)

    //rf.t.Reset(rf.genElectionTimeout())
    // Kick off election timeout
    //go rf.elecTimeFunc(rf, timeSigChan)

    rf.state = follower
    for {
        timeSigChan := make(chan string, 1)
        rf.t.Reset(rf.genElectionTimeout())
        // Kick off election timeout
        go rf.elecTimeFunc(rf, timeSigChan)

        select {
        case cmd := <- rf.flagChan:
            DPrintf("follower %d recvd %s\n", rf.me, cmd)
            switch cmd {
            case cmdExit:
                // Change role to candidate and start election
                cmdChan <- candidate
                return
            case cmdShutdown:
                close(timeSigChan)
                cmdChan <- dead
                return
            case cmdFoundLeader:
                //Tell timeout go routine to exit
                close(timeSigChan)
                //Already follower. No need to change state
            }
        }
    }
}

func (rf *Raft) candidate(cmdChan chan string) {
    DPrintf("peer %d changed to candidate\n", rf.me)
    rf.mu.Lock()
    rf.votedFor = rf.me
    rf.mu.Unlock()
    rf.state = candidate
    rf.currentTerm ++
    rf.t.Reset(rf.genElectionTimeout())

    timeSigChan := make(chan string, 1)

    // Kick off election timeout
    go rf.elecTimeFunc(rf, timeSigChan)

    votes := 1  // it vote for itself
    voteReplyChan := make(chan RequestVoteReply, 1)
    signalChan := make(chan string)

    doVote := func(rfp *Raft, i int, sigChan chan string) {
                reqVoteArgs := &RequestVoteArgs{rfp.currentTerm, rfp.me, 0, 0}
                reqVoteReply := RequestVoteReply{0, false, i, false}

                DPrintf("doVote: peer %d Requesting %d vote for itself\n",
                        rfp.me, i)
                ok := rfp.sendRequestVote(i, reqVoteArgs, &reqVoteReply)
                if ok == true {
                    reqVoteReply.Rtn = true
                } else {
                    reqVoteReply.Rtn = false
                }
                DPrintf("doVote: peer %d Requesting %d vote for itself: %t\n",
                        rfp.me, i, ok)

                select {
                case <- sigChan:
                    //sigChan closed
                    DPrintf("doVote: Request %d vote for %d: Candidate asked to exit\n",
                            i, rfp.me)
                    return
                default:
                    //sigChan is still open. Send message
                    DPrintf("doVote: Request %d vote for %d: sending result to candidate\n",
                            i, rfp.me)
                    voteReplyChan <- reqVoteReply
                }
            }

    // Send RPC in parallel
    for i := 0; i < len(rf.peers); i ++ {
        if i != rf.me {
            go doVote(rf, i, signalChan)
        }
    }

    for {
        select {
        case reqVoteReply := <- voteReplyChan:
            if reqVoteReply.Rtn == true {
                if reqVoteReply.VoteGranted == true {
                    // Have majority votes
                    if votes ++; votes >= len(rf.peers) / 2 + 1 {
                        DPrintf("candidate %d recvd enough votes\n", rf.me)
                        // Close chan so that sub-goroutines can exit
                        close(timeSigChan)
                        close(signalChan)

                        //TODO: Might need to empty voteReplyChan

                        DPrintf("candidate %d remind sm to change to leader\n", rf.me)
                        // Change role to leader
                        cmdChan <- leader
                        DPrintf("candidate %d election done\n", rf.me)
                        return
                    }
                }
            } else {
                // RPC failure, try it again
                DPrintf("Re-requesting %d vote for %d\n", reqVoteReply.Me, rf.me)
                go doVote(rf, reqVoteReply.Me, signalChan)
            }
        case cmd := <- rf.flagChan:
            DPrintf("candidate %d recvd %s\n", rf.me, cmd)
            switch cmd {
            case cmdExit:
                // Close chan so that sub-goroutines can exit
                close(signalChan)

                // Change role to candidate and start election again
                cmdChan <- candidate
                return
            case cmdShutdown:
                // Close chan so that sub-goroutines can exit
                close(timeSigChan)
                close(signalChan)

                // Change role to candidate and start election again
                cmdChan <- dead
                return
            case cmdFoundLeader:
                // Close chan so that sub-goroutines can exit
                close(signalChan)
                close(timeSigChan)
                // Change role to follower and start election again
                cmdChan <- follower
                return
            }
        }
    }
}
