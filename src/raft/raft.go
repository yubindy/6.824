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
	"log"
	"math/rand"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const (
	Foller    int = 1
	Candidate int = 2
	Leader    int = 3
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

	state       int
	currentTerm int
	votedFor    int
	log         []string
	hasheat     bool
	hasvote     bool
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	if rf.state == Leader {
		isleader = true
	} else {
		isleader = false
	}
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
	Candidateid  int
	Lastlogindex int //日志最后索引
	Lastlogterm  int //日志最后任期号
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term    int
	Votefor bool
}
type AppendEntriesArgs struct {
	Term         int      //领导人的任期
	Leaderid     int      //领导人 ID
	PrevLogIndex int      //紧邻新日志条目之前的那个日志条目的索引
	PrevLogTerm  int      //紧邻新日志条目之前的那个日志条目的任期
	Entries      []string //需要被保存的日志条目
	LeaderCommit int      //领导人的已知已提交的最高的日志条目的索引
}
type AppendEntriesReply struct {
	Term    int  //当前任期
	Success bool //如果跟随者所含有的条目和 prevLogIndex 以及 prevLogTerm 匹配上了，则为 true
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if rf.currentTerm == args.Term && rf.votedFor == -1 {
		reply.Votefor = true
		rf.hasvote = true
		//log.Printf("%v[N:%d-T:%d-VF:%d] server vote term to [N:%d-T:%d]", time.Now().UnixMicro(), rf.me, rf.currentTerm, rf.votedFor, args.Candidateid, args.Term)
		rf.votedFor = args.Candidateid
	} else if rf.currentTerm < args.Term {
		reply.Votefor = true
		rf.hasvote = true
		//log.Printf("[N:%d-T:%d-VF:%d] server vote term to [N:%d-T:%d]", rf.me, rf.currentTerm, rf.votedFor, args.Candidateid, args.Term)
		rf.votedFor = args.Candidateid
		rf.currentTerm = args.Term
	} else {
		reply.Votefor = false
		//log.Printf("[%d] server not vote term to %d", rf.me, args.Candidateid)
	}
}
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) { //实现心跳和交换日志
	rf.mu.Lock()
	if rf.currentTerm <= args.Term {
		rf.hasheat = true
		reply.Term = rf.currentTerm
		rf.state = Foller
		log.Printf("%d node become %d foller", rf.me, args.Leaderid)
	}
	log.Printf("[%d] server recv AppendEntries to %d", rf.me, args.Leaderid)
	rf.mu.Unlock()
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
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
func (rf *Raft) startvote() {
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	me := rf.me
	currentTerm := rf.currentTerm
	var num int64
	num = 1
	n := len(rf.peers)
	log.Printf("%d start vote  %d has all %d", rf.me, num, n)
	wg := sync.WaitGroup{}
	rf.mu.Unlock()
	wg.Add(n - 1)
	for node := range rf.peers {
		if node != me {
			go func(node int) {
				defer wg.Done()
				defer log.Printf(" %d go thread--%d exit", me, node)
				log.Printf("%d has vote  %d int 329", me, atomic.LoadInt64(&num))
				args := RequestVoteArgs{
					Term:         currentTerm,
					Candidateid:  me,
					Lastlogindex: 0,
					Lastlogterm:  0,
				}
				reply := RequestVoteReply{}
				log.Printf("%d has vote  %d int %d 337", me, atomic.LoadInt64(&num), node)
				ok := rf.sendRequestVote(node, &args, &reply)
				log.Printf("%d has vote  %d int %d 339", me, atomic.LoadInt64(&num), node)
				if !ok {
					log.Printf("server %d VoteCall failed to %d", me, node)
					log.Printf("%d has vote  %d int node %d", me, atomic.LoadInt64(&num), node)
					return
				}
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > args.Term {
					return
				}
				log.Printf("%d in node %d has vote  %d", me, node, atomic.LoadInt64(&num))
				if reply.Votefor {
					log.Printf("%d get a vote term form %d", me, node)
					atomic.AddInt64(&num, 1)
				} else {
					log.Printf("%d not get vote from %d", me, node)
				}
			}(node)
			//log.Printf("Vote:%d,num:%d,votefor:%v", me, num, reply.Votefor)
		}
	}
	wg.Wait()
	log.Printf("ggggg")
	rf.mu.Lock()
	defer rf.mu.Unlock()
	s := atomic.LoadInt64(&num)
	if int(s) > n/2 && !rf.hasheat && rf.state == Candidate {
		log.Printf("serve[%d] become leaderr num:%d hashert:%v", rf.me, s, rf.hasheat)
		rf.state = Leader
	} else {
		rf.state = Candidate
		log.Printf("%d server  should vote again-- %d %v", rf.me, s, rf.hasheat)
	}
}
func (rf *Raft) sendhert() {
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	me := rf.me
	sta := rf.state
	log.Printf("%d state  %d start send heart", rf.me, rf.state)
	rf.mu.Unlock()
	wg := sync.WaitGroup{}
	wg.Add(len(rf.peers) - 1)
	for node := range rf.peers {
		if node != me {
			go func(node int) {
				defer wg.Done()
				args := AppendEntriesArgs{
					Term:         currentTerm,
					Leaderid:     me,
					PrevLogIndex: 0,
					PrevLogTerm:  0,
					Entries:      nil,
					LeaderCommit: 0,
				}
				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(node, &args, &reply)
				if !ok {
					log.Printf("server %d - %d sendappendentries failed to %d", me, sta, node)
					return
				}
				if reply.Term > args.Term {
					rf.mu.Lock()
					rf.currentTerm = reply.Term
					rf.state = Foller
					log.Printf("%d become foller", rf.mu)
					rf.mu.Unlock()
					return
				}
			}(node)
		}
	}
	wg.Wait()
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	var t int
	for rf.killed() == false {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		switch state {
		case Foller:
			rf.mu.Lock()
			heat := rf.hasheat
			vote := rf.hasvote
			if heat == false && vote == false {
				log.Printf("%d became Candidate", rf.me)
				rf.state = Candidate
				go rf.startvote()
			}
			rf.mu.Unlock()
		case Candidate:
			go rf.startvote()
		case Leader:
			go rf.sendhert()
		}
		rf.mu.Lock()
		rf.hasvote = false
		rf.hasheat = false
		state = rf.state
		rf.mu.Unlock()
		rand.Seed(time.Now().UnixNano())
		if state != Leader {
			t = rand.Intn(150) + 200
			for i := 0; i < t; i++ {
				rf.mu.Lock()
				if rf.state == Candidate && rf.hasheat {
					rf.state = Foller
					log.Printf("%d become foller ", rf.me)
					rf.mu.Unlock()
					break
				} else if rf.state == Foller && (rf.hasheat == true || rf.hasvote == true) {
					rf.mu.Unlock()
					break
				} else if rf.state == Leader {
					rf.mu.Unlock()
					break
				}
				rf.mu.Unlock()
				time.Sleep(time.Millisecond)
			}
		} else {
			t = 100
			for i := 0; i < t; i++ {
				rf.mu.Lock()
				if rf.hasheat || rf.hasvote {
					rf.state = Foller
					log.Printf("%d become foller ", rf.me)
					rf.mu.Unlock()
					break
				}
				rf.mu.Unlock()
				time.Sleep(time.Millisecond)
			}
		}
	}
	// Your code here to check if a leader election should
	// be started and to randomize sleeping time using
	// time.Sleep().

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
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = Foller
	rf.hasvote = true
	rf.hasheat = true
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
