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
type nlog struct {
	Term   int
	logact interface{}
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
	//2a
	state       int
	currentTerm int
	votedFor    int
	hasheat     bool
	hasvote     bool

	logs        []nlog
	commitIndex int
	lastapplied int
	nextIndex   []int //next log的索引位置,即刚好i匹配上的index
	matchIndex  []int //已经复制log的的索引
	cond        *sync.Cond
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
	Term         int    //领导人的任期
	Leaderid     int    //领导人 ID
	PrevLogIndex int    //紧邻新日志条目之前的那个日志条目的索引
	PrevLogTerm  int    //紧邻新日志条目之前的那个日志条目的任期
	Entries      []nlog //需要被保存的日志条目
	LeaderCommit int    //领导人的已知已提交的最高的日志条目的索引
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
	if rf.currentTerm == args.Term && rf.votedFor == -1 && len(rf.logs)-1 <= args.Lastlogindex {
		reply.Votefor = true
		rf.hasvote = true
		//log.Printf("%%v v[N:%d-T:%d-VF:%d] server vote term to [N:%d-T:%d]",time.Now().UnixNano() / 1e6  - time.Now().Unix()*1000, time.Now().UnixMicro(), rf.me, rf.currentTerm, rf.votedFor, args.Candidateid, args.Term)
		rf.votedFor = args.Candidateid
	} else if rf.currentTerm < args.Term {
		reply.Votefor = true
		rf.hasvote = true
		//log.Printf("%v server %d term %d became %d term %d foller", time.Now().UnixNano()/1e6-time.Now().Unix()*1000, rf.me, rf.currentTerm, args.Candidateid, args.Term)
		rf.votedFor = args.Candidateid
		rf.currentTerm = args.Term
	} else {
		reply.Votefor = false
		//log.Printf("[%v %d] server not vote term to %d",time.Now().UnixNano() / 1e6  - time.Now().Unix()*1000, rf.me, args.Candidateid)
	}
}
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) { //实现心跳和附加日志
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if rf.currentTerm <= args.Term { //线判断任期
		rf.hasheat = true
		rf.state = Foller
		if args.PrevLogIndex == len(rf.logs)-1 { //再判断log长度
			reply.Success = true
			//log.Printf("%v %d fail append log %d ", time.Now().UnixNano()/1e6-time.Now().Unix()*1000, rf.me, args.Leaderid)
		} else {
			reply.Success = false
		}
	} else {
		reply.Success = false
	}
	if reply.Success && args.Entries == nil {
		if args.LeaderCommit > args.PrevLogIndex {
			rf.commitIndex = args.PrevLogIndex
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		rf.cond.Signal()
		log.Printf("%v %d recv hert %d ", time.Now().UnixNano()/1e6-time.Now().Unix()*1000, rf.me, args.Leaderid)
	} else if reply.Success && args.Entries != nil {
		for i := 0; i < len(args.Entries); i++ {
			rf.logs = append(rf.logs, args.Entries[i])
		}
	}
	//log.Printf("%v [%d] server recv AppendEntries to %d", time.Now().UnixNano()/1e6-time.Now().Unix()*1000, rf.me, args.Leaderid)
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
	term, isLeader = rf.GetState()
	rf.mu.Lock()
	if isLeader {
		rf.logs = append(rf.logs, nlog{
			Term:   term,
			logact: command})
	}
	index = len(rf.logs) - 1
	rf.mu.Unlock()
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
	laslogindex := len(rf.logs) - 1
	lastlogterm := rf.logs[laslogindex].Term
	var num int64
	num = 1
	n := len(rf.peers)
	log.Printf("%v %d start vote %d term %d has all %d", time.Now().UnixNano()/1e6-time.Now().Unix()*1000, rf.me, num, rf.currentTerm, n)
	wg := sync.WaitGroup{}
	rf.mu.Unlock()
	wg.Add(n - 1)
	for node := range rf.peers {
		if node != me {
			go func(node int) {
				defer wg.Done()
				args := RequestVoteArgs{
					Term:         currentTerm,
					Candidateid:  me,
					Lastlogindex: laslogindex,
					Lastlogterm:  lastlogterm,
				}
				log.Printf("%v %v send vote to %v has %d", me, time.Now().UnixNano()/1e6-time.Now().Unix()*1000, node, atomic.LoadInt64(&num))
				reply := RequestVoteReply{}
				//log.Printf(" %v%d has vote  %d int %d 337",time.Now().UnixNano() / 1e6  - time.Now().Unix()*1000, me, atomic.LoadInt64(&num), node)
				ok := rf.sendRequestVote(node, &args, &reply)
				rf.mu.Lock()
				defer rf.mu.Unlock()
				//log.Printf(" %v%d has vote  %d int %d 339",time.Now().UnixNano() / 1e6  - time.Now().Unix()*1000, me, atomic.LoadInt64(&num), node)
				if !ok {
					log.Printf("%v server %d term %d VoteCall failed to %d had vote %d", time.Now().UnixNano()/1e6-time.Now().Unix()*1000, me, rf.currentTerm, node, atomic.LoadInt64(&num))
					//log.Printf("%v %d finally has  vote %d", time.Now().UnixNano()/1e6-time.Now().Unix()*1000, rf.me, num)
					log.Printf("%v %d derver go exit--%d", time.Now().UnixNano()/1e6-time.Now().Unix()*1000, me, node)
					return
				}
				if reply.Term > args.Term {
					return
				}
				if reply.Votefor {
					atomic.AddInt64(&num, 1)
					log.Printf("%v %d term %d get vote form %d term %dhasall %d", time.Now().UnixNano()/1e6-time.Now().Unix()*1000, me, rf.currentTerm, node, reply.Term, atomic.LoadInt64(&num))
					if int(atomic.LoadInt64(&num)) > n/2 && !rf.hasheat && rf.state == Candidate {
						log.Printf("%v serve %d become leaderr num:%d term:%d hashert:%v", time.Now().UnixNano()/1e6-time.Now().Unix()*1000, rf.me, num, rf.currentTerm, rf.hasheat)
						rf.state = Leader
					}
					//atomic.AddInt64(&num, 1)
				} else {
					log.Printf("%v %d term %d not get vote from %d", time.Now().UnixNano()/1e6-time.Now().Unix()*1000, me, rf.currentTerm, node)
				}
				log.Printf("%v %d derver go exit--%d", time.Now().UnixNano()/1e6-time.Now().Unix()*1000, me, node)
			}(node)
			//log.Printf("V%v ote:%d,num:%d,votefor:%v",time.Now().UnixNano() / 1e6  - time.Now().Unix()*1000, me, num, reply.Votefor)
		}
	}
	wg.Wait()
	log.Printf("%v %d term:%d ggggg", time.Now().UnixNano()/1e6-time.Now().Unix()*1000, me, currentTerm)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		log.Printf("%v %d server term:%d should vote again-- %d %v", time.Now().UnixNano()/1e6-time.Now().Unix()*1000, rf.me, rf.currentTerm, num, rf.hasheat)
	} else {
		loglen := len(rf.logs)
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = loglen
		}
	}
}
func (rf *Raft) sendlog() {
	var prevlog []int
	var prevterm []int
	var numlog int64
	var entries [][]nlog
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	me := rf.me
	loglen := len(rf.logs)
	commit := rf.commitIndex
	for i := 0; i < len(rf.peers); i++ {
		prevlog = append(prevlog, rf.nextIndex[i]-1)
		prevterm = append(prevterm, rf.logs[prevlog[i]].Term)
		entries = append(entries, nil)
		for t := rf.nextIndex[i]; t < loglen; t++ {
			entries[i] = append(entries[i], rf.logs[t])
		}
	}
	log.Printf("S%v dtime:%v send log %v", rf.me, time.Now().UnixNano()/1e6-time.Now().Unix()*1000, entries)
	rf.mu.Unlock()
	wg := sync.WaitGroup{}
	num := len(rf.peers) - 1
	wg.Add(num)
	for node := range rf.peers {
		if node != me {
			go func(node int) {
				defer wg.Done()
				args := AppendEntriesArgs{
					Term:         currentTerm,
					Leaderid:     me,
					PrevLogIndex: prevlog[node],
					PrevLogTerm:  prevterm[node],
					Entries:      entries[node],
					LeaderCommit: commit,
				}
				reply := AppendEntriesReply{}
				log.Printf("S%v %v sendlog to %d infomation %v", time.Now().UnixNano()/1e6-time.Now().Unix()*1000, me, node, args)
				ok := rf.sendAppendEntries(node, &args, &reply)
				if !ok {
					log.Printf("S%v %v sendlog failed to %d", time.Now().UnixNano()/1e6-time.Now().Unix()*1000, me, node)
					log.Printf("%v %d derver go exit--%d", time.Now().UnixNano()/1e6-time.Now().Unix()*1000, me, node)
					return
				}
				if reply.Term > args.Term {
					rf.mu.Lock()
					rf.currentTerm = reply.Term
					rf.state = Foller
					log.Printf("%v %d become foller", time.Now().UnixNano()/1e6-time.Now().Unix()*1000, rf.me)
					rf.mu.Unlock()
					//log.Printf("%v %d derver go exit--%d", time.Now().UnixNano()/1e6-time.Now().Unix()*1000, me, node)
					return
				}
				if args.Entries != nil {
					if reply.Success {
						atomic.AddInt64(&numlog, 1)
						rf.mu.Lock()
						rf.nextIndex[node] += len(args.Entries)
						rf.matchIndex[node] = rf.nextIndex[node] - 1
						if atomic.LoadInt64(&numlog) > int64(num)/2 {
							rf.commitIndex = loglen - 1
							rf.cond.Signal()
						}
						rf.mu.Unlock()
					} else {
						rf.mu.Lock()
						rf.nextIndex[node]--
						rf.mu.Unlock()
					}
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
		if state == Foller {
			rf.mu.Lock()
			heat := rf.hasheat
			vote := rf.hasvote
			if heat == false && vote == false {
				log.Printf("%v %d became Candidate", time.Now().UnixNano()/1e6-time.Now().Unix()*1000, rf.me)
				rf.state = Candidate
				go rf.startvote()
			}
			rf.mu.Unlock()
		} else if state == Candidate {
			go rf.startvote()
		} else if state == Leader {
			go rf.sendlog()
		}
		rf.mu.Lock()
		rf.hasvote = false
		rf.hasheat = false
		state = rf.state
		rf.mu.Unlock()
		rand.Seed(time.Now().UnixNano())
		if state != Leader {
			t = rand.Intn(200) + 200
			for i := 0; i < t; i++ {
				rf.mu.Lock()
				if rf.state == Candidate && (rf.hasheat == true || rf.hasvote == true) {
					rf.state = Foller
					log.Printf("%v %d become foller ", time.Now().UnixNano()/1e6-time.Now().Unix()*1000, rf.me)
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
					log.Printf("%v leader %d become foller ", time.Now().UnixNano()/1e6-time.Now().Unix()*1000, rf.me)
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
	rf.cond = sync.NewCond(&rf.mu)
	rf.logs = append(rf.logs, nlog{0, nil})
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex = append(rf.matchIndex, 0)
		rf.nextIndex = append(rf.nextIndex, 1)
	}
	// Your initialization code here (2A, 2B, 2C).
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go func() { //通过applech 发送AppleMsg消息
		index := 0
		for {
			apply := ApplyMsg{}
			rf.mu.Lock()
			for rf.commitIndex == index {
				rf.cond.Wait()
			}
			if index != rf.commitIndex {
				apply.Command = rf.logs[index]
				apply.CommandIndex = index
				index++
				apply.CommandValid = true
			}
			applyCh <- apply
			rf.mu.Unlock()
		}
	}()
	// start ticker goroutine to start elections
	go rf.ticker()
	return rf
}
