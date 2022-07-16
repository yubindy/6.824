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
	"log"
	"math/rand"
	"sort"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"sync"
	"sync/atomic"
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
	Logact interface{}
}

type persistent struct {
	CurrentTerm int
	VotedFor    int
	Logs        []nlog
}

type Snapshots struct {
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

	applyCh      chan ApplyMsg
	Persistinfo  persistent
	Lastlogindex int
	Snapshotinfo Snapshots
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
func (rf *Raft) persist(lock bool) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if lock {
		rf.mu.Lock()
	}
	log.Printf("start persist %d term %d log %v", rf.me, rf.currentTerm, rf.logs)
	rf.Persistinfo.CurrentTerm = rf.currentTerm
	rf.Persistinfo.VotedFor = rf.votedFor
	rf.Persistinfo.Logs = rf.logs
	e.Encode(rf.Persistinfo)
	e.Encode(rf.Lastlogindex)
	e.Encode(rf.Snapshotinfo.SnapshotTerm)
	data := w.Bytes()
	//rf.persister.SaveRaftState(data)
	rf.persister.SaveStateAndSnapshot(data, rf.Snapshotinfo.Snapshot)
	if lock {
		rf.mu.Unlock()
	}
}

//
// restore previously persisted state.
//
func (rf *Raft) readSnapshotPersist(data []byte) {
	if data == nil { // bootstrap without any state?
		return
	}
	rf.mu.Lock()
	rf.Snapshotinfo.Snapshot = data
	rf.mu.Unlock()
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var Persistinfo persistent
	var LastlogindexIndex int
	var SnapshotLasterm int
	if d.Decode(&Persistinfo) != nil && d.Decode(&LastlogindexIndex) != nil && d.Decode(&SnapshotLasterm) != nil {
		log.Printf("node%d readSnapshotPersist decode==nil", rf.me)
	} else {
		rf.mu.Lock()
		log.Printf("node %d ********* readSnapshotPersist term %d log %v", rf.me, rf.me, rf.logs)
		rf.currentTerm = Persistinfo.CurrentTerm
		rf.votedFor = Persistinfo.VotedFor
		rf.logs = Persistinfo.Logs
		rf.Lastlogindex = LastlogindexIndex
		rf.Snapshotinfo.SnapshotTerm = SnapshotLasterm
		rf.mu.Unlock()
	}
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
	rf.mu.Lock()
	if index >= len(rf.logs)-1 {
		rf.Snapshotinfo.Snapshot = snapshot
		rf.Snapshotinfo.SnapshotIndex = index
		rf.Snapshotinfo.SnapshotTerm = rf.logs[index].Term
		rf.logs = rf.logs[index:]
		log.Printf("node %d Snapshot index: %v", rf.me, index)
	}
	rf.mu.Unlock()
	rf.persist(true)
	go func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.Snapshotinfo.SnapshotIndex != -1 {
			applyMsg := ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.Snapshotinfo.Snapshot,
				SnapshotIndex: rf.Snapshotinfo.SnapshotIndex,
				SnapshotTerm:  rf.Snapshotinfo.SnapshotTerm,
			}
			rf.applyCh <- applyMsg
			rf.lastapplied = rf.Lastlogindex
		}
	}()
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
	Term        int  //当前任期
	Success     bool //如果跟随者所含有的条目和 prevLogIndex 以及 prevLogTerm 匹配上了，则为 true
	Failterm    int
	Failindex   int
	Cmatchindex int
}

type InstallSnapshotArgs struct {
	Term              int //当前任期
	Leaderid          int
	LastIncludedIndex int
	lastIncludedTerm  int
	//Offset            int    //分块在快照中的字节偏移量
	Data []byte //从偏移量开始的快照分块的原始字节
	//Done              bool   //是否是最后一个分快
}
type InstallSnapshotReply struct {
	Term int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestInstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		return
	}
	if args.LastIncludedIndex > rf.Snapshotinfo.SnapshotIndex && args.lastIncludedTerm >= rf.Snapshotinfo.SnapshotTerm {
		rf.Snapshotinfo.Snapshot = args.Data
		rf.Snapshotinfo.SnapshotIndex = args.LastIncludedIndex
		rf.Snapshotinfo.SnapshotTerm = args.lastIncludedTerm
		if rf.Lastlogindex > args.LastIncludedIndex {
			rf.logs = rf.logs[rf.Lastlogindex+1:]
		} else {
			rf.logs = nil
		}
		go func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.Snapshotinfo.SnapshotIndex != -1 {
				applyMsg := ApplyMsg{
					SnapshotValid: true,
					Snapshot:      rf.Snapshotinfo.Snapshot,
					SnapshotIndex: rf.Snapshotinfo.SnapshotIndex,
					SnapshotTerm:  rf.Snapshotinfo.SnapshotTerm,
				}
				rf.applyCh <- applyMsg
				rf.lastapplied = rf.Lastlogindex
			}
		}()

	}
}
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if rf.currentTerm <= args.Term {
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.votedFor = -1
			rf.state = Foller
		}
		rf.persist(false)
		if rf.logs[len(rf.logs)-1].Term < args.Lastlogterm && rf.votedFor == -1 { //先比较term然后比len
			reply.Votefor = true
			rf.hasvote = true
			rf.state = Foller
			rf.votedFor = args.Candidateid
		} else if rf.logs[len(rf.logs)-1].Term == args.Lastlogterm && rf.Lastlogindex <= args.Lastlogindex && rf.votedFor == -1 {
			reply.Votefor = true
			rf.hasvote = true
			rf.state = Foller
			rf.votedFor = args.Candidateid
		} else {
			reply.Votefor = false
			if rf.state == Leader {
				rf.hasheat = true
			}
			log.Printf("why not got vote me:%v berterm: %v len:%v logsterm: %v to:%v", rf.me, reply.Term, len(rf.logs)-1, rf.logs[len(rf.logs)-1].Term, args)
			rf.currentTerm = args.Term
		}
		rf.persist(false)
		//log.Printf("%v server %d term %d became %d term %d foller", time.Now().UnixNano()/1e6-time.Now().Unix()*1000, rf.me, rf.currentTerm, args.Candidateid, args.Term)
	} else {
		args.Term = rf.currentTerm
		reply.Votefor = false
		log.Printf("why not 226 got vote me:%v %v %v to:%v", rf.me, rf.currentTerm, len(rf.logs)-1, args)
		//log.Printf("[%v %d] server not vote term to %d",time.Now().UnixNano() / 1e6  - time.Now().Unix()*1000, rf.me, args.Candidateid)
	}
	if reply.Votefor {
		rf.state = Foller
		log.Printf("%d give vote to %d  mylog %v", rf.me, args.Candidateid, rf.logs)
	}

}
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) { //实现心跳和附加日志
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	log.Printf("node %d term %d recvterm %d recvfrom %d log %v recv %v", rf.me, rf.currentTerm, args.Term, args.Leaderid, rf.logs, args)
	if rf.currentTerm <= args.Term { //线判断任期
		rf.hasheat = true
		rf.state = Foller
		log.Printf("node %v become foller", rf.me)
		rf.currentTerm = args.Term
		if len(rf.logs)-1 < args.PrevLogIndex {
			reply.Success = false
		} else {
			if rf.logs[args.PrevLogIndex-rf.Snapshotinfo.SnapshotIndex].Term == args.PrevLogTerm {
				rf.hasheat = true
				reply.Success = true
			} else {
				reply.Success = false
			}
		}
		//log.Printf("%v %d fail append log %d ", time.Now().UnixNano()/1e6-time.Now().Unix()*1000, rf.me, args.Leaderid)
	} else {
		reply.Success = false
		return
	}
	if reply.Success && args.Entries != nil { //log加入其中
		for i := 0; i < len(args.Entries); i++ {
			if i+args.PrevLogIndex+1 < rf.Lastlogindex {
				if rf.logs[i+args.PrevLogIndex+1-rf.Snapshotinfo.SnapshotIndex].Term == args.Entries[i].Term && rf.logs[i+args.PrevLogIndex+1-rf.Snapshotinfo.SnapshotIndex].Logact == args.Entries[i].Logact {
					continue
				}
				rf.logs = rf.logs[:i+args.PrevLogIndex+1-rf.Snapshotinfo.SnapshotIndex]
				rf.Lastlogindex = i + args.PrevLogIndex + 1
			}
			info := args.Entries[i]
			rf.logs = append(rf.logs, info)
			rf.Lastlogindex++
		}
		reply.Cmatchindex = args.PrevLogIndex + len(args.Entries)
		log.Printf("node %d change cmatch %v Entrieslen %v become %v", rf.me, reply.Cmatchindex, len(args.Entries), rf.logs)
	} else if reply.Success && args.Entries == nil {
		reply.Cmatchindex = args.PrevLogIndex
	}
	rf.persist(false)
	if reply.Success && rf.commitIndex < args.LeaderCommit {
		if args.LeaderCommit > len(rf.logs)-1 {
			rf.commitIndex = len(rf.logs) - 1
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		rf.cond.Signal()
		log.Printf("%v %d add commit to %d in line 333", time.Now().UnixNano()/1e6-time.Now().Unix()*1000, rf.me, rf.commitIndex)
	}
	if !reply.Success && args.Entries != nil {
		reply.Failterm = args.PrevLogTerm
		var tt int
		if len(rf.logs) > args.PrevLogIndex-rf.Snapshotinfo.SnapshotIndex {
			tt = args.PrevLogIndex - rf.Snapshotinfo.SnapshotIndex
		} else {
			tt = len(rf.logs) - 1
		}
		for i := tt; i > 1; i-- {
			//for i, t := range rf.logs {
			if rf.logs[i].Term != rf.logs[tt].Term {
				reply.Failindex = i
				log.Printf("node %d failindex %v failterm %v tt %v ttterm %v term %d ------- log %v fail add fail addlog %v", rf.me, reply.Failindex, rf.logs[i].Term, tt, rf.logs[tt].Term, rf.currentTerm, rf.logs, args)
				break
			}
		}
	} else if reply.Success {
		log.Printf("node %d succes some term %d", rf.me, rf.currentTerm)
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
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
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
	rf.mu.Unlock()
	if isLeader {
		rf.mu.Lock()
		rf.logs = append(rf.logs, nlog{
			Term:   term,
			Logact: command})
		rf.Lastlogindex++
		log.Printf("%v %d term %d leader get log %v index %v", time.Now().UnixNano()/1e6-time.Now().Unix()*1000, rf.me, rf.currentTerm, command, rf.Lastlogindex)
		index = rf.Lastlogindex
		rf.mu.Unlock()
	}
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
	laslogindex := rf.Lastlogindex
	lastlogterm := rf.logs[laslogindex].Term
	var num int64
	num = 1
	n := len(rf.peers)
	log.Printf("%v %d start vote %d term %d in all %d", time.Now().UnixNano()/1e6-time.Now().Unix()*1000, rf.me, num, rf.currentTerm, n)
	wg := sync.WaitGroup{}
	rf.mu.Unlock()
	go rf.persist(true)
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
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(node, &args, &reply)
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if !ok {
					log.Printf("%v server %d term %d VoteCall failed to %d had vote %d", time.Now().UnixNano()/1e6-time.Now().Unix()*1000, me, rf.currentTerm, node, atomic.LoadInt64(&num))
					return
				}
				if reply.Term > args.Term {
					return
				}
				if reply.Votefor {
					atomic.AddInt64(&num, 1)
					log.Printf("%v %d term %d get vote form %d term %d logs%v", time.Now().UnixNano()/1e6-time.Now().Unix()*1000, me, rf.currentTerm, node, reply.Term, rf.logs)
					if int(atomic.LoadInt64(&num)) > n/2 && !rf.hasheat && rf.state == Candidate {
						log.Printf("%v serve %d become leader------- num:%d term:%d log:%v", time.Now().UnixNano()/1e6-time.Now().Unix()*1000, rf.me, num, rf.currentTerm, rf.logs)
						rf.state = Leader
						for i := 0; i < len(rf.peers); i++ {
							rf.nextIndex[i] = rf.Lastlogindex + 1
							rf.matchIndex[i] = 0
						}
					}
					//atomic.AddInt64(&num, 1)
				} else {
					log.Printf("%v %d term %d not get vote from %d", time.Now().UnixNano()/1e6-time.Now().Unix()*1000, me, rf.currentTerm, node)
				}
			}(node)
		}
	}
	wg.Wait()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == Leader {
		loglen := rf.Lastlogindex + 1
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
	numlog = 1
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	me := rf.me
	loglens := rf.Lastlogindex
	commit := rf.commitIndex
	log.Printf("%v %v term %d start send %d log %v nextindex%v", time.Now().UnixNano()/1e6-time.Now().Unix()*1000, rf.me, rf.currentTerm, loglens, rf.logs, rf.nextIndex)
	for i := 0; i < len(rf.peers); i++ {
		if rf.nextIndex[i] == rf.Snapshotinfo.SnapshotIndex && rf.me != i {
			args := InstallSnapshotArgs{
				Term:              rf.currentTerm,
				Leaderid:          rf.me,
				LastIncludedIndex: rf.Snapshotinfo.SnapshotIndex,
				lastIncludedTerm:  rf.Snapshotinfo.SnapshotTerm,
				Data:              rf.Snapshotinfo.Snapshot,
			}
			reply := InstallSnapshotReply{}
			ok := rf.sendInstallSnapshot(i, &args, &reply)
			if !ok {
				log.Printf("S%v %v sendInstallSnapshot failed to %d", time.Now().UnixNano()/1e6-time.Now().Unix()*1000, me, i)
				return
			}
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.state = Foller
				log.Printf("%v %d term %d become foller", time.Now().UnixNano()/1e6-time.Now().Unix()*1000, rf.me, rf.currentTerm)
				rf.mu.Unlock()
				return
			} else {
				rf.nextIndex[i]++

			}
		}
		prevlog = append(prevlog, rf.nextIndex[i]-1)
		prevterm = append(prevterm, rf.logs[prevlog[i]-rf.Snapshotinfo.SnapshotIndex].Term)
		entries = append(entries, nil)
		for t := rf.nextIndex[i]; t < rf.Lastlogindex; t++ {
			entries[i] = append(entries[i], rf.logs[t-rf.Snapshotinfo.SnapshotIndex])
		}
	}
	rf.mu.Unlock()
	go rf.persist(true)
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
				//log.Printf("%v %v sendlog to %d info Term,Leadid,PrevIndex,PrevTerm,Entries,LeadCommit%v", time.Now().UnixNano()/1e6-time.Now().Unix()*1000, me, node, args)
				ok := rf.sendAppendEntries(node, &args, &reply)
				if !ok {
					log.Printf("S%v %v sendlog failed to %d", time.Now().UnixNano()/1e6-time.Now().Unix()*1000, me, node)
					return
				}
				if reply.Term > args.Term {
					rf.mu.Lock()
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.state = Foller
					log.Printf("%v %d term %d become foller", time.Now().UnixNano()/1e6-time.Now().Unix()*1000, rf.me, rf.currentTerm)
					rf.mu.Unlock()
					return
				}
				rf.mu.Lock()
				if rf.state == Leader {
					if reply.Success && rf.logs[loglens].Term == rf.currentTerm {
						atomic.AddInt64(&numlog, 1)
						rf.nextIndex[node] = reply.Cmatchindex + 1
						log.Printf("%v %d recv %d log nextindex add %d to %d", time.Now().UnixNano()/1e6-time.Now().Unix()*1000, rf.me, node, len(args.Entries), rf.nextIndex)
						rf.matchIndex[node] = reply.Cmatchindex
						if atomic.LoadInt64(&numlog) > int64(num)/2 {
							if rf.state == Leader {
								log.Printf("node %d commitooo from %v to %v in 607", rf.me, rf.commitIndex, loglens)
								rf.commitIndex = loglens
								rf.cond.Signal()
							}
						}
					} else if !reply.Success && rf.state == Leader {
						if reply.Failindex > len(rf.logs)-1 {
							reply.Failindex = len(rf.logs) - 1
						}
						rf.nextIndex[node] = reply.Failindex
						if reply.Failindex == 0 {
							rf.nextIndex[node] = 1
						}
						log.Printf("%v %d set nextindex [%d] = %d next:%v", time.Now().UnixNano()/1e6-time.Now().Unix()*1000, rf.me, node, rf.nextIndex[node], rf.nextIndex)
					}
				}
				if rf.state == Leader {
					log.Printf("node is %v has log %v leaderhas %v ", node, rf.matchIndex[node], len(rf.logs)-1)
					if reply.Success && rf.matchIndex[node] > rf.commitIndex && rf.logs[rf.matchIndex[node]].Term == rf.currentTerm {
						var sb []int
						for i, _ := range rf.matchIndex {
							sb = append(sb, rf.matchIndex[i])
						}
						sort.Ints(sb)
						tt := len(rf.peers)/2 + 1
						if rf.state == Leader && rf.logs[sb[tt]].Term >= rf.currentTerm { //大部分一致出问题
							rf.commitIndex = sb[tt]
							rf.cond.Signal()
							log.Printf("node %d commitooo %v in 632", rf.me, rf.commitIndex)
						}
					}
				}
				rf.mu.Unlock()
				go rf.persist(true)
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
				log.Printf("%v %d became Candidate log%v", time.Now().UnixNano()/1e6-time.Now().Unix()*1000, rf.me, rf.logs)
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
			t = rand.Intn(200) + 300
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

///home/yubtin/Desktop/codes/6.824/src/raft/raft.go
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
	rf.Snapshotinfo.SnapshotIndex = 0
	rf.Snapshotinfo.SnapshotTerm = 0
	rf.Snapshotinfo.Snapshot = nil
	rf.votedFor = -1
	rf.state = Foller
	rf.hasvote = true
	rf.hasheat = true
	rf.cond = sync.NewCond(&rf.mu)
	rf.logs = append(rf.logs, nlog{0, 123})
	rf.Lastlogindex = 0
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex = append(rf.matchIndex, 0)
		rf.nextIndex = append(rf.nextIndex, 1)
	}

	rf.readPersist(persister.ReadRaftState())
	rf.readSnapshotPersist(persister.ReadSnapshot())
	log.Printf("node %d start ====", rf.me)
	go func() { //通过applech 发送AppleMsg消息
		for {
			apply := ApplyMsg{}
			rf.mu.Lock()
			for rf.commitIndex == rf.lastapplied {
				rf.cond.Wait()
			}
			for rf.lastapplied < rf.commitIndex {
				rf.lastapplied++
				apply.Command = rf.logs[rf.lastapplied].Logact
				apply.CommandIndex = rf.lastapplied
				apply.CommandValid = true
				applyCh <- apply
				log.Printf("%v node %d log apploginde++to %d log %v", time.Now().UnixNano()/1e6-time.Now().Unix()*1000, rf.me, rf.lastapplied, apply.Command)
			}
			log.Printf("%d term %d logs %v comitindex%d", rf.me, rf.currentTerm, rf.logs, rf.commitIndex)
			rf.mu.Unlock()
		}
	}()
	// start ticker goroutine to start elections
	go rf.ticker()
	return rf
}
