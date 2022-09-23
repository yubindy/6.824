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
	"6.824/labgob"
	"6.824/labrpc"
	"bytes"
	"log"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const (
	Foller    int = 1
	Candidate int = 2
	Leader    int = 3
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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
	CurrentTerm   int
	VotedFor      int
	Logs          []nlog
	Lastlogindex  int
	Snapshotterm  int
	Snapshotindex int
}

type Snapshots struct {
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	Mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	Persister *Persister          // Object to hold this peer's persisted state
	Me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	//2a
	state       int
	currentTerm int
	votedFor    int
	hasheat     bool
	hasvote     bool
	haslog      bool

	Logs        []nlog
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
	rf.Mu.Lock()
	term = rf.currentTerm
	if rf.state == Leader {
		isleader = true
	} else {
		isleader = false
	}
	rf.Mu.Unlock()
	return term, isleader
}
func (rf *Raft) GetSapplen() int {
	rf.Mu.Lock()
	t := rf.lastapplied
	rf.Mu.Unlock()
	return t
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) Persist(lock bool) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if lock {
		rf.Mu.Lock()
	}
	//if rf.state == Leader {
	//	//log.Printf("||||node %v statue %v start persist term %v Lastindex %v Snapshotindex %v log %v", rf.Me, rf.state, rf.currentTerm, rf.Lastlogindex, rf.Snapshotinfo.SnapshotIndex, rf.Logs)
	//}
	rf.Persistinfo.CurrentTerm = rf.currentTerm
	rf.Persistinfo.VotedFor = rf.votedFor
	rf.Persistinfo.Logs = rf.Logs
	rf.Persistinfo.Lastlogindex = rf.Lastlogindex
	rf.Persistinfo.Snapshotterm = rf.Snapshotinfo.SnapshotTerm
	rf.Persistinfo.Snapshotindex = rf.Snapshotinfo.SnapshotIndex
	e.Encode(rf.Persistinfo)
	data := w.Bytes()
	rf.Persister.SaveStateAndSnapshot(data, rf.Snapshotinfo.Snapshot)
	if lock {
		rf.Mu.Unlock()
	}
}

// restore previously persisted state.
func (rf *Raft) readSnapshotPersist(data []byte) {
	if data == nil { // bootstrap without any state?
		return
	}
	rf.Mu.Lock()
	rf.Snapshotinfo.Snapshot = data
	rf.Mu.Unlock()
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var Persistinfo persistent
	if d.Decode(&Persistinfo) != nil {
	} else {
		rf.Mu.Lock()
		rf.currentTerm = Persistinfo.CurrentTerm
		rf.votedFor = Persistinfo.VotedFor
		rf.Logs = Persistinfo.Logs
		rf.Lastlogindex = Persistinfo.Lastlogindex
		rf.Snapshotinfo.SnapshotTerm = Persistinfo.Snapshotterm
		rf.Snapshotinfo.SnapshotIndex = Persistinfo.Snapshotindex
		rf.Mu.Unlock()
		go func() {
			rf.Mu.Lock()
			if rf.Snapshotinfo.SnapshotIndex > 0 {
				applyMsg := ApplyMsg{
					SnapshotValid: true,
					Snapshot:      rf.Snapshotinfo.Snapshot,
					SnapshotIndex: rf.Snapshotinfo.SnapshotIndex,
					SnapshotTerm:  rf.Snapshotinfo.SnapshotTerm,
				}
				//log.Printf("node %v addchannelin 429 lastapplied= %v", rf.Me, rf.Snapshotinfo.SnapshotIndex)
				rf.applyCh <- applyMsg
				rf.lastapplied = rf.Snapshotinfo.SnapshotIndex
				rf.commitIndex = rf.Snapshotinfo.SnapshotIndex
			}
			rf.Mu.Unlock()
		}()
	}
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
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
	rf.Mu.Lock()
	rf.Snapshotinfo.Snapshot = snapshot
	//t := rf.Logs[index-rf.Snapshotinfo.SnapshotIndex]
	rf.Logs = rf.Logs[index-rf.Snapshotinfo.SnapshotIndex:]
	rf.Logs[0].Logact = 123
	rf.Snapshotinfo.SnapshotIndex = index
	rf.Snapshotinfo.SnapshotTerm = rf.Logs[index-rf.Snapshotinfo.SnapshotIndex].Term
	////log.Printf("node %d Snapshot index: %v log[head] %v log %v ", rf.Me, index, t, rf.Logs)
	rf.Mu.Unlock()
	rf.Persist(true)
	go func() {
		rf.Mu.Lock()
		if rf.Snapshotinfo.SnapshotIndex > 0 {
			applyMsg := ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.Snapshotinfo.Snapshot,
				SnapshotIndex: rf.Snapshotinfo.SnapshotIndex,
				SnapshotTerm:  rf.Snapshotinfo.SnapshotTerm,
			}
			////log.Printf("node %v addchannel 226 Lastapplied = %v", rf.Me, rf.Snapshotinfo.SnapshotIndex)
			rf.lastapplied = rf.Snapshotinfo.SnapshotIndex
			rf.applyCh <- applyMsg
		}
		rf.Mu.Unlock()
	}()
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	Candidateid  int
	Lastlogindex int //日志最后索引
	Lastlogterm  int //日志最后任期号
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
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
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}
type InstallSnapshotReply struct {
	Term int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B)
	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	reply.Term = rf.currentTerm
	if rf.currentTerm <= args.Term {
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.votedFor = -1
			rf.state = Foller
		}
		if rf.Logs[len(rf.Logs)-1].Term < args.Lastlogterm && rf.votedFor == -1 { //先比较term然后比len
			reply.Votefor = true
			rf.hasvote = true
			rf.state = Foller
			rf.votedFor = args.Candidateid
			//log.Printf("%d Term %v give vote to %d inline 297  mylog %v", rf.Me, rf.currentTerm, args.Candidateid, rf.Logs)
		} else if rf.Logs[len(rf.Logs)-1].Term == args.Lastlogterm && rf.Lastlogindex <= args.Lastlogindex && rf.votedFor == -1 && rf.state == Foller {
			//log.Printf("%d Term %v give vote to %d inline 299  mylog %v", rf.Me, rf.currentTerm, args.Candidateid, rf.Logs)
			reply.Votefor = true
			rf.hasvote = true
			rf.state = Foller
			rf.votedFor = args.Candidateid
		} else {
			reply.Votefor = false
			if rf.state == Leader {
				rf.hasheat = true
			}
			//log.Printf("why not got vote me:%v berterm: %v len:%v logsterm: %v to:%v", rf.Me, reply.Term, len(rf.Logs)-1, rf.Logs[len(rf.Logs)-1].Term, args)
			//rf.currentTerm = args.Term
		}
		rf.Persist(false)
	} else {
		args.Term = rf.currentTerm
		reply.Votefor = false
		//log.Printf("why not 226 got vote me:%v %v %v to:%v", rf.Me, rf.currentTerm, len(rf.Logs)-1, args)
	}
}
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) { //实现心跳和附加日志
	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	reply.Term = rf.currentTerm
	////log.Printf("node %d term %d  Lastindex %v Snapshotindex %v recvterm %d recvfrom %d log %v recvPrevindex %v recvlog %v", rf.Me, rf.currentTerm, rf.Lastlogindex, rf.Snapshotinfo.SnapshotIndex, args.Term, args.Leaderid, rf.Logs, args.PrevLogIndex, args.Entries)
	if args.PrevLogTerm == -1 || args.PrevLogIndex < rf.Snapshotinfo.SnapshotIndex {
		//log.Printf("node %v failein329 Snapshotindex %v Previndex %v", rf.Me, rf.Snapshotinfo.SnapshotIndex, args.PrevLogIndex)
		reply.Success = false
		reply.Failindex = 0
		return
	}
	reply.Failindex = rf.Snapshotinfo.SnapshotIndex + 1
	if rf.currentTerm <= args.Term { //线判断任期
		rf.hasheat = true
		rf.state = Foller
		//log.Printf("node %v become foller", rf.Me)
		rf.currentTerm = args.Term
		if rf.Lastlogindex < args.PrevLogIndex {
			reply.Success = false
			//log.Printf("node %v failein338", rf.Me)
		} else {
			//log.Printf("node %v Snapindex %v recv Previndex %v", rf.Me, rf.Snapshotinfo.SnapshotIndex, args.PrevLogIndex)
			if rf.Logs[args.PrevLogIndex-rf.Snapshotinfo.SnapshotIndex].Term == args.PrevLogTerm {
				rf.hasheat = true
				reply.Success = true
			} else {
				reply.Success = false
				//log.Printf("node %v failein346", rf.Me)
			}
		}
	} else {
		reply.Success = false
		//log.Printf("node %v failein351", rf.Me)
		return
	}
	if reply.Success && args.Entries != nil { //log加入其中
		for i := 0; i < len(args.Entries); i++ {
			if i+args.PrevLogIndex+1 <= rf.Lastlogindex {
				if rf.Logs[i+args.PrevLogIndex+1-rf.Snapshotinfo.SnapshotIndex].Term == args.Entries[i].Term && rf.Logs[i+args.PrevLogIndex+1-rf.Snapshotinfo.SnapshotIndex].Logact == args.Entries[i].Logact {
					continue
				}
				rf.Logs = rf.Logs[:i+args.PrevLogIndex+1-rf.Snapshotinfo.SnapshotIndex]
				rf.Lastlogindex = i + args.PrevLogIndex //try other +1
			}
			info := args.Entries[i]
			rf.Logs = append(rf.Logs, info)
			rf.Lastlogindex++
		}
		reply.Cmatchindex = args.PrevLogIndex + len(args.Entries)
		//log.Printf("node %d change cmatch %v Entrieslen %v become %v", rf.Me, reply.Cmatchindex, len(args.Entries), rf.Logs)
	} else if reply.Success && args.Entries == nil {
		reply.Cmatchindex = args.PrevLogIndex
	}
	rf.Persist(false)
	if !reply.Success && args.PrevLogIndex == rf.Snapshotinfo.SnapshotIndex {
		reply.Failindex = rf.Snapshotinfo.SnapshotIndex - 1
		//log.Printf("node %v nextforce", rf.Me)
	}
	////log.Printf("node %d reply.Success %v reply.Failindex %v commitIndex %v args.Leaderid %v", rf.Me, reply.Success, reply.Failindex, rf.commitIndex, args.Leaderid)
	if reply.Success && rf.commitIndex < args.LeaderCommit {
		if args.LeaderCommit > rf.Lastlogindex {
			rf.commitIndex = rf.Lastlogindex
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		rf.cond.Signal()
		//log.Printf("%v %d add commit to %d in line 333", time.Now().UnixNano()/1e6-time.Now().Unix()*1000, rf.Me, rf.commitIndex)
	}
	if !reply.Success && args.Entries != nil && args.PrevLogIndex != rf.Snapshotinfo.SnapshotIndex {
		reply.Failterm = args.PrevLogTerm
		var tt int
		if len(rf.Logs)-1 > args.PrevLogIndex-rf.Snapshotinfo.SnapshotIndex {
			tt = args.PrevLogIndex - rf.Snapshotinfo.SnapshotIndex
		} else {
			tt = len(rf.Logs) - 1
		}
		for i := tt; i > 1; i-- {
			if rf.Logs[i].Term != rf.Logs[tt].Term {
				reply.Failindex += i
				//log.Printf("node %d failindex %v failterm %v tt %v ttterm %v term %d ------- log %v fail add fail addlog %v", rf.Me, reply.Failindex, rf.Logs[i].Term, tt, rf.Logs[tt].Term, rf.currentTerm, rf.Logs, args)
				break
			}
		}
	} else if reply.Success {
		//log.Printf("node %d succes some term %d", rf.Me, rf.currentTerm)
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.Mu.Lock()
	reply.Term = rf.currentTerm
	if args.Term >= rf.currentTerm && args.LastIncludedIndex > rf.Snapshotinfo.SnapshotIndex {
		rf.state = Foller
		rf.hasheat = true
		//log.Printf("node %v become foller", rf.Me)
		rf.Snapshotinfo.SnapshotIndex = args.LastIncludedIndex
		rf.Snapshotinfo.SnapshotTerm = args.LastIncludedTerm
		rf.Snapshotinfo.Snapshot = args.Data
		if rf.Lastlogindex <= args.LastIncludedIndex {
			rf.Logs = nil
			rf.Logs = append(rf.Logs, nlog{args.LastIncludedTerm, nil})
		} else {
			rf.Logs = rf.Logs[rf.Lastlogindex-args.LastIncludedIndex:]
		}
		rf.Lastlogindex = args.LastIncludedIndex
		//log.Printf("node %v InstallSnapshot from %v SnapshotIndex %v logs %v", rf.Me, args.LeaderId, rf.Snapshotinfo.SnapshotIndex, rf.Logs)
		rf.Persist(false)
		rf.Mu.Unlock()
		go func() {
			rf.Mu.Lock()
			if rf.Snapshotinfo.SnapshotIndex != -1 {
				applyMsg := ApplyMsg{
					SnapshotValid: true,
					Snapshot:      rf.Snapshotinfo.Snapshot,
					SnapshotIndex: rf.Snapshotinfo.SnapshotIndex,
					SnapshotTerm:  rf.Snapshotinfo.SnapshotTerm,
				}
				//log.Printf("node %v addchannelin 429 lastapplied= %v", rf.Me, rf.Snapshotinfo.SnapshotIndex)
				rf.applyCh <- applyMsg
				rf.lastapplied = rf.Snapshotinfo.SnapshotIndex
				rf.commitIndex = rf.Snapshotinfo.SnapshotIndex
				rf.Mu.Unlock()
			}
		}()
	} else {
		//log.Printf("node %v recv someShapshot from %v", rf.Me, args.LeaderId)
		rf.Mu.Unlock()
	}
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	term, isLeader = rf.GetState()
	if isLeader {
		rf.Mu.Lock()
		rf.Logs = append(rf.Logs, nlog{
			Term:   term,
			Logact: command})
		rf.Lastlogindex++
		//log.Printf("SSSS %v %d term %d leader get index %v log %v", time.Now().UnixNano()/1e6-time.Now().Unix()*1000, rf.Me, rf.currentTerm, rf.Lastlogindex, command)
		index = rf.Lastlogindex
		rf.Mu.Unlock()

	}
	// Your code here (2B).
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
func (rf *Raft) startvote() {
	rf.Mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.Me
	var num int64
	num = 1
	n := len(rf.peers)
	me := rf.Me
	currentTerm := rf.currentTerm
	lastlogterm := rf.Logs[len(rf.Logs)-1].Term
	lastlogindex := rf.Lastlogindex
	//log.Printf("%v %d start vote %d term %d in all %d", time.Now().UnixNano()/1e6-time.Now().Unix()*1000, rf.Me, num, rf.currentTerm, n)
	wg := sync.WaitGroup{}
	rf.Mu.Unlock()
	rf.Persist(true)
	wg.Add(n - 1)
	for node := range rf.peers {
		if node != me {
			go func(node int) {
				defer wg.Done()
				args := RequestVoteArgs{
					Term:         currentTerm,
					Candidateid:  me,
					Lastlogindex: lastlogindex,
					Lastlogterm:  lastlogterm,
				}
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(node, &args, &reply)
				rf.Mu.Lock()
				defer rf.Mu.Unlock()
				if !ok {
					//log.Printf("%v server %d term %d VoteCall failed to %d had vote %d", time.Now().UnixNano()/1e6-time.Now().Unix()*1000, me, rf.currentTerm, node, atomic.LoadInt64(&num))
					return
				}
				if reply.Term > args.Term {
					//log.Printf("node %v startvote to %v but,term litte become foller", rf.Me, node)
					rf.state = Foller
					return
				}
				if reply.Votefor {
					atomic.AddInt64(&num, 1)
					//log.Printf("%v %d term %d get vote form %d term %d logs%v", time.Now().UnixNano()/1e6-time.Now().Unix()*1000, me, rf.currentTerm, node, reply.Term, rf.Logs)
					if int(atomic.LoadInt64(&num)) > n/2 && !rf.hasheat && rf.state == Candidate {
						log.Printf("%v serve %d become leader------- num:%d term:%d lastapplied %v log:%v", time.Now().UnixNano()/1e6-time.Now().Unix()*1000, rf.Me, num, rf.currentTerm, rf.lastapplied, rf.Logs)
						rf.state = Leader
						for i := 0; i < len(rf.peers); i++ {
							rf.nextIndex[i] = rf.Lastlogindex + 1
							rf.matchIndex[i] = 0
						}
					}
					//atomic.AddInt64(&num, 1)
				} else {
					//log.Printf("%v %d term %d not get vote from %d", time.Now().UnixNano()/1e6-time.Now().Unix()*1000, me, rf.currentTerm, node)
				}
			}(node)
		}
	}
	wg.Wait()
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	//if rf.state == Leader {
	//	for i := 0; i < len(rf.peers); i++ {
	//		rf.nextIndex[i] = rf.Lastlogindex + 1
	//	}
	//}
}
func (rf *Raft) sendlog() {
	var prevlog []int
	var prevterm []int
	var numlog int64
	var entries [][]nlog
	numlog = 1
	rf.Mu.Lock()
	//log.Printf("%v %v term %d start send Lastindex %d log %v nextindex%v Snapshotindex%v", time.Now().UnixNano()/1e6-time.Now().Unix()*1000, rf.Me, rf.currentTerm, rf.Lastlogindex, rf.Logs, rf.nextIndex, rf.Snapshotinfo.SnapshotIndex)
	for i := 0; i < len(rf.peers); i++ {
		if rf.Me == i {
			rf.nextIndex[i] = rf.Lastlogindex + 1
		}
		prevlog = append(prevlog, rf.nextIndex[i]-1)
		if prevlog[i] < rf.Snapshotinfo.SnapshotIndex {
			prevterm = append(prevterm, -1)
		} else {
			prevterm = append(prevterm, rf.Logs[prevlog[i]-rf.Snapshotinfo.SnapshotIndex].Term)
		}
		entries = append(entries, nil)
		st := 1
		if rf.nextIndex[i] > rf.Snapshotinfo.SnapshotIndex /*&& rf.nextIndex[i] <= rf.Lastlogindex*/ {
			st = rf.nextIndex[i] - rf.Snapshotinfo.SnapshotIndex
		}
		////log.Printf("node %v term %v addlog from %v to %v ", i, st, rf.currentTerm, rf.Lastlogindex-rf.Snapshotinfo.SnapshotIndex)
		for t := st; t <= rf.Lastlogindex-rf.Snapshotinfo.SnapshotIndex; t++ {
			entries[i] = append(entries[i], rf.Logs[t])
		}
	}
	commit := rf.commitIndex
	me := rf.Me
	currentTerm := rf.currentTerm
	Snapshotinfo := rf.Snapshotinfo
	num := len(rf.peers) - 1
	rf.Mu.Unlock()
	rf.Persist(true)
	wg := sync.WaitGroup{}
	wg.Add(num)
	for node := range rf.peers {
		if node != me {
			go func(node int) {
				defer wg.Done()
				if prevlog[node]+1 <= Snapshotinfo.SnapshotIndex {
					args := InstallSnapshotArgs{
						Term:              currentTerm,
						LeaderId:          me,
						LastIncludedIndex: Snapshotinfo.SnapshotIndex,
						LastIncludedTerm:  Snapshotinfo.SnapshotTerm,
						Data:              Snapshotinfo.Snapshot,
					}
					reply := InstallSnapshotReply{}
					////log.Printf("node %v start send InstallSnapshot to %v", me, node)
					ok := rf.sendInstallSnapshot(node, &args, &reply)
					if !ok {
						////log.Printf("%v %v sendfail InstallSnapshot to %d", time.Now().UnixNano()/1e6-time.Now().Unix()*1000, me, node)
						return
					} else {
						prevterm[node] = Snapshotinfo.SnapshotTerm
						prevlog[node] = Snapshotinfo.SnapshotIndex
						////log.Printf("%v send InstallSnapshot success  node %v should nextindex %v", me, node, prevlog[node]+1)
					}
				}
				args := AppendEntriesArgs{
					Term:         currentTerm,
					Leaderid:     me,
					PrevLogIndex: prevlog[node],
					PrevLogTerm:  prevterm[node],
					Entries:      entries[node],
					LeaderCommit: commit,
				}
				reply := AppendEntriesReply{}
				_, b := rf.GetState()
				if !b {
					return
				}
				ok := rf.sendAppendEntries(node, &args, &reply)
				if !ok {
					//log.Printf("%v %v sendlog failed to %d", time.Now().UnixNano()/1e6-time.Now().Unix()*1000, me, node)
					return
				} else {
					////log.Printf("node %v sendinstall to %v success", me, node)
				}
				defer rf.Mu.Unlock()
				rf.Mu.Lock()
				if rf.nextIndex[node] != prevlog[node]+1 {
					rf.nextIndex[node] = prevlog[node] + 1
				}
				if reply.Term > args.Term {
					//log.Printf("%v %d term %d become follerin 696 replyterm %v", time.Now().UnixNano()/1e6-time.Now().Unix()*1000, rf.Me, args.Term, reply.Term)
					rf.hasheat = true
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.state = Foller
					return
				}
				if rf.state == Leader {
					if reply.Success {
						atomic.AddInt64(&numlog, 1)
						rf.nextIndex[node] = reply.Cmatchindex + 1
						////log.Printf("%v %d recv %d log nextindex add %d to %d", time.Now().UnixNano()/1e6-time.Now().Unix()*1000, rf.Me, node, len(args.Entries), rf.nextIndex)
						rf.matchIndex[node] = reply.Cmatchindex
						if atomic.LoadInt64(&numlog) > int64(num)/2 {
							if rf.state == Leader && rf.commitIndex < reply.Cmatchindex {
								//log.Printf("node %d  form %v add commit to %v", rf.Me, rf.commitIndex, reply.Cmatchindex)
								rf.commitIndex = rf.matchIndex[node]
								rf.cond.Signal()
							}
						}
					} else if !reply.Success && rf.state == Leader {
						if reply.Failindex > rf.Lastlogindex {
							reply.Failindex = rf.Lastlogindex
						}
						rf.nextIndex[node] = reply.Failindex
						//log.Printf("%v %d set nextindex [%d] = %d next:%v", time.Now().UnixNano()/1e6-time.Now().Unix()*1000, rf.Me, node, rf.nextIndex[node], rf.nextIndex)
					}
				}
				if rf.state == Leader {
					////log.Printf("node is %v has log %v leaderhas %v %v ", me, rf.matchIndex[node], len(rf.Logs)-1, rf.Logs)
					if reply.Success && rf.matchIndex[node] > rf.commitIndex && rf.Logs[rf.matchIndex[node]-rf.Snapshotinfo.SnapshotIndex-1].Term == rf.currentTerm {
						var sb []int
						for i, _ := range rf.matchIndex {
							sb = append(sb, rf.matchIndex[i])
						}
						sort.Ints(sb)
						tt := len(rf.peers)/2 - (len(rf.peers)+1)%2
						if rf.state == Leader && sb[tt]-rf.Snapshotinfo.SnapshotIndex-1 >= 0 && rf.Logs[sb[tt]-rf.Snapshotinfo.SnapshotIndex-1].Term >= rf.currentTerm { //大部分一致出问题
							rf.commitIndex = sb[tt]
							rf.cond.Signal()
							//log.Printf("node %d add commit to %v in 632 sb %v", rf.Me, rf.commitIndex, sb)
						}
					}
				}
				rf.Persist(false)
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
		rf.Mu.Lock()
		state := rf.state
		rf.Mu.Unlock()
		if state == Foller {
			rf.Mu.Lock()
			heat := rf.hasheat
			vote := rf.hasvote
			if heat == false && vote == false {
				//log.Printf("%v %d became Candidate log%v", time.Now().UnixNano()/1e6-time.Now().Unix()*1000, rf.Me, rf.Logs)
				rf.state = Candidate
				rf.Mu.Unlock()
				go rf.startvote()
			} else {
				rf.Mu.Unlock()
			}
		} else if state == Candidate {
			go rf.startvote()
		} else if state == Leader {
			go rf.sendlog()
		}
		rf.Mu.Lock()
		rf.hasvote = false
		rf.hasheat = false
		state = rf.state
		rf.Mu.Unlock()
		rand.Seed(time.Now().UnixNano())
		if state != Leader {
			t = rand.Intn(200) + 300
			for i := 0; i < t; i++ {
				rf.Mu.Lock()
				if rf.state == Candidate && (rf.hasheat == true || rf.hasvote == true) {
					rf.state = Foller
					//log.Printf("%v %d become foller ", time.Now().UnixNano()/1e6-time.Now().Unix()*1000, rf.Me)
					rf.Mu.Unlock()
					break
				} else if rf.state == Foller && (rf.hasheat == true || rf.hasvote == true) {
					rf.Mu.Unlock()
					break
				} else if rf.state == Leader {
					rf.Mu.Unlock()
					break
				}
				rf.Mu.Unlock()
				time.Sleep(time.Millisecond)
			}
		} else {
			t = 90
			for i := 0; i < t; i++ {
				rf.Mu.Lock()
				if rf.hasheat || rf.hasvote {
					rf.state = Foller
					//log.Printf("%v leader %d become foller ", time.Now().UnixNano()/1e6-time.Now().Unix()*1000, rf.Me)
					rf.Mu.Unlock()
					break
				}
				rf.Mu.Unlock()
				time.Sleep(time.Millisecond)
			}
		}
	}
	// Your code here to check if a leader election should
	// be started and to randomize sleeping time using
	// time.Sleep().

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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.Persister = persister
	rf.Me = me
	rf.currentTerm = 0
	rf.Snapshotinfo.SnapshotIndex = 0
	rf.Snapshotinfo.SnapshotTerm = 0
	rf.Snapshotinfo.Snapshot = nil
	rf.votedFor = -1
	rf.state = Foller
	rf.hasvote = true
	rf.hasheat = true
	rf.cond = sync.NewCond(&rf.Mu)
	rf.Logs = append(rf.Logs, nlog{0, 123})
	rf.Lastlogindex = 0
	rf.applyCh = applyCh
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex = append(rf.matchIndex, 0)
		rf.nextIndex = append(rf.nextIndex, 1)
	}
	rf.readSnapshotPersist(persister.ReadSnapshot())
	rf.readPersist(persister.ReadRaftState())
	//log.Printf("node %d start ====", rf.Me)
	go func() { //通过applech 发送AppleMsg消息
		for rf.killed() == false {
			apply := ApplyMsg{}
			rf.Mu.Lock()
			for rf.commitIndex == rf.lastapplied || rf.Logs[rf.commitIndex-rf.Snapshotinfo.SnapshotIndex].Term < rf.currentTerm || rf.lastapplied < rf.Snapshotinfo.SnapshotIndex {
				//log.Printf("Commit in 868 commit.Term:%v Trueterm:%v ",rf.Logs[rf.commitIndex-rf.Snapshotinfo.SnapshotIndex].Term,rf.currentTerm)
				//log.Printf("Commit in 869 lastapp:%v commitindex:%v Snapshotindex:%v",rf.lastapplied, rf.commitIndex,rf.Snapshotinfo.SnapshotIndex)
				rf.cond.Wait()
			}
			//log.Printf("Commit in 871 lastapp:%v commitindex:%v Snapshotindex:%v",rf.lastapplied, rf.commitIndex,rf.Snapshotinfo.SnapshotIndex)
			////log.Printf("mpde %v should commitlog to %v Lastapplied %v Snapshotindex %v", rf.Me, rf.commitIndex, rf.lastapplied, rf.Snapshotinfo.SnapshotIndex)
			for rf.lastapplied < rf.commitIndex && rf.lastapplied >= rf.Snapshotinfo.SnapshotIndex {
				rf.lastapplied++
				apply.Command = rf.Logs[rf.lastapplied-rf.Snapshotinfo.SnapshotIndex].Logact
				apply.CommandIndex = rf.lastapplied
				apply.CommandValid = true
				//log.Printf("%v node %d log apploginde--to %d log %v", time.Now().UnixNano()/1e6-time.Now().Unix()*1000, me, apply.CommandIndex, apply.Command)
				rf.Mu.Unlock()
				applyCh <- apply
				rf.Mu.Lock()
				log.Printf("%v node %d log apploginde++to %d log %v--logs ", time.Now().UnixNano()/1e6-time.Now().Unix()*1000, me, apply.CommandIndex, apply.Command)
			}
			////log.Printf("%d term %d logs %v comitindex%d", rf.Me, rf.currentTerm, rf.Logs, rf.commitIndex)
			rf.Mu.Unlock()
		}
	}()
	// start ticker goroutine to start elections
	go rf.ticker()
	return rf
}
