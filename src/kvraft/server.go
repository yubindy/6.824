package kvraft

import (
	"bytes"
	"log"
	"time"

	"sync"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	// sync "github.com/sasha-s/go-deadlock"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	Completed     = "Completed"
	InProgress    = "InProgress"
	ErrorOccurred = "ErrorOccurred"
	ErrorTimeDeny = "ErrorTimeDeny"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type  string
	Key   string
	Value string
	Index int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	storage    map[string]string
	cond       *sync.Cond
	record     map[int64]RequestInfo
	StartTime  time.Time
	StartTimer int64
	Index      int

	ApplyIndex    int
	SnapshotTerm  int
	SnapshotIndex int
}

type RequestInfo struct {
	Status string
	Rindex int
	// Type   string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	if _, ok := kv.record[kv.StartTimer]; !ok {
		reply.Err = ErrorTimeDeny
		kv.mu.Unlock()
		return
	}
	for k, v := range kv.record {
		if k == args.Index {
			if v.Status == Completed {
				if _, ok := kv.storage[args.Key]; ok {
					reply.Err = OK
					reply.Value = kv.storage[args.Key]
				} else {
					reply.Err = ErrNoKey
					reply.Value = ""
				}
			} else if v.Status == InProgress {
				for kv.record[args.Index].Status == InProgress {
					kv.cond.Wait()
				}
				if kv.record[args.Index].Status == Completed {
					if _, ok := kv.storage[args.Key]; ok {
						reply.Err = OK
						reply.Value = kv.storage[args.Key]
					} else {
						reply.Err = ErrNoKey
						reply.Value = ""
					}
				} else if kv.record[args.Index].Status == ErrorOccurred {
					// delete(kv.record, args.Index)
					reply.Err = ErrorOccurred
				}
			} else if v.Status == ErrorOccurred {
				break
			}
			kv.mu.Unlock()
			return
		}
	}

	operation := Op{
		Type:  "Get",
		Key:   args.Key,
		Index: args.Index,
	}
	// fmt.Println("----", kv.storage)
	i, _, isLeader := kv.rf.Start(operation)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	var entry RequestInfo
	entry.Rindex = i
	entry.Status = InProgress
	kv.record[args.Index] = entry

	for kv.record[args.Index].Status == InProgress {
		kv.cond.Wait()
	}
	if kv.record[args.Index].Status == ErrorOccurred {
		reply.Err = ErrorOccurred
	} else if kv.record[args.Index].Status == Completed {
		if _, ok := kv.storage[args.Key]; ok {
			reply.Err = OK
			reply.Value = kv.storage[args.Key]
		} else {
			reply.Err = ErrNoKey
			reply.Value = ""
		}
	}
	kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	if _, ok := kv.record[kv.StartTimer]; !ok {
		reply.Err = ErrorTimeDeny
		kv.mu.Unlock()
		return
	}
	for k, v := range kv.record {
		if k == args.Index {
			if v.Status == Completed {
				reply.Err = OK
			} else if v.Status == InProgress {
				for kv.record[args.Index].Status == InProgress {
					kv.cond.Wait()
				}
				if kv.record[args.Index].Status == Completed {
					reply.Err = OK
				} else if kv.record[args.Index].Status == ErrorOccurred {
					reply.Err = ErrorOccurred
				}
			} else if v.Status == ErrorOccurred {
				break
			}
			kv.mu.Unlock()
			return
		}
	}

	operation := Op{
		Type:  args.Op,
		Key:   args.Key,
		Value: args.Value,
		Index: args.Index,
	}
	i, _, isleader := kv.rf.Start(operation)
	if !isleader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	var req RequestInfo
	req.Status = InProgress
	req.Rindex = i
	kv.record[args.Index] = req
	for kv.record[args.Index].Status == InProgress {
		kv.cond.Wait()
	}
	if kv.record[args.Index].Status == ErrorOccurred {
		reply.Err = ErrorOccurred
	} else if kv.record[args.Index].Status == Completed {
		reply.Err = OK
	}
	kv.mu.Unlock()
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.storage = make(map[string]string)
	kv.cond = sync.NewCond(&kv.mu)
	// sync.Opts.DeadlockTimeout = time.Second

	kv.record = make(map[int64]RequestInfo)
	kv.StartTime = time.Now()
	kv.StartTimer = -1
	go kv.Apply()
	go kv.Timer()
	go kv.Snap()
	return kv
}

func (kv *KVServer) Apply() {
	for entry := range kv.applyCh {
		if entry.CommandValid {
			kv.mu.Lock()
			if entry.Command == nil {
				continue
			}
			if entry.CommandIndex <= kv.ApplyIndex {
				continue
			}
			op := (entry.Command).(Op)
			if op.Type == "Put" {
				kv.storage[op.Key] = op.Value
			} else if op.Type == "Append" {
				kv.storage[op.Key] += op.Value
			} else if op.Type == "LeaderTimer" {
				if kv.Index == entry.CommandIndex || kv.StartTimer == -1 {
					kv.StartTimer = op.Index
				}
			}
			for preIndex, preStatus := range kv.record {
				if preStatus.Rindex == entry.CommandIndex {
					if preIndex != op.Index {
						var req1 RequestInfo
						req1.Rindex = preStatus.Rindex
						req1.Status = ErrorOccurred
						// req1.Type = v.Type
						kv.record[preIndex] = req1
					}
				}
			}

			kv.ApplyIndex = entry.CommandIndex
			var req RequestInfo
			req.Rindex = entry.CommandIndex
			req.Status = Completed
			// req.Type = op.Type
			kv.record[op.Index] = req
			kv.mu.Unlock()
			kv.cond.Broadcast()
		} else if entry.SnapshotValid {
			kv.mu.Lock()
			kv.SnapshotIndex = entry.SnapshotIndex
			kv.SnapshotTerm = entry.SnapshotTerm
			r := bytes.NewBuffer(entry.Snapshot)
			d := labgob.NewDecoder(r)
			var index int
			storage := make(map[string]string)
			record := make(map[int64]RequestInfo)
			if d.Decode(&index) != nil || d.Decode(&storage) != nil || d.Decode(&record) != nil {
			} else {
				for k, v := range storage {
					kv.storage[k] = v
				}
				for k, v := range kv.record {
					if v.Rindex <= index && v.Status == InProgress {
						var req1 RequestInfo
						req1.Rindex = v.Rindex
						req1.Status = ErrorOccurred
						// req1.Type = v.Type
						kv.record[k] = req1
					}
				}
				for k, v := range record {
					kv.record[k] = v
				}
				kv.ApplyIndex = entry.SnapshotIndex
				kv.SnapshotIndex = entry.SnapshotIndex
			}
			kv.mu.Unlock()
			kv.cond.Broadcast()
		}

	}
}

func (kv *KVServer) Timer() {
	for {
		if kv.killed() {
			return
		}
		kv.mu.Lock()
		if kv.StartTimer != -1 {
			kv.mu.Unlock()
			break
		}
		kv.mu.Unlock()
		Operation := Op{
			Type:  "LeaderTimer",
			Index: nrand(),
		}
		i, _, isleader := kv.rf.Start(Operation)
		if isleader {
			kv.mu.Lock()
			kv.Index = i
			kv.StartTimer = Operation.Index
			kv.mu.Unlock()
			break
		} else {
			time.Sleep(time.Millisecond * 500)
		}
	}
	for {
		if kv.killed() {
			return
		}
		operation := Op{
			Type:  "Timer",
			Index: nrand(),
		}
		i, _, isleader := kv.rf.Start(operation)
		if isleader {
			log.Printf("%v add a Timer i:%v", kv.me, i)
		}
		time.Sleep(time.Second)
	}
}

func (kv *KVServer) Snap() {
	if kv.maxraftstate == -1 {
		return
	}
	for {
		if kv.killed() {
			return
		}
		kv.mu.Lock()
		if kv.ApplyIndex > kv.SnapshotIndex && kv.rf.Persister.RaftStateSize() >= kv.maxraftstate {
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(kv.ApplyIndex)
			e.Encode(kv.storage)
			record := make(map[int64]RequestInfo)
			for k, v := range kv.record {
				if v.Rindex > kv.ApplyIndex-100 {
					record[k] = v
				}
			}
			e.Encode(record)
			data := w.Bytes()
			kv.SnapshotIndex = kv.ApplyIndex
			i := kv.ApplyIndex
			kv.rf.Snapshot(i, data)
			kv.mu.Unlock()
		} else {
			kv.mu.Unlock()
		}
		time.Sleep(time.Microsecond * 50)
	}
}
