package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"log"
	"sync"
	"sync/atomic"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	//op    string
	Key      string
	Value    string
	Action   string
	Clientid int64
	Id       int
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type Request struct {
	Id    int
	Index int
}
type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	kvmaps map[string]string
	Idmaps map[int64]Request
	dataCh chan raft.ApplyMsg
	cond   *sync.Cond
	applen int
	// Your definitions here.
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, isleader := kv.rf.GetState()
	applen := kv.rf.GetSapplen()
	kv.applen = applen
	if !isleader {
		reply.Err = "notleader"
		return
	}
	it, ok := kv.Idmaps[args.Clientid]
	if it.Id >= args.Id && ok {
		reply.Err = "some"
		if it.Index < kv.applen {
			reply.Value = kv.kvmaps[args.Key]
			return
		} else {
			for it.Index > kv.applen {
				kv.cond.Wait()
			}
			reply.Value = kv.kvmaps[args.Key]
			return
		}
	}
	_, index, isleader := kv.rf.Start(Op{Key: args.Key, Action: "Get", Clientid: args.Clientid, Id: args.Id})
	if !isleader {
		reply.Err = "notleader"
		return
	}
	kv.Idmaps[args.Clientid] = Request{Id: args.Id, Index: index}
	kv.mu.Unlock()
	t := <-kv.dataCh
	kv.mu.Lock()
	log.Printf("Get rpc node %v get appliy %v", kv.rf.Me, t)
	if t.CommandValid {
		reply.Value = kv.kvmaps[args.Key]
		_, isleader := kv.rf.GetState()
		if !isleader {
			reply.Err = "notleader"
		} else {
			reply.Err = "null"
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, isleader := kv.rf.GetState()
	applen := kv.rf.GetSapplen()
	kv.applen = applen
	if !isleader {
		reply.Err = "notleader"
		return
	}
	it, ok := kv.Idmaps[args.Clientid]
	if it.Id >= args.Id && ok {
		reply.Err = "some"
		if it.Index < kv.applen {
			return
		} else {
			for it.Index > kv.applen {
				kv.cond.Wait()
			}
			return
		}
	}
	_, index, isleader := kv.rf.Start(Op{Key: args.Key, Value: args.Value, Action: args.Op, Clientid: args.Clientid, Id: args.Id})
	if !isleader {
		reply.Err = "notleader"
		return
	}
	kv.Idmaps[args.Clientid] = Request{Id: args.Id, Index: index}
	kv.mu.Unlock()
	t := <-kv.dataCh
	kv.mu.Lock()
	log.Printf("PutAppend rpc node %v get appliy %v", kv.rf.Me, t)
	if t.CommandValid {
		if args.Op == "Append" {
			kv.kvmaps[args.Key] += args.Value
		} else if args.Op == "Put" {
			kv.kvmaps[args.Key] = args.Value
		}
		_, isleader := kv.rf.GetState()
		if !isleader {
			reply.Err = "notleader"
		} else {
			reply.Err = "null"
		}
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	kv.rf.Persist(true)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
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
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	kv := new(KVServer)
	kv.mu.Lock()
	kv.mu.Unlock()
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.cond = sync.NewCond(&kv.mu)

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg, 10)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.kvmaps = make(map[string]string)
	kv.Idmaps = make(map[int64]Request)
	kv.dataCh = make(chan raft.ApplyMsg, 10)
	go func() {
		for kv.killed() == false {
			data := <-kv.applyCh
			if data.CommandValid == true {
				_, isleader := kv.rf.GetState()
				applen := kv.rf.GetSapplen()
				kv.mu.Lock()
				kv.applen = applen
				kv.mu.Unlock()
				if isleader {
					log.Printf("server get appliyin 129 %v", data)
					kv.dataCh <- data
					kv.cond.Signal()
				} else {
					t := data.Command.(Op)
					kv.mu.Lock()
					if t.Action != "Get" {
						if t.Action == "Append" {
							kv.kvmaps[t.Key] += t.Value
						} else {
							kv.kvmaps[t.Key] = t.Value
						}
					}
					kv.Idmaps[t.Clientid] = Request{Id: t.Id, Index: data.CommandIndex}
					kv.mu.Unlock()
				}
			}
		}
	}()
	// You may need initialization code here.

	return kv
}
