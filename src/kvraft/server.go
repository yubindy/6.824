package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
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
	Flag     bool
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
	t := raft.ApplyMsg{}
	log.Printf("node %v should add %v", kv.me, args)
	//log.Printf("node %v start should add %v", kv.me, args)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, isleader := kv.rf.GetState()
	if !isleader {
		reply.Err = "notleader"
		return
	}
	it, ok := kv.Idmaps[args.Clientid]
	opcommand := Op{Key: args.Key, Action: "Get", Clientid: args.Clientid, Id: args.Id, Flag: false}
	log.Printf("node %v should startin84 %v", kv.me, args)
	index, term, isleader := kv.Starts(opcommand)
	if it.Id >= args.Id && ok || term < 0 {
		reply.Err = "some"
		if it.Index <= kv.applen {
			reply.Value = kv.kvmaps[args.Key]
			log.Printf("node %v in74 %v----applied %v index %v", kv.me, args, kv.applen, it.Index)
			return
		} else {
			for it.Index > kv.applen {
				kv.cond.Wait()
			}
			reply.Value = kv.kvmaps[args.Key]
			log.Printf("node %v in81 %v", kv.me, args)
			return
		}
	}
	if !isleader {
		reply.Err = "notleader"
		return
	}
	kv.Idmaps[args.Clientid] = Request{Id: args.Id, Index: index}
	kv.mu.Unlock()
	for true {
		select {
		case t = <-kv.dataCh:
			log.Printf("node %v get command %v", kv.me, t)
		case <-time.After(1 * time.Second):
			kv.mu.Lock()
			reply.Err = "timeout"
			log.Printf("node %v sendtimeout to %v", kv.me, args)
			return
		}
		if t.Command == opcommand {
			break
		}
		s := t.Command.(Op)
		kv.mu.Lock()
		if s.Action == "Append" {
			kv.kvmaps[s.Key] += s.Value
			log.Printf("node %v Append %v K %v V %v", kv.me, t, s.Key, kv.kvmaps[s.Key])
		} else if s.Action == "Put" {
			kv.kvmaps[s.Key] = s.Value
			log.Printf("node %v Append %v K %v V %v", kv.me, t, s.Key, kv.kvmaps[s.Key])
		}
		kv.mu.Unlock()
	}
	kv.mu.Lock()
	log.Printf("Get rpc node %v get appliy %v", kv.rf.Me, t)
	if t.CommandValid {
		reply.Value = kv.kvmaps[args.Key]
		applen := kv.rf.GetSapplen()
		kv.applen = applen
		kv.cond.Broadcast()
		reply.Err = "null"

	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	t := raft.ApplyMsg{}
	log.Printf("node %v should add %v", kv.me, args)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, isleader := kv.rf.GetState()
	if !isleader {
		reply.Err = "notleader"
		return
	}
	it, ok := kv.Idmaps[args.Clientid]
	opcommand := Op{Key: args.Key, Value: args.Value, Action: args.Op, Clientid: args.Clientid, Id: args.Id, Flag: false}
	log.Printf("node %v should startin158 %v", kv.me, args)
	index, term, isleader := kv.Starts(opcommand)
	if it.Id >= args.Id && ok || term < 0 {
		reply.Err = "some"
		if it.Index <= kv.applen {
			log.Printf("node %v in152 %v", kv.me, args)
			return
		} else {
			for it.Index > kv.applen {
				log.Printf("node %v in156 %v", kv.me, args)
				kv.cond.Wait()
			}
			return
		}
	}
	if !isleader {
		reply.Err = "notleader"
		return
	}
	kv.Idmaps[args.Clientid] = Request{Id: args.Id, Index: index}
	kv.mu.Unlock()
	for true {
		select {
		case t = <-kv.dataCh:
			log.Printf("node %v putappend command %v", kv.me, t)
		case <-time.After(1 * time.Second):
			reply.Err = "timeout"
			log.Printf("node %v sendtimeout to %v", kv.me, args)
			kv.mu.Lock()
			return
		}
		if t.Command == opcommand {
			break
		}
		s := t.Command.(Op)
		kv.mu.Lock()
		if s.Action == "Append" {
			kv.kvmaps[s.Key] += s.Value
			log.Printf("node %v Append %v K %v V %v", kv.me, t, s.Key, kv.kvmaps[s.Key])
		} else if s.Action == "Put" {
			kv.kvmaps[s.Key] = s.Value
			log.Printf("node %v Put %vK %v V %v", kv.me, t, s.Key, kv.kvmaps[s.Key])
		}
		kv.mu.Unlock()
	}
	kv.mu.Lock()
	log.Printf("PutAppend rpc node %v get appliy %v", kv.rf.Me, t)
	if t.CommandValid {
		if args.Op == "Append" {
			kv.kvmaps[args.Key] += args.Value
			log.Printf("node %v Append %v K %v V %v", kv.me, t, args.Key, kv.kvmaps[args.Key])
		} else if args.Op == "Put" {
			kv.kvmaps[args.Key] = args.Value
			log.Printf("node %v Append %v K %v V %v", kv.me, t, args.Key, kv.kvmaps[args.Key])
		}
		applen := kv.rf.GetSapplen()
		kv.applen = applen
		kv.cond.Broadcast()
		reply.Err = "null"

	}
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
	kv.rf.Persist(true)
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
	kv.mu.Lock()
	kv.mu.Unlock()
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.cond = sync.NewCond(&kv.mu)

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.kvmaps = make(map[string]string)
	kv.Idmaps = make(map[int64]Request)
	kv.dataCh = make(chan raft.ApplyMsg)
	go func() {
		for kv.killed() == false {
			data := raft.ApplyMsg{}
			select {
			case data = <-kv.applyCh:
				t := data.Command.(Op)
				if data.CommandValid == true && t.Flag != true {
					_, isleader := kv.rf.GetState()
					if isleader {
						log.Printf("server get appliyin 129 %v", data)
						select {
						case kv.dataCh <- data:
							continue
						default:
							if t.Flag != true {
								if t.Action == "Append" {
									kv.kvmaps[t.Key] += t.Value
									log.Printf("node %v Append %v K %v V %v", kv.me, t, t.Key, kv.kvmaps[t.Key])
								} else {
									kv.kvmaps[t.Key] = t.Value
									log.Printf("node %v Put %v K %v V %v", kv.me, t, t.Key, kv.kvmaps[t.Key])
								}
								kv.mu.Lock()
								applen := kv.rf.GetSapplen()
								kv.applen = applen
								kv.cond.Broadcast()
								kv.mu.Unlock()
							}
						}
					} else if t.Flag != true {
						kv.mu.Lock()
						if t.Action != "Get" {
							if t.Action == "Append" {
								kv.kvmaps[t.Key] += t.Value
								log.Printf("node %v Append %v", kv.me, t)
							} else {
								kv.kvmaps[t.Key] = t.Value
								log.Printf("node %v Put %v", kv.me, t)
							}
						}
						kv.Idmaps[t.Clientid] = Request{Id: t.Id, Index: data.CommandIndex}
						applen := kv.rf.GetSapplen()
						kv.applen = applen
						kv.cond.Broadcast()
						kv.mu.Unlock()
					}
				}
			case <-time.After(1 * time.Second):
				_, _, _ = kv.rf.Start(Op{Flag: true})
				log.Printf("node %v add nil", kv.me)
			}
		}
	}()
	// You may need initialization code here.

	return kv
}
func (kv *KVServer) Starts(command interface{}) (int, int, bool) {
	st := -1
	kv.rf.Mu.Lock()
	for i := len(kv.rf.Logs) - 1; i > 0; i-- {
		if kv.rf.Logs[i].Logact.(Op).Clientid == command.(Op).Clientid {
			st = i
			break
		}
	}
	if st >= 0 && kv.rf.Logs[st].Logact == command {
		index := st
		kv.rf.Mu.Unlock()
		log.Printf("node %v notshould add %v", kv.rf.Me, command)
		return index, -1, false
	}
	if st >= 0 {
		log.Printf("ssssbbbb 1:%v  2:%v", kv.rf.Logs[st].Logact, command)
	}
	kv.rf.Mu.Unlock()
	// Your code here (2B).
	return kv.rf.Start(command)
}
