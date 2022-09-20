package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"context"
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
	t := raft.ApplyMsg{}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, isleader := kv.rf.GetState()
	if !isleader {
		reply.Err = "notleader"
		return
	}
	it, ok := kv.Idmaps[args.Clientid]
	appen := kv.applen
	opcommand := Op{Key: args.Key, Action: "Get", Clientid: args.Clientid, Id: args.Id, Flag: false}
	index, _, isleader := kv.Starts(opcommand)
	if it.Id >= args.Id && ok {
		reply.Err = "some"
		if it.Index <= appen {
			reply.Value = kv.kvmaps[args.Key]
			log.Printf("KVServer: node %v in74 %v Index:%v Applen:%v", kv.me, args, it.Index, kv.applen)
			return
		} else {
			log.Printf("KVServer: node %v in79 %v Index:%v Applen:%v", kv.me, args, it.Index, kv.applen)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			index := it.Index
			kv.mu.Unlock()
			go func(ctx context.Context) {
				select {
				case <-ctx.Done():
					log.Printf("get done")
					return
				case <-time.After(1 * time.Second):
					kv.mu.Lock()
					reply.Err = "timeout"
					kv.mu.Unlock()
					kv.cond.Signal()
				}
			}(ctx)
			for index > appen {
				_, isleader := kv.rf.GetState()
				if !isleader {
					kv.mu.Lock()
					reply.Err = "notleader"
					return
				}
				kv.mu.Lock()
				index = it.Index
				appen = kv.applen
				kv.cond.Wait()
				if reply.Err == "timeout" {
					return
				}
				kv.mu.Unlock()
			}
			kv.mu.Lock()
			reply.Value = kv.kvmaps[args.Key]
			log.Printf("KVServer: node %v in81 %v", kv.me, args)
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
			log.Printf("KVServer: node %v get command %v", kv.me, t)
		case <-time.After(1 * time.Second):
			kv.mu.Lock()
			reply.Err = "timeout"
			log.Printf("KVServer: node %v sendtimeout to %v", kv.me, args)
			return
		}
		s := t.Command.(Op)
		kv.mu.Lock()
		if s.Action == "Append" {
			kv.kvmaps[s.Key] += s.Value
			log.Printf("KVServer: node %v Append108 %v K %v V %v", kv.me, t, s.Key, kv.kvmaps[s.Key])
		} else if s.Action == "Put" {
			kv.kvmaps[s.Key] = s.Value
			log.Printf("KVServer: node %v Put113 %v K %v V %v", kv.me, t, s.Key, kv.kvmaps[s.Key])
		}
		reply.Value = kv.kvmaps[args.Key]
		kv.applen++
		kv.cond.Broadcast()
		if t.Command == opcommand {
			reply.Err = "null"
			return
		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	t := raft.ApplyMsg{}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, isleader := kv.rf.GetState()
	if !isleader {
		reply.Err = "notleader"
		return
	}
	it, ok := kv.Idmaps[args.Clientid]
	appen := kv.applen
	opcommand := Op{Key: args.Key, Value: args.Value, Action: args.Op, Clientid: args.Clientid, Id: args.Id, Flag: false}
	index, _, isleader := kv.Starts(opcommand)
	if it.Id >= args.Id && ok {
		reply.Err = "some"
		if it.Index <= appen {
			log.Printf("KVServer: node %v in152 %v applied %v index %v", kv.me, args, kv.applen, it.Index)
			return
		} else {
			log.Printf("KVServer: node %v in156 %v Index:%v Applen:%v", kv.me, args, it.Index, kv.applen)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			index := it.Index
			kv.mu.Unlock()
			go func(ctx context.Context) {
				select {
				case <-ctx.Done():
					log.Printf("put done")
					return
				case <-time.After(1 * time.Second):
					kv.mu.Lock()
					reply.Err = "timeout"
					kv.mu.Unlock()
					kv.cond.Signal()
				}
			}(ctx)
			for index > appen {
				_, isleader := kv.rf.GetState()
				if !isleader {
					kv.mu.Lock()
					reply.Err = "notleader"
					return
				}
				kv.mu.Lock()
				index = it.Index
				appen = kv.applen
				kv.cond.Wait()
				if reply.Err == "timeout" {
					return
				}
				kv.mu.Unlock()
			}
			kv.mu.Lock()
			kv.rf.Mu.Lock()
			log.Printf("SSSSSS node %v should success %v V:%v inlog ", kv.me, args, kv.kvmaps[args.Key])
			if kv.kvmaps[args.Key] != kv.rf.Logs[it.Index].Logact {
				reply.Err = "error"
				log.Printf("KVServer: node %v put210 command %v != logs %v", kv.me, kv.kvmaps[args.Key], kv.rf.Logs[it.Index].Logact)
			}
			kv.rf.Mu.Unlock()
			return
		}
	}
	kv.Idmaps[args.Clientid] = Request{Id: args.Id, Index: index}
	kv.mu.Unlock()
	for true {
		select {
		case t = <-kv.dataCh:
			log.Printf("KVServer: node %v putappend161 command %v", kv.me, t)
		case <-time.After(1 * time.Second):
			kv.mu.Lock()
			reply.Err = "timeout"
			log.Printf("KVServer: node %v sendtimeout to %v", kv.me, args)
			return
		}
		s := t.Command.(Op)
		kv.mu.Lock()
		if s.Action == "Append" {
			kv.kvmaps[s.Key] += s.Value
			log.Printf("KVServer: node %v Append172 %v K %v V %v", kv.me, t, s.Key, kv.kvmaps[s.Key])
		} else if s.Action == "Put" {
			kv.kvmaps[s.Key] = s.Value
			log.Printf("KVServer: node %v Put183 %vK %v V %v", kv.me, t, s.Key, kv.kvmaps[s.Key])
		}
		kv.applen++
		kv.cond.Broadcast()
		if t.Command == opcommand {
			reply.Err = "null"
			return
		}
		kv.mu.Unlock()
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
				if data.CommandValid == true {
					if t.Flag == true {
						kv.mu.Lock()
						kv.applen++
						kv.mu.Unlock()
						continue
					}
					select {
					case kv.dataCh <- data:
						log.Printf("KVServer %v : server get appliyin312 %v", kv.me, data)
						continue
					case <-time.After(3 * time.Millisecond):
						if t.Flag != true {
							if kv.Idmaps[t.Clientid].Id != t.Id+1 {
								continue
							}
							kv.mu.Lock()
							kv.Idmaps[t.Clientid] = Request{Id: t.Id, Index: data.CommandIndex}
							if t.Action == "Append" {
								kv.kvmaps[t.Key] += t.Value
								log.Printf("KVServer: node %v Append262 %v K %v V %v", kv.me, t, t.Key, kv.kvmaps[t.Key])
							} else if t.Action == "Put" {
								kv.kvmaps[t.Key] = t.Value
								log.Printf("KVServer: node %v Put275 %v K %v V %v", kv.me, t, t.Key, kv.kvmaps[t.Key])
							}
							kv.applen++
							kv.mu.Unlock()
							kv.cond.Broadcast()
						}
					}
				}
			case <-time.After(1 * time.Second):
				_, isleader := kv.rf.GetState()
				if isleader {
					_, _, isleader := kv.Starts(Op{Flag: true})
					if isleader {
						log.Printf("KVserver: node %v add nil", kv.me)
					}
				}
			}
		}
	}()
	// You may need initialization code here.

	return kv
}
func (kv *KVServer) Starts(command interface{}) (int, int, bool) {
	st := -1
	t := command.(Op)
	kv.rf.Mu.Lock()
	for i := len(kv.rf.Logs) - 1; i > 0; i-- {
		if kv.rf.Logs[i].Logact.(Op).Clientid == t.Clientid {
			st = i
			break
		}
	}
	if st >= 0 && kv.rf.Logs[st].Logact == command && t.Flag != true {
		index := st
		sb := kv.rf.Logs[st].Logact
		kv.rf.Mu.Unlock()
		log.Printf("KVServer: node %v notshould add %v index:%v commandvalue:%v", kv.rf.Me, command, index, sb)
		return index, -1, false
	}
	log.Printf("KVServer: node %v add %v", kv.rf.Me, command)
	kv.rf.Mu.Unlock()
	// Your code here (2B).
	return kv.rf.Start(command)
}
