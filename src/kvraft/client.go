package kvraft

import (
	"6.824/labrpc"
	"log"
	"sync"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers   []*labrpc.ClientEnd
	mayleader int //记住最后一个 RPC 的领导者是哪个服务器，并首先将下一个 RPC 发送到该服务器
	Clientid  int64
	Id        int //请求序号用来去重
	mu        sync.Mutex
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.mayleader = 0
	ck.Clientid = nrand()
	ck.Id = 0
	// You'll have to add code here.
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	args := GetArgs{
		Key:      key,
		Id:       ck.Id,
		Clientid: ck.Clientid,
	}
	ck.Id++
	ck.mu.Unlock()
	reply := GetReply{}
	log.Printf("KVServer: client %v start  id: %v  send Get isleader: %v Key:%v ", args.Clientid, args.Id, ck.mayleader, key)
	ok := ck.servers[ck.mayleader].Call("KVServer.Get", &args, &reply)
	if !ok {
		log.Printf("KVServer: send61 id: %v GetReply to node %v fail", args.Id, ck.mayleader)
	} else if reply.Err == "null" || reply.Err == "some" {
		log.Printf("KVServer: client %v id: %v success63 node %v Get %v is Value %v reply.Err is %v", args.Clientid, args.Id, ck.mayleader, key, reply.Value, reply.Err)
		return reply.Value
	}
	for true {
		for i, _ := range ck.servers {
			log.Printf("KVServer: client %v start id: %v send %v Get %v", args.Clientid, args.Id, i, key)
			ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
			if !ok {
				log.Printf("KVServer: send71 id: %v GetReply to node %v fail", args.Id, i)
			} else if reply.Err == "null" || reply.Err == "some" {
				ck.mayleader = i
				log.Printf("KVServer: client %v id: %v success74 node %v Get %v is Value %v reply.Err is %v", args.Clientid, args.Id, i, key, reply.Value, reply.Err)
				return reply.Value
			} else if reply.Err == "timeout" {
				log.Printf("KVServer: timeout %v", args)
			}
		}
		time.Sleep(5 * time.Millisecond) //slepp 10 mill防止rpc发送频繁
	}
	return ""
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.mu.Lock()
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		Id:       ck.Id,
		Clientid: ck.Clientid,
	}
	ck.Id++
	ck.mu.Unlock()
	reply := PutAppendReply{}
	log.Printf("KVServer: client %v start  id: %v  send PutAppend to : %v Key:%v to Value:%v", args.Clientid, ck.mayleader, args.Id, key, value)
	ok := ck.servers[ck.mayleader].Call("KVServer.PutAppend", &args, &reply)
	if !ok {
		log.Printf("KVServer: client %v fail109 id: %v send %v Key:%v to Value:%v", args.Clientid, args.Id, ck.mayleader, key, value)
	} else if reply.Err == "null" || reply.Err == "some" {
		log.Printf("KVServer: client %v success111 id: %v send PutAppend %v Key:%v to Value:%v reply.Err is %v", args.Clientid, args.Id, ck.mayleader, key, value, reply.Err)
		return
	}
	for true {
		for i, _ := range ck.servers {
			log.Printf("KVServer: client %v start id: %v send %v Key:%v to Value:%v", args.Clientid, args.Id, i, key, value)
			ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
			if !ok {
				log.Printf("KVServer: client %v fail119 id: %v send %v Key:%v to Value:%v", args.Clientid, args.Id, i, key, value)
			} else if reply.Err == "null" || reply.Err == "some" {
				ck.mayleader = i
				log.Printf("KVServer: client %v success122 id: %v send PutAppend %v Key:%v to Value:%v reply.Err is %v", args.Clientid, args.Id, i, key, value, reply.Err)
				return
			} else if reply.Err == "timeout" {
				log.Printf("KVServer: timeout %v", args)
			} else if reply.Err == "error" {
				log.Printf("KVServer: error %v", args)
			}
		}
		time.Sleep(5 * time.Millisecond) //slepp 10 mill防止rpc发送频繁
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
