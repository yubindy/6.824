package kvraft

import (
	"6.824/labrpc"
	"log"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers   []*labrpc.ClientEnd
	mayleader int //记住最后一个 RPC 的领导者是哪个服务器，并首先将下一个 RPC 发送到该服务器
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
	ck.mayleader = -1
	// You'll have to add code here.
	return ck
}

//
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
//
func (ck *Clerk) Get(key string) string {

	args := GetArgs{
		Key: key,
	}
	id := nrand()
	reply := GetReply{}
	if ck.mayleader != -1 {
		log.Printf("client %v start send Get %v", id, key)
		ok := ck.servers[ck.mayleader].Call("KVServer.Get", &args, &reply)
		if !ok {
			log.Printf("send GetReply to node %v fail", ck.mayleader)
		} else if reply.Err == "null" {
			log.Printf("client %v success Get %v is Value %v", id, key, reply.Value)
			return reply.Value
		}
	}
	for true {
		for i, _ := range ck.servers {
			log.Printf("client %v start send Get %v", id, key)
			ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
			if !ok {
				log.Printf("send GetReply to node %v fail", ck.mayleader)
			} else if reply.Err == "null" {
				ck.mayleader = i
				log.Printf("client %v success node %v Get %v is Value %v", id, i, key, reply.Value)
				return reply.Value
			}
		}
		time.Sleep(1 * time.Millisecond) //slepp 10 mill防止rpc发送频繁
	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	id := nrand()
	args := PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
	}
	reply := PutAppendReply{}
	if ck.mayleader != -1 {
		ok := ck.servers[ck.mayleader].Call("KVServer.PutAppend", &args, &reply)
		if !ok {
			log.Printf("send PutAppendReply to node %v fail94", ck.mayleader)
		} else if reply.Err == "null" {
			return
		}
	}
	for true {
		for i, _ := range ck.servers {
			log.Printf("client %v start send PutAppend %v to %v", id, key, value)
			ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
			if !ok {
				log.Printf("send PutAppendReply to node %v fail103", ck.mayleader)
			} else if reply.Err == "null" {
				ck.mayleader = i
				log.Printf("client %v success node %v PutAppend %v to %v", id, i, key, value)
				return
			}
		}
		time.Sleep(1 * time.Millisecond) //slepp 10 mill防止rpc发送频繁
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
