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
	Clientid  int64
	Id        int //请求序号用来去重
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
	ck.Clientid = nrand()
	ck.Id = 0
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
		Key:      key,
		Id:       ck.Id,
		Clientid: ck.Clientid,
	}
	ck.Id++
	reply := GetReply{}
	if ck.mayleader != -1 {
		log.Printf("client %v start send isleader %v Get %v id: %v", ck.Id, ck.mayleader, key, args.Id)
		ok := ck.servers[ck.mayleader].Call("KVServer.Get", &args, &reply)
		if !ok {
			log.Printf("send GetReply to node %v fail", ck.mayleader)
		} else if reply.Err == "null" || reply.Err == "some" {
			log.Printf("client %v isleader %v success Get %v is Value %v id %v", ck.Id, ck.mayleader, key, reply.Value, args.Id)
			return reply.Value
		}
	}
	for true {
		for i, _ := range ck.servers {
			log.Printf("client %v start send %v Get %v id %v", ck.Id, i, key, args.Id)
			ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
			if !ok {
				log.Printf("send GetReply to node %v fail id %v", ck.mayleader, args.Id)
			} else if reply.Err == "null" || reply.Err == "some" {
				ck.mayleader = i
				log.Printf("client %v success node %v Get %v is Value %v id %v", ck.Id, i, key, reply.Value, args.Id)
				return reply.Value
			}
		}
		time.Sleep(10 * time.Millisecond) //slepp 10 mill防止rpc发送频繁
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
		Key:      key,
		Value:    value,
		Op:       op,
		Id:       ck.Id,
		Clientid: ck.Clientid,
	}
	ck.Id++
	reply := PutAppendReply{}
	if ck.mayleader != -1 {
		log.Printf("client %v start send PutAppend isleader:%v Key:%v to Value:%v id: %v", id, ck.mayleader, key, value, args.Id)
		ok := ck.servers[ck.mayleader].Call("KVServer.PutAppend", &args, &reply)
		if !ok {
			log.Printf("send PutAppendReply to node %v fail94 id: %v", ck.mayleader, args.Id)
		} else if reply.Err == "null" {
			log.Printf("client %v success isleader %v PutAppend %v to %v id: %v", id, ck.mayleader, key, value, args.Id)
			return
		} else if reply.Err == "some" {
			log.Printf("client %v somerequest isleader %v PutAppend %v to %v id: %v", id, ck.mayleader, key, value, args.Id)
			return
		}
	}
	for true {
		for i, _ := range ck.servers {
			log.Printf("client %v start send PutAppend %v Key:%v to Value:%v id: %v", id, i, key, value, args.Id)
			ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
			if !ok {
				log.Printf("send PutAppendReply to node %v fail103 id %v", ck.mayleader, args.Id)
			} else if reply.Err == "null" {
				ck.mayleader = i
				log.Printf("client %v success node %v PutAppend %v to %v id %v", id, i, key, value, args.Id)
				return
			} else if reply.Err == "some" {
				ck.mayleader = i
				log.Printf("client %v somerequest node %v PutAppend %v to %v id %v", id, i, key, value, args.Id)
				return
			}
		}
		time.Sleep(10 * time.Millisecond) //slepp 10 mill防止rpc发送频繁
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
