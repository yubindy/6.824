package mr

import (
	"fmt"
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Taskinter interface {
}

const (
	waiting     int = 0
	doingmap    int = 1
	doingreduce int = 2
	doed        int = 3
	nowait      int = 4
)

type Coordinator struct {
	// Your definitions here.
	nmap    int
	nreduce int
	smap    int
	sreduce int
	stat    map[int]int //节点所对应状态的映射
	times   map[int]int //节点对应时间映射
	files   []string
	mut     sync.RWMutex
	cond    sync.Cond
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
func (c *Coordinator) Getinfo(args *Args, reply *Reply) error { //分配任务
	c.stat[args.tasknum] = args.localstate
	ok := c.stat[args.tasknum]
	if ok == doingreduce {
		c.Map(args, reply)
	} else if ok == waiting {
		c.mut.RLock()
		nmap := c.nmap
		c.mut.RUnlock()
		if nmap < len(c.files) {
			c.Map(args, reply)
		} else {
			reply.t = waiting
		}

	} else { //Map
		c.Reduce(args, reply)
	}
	return nil
}
func (c *Coordinator) Map(args *Args, reply *Reply) {
	if c.nmap >= len(c.files) {
		reply.t = WorkWait
		c.stat[args.tasknum] = waiting
	}
	reply.t = WorkMap
	c.times[args.tasknum] = 10 //将该状态下的任务定为10s
	c.stat[args.tasknum] = doingmap
	c.mut.Lock()
	c.nmap++
	c.mut.Unlock()
	reply.num = c.nmap
	reply.filepath[0] = c.files[reply.num]
}
func (c *Coordinator) Reduce(args *Args, reply *Reply) {
	reply.t = WorkReduce
	c.times[args.tasknum] = 10 //将该状态下的任务定为10s
	c.stat[args.tasknum] = doingreduce
	c.mut.Lock()
	c.nreduce++
	c.mut.Unlock()
	reply.num = c.nreduce
	for i := 0; i < c.nmap; i++ {
		reply.filepath[i] = fmt.Sprintf("mr-%v%v", i, c.nreduce)
	}
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false
	c.server()

	// Your code here.
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.nmap = 0
	c.smap = len(files)
	c.sreduce = nReduce
	c.nreduce = 0
	c.files = files
	c.server()
	return &c
}
