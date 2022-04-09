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
type taskstate uint32

const (
	waiting     taskstate = 0
	doingmap    taskstate = 1
	doingreduce taskstate = 2
	doed        taskstate = 3
	nowait      taskstate = 4
)

type Tasks struct {
	nReduce int
}
type Taskqueue struct {
	num []Taskinter
	fnt int
	mut sync.Mutex
}
type Coordinator struct {
	// Your definitions here.
	nmap    int
	nreduce int
	stat    map[int]taskstate //节点所对应状态的映射
	times   map[int]int       //节点对应时间映射
	files   []string
	num     int
	alln    int
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
	ok := c.stat[args.tasknum]
	if ok == doingreduce {
		c.Map(args, reply)
	} else if ok == waiting {

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
	c.num++
	reply.num = c.num
	c.nmap++
	reply.filepath[0] = c.files[reply.num]
}
func (c *Coordinator) Reduce(args *Args, reply *Reply) {
	reply.t = WorkReduce
	c.times[args.tasknum] = 10 //将该状态下的任务定为10s
	c.stat[args.tasknum] = doingreduce
	reply.num = c.num
	for i := 0; i < c.nmap; i++ {
		reply.filepath[i] = fmt.Sprintf("mr-%v%v", i, c.nreduce)
	}
	c.num--
	c.nreduce++
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
	c.num = 0
	c.nmap = 0
	c.nreduce = 0
	c.alln = 0
	c.files = files
	c.server()
	return &c
}
