package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Queue interface {
	push()
	pop()
	isempty()
}
type taskqueue struct { //用于重新分配的任务
	Knum [100]int
	mut  sync.Mutex
	frn  int
	end  int
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
	queue   taskqueue
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
func (q *taskqueue) push(K int) bool {
	q.mut.Lock()
	q.Knum[q.end] = K
	q.end = (q.end + 1) % 100
	q.mut.Unlock()
	return true
}
func (q *taskqueue) pop() int {
	q.mut.Lock()
	tnum := q.Knum[q.frn]
	q.frn = (q.frn + 1) % 100
	q.mut.Unlock()
	return tnum
}
func (q *taskqueue) isempty() bool {
	if q.frn == q.end {
		return true
	} else {
		return false
	}

}
func (c *Coordinator) Getinfo(args *Args, reply *Reply) error { //分配任务
	var ok int
	if args.tasknum != -1 {
		c.stat[args.tasknum] = args.localstate
		ok = c.stat[args.tasknum]
	} else {
		ok = waiting
	}
	if ok == doingreduce {
		c.Reduce(args, reply)
	} else if ok == waiting {
		c.mut.RLock()
		nmap := c.nmap
		c.mut.RUnlock()
		if nmap < c.smap {
			c.Map(args, reply)
		} else {
			reply.t = waiting
		}
	} else { //Map
		c.Map(args, reply)
	}
	return nil
}
func (c *Coordinator) Map(args *Args, reply *Reply) {
	if c.nmap >= c.smap {
		reply.t = WorkWait
	}
	reply.t = WorkMap
	if !c.queue.isempty() {
		reply.num = c.queue.pop()
		c.nmap++
	} else {
		c.mut.Lock()
		c.nmap++
		c.mut.Unlock()
		reply.num = c.nmap
	}
	c.times[reply.num] = 10 //将该状态下的任务定为10s
	c.stat[reply.num] = doingmap
	reply.filepath[0] = c.files[reply.num]
}
func (c *Coordinator) Reduce(args *Args, reply *Reply) {
	if c.nreduce >= c.sreduce {
		reply.t = WorkWait
	}
	reply.t = WorkReduce
	if !c.queue.isempty() {
		reply.num = c.queue.pop()
		c.nreduce++
	} else {
		c.mut.Lock()
		c.nreduce++
		c.mut.Unlock()
		reply.num = c.nreduce
	}
	c.times[reply.num] = 10 //将该状态下的任务定为10s
	c.stat[reply.num] = doingreduce
	for i := 0; i < c.nmap; i++ {
		reply.filepath[i] = fmt.Sprintf("mr-%v%v", i, c.nreduce)
	}
}

//
// start a thread that listens for RPCs from worker.go
//

func (c *Coordinator) checktime() {
	for {
		for a, b := range c.times {
			if c.stat[a] != waiting {
				c.times[a] = b - 1
				if c.times[a] == 0 {
					c.queue.push(a)
					if c.stat[a] == doingreduce {
						c.nreduce--
					} else if c.stat[a] == doingmap {
						c.nmap--
					}
				}
			}
		}
	}
	time.Sleep(time.Second)
}
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
	go c.checktime()
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
	c.smap = len(files)
	c.sreduce = nReduce
	c.files = files
	c.server()
	return &c
}
