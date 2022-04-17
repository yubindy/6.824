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
	doerror     int = 4
)

type Coordinator struct {
	// Your definitions here.
	nmap    int
	nreduce int
	smap    int
	sreduce int
	nodeall int
	quit    bool
	stat    map[int]int //节点所对应状态的映射
	wf      map[int]int //任务节点num对应点num
	problem map[int]int //节点num 对状态
	times   map[int]int //节点对应时间映射
	files   []string
	mut     sync.Mutex
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
func (c *Coordinator) Doed(args *Args, reply *Reply) error {
	c.mut.Lock()
	delete(c.times, args.Tasknum)
	if args.Nodenum == 0 {
		c.nodeall++
		reply.Nodenum = c.nodeall
	}
	if c.problem[c.wf[args.Tasknum]] == doerror {
		reply.Get = false
	} else {
		c.stat[args.Tasknum] = waiting
		log.Println("change task---state by doed", args.Tasknum, args.Localstate)
		reply.Get = true
	}
	c.mut.Unlock()
	return nil
}
func (c *Coordinator) Getinfo(args *Args, reply *Reply) error { //分配任务
	c.mut.Lock()
	var ok int
	if args.Tasknum != -1 && c.problem[args.Nodenum] != doerror {
		c.stat[args.Tasknum] = args.Localstate
		log.Println("change task---state by getinfo", args.Tasknum, args.Localstate)
		ok = c.stat[args.Tasknum]
	} else {
		ok = waiting
	}
	if ok == doingreduce {
		c.Reduce(args, reply)
	} else if ok == waiting {
		if c.nmap < c.smap {
			log.Println("nmap:%v nreduce:%v", c.nmap, c.nreduce)
			c.Map(args, reply)
		} else if c.nreduce < c.sreduce {
			for a, _ := range c.stat {
				if c.stat[a] == doingmap {
					reply.T = waiting
					//log.Println("not reduce", a, c.stat[a])
					c.mut.Unlock()
					return nil
				}
			}
			log.Println("kv:%v", c.stat)
			c.Reduce(args, reply)
		} else {
			reply.T = waiting
		}
	} else { //Map
		c.Map(args, reply)
	}
	c.mut.Unlock()
	return nil
}
func (c *Coordinator) Map(args *Args, reply *Reply) {
	if c.nmap >= c.smap {
		reply.T = WorkWait
	}
	reply.T = WorkMap
	if !c.queue.isempty() {
		reply.Num = c.queue.pop()
		c.nmap++
	} else {
		c.nmap++
		reply.Num = c.nmap
	}
	c.nodeall++
	c.problem[args.Nodenum] = doed
	c.wf[reply.Num] = c.nodeall
	c.times[reply.Num] = 10 //将该状态下的任务定为10s
	c.stat[reply.Num] = doingmap
	c.problem[args.Nodenum] = doingmap
	reply.Filepath = append(reply.Filepath, c.files[reply.Num-1])
	log.Println("add map for", reply.Num)
}
func (c *Coordinator) Reduce(args *Args, reply *Reply) {
	if c.nreduce >= c.sreduce {
		reply.T = WorkWait
	}
	reply.T = WorkReduce
	if !c.queue.isempty() {
		reply.Num = c.queue.pop()
		c.nreduce++
	} else {
		c.nreduce++
		reply.Num = c.nreduce
	}
	c.nodeall++
	c.wf[reply.Num] = c.nodeall
	c.problem[args.Nodenum] = doed
	c.times[reply.Num] = 10 //将该状态下的任务定为10s
	c.stat[reply.Num] = doingreduce
	c.problem[args.Nodenum] = doingreduce
	for i := 0; i < c.nmap; i++ {
		oname := fmt.Sprintf("mr-%v%v", i+1, c.nreduce)
		reply.Filepath = append(reply.Filepath, oname)
	}
	log.Println("add Reduce for", reply.Num)
}

//
// start a thread that listens for RPCs from worker.go
//

func (c *Coordinator) checktime() {
	c.mut.Lock()
	for a, b := range c.times {
		if c.stat[a] == doingmap || c.stat[a] == doingreduce {
			c.times[a] = b - 1
			if c.times[a] == 0 {
				c.queue.push(a)
				delete(c.times, a)
				if c.stat[a] == doingreduce {
					c.nreduce--
				} else if c.stat[a] == doingmap {
					c.nmap--
				}
				c.problem[c.wf[a]] = doerror
			}
		}
	}
	if len(c.times) == 0 && c.nreduce == c.sreduce && c.nmap == c.smap && c.queue.isempty() {
		c.quit = true
		log.Println("process should exit")
	}
	log.Println("check time", c.times)
	log.Println("check times", c.stat)
	c.mut.Unlock()
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
	go c.checktime()
	// Your code here.
	c.mut.Lock()
	ret = c.quit
	c.mut.Unlock()
	if ret == true {
		log.Println("process true exit")
	}
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
	c.problem = make(map[int]int)
	c.wf = make(map[int]int)
	c.times = make(map[int]int)
	c.stat = make(map[int]int)
	c.files = files
	c.quit = false
	c.server()
	return &c
}
