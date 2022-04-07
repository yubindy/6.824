package mr

import (
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
	waiting taskstate = 0
	doing   taskstate = 1
	doed    taskstate = 2
	nowait  taskstate = 3
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
	nMap    int
	nReduce int
	stat    map[int]taskstate //节点所对应状态的映射
	info    map[int]string    //对于nReduce桶到文件路径映射
	files   []string
	chn     chan int
	time    int
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
	c.nMap = len(files)
	c.nReduce = nReduce
	c.files = files
	c.time = 20
	c.server()
	return &c
}
