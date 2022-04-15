package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}
type Args struct {
	Filepath   string
	Tasknum    int
	Localstate int
	Beforstate int
	Nodenum    int
}
type Reply struct {
	Filepath []string
	Get      bool
	Num      int //任务编号
	T        int //返回调用函数 0：Map,1:Reduce
	Nodenum  int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
