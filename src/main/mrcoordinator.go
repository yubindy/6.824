package main

//
// start the coordinator process, which is implemented
// in ../mr/coordinator.go
//
// go run mrcoordinator.go pg*.txt
//
// Please do not change this file.
//

import "6.824/mr"
import "time"
import "os"
import "fmt"

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
		os.Exit(1)
	}

	m := mr.MakeCoordinator(os.Args[1:], 10)
	for m.Done() == false {
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)
}

//mu        sync.Mutex          // Lock to protect shared access to this peer's state
//peers     []*labrpc.ClientEnd // RPC end points of all peers
//persister *Persister          // Object to hold this peer's persisted state
//me        int                 // this peer's index into peers[]
//dead      int32               // set by Kill()
//
//state       string
//currentTerm int
//votedFor    int
//log         []string
//Tchaotime   int //判断超时
