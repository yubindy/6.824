package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"sync"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
var wg sync.WaitGroup

type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

const (
	WorkWait   int = 0
	WorkMap    int = 1
	WorkReduce int = 2
	WorkExit   int = 3
)

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func GetaskCall(state int, node int) (*Reply, bool) {
	args := Args{}
	args.Tasknum = -1
	args.Localstate = state
	args.Nodenum = node
	reply := Reply{}
	ok := call("Coordinator.Getinfo", &args, &reply)
	return &reply, ok
}
func DoedCall(tasknum int, before int) (*Reply, bool) {
	args := Args{}
	args.Tasknum = tasknum
	args.Localstate = waiting
	args.Beforstate = before
	reply := Reply{}
	ok := call("Coordinator.Doed", &args, &reply)
	return &reply, ok

}
func dowork(num int, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	exitn := 0
	node := 0
	defer func() {
		if err := recover(); err != nil {
			log.Println("work failed:", err)
		}
	}()
	localstate := waiting
	for {
		t, ok := GetaskCall(localstate, node)
		node = t.Nodenum
		if !ok {
			exitn++
		}
		if exitn >= 10 {
			t.T = WorkExit
		}
		if t.T == WorkMap { //Map
			exitn = 0
			log.Println("start map", t.Num)
			file, err := os.Open(t.Filepath[0])
			if err != nil {
				log.Fatalf("cannot open %v", t.Filepath)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", t.Filepath)
			}
			file.Close()
			kva := mapf(t.Filepath[0], string(content))
			var ofile [10]*os.File
			var space [10][]KeyValue
			for i := 1; i <= 10; i++ {
				oname := fmt.Sprintf("mr-mid-%v%v", t.Num, i)
				ofile[i-1], _ = os.Create(oname)
			}
			var s int
			for _, a := range kva {
				s = ihash(a.Key) % 10
				space[s] = append(space[s], a)
			}
			for i := 0; i < 10; i++ {
				enc := json.NewEncoder(ofile[i])
				for _, kv := range space[i] {
					err := enc.Encode(&kv)
					if err != nil {
						log.Fatalf("cannot read to file:%v by json\n", i)
					}
				}
				ofile[i].Close()
			}
			localstate = waiting
			ss := t.Num
			t, ot := DoedCall(t.Num, doingmap)
			t.Num = ss
			if t.Get == true && ot == true {
				for a, b := range ofile {
					onames := fmt.Sprintf("mr-%v%v", t.Num, a+1)
					err := os.Rename(b.Name(), onames)
					ofile[a], _ = os.Open(onames)
					if err != nil {
						log.Fatalf("FAFl 131", err)
					}
				}
				for _, y := range ofile {
					fmt.Println("file:", y.Name())
				}
				fmt.Println("\n")
				log.Println("doed map", t.Num)
			} else {
				for _, b := range ofile {
					os.Remove(b.Name())
				}
				log.Println("doed map falled", t.Num)

			}
		} else if t.T == WorkReduce { //Reduce
			exitn = 0
			log.Println("start reduce", t.Num)
			var kva []KeyValue
			for i, _ := range t.Filepath {
				file, err := os.Open(t.Filepath[i])
				if err != nil {
					log.Fatalf("cannot open %v", t.Filepath[i])
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
			}
			sort.Sort(ByKey(kva))
			oname := fmt.Sprintf("mr-out-mid-%v", t.Num)
			ofiler, _ := os.Create(oname)
			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)
				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofiler, "%v %v\n", kva[i].Key, output)
				i = j
			}
			ofiler.Close()
			localstate = waiting
			ss := t.Num
			t, ot := DoedCall(t.Num, doingreduce)
			t.Num = ss
			if t.Get == true && ot == true {
				onames := fmt.Sprintf("mr-out-%v", t.Num)
				os.Rename(ofiler.Name(), onames)
				ofiler, _ = os.Open(onames)
				log.Println("doed reduce%v  %v", t.Num, ofiler.Name())
			} else {
				os.Remove(ofiler.Name())
				log.Println("doed map fail", t.Num)
			}
		} else if t.T == WorkWait {
			exitn = 0
			time.Sleep(time.Second)
		} else if t.T == WorkExit {
			wg.Done()
			return
		}
	}
	wg.Done()
}
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	wg.Add(10)
	/*for i := 0; i < 10; i++ { //在单个主机跑10并发计算
		go dowork(i, mapf, reducef)
	}*/
	wg.Wait()

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}
	Worker(nil, nil)
	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
