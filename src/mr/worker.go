package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

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
func GetaskCall(num int) *Reply {
	args := Args{}
	t := os.Getegid()
	args.info = fmt.Sprintf("%v:%v",t,num)
	reply := Reply{}
	ok := call("Coordinator.GetaskCall", &args, &reply)
	if !ok {
		fmt.Printf("call failed!\n")
	}
	return &reply
}
func dowork(num int, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		t := GetaskCall(num)
		if t.t == 0 {
			file, err := os.Open(t.filepath)
			if err != nil {
				log.Fatalf("cannot open %v", t.filepath)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", t.filepath)
			}
			file.Close()
			kva := mapf(t.filepath, string(content))
			var ofile [10]int
			var filer [10][]int
			var space [10][]KeyValue
			var err error
			for i := 0; i < 10; i++ {
				oname:=fmt.Sprintf("mr-%v%v",t.num,i)
				ofile[i], _ := os.Create(oname)
				filer[i],err=os.Open(oname)
				if err != nil {
					log.Fatalf("cannot read %v", filename)
				}
			}
			var s int
			for _,a :=range kva{
				s=ihash(a.Key)%10
				space[s]=append(space[s],a...)
			}
			for i := 0; i < 10; i++ {
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
			}

		} else {

		}
	}

}
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	for i := 0; i < 100; i++ { //在单个主机跑100并发计算
		go dowork(i, mapf, reducef)
	}

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
