package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
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
func GetaskCall(num int, alln int, state int) (*Reply, bool) {
	args := Args{}
	args.tasknum = -1
	args.allnum = alln
	args.localstate = state
	reply := Reply{}
	ok := call("Coordinator.GetaskCall", &args, &reply)
	return &reply, ok
}
func dowork(num int, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	exitn := 0
	var alln int = 0
	var localstate int
	for {
		t, ok := GetaskCall(num, alln, localstate)
		if !ok {
			exitn++
		}
		if exitn >= 20 {
			t.t = exitn
		} else {
			continue
		}
		if t.t == WorkMap { //Map
			exitn = 0
			file, err := os.Open(t.filepath[0])
			if err != nil {
				log.Fatalf("cannot open %v", t.filepath)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", t.filepath)
			}
			file.Close()
			kva := mapf(t.filepath[0], string(content))
			var ofile [10]*os.File
			var space [10][]KeyValue
			for i := 0; i < 10; i++ {
				oname := fmt.Sprintf("mr-%v%v", t.num, i)
				ofile[i], _ = os.Create(oname)
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
			}

		} else if t.t == WorkReduce { //Reduce
			exitn = 0
			var kva []KeyValue
			for i, _ := range t.filepath {
				file, err := os.Open(t.filepath[i])
				if err != nil {
					log.Fatalf("cannot open %v", t.filepath)
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
			oname := fmt.Sprintf("mr-%v", t.num)
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
			alln++
		} else if t.t == WorkWait {
			exitn = 0
			time.Sleep(time.Second)
		} else if t.t == WorkExit {
			log.Fatalln("task i exit")
		}
	}

}
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	for i := 0; i < 10; i++ { //在单个主机跑10并发计算
		go dowork(i, mapf, reducef) //TODO同步来让Worker阻塞
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
