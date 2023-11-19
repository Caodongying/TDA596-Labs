package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"strconv"
)

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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	args := Args{}
	// fill in args
	reply := Reply{}
	
	ok := call("Coordinator.RPCHandleInitialize", &args, &reply)
	
	if ok {
		fmt.Printf("reply.FileName %v\n", reply.File.Value)
		// read the file and call mapf
		fileContent, err := os.ReadFile("./" + reply.File.Value) // not sure
		if err!=nil {
			// TODO: file not exist
		} else {
			// Split Map output into NReduce chunks
			intermediateOutputs := mapf(reply.File.Value, string(fileContent[:]))
			mapOutputBuckets := make([][]KeyValue, reply.NReduce)
			for _, pair := range intermediateOutputs{
				ReduceNumber := ihash(pair.Key) % reply.NReduce
				mapOutputBuckets[ReduceNumber] = append(mapOutputBuckets[ReduceNumber], pair)
			}

			// Write NReduce chunks into files naming like mr-X-Y
			MapNumber := reply.File.Key
			for ReduceNumber, content := range mapOutputBuckets{
				intermediateFile := "./mr-" + MapNumber + "-" + strconv.Itoa(ReduceNumber) + ".txt"
				file, err := os.Create(intermediateFile)
				if err != nil {
					// DO MORE
					log.Fatal(err)
				}
				enc := json.NewEncoder(file)
				for _, kv := range content {
					enc.Encode(&kv)
				}
			}
			
		}

	} else {
		fmt.Printf("call failed!\n") // ????
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

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.FileName should be 100.
		fmt.Printf("reply.FileName %v\n", reply.Y)
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
