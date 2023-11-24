package mr

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// type ReduceDictionary struct {
// 	Key string
// 	Values []string
// }
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
	isActive := true // indicate if worker is active
	for isActive {
		args := Args{}
		reply := Reply{}
		ok := call("Coordinator.RPCGiveTask", &args, &reply)
		
		if ok {
			switch reply.ReplyType {
			case "Map":
				fmt.Println("The worker receives reply type Map")
				handleMapTask(&args, &reply, mapf)
				// err := handleMapTask(&args, &reply, mapf)
				// if err != nil {
				// 	fmt.Println(err)
				// }
			case "Reduce":
				fmt.Println("The worker receives reply type Reduce")
				handleReduceTask(&args, &reply, reducef)
			case "Wait":
				fmt.Println("The worker receives reply type Wait")
				time.Sleep(1*time.Second)
			case "Done":
				fmt.Println("The worker receives reply type Done")
				fmt.Println("Coordinator is done. Shut down the worker.")
				isActive = false
			default:
				fmt.Println(errors.New("ReplyType unrecognized. Shut down the worker!"))
				isActive = false
			}
		} else {
			// should crash
			fmt.Printf("The coordinator is shut down. Worker exits.\n") // ????
			isActive = false
		}
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

	return false
}

func handleMapTask(args *Args, reply *Reply, mapf func(string, string) []KeyValue) {
	// read the file and call mapf
	fileContent, err := os.ReadFile("./" + reply.MapTask.Value)
	if err!=nil {
		fmt.Printf("Error when opening file %v\n", reply.MapTask.Value)
	} else {
		// Split Map output into NReduce chunks
		intermediateOutputs := mapf(reply.MapTask.Value, string(fileContent[:])) // mapf takes filename and file content
		mapOutputBuckets := make([][]KeyValue, reply.NReduce)
		for _, pair := range intermediateOutputs{
			ReduceNumber := ihash(pair.Key) % reply.NReduce
			mapOutputBuckets[ReduceNumber] = append(mapOutputBuckets[ReduceNumber], pair)
		}

		// Write NReduce chunks into files naming like mr-X-Y
		MapNumber := reply.MapTask.Key
		for ReduceNumber, content := range mapOutputBuckets{
			intermediateFile := "./mr-" + MapNumber + "-" + strconv.Itoa(ReduceNumber) + ".txt"
			file, err := os.Create(intermediateFile)
			if err != nil {
				log.Fatal(err)
				return
			}
			enc := json.NewEncoder(file)
			for _, kv := range content {
				enc.Encode(&kv)
			}
		}

		args.StartTime = reply.StartTime
		args.IsMap = true
		args.MapTask = reply.MapTask

		mapFinishOk := call("Coordinator.RPCFinishTask", &args, &reply)
		if !mapFinishOk {
			fmt.Println("call RPCFinishTask fails!")
		}
	}
}

func handleReduceTask(args *Args, reply *Reply, reducef func(string, []string) string){
	// reducef will be called in a loop
	// First group/merge the files for the reducer
	// Create an empty dictionary like {key, [value1, value2...]}
	// Read files that should be handled by the reducer
	// if the key is in the dictionary, append the value to the list
	// if the key doesn't exist, create an entry
	reducer := reply.ReduceTask
	reduceDic := make(map[string][]string)
	files, err := filepath.Glob("../main/mr-*-" + strconv.Itoa(reducer) + ".txt")
	if err != nil {
		fmt.Println("Cannot get the files via pattern:", err)
		return
	}

	for _, file := range files {
		// before using NewDecoder, open the file
		fileContent, err := os.ReadFile(file)
		if err != nil {
			fmt.Println("Cannot read the file", err)
			return
		}
		dec := json.NewDecoder(bytes.NewReader(fileContent))
		// process the key-value pairs and put them in reduceDic

		for {
			var kv KeyValue
			if err:= dec.Decode(&kv); err != nil {
				break
			}
			// check if the key is inside reduceDic
			val, ok := reduceDic[kv.Key]
			if ok {
				// the key exists in reduceDic, append the value to the list
				reduceDic[kv.Key] = append(val, kv.Value)		
			}else{
				// create a new entry
				reduceDic[kv.Key] = []string{kv.Value}
			}
		}

	}

	// Apply reducef on the dictionary
	// create the output file
	filePath := "../main/mr-out-" + strconv.Itoa(reducer) + ".txt"  // not sure if .txt is needed
	reduceOutputFile, err := os.Create(filePath)
	if err != nil {
		log.Fatal(err)
		return
	}
	for key, values := range reduceDic {
		output := reducef(key, values)
		stringToWrite := fmt.Sprintf("%v %v\n", key, output)
		_, err := reduceOutputFile.WriteString(stringToWrite)
		if err != nil {
			fmt.Println("Cannot write reduce output", err)
			return
		}
	}

	// Notify the coordinator that this is done
	args.StartTime = reply.StartTime
	args.IsMap = false
	args.ReduceTask = reply.ReduceTask

	reduceFinishOk := call("Coordinator.RPCFinishTask", &args, &reply)
	if !reduceFinishOk {
		fmt.Println("call RPCFinishTask fails!")
	}
}