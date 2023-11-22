package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
)



type Coordinator struct {
	// TaskState for map/reduce tasks: Unstarted, Running, Finished
	// AllMapTasks and AllReduceTasks have fixed length
	AllMapTasks []KeyValue // Key: FileName, Value: TaskState
	AllReduceTasks []KeyValue // Key: ReduceTask, Value: TaskState
	MapChannel chan string
	ReduceChannel chan int
	State string // Map, Reduce, Wait    ?????????, Done
	FinishedMapTaskCount int
	FinishedReduceTaskCount int
	NMap int
	NReduce int
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RPCHandleInitialize(args *Args, reply *Reply) error {
	// 1. Start on a new Map Task
	// 2. All map tasks started but not all finished, wait
	// 3. All map tasks finished, start on a Reduce Task
	// 4. All reduce tasks started but not all finished, wait

	// ?. Not Sure due to Done() - All reduce tasks finished. Nothing to do! 😁

	if len(c.UnStartedMapTask) != 0 {
		// Assign a new map task
		reply.ReplyType = "map"
		reply.File = c.UnStartedMapTask[0]
		reply.NReduce = c.NReduce
		c.UnStartedMapTask = c.UnStartedMapTask[1:]
		c.StartedMapTask = append(c.StartedMapTask, reply.File)	
		return nil
	}

	if len(c.StartedMapTask) != 0 {
		// Wait for all map tasks to be done
		reply.ReplyType = "sleep"
		return nil
	}

	if len(c.UnStartedReduceTask) != 0 {
		// Assign a new reduce task
		reply.ReplyType = "reduce"
		reply.ReduceNumber = c.UnStartedReduceTask[0]
		c.UnStartedReduceTask = c.UnStartedReduceTask[1:]
		c.StartedReduceTask = append(c.StartedReduceTask, reply.ReduceNumber)
		return nil
	}

	if len(c.StartedReduceTask) != 0 {
		// Wait for all reduce tasks to be done
		reply.ReplyType = "sleep"
		return nil
	}



	
	
	// if len(c.StartedMapTask) != 0 {
	// 	// Wa
	// }
	//c.MapFinishedTask = []
	return nil
}


func (c *Coordinator) RPCHandleMapFinish(args *Args, reply *Reply) error {
	c.FinishedMapTask = append(c.FinishedMapTask, args.File)
	// Remove the finished map task from the StartedMapTask list
	for i, v := range c.StartedMapTask {
		if v == args.File {
			c.StartedMapTask[i] = c.StartedMapTask[len(c.StartedMapTask)-1]
			c.StartedMapTask = c.StartedMapTask[:len(c.StartedMapTask)-1]
			break
		}
	}
	// TODO - check validation

	return nil
}

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

	// Your code here.
	c.NMap = len(files)
	c.NReduce = nReduce
	c.State = "Map"

	c.MapChannel = make(chan string, c.NMap)
	c.ReduceChannel = make(chan int, c.NReduce)

	// Initialize all fields
	for _, file := range files {
		mapTask := KeyValue{Key: file, Value: "Unstarted"}
		c.AllMapTasks = append(c.AllMapTasks, mapTask)
		c.MapChannel <- file
	}

	for i:=0; i<nReduce; i++ {
		reduceTask := KeyValue{Key: strconv.Itoa(i), Value: "Unstarted"}
		c.AllReduceTasks = append(c.AllReduceTasks, reduceTask)
		c.ReduceChannel <- i
	}

	c.server()
	return &c
}
