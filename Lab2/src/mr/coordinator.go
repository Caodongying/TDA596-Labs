package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

// ADD LOCKS EVERYWHERE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
// Careful with variable names!!!!!!

type Coordinator struct {
	// TaskState for map/reduce tasks: Unstarted, Running, Finished
	// MapTaskStates and ReduceTaskStates have fixed length
	MapTaskStates           []KeyValue // Key: FileName, Value: TaskState
	ReduceTaskStates        []KeyValue // Key: ReduceTask, Value: TaskState
	MapChannel              chan KeyValue
	ReduceChannel           chan int
	State                   string // Map, Reduce, Wait    ?????????, Done
	FinishedMapTaskCount    int
	FinishedReduceTaskCount int
	NMap                    int
	NReduce                 int
	waitTime                int
	// locks
	LockMapTaskStates           sync.Mutex
	LockReduceTaskStates        sync.Mutex
	LockState                   sync.Mutex
	LockFinishedMapTaskCount    sync.Mutex
	LockFinishedReduceTaskCount sync.Mutex
}

func (c *Coordinator) CheckIfWait() {
	if len(c.MapChannel) == 0 {
		c.LockState.Lock()
		if c.State != "Reduce" {
			c.State = "Wait"
		}
		c.LockState.Unlock()
		return
	}
	if len(c.ReduceChannel) == 0 {
		c.LockState.Lock()
		if c.State == "Reduce" {
			c.State = "Wait"
		}
		c.LockState.Unlock()
		return
	}
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RPCGiveTask(args *Args, reply *Reply) error {
	// 1. Start on a new Map Task
	// 2. All map tasks started but not all finished, wait
	// 3. All map tasks finished, start on a Reduce Task
	// 4. All reduce tasks started but not all finished, wait

	c.LockState.Lock()
	if c.State == "Done" {
		c.LockState.Unlock()
		reply.ReplyType = "Done"
		return nil
	}
	c.LockState.Unlock()
	c.CheckIfWait()

	c.LockState.Lock()
	switch c.State {

	case "Wait":
		c.LockState.Unlock()
		reply.ReplyType = "Wait"
		return nil

	case "Map":
		c.LockState.Unlock()
		// Assign a new map task
		reply.ReplyType = "Map"
		reply.NReduce = c.NReduce
		reply.MapTask = <-c.MapChannel

		var taskIndex int
		for i := range c.MapTaskStates {
			if c.MapTaskStates[i].Key == reply.MapTask.Value {
				c.LockMapTaskStates.Lock()
				c.MapTaskStates[i].Value = "Running"
				c.LockMapTaskStates.Unlock()
				taskIndex = i
				break
			}
		}
		reply.StartTime = time.Now()
		go c.handleMapTaskTimer(taskIndex, reply.MapTask)
		return nil

	case "Reduce":
		c.LockState.Unlock()
		// Assign a new reduuce task
		reply.ReplyType = "Reduce"
		reply.ReduceTask = <-c.ReduceChannel

		var taskIndex int
		for i := range c.ReduceTaskStates {
			if c.ReduceTaskStates[i].Key == strconv.Itoa(reply.ReduceTask) {
				c.LockReduceTaskStates.Lock()
				c.ReduceTaskStates[i].Value = "Running"
				c.LockReduceTaskStates.Unlock()
				taskIndex = i
				break
			}
		}
		reply.StartTime = time.Now()
		go c.handleReduceTaskTimer(taskIndex, reply.ReduceTask)
		return nil

	default:
		fmt.Println(c.State)
		c.LockState.Unlock()
		return errors.New("Unknown State! Cannot give task!")
	}
}

func (c *Coordinator) handleMapTaskTimer(taskIndex int, mapTask KeyValue) {
	timer := time.NewTimer(time.Duration(c.waitTime) * time.Second)
	<-timer.C
	c.LockMapTaskStates.Lock()
	if c.MapTaskStates[taskIndex].Value != "Finished" {
		c.MapTaskStates[taskIndex].Value = "Unstarted"
		c.MapChannel <- mapTask
		c.LockState.Lock()
		c.State = "Map"
		c.LockState.Unlock()
	}
	c.LockMapTaskStates.Unlock()
}

func (c *Coordinator) handleReduceTaskTimer(taskIndex int, reduceTask int) {
	timer := time.NewTimer(time.Duration(c.waitTime) * time.Second)
	<-timer.C
	c.LockReduceTaskStates.Lock()
	if c.ReduceTaskStates[taskIndex].Value != "Finished" {
		c.ReduceTaskStates[taskIndex].Value = "Unstarted"
		c.ReduceChannel <- reduceTask
		c.LockState.Lock()
		c.State = "Reduce"
		c.LockState.Unlock()
	}
	c.LockReduceTaskStates.Unlock()
}

func (c *Coordinator) RPCFinishTask(args *Args, reply *Reply) error {
	fmt.Println("Enter RPCFinishTask")
	//Check Timeout
	elapsed := time.Since(args.StartTime)
	fmt.Println("duration: ", time.Duration.Seconds(elapsed))
	if time.Duration.Seconds(elapsed) > float64(c.waitTime) {
		return nil
	}

	if args.IsMap {
		c.LockFinishedMapTaskCount.Lock()
		c.FinishedMapTaskCount++
		fmt.Println("Map task file is", reply.MapTask.Value, "FinishedMapTaskCount is ", c.FinishedMapTaskCount, " NMap is ", c.NMap)
		c.LockFinishedMapTaskCount.Unlock()

		c.LockMapTaskStates.Lock()
		for _, mapTask := range c.MapTaskStates {
			if mapTask.Key == args.MapTask.Value {
				mapTask.Value = "Finished"
				break
			}
		}
		c.LockMapTaskStates.Unlock()

		c.LockFinishedMapTaskCount.Lock()
		if c.FinishedMapTaskCount == c.NMap {
			c.LockFinishedMapTaskCount.Unlock()
			c.LockState.Lock()
			c.State = "Reduce"
			c.LockState.Unlock()
			fmt.Println("We are now switching to Reduce")
		} else {
			c.LockFinishedMapTaskCount.Unlock()
		}
	} else {
		c.LockFinishedReduceTaskCount.Lock()
		c.FinishedReduceTaskCount++
		c.LockFinishedReduceTaskCount.Unlock()

		for _, reduceTask := range c.ReduceTaskStates {
			if reduceTask.Key == strconv.Itoa(args.ReduceTask) {
				c.LockReduceTaskStates.Lock()
				reduceTask.Value = "Finished"
				c.LockReduceTaskStates.Unlock()
				break
			}
		}

		c.LockFinishedReduceTaskCount.Lock()
		if c.FinishedReduceTaskCount == c.NReduce {
			c.LockFinishedReduceTaskCount.Unlock()
			c.LockState.Lock()
			c.State = "Done"
			c.LockState.Unlock()
		} else {
			c.LockFinishedReduceTaskCount.Unlock()
		}
	}

	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.LockState.Lock()
	ret := c.State == "Done"
	c.LockState.Unlock()
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Set the state
	if len(files) == 0 {
		c.State = "Done"
		return &c
	}

	c.State = "Map"

	c.waitTime = 20 // initialise the wait time
	c.NMap = len(files)
	c.NReduce = nReduce

	c.MapChannel = make(chan KeyValue, c.NMap)
	c.ReduceChannel = make(chan int, c.NReduce)

	// Initialize all fields
	for i, file := range files {
		mapTask := KeyValue{Key: file, Value: "Unstarted"}
		c.MapTaskStates = append(c.MapTaskStates, mapTask)
		c.MapChannel <- KeyValue{Key: strconv.Itoa(i), Value: file}
	}

	for i := 0; i < nReduce; i++ {
		reduceTask := KeyValue{Key: strconv.Itoa(i), Value: "Unstarted"}
		c.ReduceTaskStates = append(c.ReduceTaskStates, reduceTask)
		c.ReduceChannel <- i
	}

	c.server()
	return &c
}
