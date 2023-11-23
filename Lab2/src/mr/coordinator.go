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
}

func (c *Coordinator) CheckIfWait() {
	if len(c.MapChannel) == 0 {
		if c.State != "Reduce" {
			c.State = "Wait"
		}
		return
	}
	if len(c.ReduceChannel) == 0 {
		if c.State == "Reduce" {
			c.State = "Wait"
		}
		return
	}
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RPCGiveTask(args *Args, reply *Reply) error {
	// 1. Start on a new Map Task
	// 2. All map tasks started but not all finished, wait
	// 3. All map tasks finished, start on a Reduce Task
	// 4. All reduce tasks started but not all finished, wait

	if c.State == "Done" {
		reply.ReplyType = "Done"
		return nil
	}

	c.CheckIfWait()

	if c.State == "Wait" {
		reply.ReplyType = "Wait"
		return nil
	}

	if c.State == "Map" {
		// Assign a new map task
		reply.ReplyType = "Map"
		reply.NReduce = c.NReduce
		reply.MapTask = <-c.MapChannel

		var taskIndex int
		for i := range c.MapTaskStates {
			if c.MapTaskStates[i].Key == reply.MapTask.Value {
				c.MapTaskStates[i].Value = "Running"
				taskIndex = i
				break
			}
		}
		reply.StartTime = time.Now()
		timer := time.NewTimer(10 * time.Second)
		defer c.handleMapTaskTimer(timer, taskIndex, reply.MapTask)
		return nil
	}

	if c.State == "Reduce" {
		// Assign a new reduuce task
		reply.ReplyType = "Reduce"
		reply.ReduceTask = <-c.ReduceChannel

		var taskIndex int
		for i := range c.ReduceTaskStates {
			if c.ReduceTaskStates[i].Key == strconv.Itoa(reply.ReduceTask) {
				c.ReduceTaskStates[i].Value = "Running"
				taskIndex = i
				break
			}
		}
		reply.StartTime = time.Now()
		timer := time.NewTimer(10 * time.Second)
		defer c.handleReduceTaskTimer(timer, taskIndex, reply.ReduceTask)
		return nil
	}

	return errors.New("Unknown State! Cannot give task!")
}

func (c *Coordinator) handleMapTaskTimer(timer *time.Timer, taskIndex int, mapTask KeyValue) {
	<-timer.C
	if c.MapTaskStates[taskIndex].Value != "Finished" {
		c.MapTaskStates[taskIndex].Value = "Unstarted"
		c.MapChannel <- mapTask
	}
}

func (c *Coordinator) handleReduceTaskTimer(timer *time.Timer, taskIndex int, reduceTask int) {
	<-timer.C
	if c.ReduceTaskStates[taskIndex].Value != "Finished" {
		c.ReduceTaskStates[taskIndex].Value = "Unstarted"
		c.ReduceChannel <- reduceTask
	}
}

func (c *Coordinator) RPCFinishTask(args *Args, reply *Reply) error {
	fmt.Println("Enter RPCFinishTask")
	//Check Timeout
	elapsed := time.Since(args.StartTime)
	fmt.Println("duration: ", time.Duration.Seconds(elapsed))
	if time.Duration.Seconds(elapsed) > float64(20) {
		return nil
	}

	if args.IsMap {
		c.FinishedMapTaskCount++
		fmt.Println("FinishedMapTaskCount is ", c.FinishedMapTaskCount, " NMap is ", c.NMap)

		for _, mapTask := range c.MapTaskStates {
			if mapTask.Key == args.MapTask.Value {
				mapTask.Value = "Finished"
				break
			}
		}

		if c.FinishedMapTaskCount == c.NMap {
			c.State = "Reduce"
			fmt.Println("We are now switching to Reduce")
		}
	} else {
		c.FinishedReduceTaskCount++

		for _, reduceTask := range c.ReduceTaskStates {
			if reduceTask.Key == strconv.Itoa(args.ReduceTask) {
				reduceTask.Value = "Finished"
				break
			}
		}

		if c.FinishedReduceTaskCount == c.NReduce {
			c.State = "Done"
			// TODO:   call Done() !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
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
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.NMap = len(files)
	c.NReduce = nReduce
	c.State = "Map"

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
