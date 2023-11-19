package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)


type Coordinator struct {
	// Your definitions here.
	UnstartedMapTask []string
	StartedMapTask []string
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RPCHandleInitialize(args *Args, reply *Reply) error {
	if len(c.UnstartedMapTask) == 0 {
		return errors.New("All tasks started/finished!")
	}
	reply.Y = c.UnstartedMapTask[0]
	c.StartedMapTask = append(c.StartedMapTask, reply.Y)
	c.UnstartedMapTask = c.UnstartedMapTask[1:]
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
	c.UnstartedMapTask = files

	c.server()
	return &c
}
