package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type Args struct {
	StartTime time.Time
	IsMap bool
	MapTask KeyValue   // Key is the MapNumber, Value is the Filename
	ReduceTask int
}

type Reply struct {
	StartTime time.Time
	ReplyType string // Map, Reduce, Wait, Done
	MapTask KeyValue // Key is the MapNumber, Value is the Filename
	ReduceTask int
	NReduce int
}

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
