package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

// example to show how to declare the arguments
// and reply for an RPC.
var (
	MAP_TASK    = 1
	REDUCE_TASK = 2
	WAIT        = 3
	PLEASE_QUIT = 4
)

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type Task struct {
	TaskType int
	TaskNum  int
	FileName []string
}
type AskTaskArgs struct {
	Ask string
}

type AskTaskReply struct {
	Task      Task
	ReduceNum int
}
type SubmitTaskArgs struct {
	Task Task
}
type SubmitTaskReply struct {
	Reply string
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
