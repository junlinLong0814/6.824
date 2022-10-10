package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

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

type TaskKind int

const (
	Map_Task 	TaskKind = 1
	Reduce_Task	TaskKind = 2
	Idle_Task	TaskKind = 3	//let worker sleep
)

// Add your RPC definitions here.
type RequestTaskArgs struct {

}

type RequestTaskReply struct {
	Task_kind 		TaskKind			
	Map_file_name	string		//filename need in map func
	Reducer_num		int			//reducer number
	Map_num			int
	Task_idx		int			//idx for task
}

type DoneTaskArgs struct {
	Task_kind 		TaskKind
	Task_idx		int
}

type DoneTaskReply struct{
	
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
