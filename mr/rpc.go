package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

const wait = 1
const done = 3

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Request Dummy request, carries no information
type Request struct {
}

// Reply Dummy reply, carries no information
type Reply struct {
}

// MapTask Specifies task information for map operations
type MapTask struct {
	Filepath    string
	NReduce     int
	MapTaskID   int
	Instruction int
}

// ReduceTask Specifies task information for reduce operations
type ReduceTask struct {
	TaskID      int
	Instruction int
}

// IntermediateDataRequest Specifies the reduce task for which intermediate files need to be accessed
type IntermediateDataRequest struct {
	TaskID int
}

// IntermediateFiles Collection of intermediate files for a specific request task
type IntermediateFiles struct {
	TaskID      int
	Files       []string
	Instruction int
}

// CompletedJob Empty struct, indicates that all tasks for job are completed
type CompletedJob struct {
}

// MapCompletion Completion details for map task. Meant to be sent to master
type MapCompletion struct {
	TaskID            int
	IntermediateFiles []string
}

// ReduceCompletion Completion details for reduce task. Meant to be sent to master
type ReduceCompletion struct {
	TaskID int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
