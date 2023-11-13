package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"errors"
	"os"
)
import "strconv"

// Add your RPC definitions here.

var ErrWaitingUncompletedMap = errors.New("map not finished yet")
var ErrNoMoreMapTask = errors.New("no more map task")
var ErrNoMoreReduceTask = errors.New("no more reduce task")

type RegisterWorkerArgs struct{}

type RegisterWorkerReply struct{}

type GetMapTaskArgs struct{}

type MapTask struct {
	Id       int
	File     string
	NReduces int
}

type CompleteMapTaskArgs struct {
	Id int
}

type CompleteMapTaskReply struct{}

type GetReduceTasksArgs struct{}

type ReduceTask struct {
	Id     int
	NTasks int
}

type CompleteReduceTasksArgs struct {
	Ids []int
}

type CompleteReduceTasksReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
