package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type File struct {
	filename     string
	mapTaskId    int
	mapTaskAt    time.Time
	mapCompleted bool
}

type Reduce struct {
	completed bool
	AssignAt  time.Time
}

type Coordinator struct {
	// Your definitions here.
	files []*File

	completedMapFiles int

	nReduces int
	Reduces  []*Reduce

	completedReduces int

	sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetMapTask(_ *GetMapTaskArgs, mapTask *MapTask) error {
	c.Lock()
	defer c.Unlock()

	if c.completedMapFiles == len(c.files) {
		return ErrNoMoreMapTask
	}

	mapTask.NReduces = c.nReduces
	now := time.Now()
	for i := 0; i < len(c.files); i++ {
		fs := c.files[i]
		if fs.mapCompleted {
			continue // file is completed
		}
		if !fs.mapTaskAt.IsZero() && now.Sub(fs.mapTaskAt) < time.Second*10 {
			continue // timeout
		}
		fs.mapTaskId = i
		fs.mapTaskAt = now
		mapTask.Id = fs.mapTaskId
		mapTask.File = fs.filename
		return nil
	}

	return nil
}

func (c *Coordinator) CompleteMapTask(args *CompleteMapTaskArgs, _ *CompleteMapTaskReply) error {
	c.Lock()
	defer c.Unlock()

	id := args.Id
	if !c.files[id].mapCompleted {
		c.files[id].mapCompleted = true
		c.completedMapFiles++
	}
	log.Println("complete map task", args.Id, c.completedMapFiles)

	// create reduce task if all map tasks are done
	if c.completedMapFiles == len(c.files) {
		c.Reduces = make([]*Reduce, c.nReduces)
		for i := 0; i < c.nReduces; i++ {
			c.Reduces[i] = &Reduce{}
		}
	}

	return nil
}

func (c *Coordinator) GetReduceTasks(_ *GetReduceTasksArgs, reduceTask *ReduceTask) error {
	c.Lock()
	defer c.Unlock()

	if c.completedMapFiles < len(c.files) {
		return ErrWaitingUncompletedMap
	}
	if c.completedReduces == c.nReduces {
		return ErrNoMoreReduceTask
	}
	now := time.Now()
	for i := 0; i < c.nReduces; i++ {
		r := c.Reduces[i]
		if r.completed {
			continue // completed
		}
		if !r.AssignAt.IsZero() && now.Sub(r.AssignAt) < time.Second*10 {
			continue // still alive
		}
		r.AssignAt = now

		reduceTask.Id = i
		reduceTask.NTasks = len(c.files)
		break
	}

	return nil
}

func (c *Coordinator) CompleteReduceTasks(args *CompleteReduceTasksArgs, _ *CompleteReduceTasksReply) error {
	c.Lock()
	defer c.Unlock()

	for _, id := range args.Ids {
		c.Reduces[id].completed = true
		c.completedReduces++
	}
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
	c.Lock()
	defer c.Unlock()

	ret = c.completedReduces == c.nReduces

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	fileStatuses := make([]*File, 0, len(files))
	for _, filename := range files {
		fileStatuses = append(fileStatuses, &File{filename: filename})
	}
	c.nReduces = nReduce
	c.files = fileStatuses

	c.server()
	return &c
}
