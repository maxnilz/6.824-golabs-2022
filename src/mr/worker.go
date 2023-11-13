package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	w := &worker{
		mapf:    mapf,
		reducef: reducef,
	}

	for {
		mapTask, err := w.getMapTask()
		if err != nil {
			if err.Error() == ErrNoMoreMapTask.Error() {
				break
			}
			log.Fatal(err)
		}
		if mapTask.File == "" {
			time.Sleep(time.Second)
			continue // waiting dead worker to be timeout at coordinator
		}
		log.Println("map file", mapTask.File)
		if err = w.Map(mapTask); err != nil {
			log.Fatal(err)
		}
		if err = w.completeMapTask(mapTask.Id); err != nil {
			log.Fatal(err)
		}
	}

	for {
		reduceTask, err := w.getReduceTask()
		if err != nil {
			if err.Error() == ErrWaitingUncompletedMap.Error() {
				time.Sleep(time.Second)
				continue
			}
			if err.Error() == ErrNoMoreReduceTask.Error() {
				break
			}
			log.Fatal(err)
		}
		if reduceTask.NTasks == 0 {
			time.Sleep(time.Second)
			continue // waiting dead worker to be timeout at coordinator
		}
		log.Println("reduce", reduceTask.Id, reduceTask.NTasks)
		if err = w.Reduce(reduceTask.Id, reduceTask.NTasks); err != nil {
			log.Fatal(err)
		}
		if err = w.completeReduceTasks([]int{reduceTask.Id}); err != nil {
			log.Fatal(err)
		}
	}
}

type worker struct {
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
}

func (w *worker) Register() error {
	args := RegisterWorkerArgs{}

	reply := RegisterWorkerReply{}
	err := call("Coordinator.Register", &args, &reply)
	if err != nil {
		return err
	}
	return nil
}

func (w *worker) Map(task *MapTask) error {
	var fds []*os.File
	defer func() {
		for _, fd := range fds {
			fd.Close()
		}
	}()

	filename := task.File
	var intermediate []KeyValue
	file, err := os.Open(filename)
	if err != nil {
		return errors.New(fmt.Sprintf("cannot open %v", filename))
	}
	defer file.Close()
	content, err := ioutil.ReadAll(file)
	if err != nil {
		return errors.New(fmt.Sprintf("cannot read %v", filename))
	}
	kva := w.mapf(filename, string(content))
	intermediate = append(intermediate, kva...)

	encoders := make(map[int]*json.Encoder)
	for _, kv := range intermediate {
		reduceId := ihash(kv.Key) % task.NReduces
		encoder, ok := encoders[reduceId]
		if !ok {
			interFilename := fmt.Sprintf("mr-%v-%v", task.Id, reduceId)
			fd, err := os.Create(interFilename)
			if err != nil {
				return errors.New(fmt.Sprintf("cannot open file %v, err: %v", interFilename, err))
			}
			fds = append(fds, fd)
			encoder = json.NewEncoder(fd)
			encoders[reduceId] = encoder
		}
		if err := encoder.Encode(&kv); err != nil {
			return errors.New(fmt.Sprintf("write key %v failed: %v", kv.Key, err))
		}
	}

	return nil
}

func (w *worker) Reduce(id int, nMapTask int) error {
	intermediate := []KeyValue{}
	for i := 0; i < nMapTask; i++ {
		interFilename := fmt.Sprintf("mr-%v-%v", i, id)
		f, err := os.Open(interFilename)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return errors.New(fmt.Sprintf("cannot open %v", interFilename))
		}
		defer f.Close()
		decoder := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err = decoder.Decode(&kv); err != nil {
				if err == io.EOF {
					break
				}
				return errors.New(fmt.Sprintf("read %v failed: %v", interFilename, err))
			}
			intermediate = append(intermediate, kv)
		}
	}

	sort.Sort(ByKey(intermediate))

	ofile, err := ioutil.TempFile("", "mr-out-tmp-")
	if err != nil {
		return errors.New(fmt.Sprintf("cannot create tmp file failed: %v", err))
	}

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-id.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := w.reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	ofile.Close()

	oname := fmt.Sprintf("mr-out-%v", id)
	if err = os.Rename(ofile.Name(), oname); err != nil {
		return errors.New(fmt.Sprintf("create %v failed: %v", oname, err))
	}

	return nil
}

func (w *worker) getMapTask() (*MapTask, error) {
	args := GetMapTaskArgs{}
	mapTask := MapTask{}
	err := call("Coordinator.GetMapTask", &args, &mapTask)
	if err != nil {
		return nil, err
	}
	return &mapTask, nil
}

func (w *worker) completeMapTask(id int) error {
	args := CompleteMapTaskArgs{Id: id}
	reply := CompleteMapTaskReply{}
	err := call("Coordinator.CompleteMapTask", &args, &reply)
	if err != nil {
		return err
	}
	return nil
}

func (w *worker) getReduceTask() (*ReduceTask, error) {
	args := GetReduceTasksArgs{}
	reduceTask := ReduceTask{}
	err := call("Coordinator.GetReduceTasks", &args, &reduceTask)
	if err != nil {
		return nil, err
	}
	return &reduceTask, nil
}

func (w *worker) completeReduceTasks(ids []int) error {
	args := CompleteReduceTasksArgs{Ids: ids}
	reply := CompleteReduceTasksReply{}
	if err := call("Coordinator.CompleteReduceTasks", &args, &reply); err != nil {
		return err
	}
	return nil
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) error {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		return errors.New(fmt.Sprintf("dialing: %v", err))
	}
	defer c.Close()

	if err = c.Call(rpcname, args, reply); err != nil {
		return err
	}

	return nil
}
