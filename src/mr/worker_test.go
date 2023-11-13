package mr

import (
	"errors"
	"fmt"
	"testing"
)

func TestExampleWorker(t *testing.T) {
	Worker(nil, nil)
}

func TestMakeCoordinator(t *testing.T) {
	args := RegisterWorkerArgs{}

	reply := RegisterWorkerReply{}
	err := call("Coordinator.Register", &args, &reply)
	b := fmt.Errorf("aaa %w", err)
	fmt.Println(errors.Is(b, ErrWaitingUncompletedMap))
	fmt.Println(err.Error() == ErrWaitingUncompletedMap.Error())
}
