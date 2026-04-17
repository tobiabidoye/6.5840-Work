package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"slices"
	"sync"
	"time"
)

type TaskStatus string

const (
	StatusPending    TaskStatus = "pending"
	StatusinProgress TaskStatus = "inProgress"
	StatusDone       TaskStatus = "done"
)

type Coordinator struct {
	// Your definitions here.
	filenames        []string
	nReduce          int
	mapTaskStatus    []TaskStatus
	reduceTaskStatus []TaskStatus
	jobsDone         bool
	mapTimeStamp     map[int]time.Time
	reduceTimeStamp  map[int]time.Time
	mu               sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) TasksDone(req *TaskDoneReq, reply *TaskDoneResp) error {
	//only for map tasks
	c.mu.Lock()
	defer c.mu.Unlock()
	taskId := req.CurTask.TaskId
	taskType := req.CurTask.CurType

	if taskType == Map {
		c.mapTaskStatus[taskId] = StatusDone
	} else {
		c.reduceTaskStatus[taskId] = StatusDone
	}

	return nil
}

func (c *Coordinator) WaitCheck() bool {
	if slices.Contains(c.mapTaskStatus, StatusinProgress) && !slices.Contains(c.mapTaskStatus, StatusPending) {
		return true
	}

	return false
}

func (c *Coordinator) EndCheck() bool {
	if !slices.Contains(c.mapTaskStatus, StatusPending) && !slices.Contains(c.mapTaskStatus, StatusinProgress) && !slices.Contains(c.reduceTaskStatus, StatusPending) && !slices.Contains(c.reduceTaskStatus, StatusinProgress) {
		return true
	}
	return false
}

func (c *Coordinator) AssignTask(req *WorkerRequest, reply *WorkerResp) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if slices.Contains(c.mapTaskStatus, StatusPending) {
		for ind, val := range c.mapTaskStatus {
			if val == StatusPending {
				newTask := Task{CurType: Map,
					Filename: c.filenames[ind],
					TaskId:   ind,
					Nreduce:  c.nReduce,
					Nmap:     len(c.filenames),
				}
				reply.CurTask = newTask
				c.mapTaskStatus[ind] = StatusinProgress
				c.mapTimeStamp[ind] = time.Now()
				return nil
			}
		}
	}

	if c.WaitCheck() {
		//send an empty waiting task
		waitTask := Task{CurType: Wait}
		reply.CurTask = waitTask
		return nil
	}

	if slices.Contains(c.reduceTaskStatus, StatusPending) && !slices.Contains(c.mapTaskStatus, StatusPending) && !slices.Contains(c.mapTaskStatus, StatusinProgress) {
		for ind, val := range c.reduceTaskStatus {
			if val == StatusPending {
				newTask := Task{CurType: Reduce,
					Filename: "",
					TaskId:   ind,
					Nreduce:  c.nReduce,
					Nmap:     len(c.filenames),
				}
				reply.CurTask = newTask
				c.reduceTaskStatus[ind] = StatusinProgress
				c.reduceTimeStamp[ind] = time.Now()
				return nil
			}
		}
	}

	if c.EndCheck() {
		//send an empty waiting task
		DoneTask := Task{CurType: Exit}
		reply.CurTask = DoneTask
		return nil
	} else {
		//we wait in this case
		waitTask := Task{CurType: Wait}
		reply.CurTask = waitTask
		return nil
	}

}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.EndCheck()
}

func (c *Coordinator) TimeOutCheck() {
	for {
		c.mu.Lock()
		for id, val := range c.mapTimeStamp {
			//reassign the task
			if time.Since(val) > 10*time.Second && c.mapTaskStatus[id] == StatusinProgress {
				c.mapTaskStatus[id] = StatusPending
			}
		}

		for id, val := range c.reduceTimeStamp {
			if time.Since(val) > 10*time.Second && c.reduceTaskStatus[id] == StatusinProgress {
				c.reduceTaskStatus[id] = StatusPending
			}
		}
		//have to manually unlock since defer doesnt work well with infinite loops
		// for infinite loops must manually unlock since the goroutine will never exit
		// it will only keep running so it will never release the lock
		done := c.EndCheck()
		c.mu.Unlock()

		if done {
			return
		}
		time.Sleep(2 * time.Second)
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {

	c := Coordinator{mapTaskStatus: make([]TaskStatus, 0),
		reduceTaskStatus: make([]TaskStatus, 0),
		reduceTimeStamp:  map[int]time.Time{},
		mapTimeStamp:     map[int]time.Time{}}
	//append filenames
	c.filenames = files
	c.nReduce = nReduce
	//prepopulate map with tasks
	for range c.filenames {
		c.mapTaskStatus = append(c.mapTaskStatus, StatusPending)
	}

	for range nReduce {
		c.reduceTaskStatus = append(c.reduceTaskStatus, StatusPending)
	}

	go c.TimeOutCheck()
	// Your code here.
	c.server(sockname)
	return &c
}
