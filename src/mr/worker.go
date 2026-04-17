package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	//get a task from the coordinator
	for {
		//keep looping until success of rpc call
		workerResp, rpcStatus := CallAssignTask()
		task := workerResp.CurTask

		if !rpcStatus {
			//exit on failed rpc
			return
		}

		switch task.CurType {
		case Map:
			MapTask(task, mapf)
			CallTaskDone(task)
		case Reduce:
			ReduceTask(task, reducef)
			CallTaskDone(task)
		case Wait:
			time.Sleep(5 * time.Second)
		case Exit:
			return
		}

	}

}

func ReduceTask(task Task, reducef func(string, []string) string) {
	bucketsToRead := task.TaskId
	//read buckets of id
	kvList := []KeyValue{}
	for i := range task.Nmap {
		fileName := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(bucketsToRead)
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("intermediate bucket with name %s could not be found", fileName)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kvList = append(kvList, kv)
		}
		file.Close()
	}

	//sort by keys
	sort.Sort(ByKey(kvList))
	outputName := "mr-out-" + strconv.Itoa(bucketsToRead)
	tempName := "mr-out-*"
	ofil, err := os.CreateTemp(".", tempName)

	if err != nil {
		log.Fatal("output file could not be created")
	}

	i := 0

	for i < len(kvList) {
		j := i + 1

		for j < len(kvList) && kvList[j].Key == kvList[i].Key {
			j++
		}
		values := []string{}

		for k := i; k < j; k++ {
			values = append(values, kvList[k].Value)
		}

		output := reducef(kvList[i].Key, values)
		//write to file the key and its total counts
		fmt.Fprintf(ofil, "%v %v\n", kvList[i].Key, output)
		i = j
	}

	ofil.Close()
	os.Rename(ofil.Name(), outputName)
}

func MapTask(task Task, mapf func(string, string) []KeyValue) {
	curFile := task.Filename
	file, err := os.Open(curFile)
	if err != nil {
		log.Fatalf("file with name %s could not be opened", curFile)
	}

	defer file.Close()
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("file with name %s could not be read", curFile)
	}

	kvPairs := mapf(curFile, string(content))
	bucketArr := []*os.File{}
	for cur := range task.Nreduce {
		tempName := "mr-*-" + strconv.Itoa(cur)
		tempFile, err := os.CreateTemp(".", tempName)
		if err != nil {
			log.Fatalf("os could not create temp file")
		}
		bucketArr = append(bucketArr, tempFile)
	}

	for _, pair := range kvPairs {
		//reduce file pointer will be in bucketarr[routeval]
		routeVal := ihash(pair.Key) % task.Nreduce
		fp := bucketArr[routeVal]
		//write data to temp file
		enc := json.NewEncoder(fp)
		err := enc.Encode(&pair)
		if err != nil {
			log.Fatal("kv could not be json endcoded")
		}
	}

	//after writing to temp files rename them
	for ind, fp := range bucketArr {
		newName := "mr-" + strconv.Itoa(task.TaskId) + "-" + strconv.Itoa(ind)
		fp.Close()
		err := os.Rename(fp.Name(), newName)
		if err != nil {
			log.Fatalf("rename failed at file %s", newName)
		}
	}

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func CallTaskDone(task Task) {
	req := TaskDoneReq{CurTask: task}
	emptyResp := TaskDoneResp{}
	ok := call("Coordinator.TasksDone", &req, &emptyResp)
	if ok {
		log.Print("worker completed task")
	} else {
		log.Print("worker could not complete task")
	}
}

func CallAssignTask() (WorkerResp, bool) {
	emptyReq := WorkerRequest{}
	workerTask := WorkerResp{}
	ok := call("Coordinator.AssignTask", &emptyReq, &workerTask)
	if !ok {
		fmt.Printf("Worker rpc call failed")
		return workerTask, false
	} else {
		fmt.Printf("Worker rpc call successful")
	}

	return workerTask, true
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
