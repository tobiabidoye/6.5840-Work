package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

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

// Add your RPC definitions here.
type TaskType int

// iota acts as enum
const (
	Map TaskType = iota
	Reduce
	Wait
	Exit
)

// Request that a worker sends to a coordinator
type WorkerRequest struct {
}

// Response that worker is sent from a coordinator
// some way to indicate that workers should wait for reduce phase
type WorkerResp struct {
	CurTask Task
}

// worker resp is composed of a task
type Task struct {
	//weak but works if i am careful
	CurType  TaskType
	Filename string
	//Nmap for map bucket
	TaskId int
	//intermediate bucket to target for reduce
	Nreduce int
	Nmap    int
}

type TaskDoneReq struct {
	CurTask Task
}

type TaskDoneResp struct {
	//empty response
}
