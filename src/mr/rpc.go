package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

type TaskType uint8

const (
	NoTask TaskType = iota
	MapTask
	ReduceTask
	Close
)

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
type GetTaskArgs struct {
	WorkerId int
	Host     string
}

type GetTaskReply struct {
	TaskType TaskType
	WorkerId int
	TaskId   int
	NReduce  int
}

type GetMapTaskArgs struct {
	WorkerId int
	TaskId   int
}

type GetMapTaskReply struct {
	FileName string
	Content  string
}

type GetReduceTaskArgs struct {
	WorkerId int
	TaskId   int
}

type GetReduceTaskReply struct {
	MapWorkerHosts []string
}

type GetMapResultArgs struct {
	ReduceId   int
	ReadOffSet int
}

type GetMapResultReply struct {
	Data   []byte
	IsDone bool
}

type ReceiveWrongWorkerArgs struct {
	WrongHost string
	NewHost   string
}

type ReceiveWrongWorkerReply struct {
}

type FinishTaskArgs struct {
	TaskType TaskType
	WorkerId int
	TaskId   int
}

type FinishTaskReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	//s := "/var/tmp/5840-mr-"
	//s += strconv.Itoa(os.Getuid())
	return "127.0.0.1:1234"
}
