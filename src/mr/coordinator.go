package mr

import (
	"fmt"
	"io"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	free = iota
	busy
	success
	wrong
)

var (
	workIdStart = 1
)

type Coordinator struct {
	muCondition     sync.Mutex
	nReduce         int
	files           []string
	mapCondition    []dealStatus
	reduceCondition []dealStatus

	muWorkers  sync.RWMutex
	workerInfo map[int]workerInfo

	TaskDone bool
}

type dealStatus struct {
	workerId int
	status   int
	busyTime time.Time
}

type workerInfo struct {
	workId   int
	host     string
	taskType TaskType
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	if c.TaskDone {
		reply.TaskType = Close
		return nil
	}

	if args.WorkerId == 0 {
		reply.WorkerId = workIdStart
		args.WorkerId = workIdStart
		workIdStart++
	}

	c.muCondition.Lock()
	defer func() {
		c.muCondition.Unlock()
		c.muWorkers.Lock()
		c.workerInfo[args.WorkerId] = workerInfo{workId: args.WorkerId, taskType: reply.TaskType, host: args.Host}
		c.muWorkers.Unlock()
	}()
	for index, dealCondition := range c.mapCondition {
		if dealCondition.status == free || dealCondition.status == wrong {
			if dealCondition.status == wrong {
				wrongHost := c.workerInfo[c.mapCondition[index].workerId].host
				err := c.notifyReduce(wrongHost, args.Host)
				if err != nil {
					return err
				}
			}
			reply.TaskType = MapTask
			reply.NReduce = c.nReduce
			reply.TaskId = index
			c.mapCondition[index] = dealStatus{
				status:   busy,
				busyTime: time.Now(),
				workerId: args.WorkerId,
			}
			return nil
		}
	}
	for index, dealCondition := range c.reduceCondition {
		if dealCondition.status == free {
			reply.TaskType = ReduceTask
			reply.TaskId = index
			c.reduceCondition[index] = dealStatus{
				status:   busy,
				busyTime: time.Now(),
				workerId: args.WorkerId,
			}
			return nil
		}
	}

	reply.TaskType = NoTask

	return nil
}

func (c *Coordinator) GetMapTask(args *GetMapTaskArgs, reply *GetMapTaskReply) error {
	condition := c.mapCondition[args.TaskId]
	if condition.workerId != args.WorkerId {
		return fmt.Errorf("not your task")
	}
	fileName := c.files[args.TaskId]
	file, err := os.Open(fileName)
	if err != nil {
		return err
	}
	defer file.Close()
	data, err := io.ReadAll(file)
	if err != nil {
		return err
	}
	reply.Content = string(data)
	reply.FileName = fileName

	return nil
}

func (c *Coordinator) GetReduceTask(args *GetReduceTaskArgs, reply *GetReduceTaskReply) error {
	condition := c.reduceCondition[args.TaskId]
	if condition.workerId != args.WorkerId {
		return fmt.Errorf("not your task")
	}
	reply.MapWorkerHosts = make([]string, 0, len(c.mapCondition))
	for _, condition := range c.mapCondition {
		host := c.workerInfo[condition.workerId].host
		reply.MapWorkerHosts = append(reply.MapWorkerHosts, host)
	}

	return nil
}

func (c *Coordinator) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	c.muCondition.Lock()
	defer c.muCondition.Unlock()
	if args.TaskType == MapTask {
		// 修改 map 任务状态
		c.mapCondition[args.TaskId].status = success
	}
	if args.TaskType == ReduceTask {
		c.reduceCondition[args.TaskId].status = success
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
	l, e := net.Listen("tcp", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {

	// Your code here.
	c.muCondition.Lock()
	defer c.muCondition.Unlock()
	c.dealWorkerWrong()
	for _, dealstatus := range c.reduceCondition {
		if dealstatus.status != success {
			return false
		}
	}
	c.TaskDone = true
	time.Sleep(2 * time.Second)

	return true
}

func (c *Coordinator) dealWorkerWrong() {
	go c.dealMapWorkerWrong()
	go c.dealReduceWorkerWrong()
}

func (c *Coordinator) dealMapWorkerWrong() {
	c.muCondition.Lock()
	defer c.muCondition.Unlock()

	for index, condition := range c.mapCondition {
		if condition.status == busy {
			timeNow := time.Now().Add(-10 * time.Second)
			if condition.busyTime.Before(timeNow) {
				c.mapCondition[index] = dealStatus{status: wrong}
			}
		}
	}
}

// 已经被调用者加锁
func (c *Coordinator) notifyReduce(wrongHost string, newHost string) (err error) {
	for _, condition := range c.reduceCondition {
		if condition.status == busy {
			c.muWorkers.Lock()
			getHost := c.workerInfo[condition.workerId].host
			c.muWorkers.Unlock()
			args := ReceiveWrongWorkerArgs{wrongHost, newHost}
			reply := ReceiveWrongWorkerReply{}
			err = call(getHost, "worker.ReceiveWrongWorker", &args, &reply)
			if err != nil {
				return
			}
		}
	}

	return nil
}

func (c *Coordinator) dealReduceWorkerWrong() {
	c.muCondition.Lock()
	defer c.muCondition.Unlock()

	for index, condition := range c.reduceCondition {
		if condition.status == busy {
			timeNow := time.Now().Add(-10 * time.Second)
			if condition.busyTime.Before(timeNow) {
				c.reduceCondition[index] = dealStatus{}
			}
		}
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:           files,
		mapCondition:    make([]dealStatus, len(files)),
		reduceCondition: make([]dealStatus, nReduce),
		nReduce:         nReduce,
		workerInfo:      make(map[int]workerInfo, 0),
	}
	log.SetFlags(log.Llongfile)
	// Your code here.

	c.server()
	return &c
}
