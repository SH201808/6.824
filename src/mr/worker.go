package mr

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sort"
	"sync"
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

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrMyWorker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your MyWorker implementation here.
	log.SetFlags(log.Llongfile)
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	w := MyWorker{}
	go w.server()
	noTaskCnt := 0
	for {
		if w.host == "" {
			continue
		}
		taskType, taskId, MyWorkerId, nReduce, err := w.callGetTask()
		if err != nil {
			log.Println(err)
			return
		}
		if taskType == Close {
			return
		}
		if taskType == NoTask {
			if noTaskCnt == 5 {
				break
			}
			w.isDone = true
			time.Sleep(1 * time.Second)
			noTaskCnt++
			continue
		}
		if w.Id == 0 {
			w.Id = MyWorkerId
		}
		w.nReduce = nReduce
		w.taskId = taskId
		if taskType == MapTask {
			w.isDone = false
			if len(w.files) == 0 {
				w.files = make([]string, 0, nReduce)
				w.filesEncoder = make([]*gob.Encoder, 0, nReduce)
			}
			err = w.dealMapTask(mapf)
		}
		if taskType == ReduceTask {
			w.isDone = true
			w.mapMyWorkers = make(map[string]*ReadFromMyWorker, 0)
			w.MapResult = make([]KeyValue, 0)
			err = w.dealReduceTask(reducef)
		}
		if err != nil {
			log.Println(err)
			return
		}
		err = w.callFinishTask(taskType)
		if err != nil {
			log.Println(err)
			return
		}
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(sockName string, rpcname string, args interface{}, reply interface{}) error {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")

	c, err := rpc.DialHTTP("tcp", sockName)
	if err != nil {
		log.Println(sockName, rpcname)
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)

	return err
}

type MyWorker struct {
	host   string
	Id     int
	taskId int

	nReduce      int
	mapFileLock  []sync.RWMutex
	filesEncoder []*gob.Encoder
	files        []string
	isDone       bool

	mapMyWorkers map[string]*ReadFromMyWorker

	reduceTempLock sync.RWMutex
	MapResult      []KeyValue
}

type ReadFromMyWorker struct {
	Host     string
	ReadSize int64
}

func (w *MyWorker) server() {
	rpc.Register(w)
	rpc.HandleHTTP()
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		panic(err)
	}
	l, e := net.ListenTCP("tcp", addr)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	//addr := l.Addr()
	w.host = fmt.Sprintf("127.0.0.1:%d", l.Addr().(*net.TCPAddr).Port)
	go http.Serve(l, nil)
}

func (w *MyWorker) dealMapTask(mapf func(string, string) []KeyValue) error {
	path := fmt.Sprintf("./MyWorker/%d", w.Id)
	err := mkMyWorkerDir(path)
	if err != nil {
		log.Println("mkMyWorkerDir err: ", err)
		return err
	}
	for i := 0; i < w.nReduce && len(w.files) != w.nReduce; i++ {
		path = fmt.Sprintf("./MyWorker/%d/%d", w.Id, i)
		file, err := os.Create(path)
		if err != nil {
			log.Println("create file err: ", err)
			return err
		}
		w.files = append(w.files, path)
		w.filesEncoder = append(w.filesEncoder, gob.NewEncoder(file))
	}
	w.mapFileLock = make([]sync.RWMutex, len(w.files))

	fileName, content, err := w.callGetMapTask()
	if err != nil {
		return err
	}
	pairs := mapf(fileName, content)
	for _, pair := range pairs {
		reduceId := ihash(pair.Key) % w.nReduce
		encoder := w.filesEncoder[reduceId]
		w.mapFileLock[reduceId].Lock()
		err = encoder.Encode(pair)
		w.mapFileLock[reduceId].Unlock()
		if err != nil {
			log.Println("encode err: ", err)
			return err
		}
	}

	return nil
}

func (w *MyWorker) callGetMapTask() (fileName, content string, err error) {
	args := GetMapTaskArgs{w.Id, w.taskId}
	reply := GetMapTaskReply{}
	sockName := coordinatorSock()
	err = call(sockName, "Coordinator.GetMapTask", &args, &reply)
	content = reply.Content
	fileName = reply.FileName
	return
}

func mkMyWorkerDir(path string) error {
	_, err := os.Stat(path)
	if err != nil && os.IsExist(err) {
		return nil
	}
	err = os.MkdirAll(path, 0777)
	if err != nil {
		return err
	}
	return nil
}

func (w *MyWorker) dealReduceTask(reducef func(string, []string) string) error {
	MyWorkersHost, err := w.callGetReduceTask()
	if err != nil {
		return err
	}
	for _, host := range MyWorkersHost {
		if _, ok := w.mapMyWorkers[host]; !ok {
			w.mapMyWorkers[host] = &ReadFromMyWorker{Host: host, ReadSize: 0}
		}
	}

	var wg sync.WaitGroup
	wg.Add(len(w.mapMyWorkers))
	for _, mapMyWorker := range w.mapMyWorkers {
		go w.callGetMapResult(mapMyWorker, &wg)
	}
	wg.Wait()

	sort.Slice(w.MapResult, func(i, j int) bool {
		return w.MapResult[i].Key < w.MapResult[j].Key
	})
	outPutFilePath := fmt.Sprintf("./mr-out-%d", w.taskId)
	file, err := os.Create(outPutFilePath)
	if err != nil {
		log.Println(err)
		return err
	}
	defer file.Close()

	nowKey := w.MapResult[0].Key
	nowMap := []string{w.MapResult[0].Value}
	for i := 1; i <= len(w.MapResult); i++ {
		if i == len(w.MapResult) || w.MapResult[i].Key != nowKey {
			res := reducef("", nowMap)
			s := nowKey + " " + res + "\n"
			_, err = file.Write([]byte(s))
			if err != nil {
				log.Println(err)
				return err
			}
			if i != len(w.MapResult) {
				nowMap = []string{w.MapResult[i].Value}
				nowKey = w.MapResult[i].Key
			}
		} else {
			nowMap = append(nowMap, w.MapResult[i].Value)
		}
	}

	return nil
}

func (w *MyWorker) callGetMapResult(mapMyWorker *ReadFromMyWorker, wg *sync.WaitGroup) {
	readOffset := 0
	var temp []byte
	for {
		args := GetMapResultArgs{w.taskId, readOffset}
		reply := GetMapResultReply{}
		err := call(mapMyWorker.Host, "MyWorker.GetMapResult", &args, &reply)
		if err != nil {
			log.Println("get result err: ", err)
			return
		}
		temp = append(temp, reply.Data...)
		readOffset += len(reply.Data)
		if reply.IsDone {
			break
		}
	}
	decoder := gob.NewDecoder(bytes.NewReader(temp))
	var data []KeyValue
	for {
		pair := KeyValue{}
		if err := decoder.Decode(&pair); err != nil {
			if err == io.EOF {
				break
			}
			log.Fatalln(err)
		}
		data = append(data, pair)
	}
	w.reduceTempLock.Lock()
	w.MapResult = append(w.MapResult, data...)
	w.reduceTempLock.Unlock()
	wg.Done()
}

func (w *MyWorker) ReceiveWrongMyWorker() {
	args := ReceiveWrongWorkerArgs{}
	w.mapMyWorkers[args.WrongHost].Host = args.NewHost
}

func (w *MyWorker) callGetReduceTask() ([]string, error) {
	args := GetReduceTaskArgs{w.Id, w.taskId}
	reply := GetReduceTaskReply{}
	sockName := coordinatorSock()
	err := call(sockName, "Coordinator.GetReduceTask", &args, &reply)
	if err != nil {
		return nil, err
	}
	return reply.MapWorkerHosts, nil
}

func (w *MyWorker) callGetTask() (taskType TaskType, taskId int, MyWorkerId int, nReduce int, err error) {
	args := GetTaskArgs{w.Id, w.host}
	reply := GetTaskReply{}
	sockName := coordinatorSock()
	err = call(sockName, "Coordinator.GetTask", &args, &reply)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	return reply.TaskType, reply.TaskId, reply.WorkerId, reply.NReduce, nil
}

func (w *MyWorker) callFinishTask(taskType TaskType) error {
	args := FinishTaskArgs{TaskType: taskType, WorkerId: w.Id, TaskId: w.taskId}
	reply := FinishTaskReply{}
	sockName := coordinatorSock()
	return call(sockName, "Coordinator.FinishTask", &args, &reply)
}

var (
	readOnceSize = int64(10000)
)

func (w *MyWorker) GetMapResult(args *GetMapResultArgs, reply *GetMapResultReply) error {
	fileLoc := w.files[args.ReduceId]
	w.mapFileLock[args.ReduceId].Lock()
	file, err := os.Open(fileLoc)
	if err != nil {
		return err
	}
	defer file.Close()
	fileInfo, _ := file.Stat()

	_, err = file.Seek(int64(args.ReadOffSet), 0)
	if err != nil {
		return err
	}
	reader := bufio.NewReader(file)
	readSize := readOnceSize
	lastRead := fileInfo.Size()-readOnceSize <= int64(args.ReadOffSet)
	if lastRead {
		readSize = fileInfo.Size() - int64(args.ReadOffSet)
	}
	reply.Data = make([]byte, readSize)
	_, err = reader.Read(reply.Data)
	w.mapFileLock[args.ReduceId].Unlock()

	if err != nil && err != io.EOF {
		log.Println(err)
		return err
	}

	reply.IsDone = w.isDone
	if reply.IsDone {
		reply.IsDone = lastRead
	}

	return nil
}
