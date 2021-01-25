package mr

import (
	"bufio"
	"log"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const InputFilePath = ""
const FilePath = ""
const InitPhase = "Initiation"
const MapPhase = "Map"
const ReducePhase = "Reduce"
const DonePhase = "Done"
const TaskDone = "TaskDone"
const GetTaskTimeOut = 3 * time.Second
const MaxTaskWaitTime = 20 * time.Second
type Master struct {
	mMap           int
	nReduce        int
	phase          string
	phaseLock      sync.Mutex
	taskQuantity   int
	taskCounter    uint64
	taskChan       chan Task
	retryTaskChan  chan Task
	traceTaskChan  map[Task] chan string
	files          map[int] string
}
type Task struct {
	Phase string
	Id    int
}
func (m *Master) getTask() Task {
	m.checkAndChangePhase()
	select {
		case task:= <-m.taskChan:
			go m.traceTask(task)
			return task
		case <-time.After(GetTaskTimeOut):

			return m.getTask()
	}
}
func (m *Master) checkAndChangePhase() {

	if int(m.taskCounter) != m.taskQuantity {
		return
	}
	m.phaseLock.Lock()

	if m.phase == MapPhase {
		log.Println("Start Phase Reduce")
		m.startPhase(ReducePhase, m.nReduce)
	} else if m.phase == ReducePhase {
		m.startPhase(DonePhase, 0)
		log.Println("Start Phase Done")
	} else if m.phase == DonePhase {

	}
	m.phaseLock.Unlock()
}
func (m *Master) startPhase(phaseName string, taskQuantity int) {
	m.taskQuantity = taskQuantity
	m.taskCounter = 0
	m.phase = phaseName
	if phaseName == DonePhase {
		return
	}
	for taskId := 0; taskId < taskQuantity; taskId++ {
		go m.addTask(Task{
			Phase: phaseName,
			Id:    taskId,
		})
	}
	log.Printf("start phase: %v, Task quantity: %d", phaseName, taskQuantity)
}
func (m *Master) addTask(task Task) {
	log.Printf("Task added: Task(%s, %d) \n", task.Phase, task.Id)
	m.taskChan <- task
}
func (m *Master) getPhase() string {
	m.phaseLock.Lock()
	phase := m.phase
	m.phaseLock.Unlock()
	return phase
}
func (m *Master) traceTask(task Task) {
	c := m.traceTaskChan[task]
	select {
	case taskStatus := <-c:
		if taskStatus == TaskDone {
			atomic.AddUint64(&m.taskCounter, 1)
			log.Printf("Task finished: Task(%s, %d)", task.Phase, task.Id)
		}
	case <- time.After(MaxTaskWaitTime):
		m.retryTask(task)
		log.Printf("TraceTaskTimeOut: Task(%s, %d), retry...", task.Phase, task.Id)
	}
}
func (m *Master) retryTask(task Task) {
	m.retryTaskChan <- task
}
func (m *Master) finishTask(task Task) {
	c := m.traceTaskChan[task]
	c <- TaskDone
}

func (m *Master) initiation(files []string, nReduce int) {
	m.phase = InitPhase
	m.mMap = len(files)
	m.nReduce = nReduce
	m.taskChan = make(chan Task)
	m.retryTaskChan = make(chan Task)
	m.traceTaskChan = make(map[Task] chan string)
	m.files = make(map[int] string)
	m.fillFilesMap(files)
	m.initTraceTaskChan()
	m.startRetryRouter()
	//m.splitInputFiles(files, mMap)
	m.startPhase("Map", m.mMap)
}

func (m *Master) fillFilesMap(files []string) {
	for i, file := range files {
		m.files[i] = file
	}
}
func (m *Master) initTraceTaskChan() {
	for id := 0; id < m.mMap; id++ {
		m.traceTaskChan[Task{Phase: MapPhase, Id: id}] = make(chan string)
	}
	for id := 0; id < m.nReduce; id++ {
		m.traceTaskChan[Task{Phase: ReducePhase, Id: id}] = make(chan string)
	}
}
func (m *Master) startRetryRouter() {
	go m.retryRouter()
}
func (m *Master) retryRouter() {
	for {
		c := <-m.retryTaskChan
		m.taskChan <- c
	}
}
func (m *Master) splitInputFiles(files []string, mMap int) {
	content := m.groupInputFile(files)
	var wg sync.WaitGroup
	size := len(content)
	for id := 0; id < mMap; id++ {
		wg.Add(1)
		start := size/mMap*id
		end := size/mMap*(id+1)
		if id == mMap - 1 {
			log.Printf("Generating Map File: start %d, end end \n", start)
			go m.generateMapFile(content[start:], id, &wg)
		} else {
			log.Printf("Generating Map File: start %d, end %d \n", start, end)
			go m.generateMapFile(content[start:end], id, &wg)
		}
	}
	wg.Wait()
}
func (m *Master) groupInputFile(files []string) []string {
	var ret []string
	for _, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			ret = append(ret, scanner.Text())
		}
		file.Close()
	}
	return ret
}
func (m *Master) generateMapFile(content []string, id int, wg *sync.WaitGroup ) {
	f := m.createMapFile(id)
	m.writeMapFile(content, f)
	wg.Done()
}
func (m *Master) createMapFile(id int) *os.File {
	filename := "mr-in-" + strconv.Itoa(id)
	f, err := os.Create(FilePath + filename)
	if err != nil {
		panic(err)
		log.Fatalf("cannot create %v", filename)
	}
	return f
}
func (m *Master) writeMapFile(content []string, f *os.File) {
	for _, str := range content {
		_, err := f.WriteString(str + "\n")
		if err != nil {
			panic(err)
		}
	}
	log.Printf("finish writing file: %v", f.Name() )
}

//
// create a Master.
// main/mrmaster.go calls this function.
// NReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.initiation(files, nReduce)
	m.server()
	return &m
}
func GetMaster() *Master {
	m := Master{}
	return &m
}
//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	if m.getPhase() == DonePhase {
		ret = true
	}

	return ret
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	task := m.getTask()
	reply.Task = task
	reply.MMap = m.mMap
	reply.NReduce = m.nReduce
	if task.Phase == MapPhase {
		reply.FileName = m.files[task.Id]
	} else {
		reply.FileName = ""
	}
	//TODO: Error Handling
	return nil
}
func (m *Master) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	go m.finishTask(args.Task)
	return nil
}
