# 一、简介
本项目基于MIT 6.824 lab1 实现了一个简易的分布式MapReduce\
运行test： 进入src/main目录 运行 sh test-mr.sh
# 二、MapReduce
MapReduce是Google提出的一种适用于大规模数据的分布式计算框架。\
**Paper link**： https://research.google/pubs/pub62/ 
![](https://github.com/SpikerYang/Distributed-MapReduce/blob/master/src/%E6%88%AA%E5%B1%8F2021-01-24%20%E4%B8%8B%E5%8D%886.16.43.png?raw=true)
# 三、lab1的要求
Master可以实现任务分发 \
Worker需要实现通过rpc从Master获取任务，执行任务，再反馈任务结果给Master \
\
**Parallelism**: Worker需要支持并行， 即一个Master向多个Worker分配任务，多个worker并行执行任务 \
**Fault Tolerance**: Worker fail 或者Worker执行任务超时都认为任务失败，Master需要重新为这个任务分配Worker，执行任务失败的Worker不应该产生不完整的输出文件

# 四、设计思路
## 1. 一些约定
**M**: Map任务的数量， 暂定为输入文件的数量（即一个输入文件对应一个Map任务）\
**N**: Reduce任务的数量， 即最终输出文件的数量\
**Intermediate File**: Map任务产生的中间文件， 共有 M * N 个，命名为“mr-X-Y”，X为Map任务id， Y为Reduce任务id\
**输出文件**: Reduce任务产生的输出文件，命名为“mr-out-Y”，Y为Reduce任务id\
GetTaskTimeOut: Worker获取任务的的间隔\
MaxTaskWaitTime: Master等待Worker执行任务的最长时间（如果在规定时间内没有完成任务即认为worker fail，任务失败）\
\
**Phase**: 整个MapReduce任务分为四个阶段 \
Init（初始化）Map Reduce Done（完成）\
只有在前一个阶段完成后才能进入后一个阶段，所以Map阶段会出现Worker获取任务需要一直等待的情况(Map任务已经分配完但是还没有全部完成，不能进入Reduce阶段)\
Worker获取任务的等待在Master端实现

## 2. Master
### Task
```
type Task struct {
	Phase string
	Id    int
}
```
Task的定义很简单，phase表示Task是属于Map任务还是Reduce任务\

### Master
```
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
```
Master存储了MapReduce任务的M， N，当前阶段phase，当前阶段总任务数taskQuantity以及已经完成的任务taskCounter\
**taskChan**: 任务队列channel，当Map或Reduce阶段开始的时候会向taskChan中push所有的任务，Worker则会从taskChan中获得任务\
**retryTaskChan**: 重试任务队列channel，当分配给Worker的任务失败后，需要将任务加入retryTaskChan，通过我们的router，任务会被重新push回taskChan等待下次分配\
**traceTaskChan**: 任务追踪channel map，当分配给Worker任务后，Master会启动一个新的go runtime从这个task对应的channel中监听任务完成的signal，这个map就是存储task对应的channel\
**files**: 输入文件map，存储map任务id和对应的输入文件名

```
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.initiation(files, nReduce)
	m.server()
	return &m
}
```
MakeMaster是启动Master的入口，实例化一个Master对象并返回。其中主要做了两件事，一个是整个MapReduce的初始化，一个是启动rpc server\
```
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
```
```
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
```
在initiation中，主要是对Master的参数进行了初始化，还有启动了retryRouter，最后开启Map Phase
```
func (m *Master) startRetryRouter() {
	go m.retryRouter()
}
func (m *Master) retryRouter() {
	for {
		c := <-m.retryTaskChan
		m.taskChan <- c
	}
}
```
startRetryRouter其实就是启动一个go runtime，不断的把retryTaskChan中接收到的失败任务push回taskChan
```
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
```
startPhase中初始化本阶段的任务数量，重置taskCounter，并将所有任务添加到taskChan中。由于channel的特性，现在的m.taskChan <- task其实是处于阻塞状态的，只有worker分配到任务，从channel中接收task后，task才算传送完成。所以需要为每个addTask启动一个go runtime
```
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
```
getTask是分配任务的核心函数，Worker通过rpc获取任务实际上调用的就是这个函数。在每次获取任务前需要先检查并更新phase。\
在checkAndChangePhase中，首先check是否当前phase所有任务都已经完成，如果没有完成则直接返回并继续请求任务。如果任务都完成了则代表当前phase已经结束，需要更新到下一个phase。更新phase的过程需要通过phaseLock保证并发安全。\
\
这里检查int(m.taskCounter) != m.taskQuantity时好像会有并发安全的问题，但其实是没有的；一方面taskCounter的增加都是用的atomic操作保证并发安全，另一方面虽然taskCounter可能读取到的不是最新值，但这只会导致Master认为还有任务可以分配，checkAndChangePhase直接返回，但实际上没有任务了，select会一直等待直到超时，然后重新getTask。
\
select中监听taskChan，如果从taskChan获得了之前startPhase中添加的task或者任务失败加到retryTaskChan的重试task就返回给Worker，如果等待超时就重新getTask。
```
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
```
traceTask首先获得task对应的追踪channel，之后一直等待TaskDone信号，如果在MaxTaskWaitTime中还没等到，那么认为任务失败。
```
func (m *Master) finishTask(task Task) {
	c := m.traceTaskChan[task]
	c <- TaskDone
}
```
finishTask为Worker完成任务后调用的函数，发送TaskDone信号到对应的traceTask channel
## 3. RPC
```
type GetTaskArgs struct {

}
type GetTaskReply struct {
	Task    Task
	FileName string
	MMap int
	NReduce int
}
type FinishTaskArgs struct {
	Task Task
}
type FinishTaskReply struct {

}
```
RPC的设计是很简单的，Worker和Master之间的操作其实只有getTask和finishTask。getTask中Master会返回M, N, Task和Task需要操作的输入文件filename(如果是Reduce任务则为"")

## 4. Worker
```
type KeyValue struct {
	Key   string
	Value string
}
type mrFunction struct {
	mapf func(string, string) []KeyValue
	reducef func(string, []string) string
}
```
这里定义了KV键值对和mr函数，KeyValue是用来抽象统计结果的Key和值。mrFunction表示整个MapReduce使用的map和reduce函数
```
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	mrFunction := mrFunction{mapf: mapf, reducef: reducef}
	for {
		task, inputFileName := getTask()
		doTask(&task, inputFileName, mrFunction)
		finishTask(task)
	}
}
```
Worker函数是Worker运行的入口函数，启动了之后Worker会循环的获取任务，执行任务和结束任务。\
这里出现的一个问题是Worker怎么停止。由于整个MapReduce是由Master控制的，test中只要监听到Master退出就认为全部任务结束，所以在这里没有做过多的处理。mrMaster.go文件中会轮询Master的Done（）函数检查整个MapReduce是否结束，如果已经结束Master就会直接退出。
```
func doTask(task *Task, inputFileName string, function mrFunction) {
	if task.Phase == MapPhase {
		doMapTask(task, inputFileName, function.mapf)
	} else if task.Phase == ReducePhase{
		doReduceTask(task, function.reducef)
	}
}
```
doTask会根据任务类型执行map或reduce任务
```
func doMapTask(task *Task, inputFileName string, mapf func(string, string) []KeyValue) {
	content := getInputFileContent(inputFileName)
	kva := mapf(inputFileName, string(content))
	tempFiles := createTempIntermediateFiles(task)
	splitDataAndWriteFile(kva, tempFiles)
	renameTempIntermediateFiles(tempFiles, task.Id)
	//TODO: error handling
}
```
doMapTask会读取input文件，通过map函数获得KV结果，之后创建临时intermediate file，再通过splitDataAndWriteFile把KV写到对应的临时文件中，最后把临时文件改名为真正的intermediate file。\
这里需要先使用临时中间文件来做IO是为了避免写入过程中Worker fail，留下写了一半的intermediate file。
```
func doReduceTask(task *Task, reducef func(string, []string) string) {
	intermediate := groupDataFromIntermediateFiles(task.Id)
	sortDataByKey(intermediate)
	f := createTempOutputFile(task.Id)
	writeOutputFile(f, intermediate, reducef)
	renameOutputFile(f, task.Id)
}
```
doReduceTask的过程也是类似的，首先读取intermediate file, 然后根据key排序，接着对临时输出文件写入数据，最后rename输出文件。

# 五、总结
这里的MapReduce实现虽然可以通过所有test，但依然只是比较简略的版本，可能还存在没有发现的问题和bug，欢迎大家指正。\
这个项目也是我看完Clean Code以后的首次实践，感觉其中提到的一些原则虽然可能会增加一些代码量，但确实可以提高代码的整洁性和可读性，附上我总结的一些知识点：
## 命名
```
避免误导： 不要使用acountList来命名一组账号的容器，即使这真的是个list； 小心使用字母l和O（与数字1和0混淆）
做有意义的区分：不要使用数字系列（a1,a2,a3... aN); 避免出现冗余变量或函数，如 moneyAcount&money, theMessage&Message, getAcount()&getAcountInfo()
使用可搜索的名称：命名长短与作用域正相关；单字母名称适合用在本地变量，长名称更便于在全文搜索
接口和实现： 使用ShapeFactory&ShapeFactoryImpl
把读代码的人当成白痴：明确是王道。不要假设读代码的人是专业人士
一致性：相同概念持续使用相同的命名，例如get&fetch&retrive，manager&controller&driver，避免混淆
```
## 函数
```
短小：if， else，while语句中最好只有一行，那就是调用另一个函数
只做一件事：合理的函数内代码应该是不可再分的
每个函数一个抽象层级：自顶向下原则
参数：越少越好；多参数可以抽象成一个类
错误处理就是一件事：函数中try/catch/finally代码块外不应该有代码
使用异常代替enum错误码：新加的异常可以从已有异常派生出来，无需重新编译和部署使用旧异常的类
消灭重复代码
可以先按思路写，再做更名，拆分和优化
```
