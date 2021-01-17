package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"sync"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
type mrFunction struct {
	mapf func(string, string) []KeyValue
	reducef func(string, []string) string
}
// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// Task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var mMap int
var nReduce int
//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	mrFunction := mrFunction{mapf: mapf, reducef: reducef}
	for {
		task, inputFileName := getTask()
		doTask(&task, inputFileName, mrFunction)
		finishTask(task)
	}
}

func getTask() (Task, string) {
	args := GetTaskArgs{}
	reply := GetTaskReply{}
	call("Master.GetTask", &args, &reply)
	mMap = reply.MMap
	nReduce = reply.NReduce
	log.Printf("Get Task From Master: %v, %d, %v", reply.Task.Phase, reply.Task.Id, reply.FileName)
	return reply.Task, reply.FileName
}
func finishTask(task Task) {
	args := FinishTaskArgs{Task: task}
	reply := FinishTaskReply{}
	call("Master.FinishTask", &args, &reply)
	log.Printf("Finish Task : %v, %d", task.Phase, task.Id)
}
func doTask(task *Task, inputFileName string, function mrFunction) {
	if task.Phase == MapPhase {
		doMapTask(task, inputFileName, function.mapf)
	} else if task.Phase == ReducePhase{
		doReduceTask(task, function.reducef)
	}
}
func doMapTask(task *Task, inputFileName string, mapf func(string, string) []KeyValue) {
	content := getInputFileContent(inputFileName)
	kva := mapf(inputFileName, string(content))
	tempFiles := createTempIntermediateFiles(task)
	splitDataAndWriteFile(kva, tempFiles)
	renameTempIntermediateFiles(tempFiles, task.Id)
	//TODO: error handling
}
func getInputFileContent(fileName string) []byte {
	file, err := os.Open(InputFilePath + fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()
	return content
}
func createTempIntermediateFiles(task *Task) map[int]*os.File {
	var wg sync.WaitGroup
	tempFiles := make(map[int]*os.File)
	for id := 0; id < nReduce; id++ {
		wg.Add(1)
		createTempIntermediateFile(tempFiles, id, task.Id, &wg)
		//TODO go parallel
	}
	wg.Wait()
	return tempFiles
}
func createTempIntermediateFile(tempFiles map[int]*os.File, id int, taskId int, wg *sync.WaitGroup) {
	filename := "mr-" + strconv.Itoa(taskId) + "-" + strconv.Itoa(id)
	f ,err := ioutil.TempFile(FilePath, filename)
	if err != nil {
		log.Fatalf("cannot create tempFile: %v", filename)
	}
	tempFiles[id] = f
	wg.Done()
}
func splitDataAndWriteFile(kva []KeyValue, tempFiles map[int]*os.File) {
	for _, kv := range kva {
		reduceId := ihash(kv.Key) % nReduce
		tempFile := tempFiles[reduceId]
		content, _ := json.Marshal(kv)
		tempFile.Write(content)
		tempFile.Write([]byte("\n"))
	}
}
func renameTempIntermediateFiles(files map[int]*os.File, taskId int) {
	for id, f := range files {
		filename := "mr-" + strconv.Itoa(taskId) + "-" + strconv.Itoa(id)
		err := os.Rename(f.Name(), FilePath + filename)
		if err != nil {
			log.Fatal(err)
		}
	}
}
func doReduceTask(task *Task, reducef func(string, []string) string) {
	intermediate := groupDataFromIntermediateFiles(task.Id)
	sortDataByKey(intermediate)
	f := createTempOutputFile(task.Id)
	writeOutputFile(f, intermediate, reducef)
	renameOutputFile(f, task.Id)
}
func groupDataFromIntermediateFiles(reduceId int) []KeyValue {
	var intermediate []KeyValue
	for mapId := 0; mapId < mMap; mapId++ {
		file := openIntermediateFile(mapId, reduceId)
		kva := transformJsonToKeyValue(file)
		intermediate = append(intermediate, kva...)
		file.Close()
	}
	return intermediate
}

func openIntermediateFile(mapId int, reduceId int) *os.File {
	fileName := "mr-" + strconv.Itoa(mapId) + "-" + strconv.Itoa(reduceId)
	file, err := os.Open(FilePath + fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	return file
}
func transformJsonToKeyValue(file *os.File) []KeyValue {
	var kva []KeyValue
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		jsonStr := scanner.Text()
		kv := KeyValue{}
		err := json.Unmarshal([]byte(jsonStr), &kv)
		if err != nil {
			log.Fatal("json transform error")
		}
		kva = append(kva, kv)
	}
	return kva
}
func sortDataByKey(kva []KeyValue) {
	sort.Sort(ByKey(kva))
}
func createTempOutputFile(reduceId int) *os.File {
	filename := "mr-out-" + strconv.Itoa(reduceId)
	f ,err := ioutil.TempFile(FilePath, filename)
	if err != nil {
		log.Fatalf("cannot create tempFile: %v", filename)
	}
	return f
}
func renameOutputFile(f *os.File, reduceId int) {
	fileName := "mr-out-" + strconv.Itoa(reduceId)
	err := os.Rename(f.Name(), FilePath + fileName)
	if err != nil {
		log.Fatal(err)
	}
}

func writeOutputFile(f *os.File, intermediate []KeyValue, reducef func(string, []string) string) {
	i := 0
	log.Print(len(intermediate))
	var c int64
	c = 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		v, _ := strconv.ParseInt(output, 0, 64)
		c = c + v
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(f, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	log.Print(c)
}
//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
