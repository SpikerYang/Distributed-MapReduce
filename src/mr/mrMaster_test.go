package mr

import (
	"log"
	"testing"
	"time"
)


//func TestSplitInputFiles(t *testing.T) {
//	files := []string{"pg-dorian_gray.txt"}
//	m := Master{}
//	m.splitInputFiles(files, 10)
//}
//func TestMakeMaster(t *testing.T) {
//	files := []string{"pg-dorian_gray.txt"}
//	MakeMaster(files, 10)
//}
//func TestTaskChan(t *testing.T) {
//	m := Master{}
//	m.taskChan = make(chan Task)
//	go func() {
//		m.taskChan <- Task{Id: 1, Phase: MapPhase}
//	}()
//	Task := <-m.taskChan
//	log.Printf("Get Task: Task(%s, %d)", Task.Phase, Task.Id)
//}
//func TestRetryTask(t *testing.T) {
//	files := []string{"pg-dorian_gray.txt"}
//	m := MakeMaster(files, 10)
//	for i := 0; i < 5; i++ {
//		Task := m.getTask()
//		log.Printf("Get Task: Task(%s, %d)", Task.Phase, Task.Id)
//	}
//
//	for i := 0; i < 4; i++ {
//		m.finishTask(Task{Phase: MapPhase, Id: i})
//	}
//	time.Sleep(6 * time.Second)
//	go m.finishTask(Task{Phase: MapPhase, Id: 4})
//	for i := 0; i < 2; i++ {
//		Task := m.getTask()
//		log.Printf("Get Task: Task(%s, %d)", Task.Phase, Task.Id)
//	}
//}
func TestGetTask(t *testing.T) {
	files := []string{"pg-dorian_gray.txt"}
	m := MakeMaster(files, 10)
	for i := 0; i < 5; i++ {
		task := m.getTask()
		log.Printf("Get Task: Task(%s, %d)", task.Phase, task.Id)
	}
	for i := 0; i < 5; i++ {
		go m.finishTask(Task{Phase: MapPhase, Id: i})
	}
	for i := 0; i < 10; i++ {
		go func() {
			task := m.getTask()
			log.Printf("Get Task: Task(%s, %d)", task.Phase, task.Id)
		}()
	}
	time.Sleep(10 * time.Second)
	for i := 0; i < 10; i++ {
		go m.finishTask(Task{Phase: ReducePhase, Id: i})
	}
	for i := 0; i < 3; i++ {
		task := m.getTask()
		log.Printf("Get Task: Task(%s, %d)", task.Phase, task.Id)
	}
}

