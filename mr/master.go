package mr

import (
	"container/list"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

const idle = 0
const running = 1
const completed = 2
const shuffling = 3
const timeOut = 10 // seconds

// MapTaskData Stores map task state and other relevant information
type MapTaskData struct {
	State                 int
	IntermediateFilePaths []string
	FilePath              string
	TaskID                int
}

type NoData struct {
}

// ReduceTaskData Stores reduce task state and other relevant information
type ReduceTaskData struct {
	State    int
	MapTasks map[int](struct{})
	TaskID   int
}

// ReduceTaskList Maintains state of reduce tasks
type ReduceTaskList struct {
	tasks map[int](ReduceTaskData)
}

// MapTaskList Maintains state of the map tasks
type MapTaskList struct {
	tasks map[int](MapTaskData) // input file, task state
}

// TimeStamp Holds time at specific point
type TimeStamp struct {
	Deadline time.Time
	TaskID   int
}

func (t *TimeStamp) print() {
	fmt.Printf("task: %v, time: %v\n", strconv.Itoa(t.TaskID), t.Deadline.Local().String())
}

//
// Master Master server code. Holds data structures and logic for assigning and managing map, reduce tasks
// Its exported methods are exposed by RPC which are used by workers
//
type Master struct {
	// Your definitions here.
	nReduce            int
	mapTaskList        MapTaskList
	reduceTaskList     ReduceTaskList
	runningTasks       list.List
	runningReduceTasks list.List
	taskUpdateMutex    sync.Mutex
	cv                 *sync.Cond
	done               bool
}

//
// GetMapTask RPC handler, returns map tasks if any
//
func (m *Master) GetMapTask(args *Request, reply *MapTask) error {
	m.setMapTaskRunning(reply)
	reply.NReduce = m.nReduce
	return nil
}

//
// GetReduceTask RPC handler, returns reduce task if any
//
func (m *Master) GetReduceTask(args *Request, reply *ReduceTask) error {
	m.setReduceTaskShuffling(reply)
	return nil
}

//
// NotifyMapTaskCompletion RPC handler, updates a currently running task to be completed, and stores intermediate file path
//
func (m *Master) NotifyMapTaskCompletion(args *MapCompletion, reply *Reply) error {
	m.setMapTaskCompleted(args)
	return nil
}

//
// NotifyReduceTaskCompletion RPC handler, updates a currently running task to completed
//
func (m *Master) NotifyReduceTaskCompletion(args *ReduceCompletion, reply *Reply) error {
	m.setReduceTaskCompleted(args)
	return nil
}

// GetIntermediateFiles RPC handler, return files for reduce task
func (m *Master) GetIntermediateFiles(args *IntermediateDataRequest, reply *IntermediateFiles) error {
	m.getRemainingIntermediateFiles(args.TaskID, reply)
	return nil
}

//
// Done main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := true
	for _, val := range m.reduceTaskList.tasks {
		if val.State != completed {
			ret = false
		}
	}
	return ret
}

//
// MakeMaster create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// populate map task list based on read files,
	// all tasks must be idle, assign taskID using counter,
	// M = # of files

	// initialize master data strucutre
	m.nReduce = nReduce
	m.runningTasks.Init()
	m.runningReduceTasks.Init()
	// mutex := sync.Mutex{}
	m.cv = sync.NewCond(&m.taskUpdateMutex)
	var counter = 0
	m.mapTaskList = MapTaskList{tasks: make(map[int](MapTaskData))}
	m.reduceTaskList = ReduceTaskList{tasks: make(map[int](ReduceTaskData))}
	for _, filepath := range files {
		counter = counter + 1
		m.mapTaskList.tasks[counter] = MapTaskData{State: idle, TaskID: counter, FilePath: filepath}
	}
	for i := 1; i <= nReduce; i++ {
		m.reduceTaskList.tasks[i] = ReduceTaskData{State: idle, TaskID: i, MapTasks: make(map[int]struct{})}
	}
	go m.housekeep()
	m.server()
	return &m
}

// gets the next available map task (i.e. not completed or running).
// must lock before reading because master background thread may be writing to it
func (m *Master) setMapTaskRunning(task *MapTask) {
	m.taskUpdateMutex.Lock()
	var taskID int

	// find an idle map task
	for key, val := range m.mapTaskList.tasks {
		if val.State == running {
			task.Instruction = wait
		}
		if val.State == idle {

			task.Filepath = val.FilePath
			task.MapTaskID = key
			taskID = val.TaskID
			break
		}
	}
	// set idle task to running
	if taskID != 0 {
		m.mapTaskList.tasks[taskID] = MapTaskData{State: running, TaskID: taskID, FilePath: task.Filepath}
		m.runningTasks.PushBack(TimeStamp{Deadline: time.Now().Add(time.Second * time.Duration(timeOut)), TaskID: taskID})
		m.cv.Signal()

	}
	m.taskUpdateMutex.Unlock()
}

// gets the next available map task (i.e. not completed or running).
// must lock before reading because master background thread may be writing to it
func (m *Master) setReduceTaskShuffling(task *ReduceTask) {
	m.taskUpdateMutex.Lock()
	// find an idle reduce task
	for key, val := range m.reduceTaskList.tasks {
		if val.State == running || val.State == shuffling {
			task.Instruction = wait
		}
		if val.State == idle {
			task.TaskID = key
			break
		}
	}
	// set idle task to shuffling, we don't set to running, because that implies reduce task has started reducing
	// note that we have not added reduce task to running queue, countdown only begins when reduce task is doing actual work
	if task.TaskID != 0 {
		m.reduceTaskList.tasks[task.TaskID] = ReduceTaskData{State: shuffling, TaskID: task.TaskID, MapTasks: make(map[int](struct{}))}
	}
	m.taskUpdateMutex.Unlock()
}

func (m *Master) setMapTaskCompleted(task *MapCompletion) {

	m.taskUpdateMutex.Lock()
	// check if task is running before updating to completed. It is possible that this task was reassigned due to delay in processing,
	// and therefore this job completion request should not be applied
	_, ok := m.mapTaskList.tasks[task.TaskID]
	if ok { // hit
		if m.mapTaskList.tasks[task.TaskID].State == running {
			m.mapTaskList.tasks[task.TaskID] = MapTaskData{State: completed, IntermediateFilePaths: task.IntermediateFiles, TaskID: task.TaskID}
			var elem *list.Element

			// remove task from running task list
			for e := m.runningTasks.Front(); e != nil; e = e.Next() {
				t, ok := e.Value.(TimeStamp)
				if ok {
					if t.TaskID == task.TaskID {
						elem = e
						break
					}
				}
			}

			if elem != nil {
				m.runningTasks.Remove(elem)
			}

		}
	}
	defer m.taskUpdateMutex.Unlock()
}

func (m *Master) getRemainingIntermediateFiles(reduceTaskID int, files *IntermediateFiles) {
	m.taskUpdateMutex.Lock()
	// collect all map ids that are not already assigned to reduce task
	// extract respective file names if any

	unassignedMapTasks := []int{}
	intermediateFiles := []string{}
	for key, val := range m.mapTaskList.tasks {
		_, ok := m.reduceTaskList.tasks[reduceTaskID].MapTasks[key]
		if !ok { // if map task not assigned
			if val.State == completed {
				unassignedMapTasks = append(unassignedMapTasks, key)
				for _, filename := range m.mapTaskList.tasks[key].IntermediateFilePaths {
					if filename == ("xr-" + strconv.Itoa(key) + "-" + strconv.Itoa(reduceTaskID)) {
						intermediateFiles = append(intermediateFiles, filename)
					}

				}

			}
		}
	}
	for _, mapTaskID := range unassignedMapTasks {
		m.reduceTaskList.tasks[reduceTaskID].MapTasks[mapTaskID] = NoData{}
	}
	if len(m.reduceTaskList.tasks[reduceTaskID].MapTasks) == len(m.mapTaskList.tasks) { // all map tasks have been accounted for
		files.Instruction = done
		reduceTask := m.reduceTaskList.tasks[reduceTaskID]
		m.reduceTaskList.tasks[reduceTaskID] = ReduceTaskData{State: running, TaskID: reduceTaskID, MapTasks: reduceTask.MapTasks}
		m.runningReduceTasks.PushBack(TimeStamp{Deadline: time.Now().Add(time.Second * time.Duration(timeOut)), TaskID: reduceTaskID})
		m.cv.Signal()
	} else {
		files.Instruction = wait
	}
	files.Files = intermediateFiles
	files.TaskID = reduceTaskID
	m.taskUpdateMutex.Unlock()
}

func (m *Master) setReduceTaskCompleted(task *ReduceCompletion) {
	m.taskUpdateMutex.Lock()
	defer m.taskUpdateMutex.Unlock()
	// check if task is running before updating to completed. It is possible that this task was reassigned due to delay in processing,
	// and therefore this job completion request should not be applied
	_, ok := m.reduceTaskList.tasks[task.TaskID]
	if ok { // hit
		if m.reduceTaskList.tasks[task.TaskID].State == running {
			m.reduceTaskList.tasks[task.TaskID] = ReduceTaskData{State: completed, TaskID: task.TaskID}
			var elem *list.Element

			// remove task from running task list
			for e := m.runningReduceTasks.Front(); e != nil; e = e.Next() {
				t, ok := e.Value.(TimeStamp)
				if ok {
					if t.TaskID == task.TaskID {
						elem = e
						break
					}
				}
			}

			if elem != nil {
				m.runningReduceTasks.Remove(elem)
			}
		}
	}

}

func (m *Master) housekeep() {
	for true {
		m.taskUpdateMutex.Lock()
		for m.runningTasks.Len() == 0 && m.runningReduceTasks.Len() == 0 { // both map, reduce running queues are empty
			m.cv.Wait() // release lock before going to sleep
		}
		m.taskUpdateMutex.Unlock()
		var front *list.Element

		for true {
			m.taskUpdateMutex.Lock()

			var reduceType bool
			var list *list.List                                               // false => map, true => reduce
			if m.runningTasks.Len() != 0 || m.runningReduceTasks.Len() != 0 { // there is atleast one map or reduce task that is running
				if m.runningTasks.Len() == 0 { // if map queue is empty, select reduce task
					reduceType = true
					front = m.runningReduceTasks.Front()
					list = &m.runningReduceTasks
				} else if m.runningReduceTasks.Len() == 0 { // if reduce queue is empty, select map task
					reduceType = false
					front = m.runningTasks.Front()
					list = &m.runningTasks
				} else { // if both non-empty, select the one with a closer deadline
					deadlineMap, _ := m.runningTasks.Front().Value.(TimeStamp)
					deadlineReduce, _ := m.runningReduceTasks.Front().Value.(TimeStamp)
					if deadlineMap.Deadline.Before(deadlineReduce.Deadline) { // map tasks deadline closer than reduce task
						reduceType = false
						list = &m.runningTasks
						front = m.runningTasks.Front()
					} else {
						reduceType = true
						list = &m.runningReduceTasks
						front = m.runningReduceTasks.Front()
					}
				}

				t, _ := front.Value.(TimeStamp)
				if time.Now().After(t.Deadline) { // if deadline exceed, remove item from running queue and update task list
					list.Remove(front)
					// m.runningTasks.Remove(front)
					if reduceType {
						taskData, _ := m.reduceTaskList.tasks[t.TaskID]
						m.reduceTaskList.tasks[t.TaskID] = ReduceTaskData{State: idle, MapTasks: taskData.MapTasks, TaskID: t.TaskID}
					} else {
						taskData, _ := m.mapTaskList.tasks[t.TaskID]
						m.mapTaskList.tasks[t.TaskID] = MapTaskData{State: idle, FilePath: taskData.FilePath, TaskID: t.TaskID}
					}

					// if both map, reduce queues are not empty, move on
					if m.runningTasks.Len() == 0 && m.runningReduceTasks.Len() == 0 {
						m.taskUpdateMutex.Unlock()
						continue
					}

					// select the next closest deadline element and sleep for that duration
					if m.runningTasks.Len() == 0 {
						list = &m.runningReduceTasks
					}

					if m.runningReduceTasks.Len() == 0 {
						list = &m.runningTasks
					}

					if m.runningTasks.Len() != 0 && m.runningReduceTasks.Len() != 0 {
						deadlineMap, _ := m.runningTasks.Front().Value.(TimeStamp)
						deadlineReduce, _ := m.runningReduceTasks.Front().Value.(TimeStamp)
						if deadlineMap.Deadline.Before(deadlineReduce.Deadline) { // map tasks deadline closer than reduce task
							list = &m.runningTasks
						} else {
							list = &m.runningReduceTasks
						}
					}

					peekt, _ := list.Front().Value.(TimeStamp)
					m.taskUpdateMutex.Unlock()                             // important!!!, must do this step, otherwise might block execution of master RPC handlers
					time.Sleep(peekt.Deadline.UTC().Sub(time.Now().UTC())) // if negative, i.e. deadline exceed, returns immediately, and removes in the next round
				} else {
					m.taskUpdateMutex.Unlock()
					time.Sleep(t.Deadline.UTC().Sub(time.Now().UTC()))
				}

			} else {
				m.taskUpdateMutex.Unlock()
				break
			}
		}

	}
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
