package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	var wg sync.WaitGroup
	wg.Add(1)
	delay := 0
	go runMapTasks(mapf, delay, &wg)
	wg.Add(1)
	go runReduceTasks(reducef, delay, &wg)

	wg.Wait()
	// fmt.Println("job completed")
}

func runMapTasks(mapf func(string, string) []KeyValue, delay int, wg *sync.WaitGroup) {
	var callFailed bool
	defer wg.Done()
	// repeatedly checks for available map tasks
	var replyMapTask MapTask
	args := Request{} // empty request
	args2 := Reply{}
	for true {
		// get map task
		replyMapTask = MapTask{}
		callFailed = !call("Master.GetMapTask", &args, &replyMapTask)
		if callFailed {
			return
		}

		if replyMapTask.Filepath != "" { // start processing request
			// time.Sleep(time.Second * time.Duration(delay))
			intermediateKvs := mapf(replyMapTask.Filepath, readFileContents(replyMapTask.Filepath))
			intermediateFiles := storeIntermediateKvs(replyMapTask.MapTaskID, replyMapTask.NReduce, intermediateKvs)
			// RPC to send map task results
			mapTaskCompletion := MapCompletion{TaskID: replyMapTask.MapTaskID, IntermediateFiles: intermediateFiles}
			callFailed = !call("Master.NotifyMapTaskCompletion", &mapTaskCompletion, &args2)
			if callFailed {
				return
			}
		} else if replyMapTask.Instruction == wait {
			time.Sleep(time.Second * time.Duration(1))
		} else { // no more map tasks available, exit the loop
			break
		}

	}
}

func runReduceTasks(reducef func(string, []string) string, delay int, wg *sync.WaitGroup) {
	var callFailed bool
	defer wg.Done()
	var replyReduceTask ReduceTask
	var intermediateFiles IntermediateFiles
	var intermediateDataRequest IntermediateDataRequest
	args := Request{}
	args2 := Reply{}
	for true { // keep running until all reduce tasks are completed
		replyReduceTask = ReduceTask{}
		callFailed = !call("Master.GetReduceTask", &args, &replyReduceTask)
		if callFailed {
			return
		}
		if replyReduceTask.TaskID != 0 {
			kva := []KeyValue{}
			for true {
				intermediateDataRequest = IntermediateDataRequest{TaskID: replyReduceTask.TaskID}
				intermediateFiles = IntermediateFiles{}
				callFailed = !call("Master.GetIntermediateFiles", &intermediateDataRequest, &intermediateFiles)
				if callFailed {
					return
				}
				// process intermediateFiles
				for _, filename := range intermediateFiles.Files {
					ifile, _ := os.Open(filename)
					dec := json.NewDecoder(ifile)
					for {
						var kv KeyValue
						if err := dec.Decode(&kv); err != nil {
							break
						}
						kva = append(kva, kv)
					}
					ifile.Close()
				}
				if intermediateFiles.Instruction == done {
					break
				}

				time.Sleep(time.Second * time.Duration(1))
			}
			sort.Sort(ByKey(kva))

			// run reduce
			oname := "mr-out-" + strconv.Itoa(replyReduceTask.TaskID)
			ofile, err := ioutil.TempFile("./", "tempmr-out*")
			if err != nil {
				log.Fatalf("cannot outout file %v", oname)
				continue
			}
			defer os.Remove(ofile.Name())
			// time.Sleep(time.Second * time.Duration(delay))

			// ofile, _ := os.Create(oname)

			//
			// call Reduce on each distinct key in intermediate[],
			// and print the result to mr-out-0.
			//
			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

				i = j
			}
			os.Rename(ofile.Name(), "./"+oname)
			ofile.Close()

			reduceTaskCompletion := ReduceCompletion{TaskID: replyReduceTask.TaskID}
			callFailed = !call("Master.NotifyReduceTaskCompletion", &reduceTaskCompletion, &args2)
			if callFailed {
				return
			}
		} else if replyReduceTask.Instruction == wait { // more reduce tasks
			time.Sleep(time.Second * time.Duration(1))
		} else { // no more reduce tasks
			break
		}
	}
}

func readFileContents(filepath string) string {
	file, err := os.Open(filepath)
	if err != nil {
		log.Fatalf("cannot open %v", filepath)
	}
	contents, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filepath)
	}
	file.Close()
	return string(contents)
}

func storeIntermediateKvs(mapID int, nReduce int, kvs []KeyValue) []string {
	// create nReduce output files for storing intermediate values
	ofiles := make(map[int]*os.File)
	filenamMapping := make(map[string]string)
	encoders := make(map[int]*json.Encoder)
	var writtenFiles []string
	for i := 1; i <= nReduce; i++ {
		ofile, err := ioutil.TempFile("./", "tempxr*")
		// ofile, err := os.Create("xr-" + strconv.Itoa(mapID) + "-" + strconv.Itoa(i))
		if err != nil {
			log.Fatalf("cannot create intermediate file %v", "xr-"+string(mapID)+"-"+string(i))
			continue
		}
		defer os.Remove(ofile.Name())
		filenamMapping[ofile.Name()] = "xr-" + strconv.Itoa(mapID) + "-" + strconv.Itoa(i)
		writtenFiles = append(writtenFiles, "xr-"+strconv.Itoa(mapID)+"-"+strconv.Itoa(i))
		ofiles[i-1] = ofile
		encoders[i-1] = json.NewEncoder(ofile)
	}

	// write output to file, one key,val pair at a time
	for _, keyVal := range kvs {
		encoder, ok := encoders[ihash(keyVal.Key)%nReduce] // check if there is file stream open for storing this specific key, if not skip
		if ok {
			err := encoder.Encode(&keyVal)
			if err != nil {
				log.Fatalf("failed to write to file, mapID: %v, reduceID: %v", mapID, ihash(keyVal.Key))
			}
		}
	}

	// close all files
	for _, ofile := range ofiles {
		os.Rename(ofile.Name(), "./"+filenamMapping[ofile.Name()])
		ofile.Close()
	}

	return writtenFiles

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
