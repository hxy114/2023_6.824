package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
//
// for sorting by key.
type byKey []KeyValue

var pwd = "/home/hxy/go/src/6.5840/src/main/mr-tmp/"

// for sorting by key.
func (a byKey) Len() int           { return len(a) }
func (a byKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {

		args := &AskTaskArgs{Ask: "give me task!"}
		reply := AskTaskReply{Task: Task{FileName: make([]string, 0, 100)}}
		ok := call("Coordinator.AssignTask", args, &reply)
		if !ok {
			os.Exit(1)
		}

		//log.Println(fmt.Sprintf("获得任务:%d-%d", reply.Task.TaskType, reply.Task.TaskNum))
		if reply.Task.TaskType == REDUCE_TASK {
			kva := make([]KeyValue, 0, 100)
			for _, filename := range reply.Task.FileName {
				//log.Printf("filename:%s", filename)
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", reply.Task.FileName[0])
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
			}
			sort.Sort(byKey(kva))

			oname := fmt.Sprintf("mr-out-%d", reply.Task.TaskNum)
			ofile, _ := ioutil.TempFile(pwd, oname)
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
			//log.Println(ofile.Name())
			os.Rename(ofile.Name(), pwd+oname)
			ofile.Close()

			submitFile := make([]string, 0, 1)
			submitFile = append(submitFile, pwd+oname)
			submitArgs := &SubmitTaskArgs{
				Task: Task{TaskNum: reply.Task.TaskNum, TaskType: REDUCE_TASK, FileName: submitFile},
			}
			var submitReply SubmitTaskReply
			ok = call("Coordinator.SubmitTask", submitArgs, &submitReply)
			//log.Println(fmt.Sprintf("提交任务:%d-%d", submitArgs.Task.TaskType, submitArgs.Task.TaskNum))
			if !ok {
				os.Exit(1)

			}
			if submitReply.Reply == "wait" {
				time.Sleep(1 * time.Second)
			} else if submitReply.Reply == "quit" {
				os.Exit(1)
			}

		} else if reply.Task.TaskType == MAP_TASK {

			file, err := os.Open(reply.Task.FileName[0])
			if err != nil {
				log.Fatalf("cannot open %v", reply.Task.FileName[0])
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", reply.Task.FileName[0])
			}
			file.Close()
			files := make([]*os.File, reply.ReduceNum, reply.ReduceNum)
			encs := make([]*json.Encoder, reply.ReduceNum, reply.ReduceNum)
			for i := 0; i < reply.ReduceNum; i++ {
				files[i], err = ioutil.TempFile(pwd, fmt.Sprintf("map-%d-%d", reply.Task.TaskNum, i))
				if err != nil {
					log.Fatalf("open file err:%s", err.Error())
				}
				//files = append(files, file)

				//enc := json.NewEncoder(file)
				//encs = append(encs, enc)
				encs[i] = json.NewEncoder(files[i])
			}

			kva := mapf(reply.Task.FileName[0], string(content))
			for _, kv := range kva {
				err = encs[ihash(kv.Key)%reply.ReduceNum].Encode(&kv)
				if err != nil {
					log.Fatalf("encode json failed %s,%s,%s", kv.Key, kv.Value, err.Error())
				}
			}
			submitFiles := make([]string, 0, 10)
			for i := 0; i < reply.ReduceNum; i++ {
				os.Rename(files[i].Name(), fmt.Sprintf("%smap-%d-%d", pwd, reply.Task.TaskNum, i))
				files[i].Close()
				submitFiles = append(submitFiles, fmt.Sprintf("%smap-%d-%d", pwd, reply.Task.TaskNum, i))
			}
			submitArgs := &SubmitTaskArgs{
				Task: Task{TaskNum: reply.Task.TaskNum, TaskType: MAP_TASK, FileName: submitFiles},
			}
			var submitReply SubmitTaskReply
			ok := call("Coordinator.SubmitTask", submitArgs, &submitReply)
			if !ok {
				os.Exit(1)

			}
			//log.Println(fmt.Sprintf("提交任务:%d-%d", submitArgs.Task.TaskType, submitArgs.Task.TaskNum))
			if submitReply.Reply == "wait" {
				time.Sleep(2 * time.Second)
			} else if submitReply.Reply == "quit" {
				os.Exit(1)
			}

		} else if reply.Task.TaskType == WAIT {
			time.Sleep(1 * time.Second)

		} else if reply.Task.TaskType == PLEASE_QUIT {
			os.Exit(1)

		}
	}

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
