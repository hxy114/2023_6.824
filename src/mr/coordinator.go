package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

var (
	PREPARE_MAP_TASK_STAGE    = 1
	MAP_STEP_STAGE            = 2
	PREPARE_REDUCE_TASK_STAGE = 3
	REDUCE_STEP_STAGE         = 4
	COMPLETE_STAGE            = 5
)

type Coordinator struct {
	CoordinatorType    int
	MapTask            chan Task
	ProcessMapTask     map[int]int
	CompleteMapTask    map[int]int
	MapTaskNum         int
	ReduceFile         [][]string
	ReduceTask         chan Task
	ProcessReduceTask  map[int]int
	CompleteReduceTask map[int]int
	ReduceTaskNum      int
	ReduceNum          int
	// Your definitions here.
	Mutex *sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AssignTask(args *AskTaskArgs, reply *AskTaskReply) error {
	//log.Println("找我要任务")
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	if c.CoordinatorType == MAP_STEP_STAGE {
		if len(c.MapTask) > 0 {
			reply.Task = <-c.MapTask
			reply.ReduceNum = 10
			c.ProcessMapTask[reply.Task.TaskNum] = reply.Task.TaskNum
			//log.Println(fmt.Sprintf("给予任务:%d-%d", reply.Task.TaskType, reply.Task.TaskNum))
			go func(task Task) {
				time.Sleep(10 * time.Second)
				c.Mutex.Lock()
				defer c.Mutex.Unlock()
				if _, ok := c.CompleteMapTask[task.TaskNum]; !ok {
					c.MapTask <- task
					delete(c.ProcessMapTask, task.TaskNum)
				}
			}(reply.Task)
		} else {
			reply.Task.TaskType = WAIT
		}

		return nil

	} else if c.CoordinatorType == PREPARE_REDUCE_TASK_STAGE {
		reply.Task.TaskType = WAIT
		return nil

	} else if c.CoordinatorType == REDUCE_STEP_STAGE {
		reply.Task.TaskType = REDUCE_TASK
		if len(c.ReduceTask) > 0 {
			reply.Task = <-c.ReduceTask
			c.ProcessReduceTask[reply.Task.TaskNum] = reply.Task.TaskNum
			//log.Println(fmt.Sprintf("给予任务:%d-%d", reply.Task.TaskType, reply.Task.TaskNum))
			go func(task Task) {
				time.Sleep(10 * time.Second)
				c.Mutex.Lock()
				defer c.Mutex.Unlock()
				if _, ok := c.CompleteReduceTask[task.TaskNum]; !ok {
					c.ReduceTask <- task
					delete(c.ProcessReduceTask, task.TaskNum)
				}
			}(reply.Task)
		} else {
			reply.Task.TaskType = WAIT
		}

		return nil
	} else if c.CoordinatorType == COMPLETE_STAGE {
		reply.Task.TaskType = PLEASE_QUIT
		return nil
	}
	return nil

}
func (c *Coordinator) SubmitTask(args *SubmitTaskArgs, reply *SubmitTaskReply) error {
	//log.Println(fmt.Sprintf("提交任务:%d-%d", args.Task.TaskType, args.Task.TaskNum))
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	if c.CoordinatorType == MAP_STEP_STAGE && args.Task.TaskType == MAP_TASK {
		if _, ok := c.ProcessMapTask[args.Task.TaskNum]; ok {
			//log.Println(fmt.Sprintf("提交任务成功:%d-%d", args.Task.TaskType, args.Task.TaskNum))
			for index, filename := range args.Task.FileName {
				c.ReduceFile[index] = append(c.ReduceFile[index], filename)
			}
			c.CompleteMapTask[args.Task.TaskNum] = args.Task.TaskNum
			delete(c.ProcessMapTask, args.Task.TaskNum)
			if len(c.CompleteMapTask) == c.MapTaskNum {
				c.CoordinatorType = PREPARE_REDUCE_TASK_STAGE
				reply.Reply = "wait"
				//log.Println("进入准备reduce阶段")
				go func() {
					c.Mutex.Lock()
					defer c.Mutex.Unlock()
					for i := 0; i < c.ReduceNum; i++ {
						task := Task{TaskNum: i, TaskType: REDUCE_TASK, FileName: c.ReduceFile[i]}
						c.ReduceTask <- task
						c.ReduceTaskNum += 1
					}
					c.CoordinatorType = REDUCE_STEP_STAGE
					//log.Println("进入ruduce阶段")
				}()
			}
		}

		return nil

	} else if c.CoordinatorType == REDUCE_STEP_STAGE && args.Task.TaskType == REDUCE_TASK {
		if _, ok := c.ProcessReduceTask[args.Task.TaskNum]; ok {
			/*for index, filename := range args.Task.FileName {
				c.ReduceFile[index] = append(c.ReduceFile[index], filename)
			}*/
			c.CompleteReduceTask[args.Task.TaskNum] = args.Task.TaskNum
			delete(c.ProcessReduceTask, args.Task.TaskNum)
			if len(c.CompleteReduceTask) == c.ReduceTaskNum {
				c.CoordinatorType = COMPLETE_STAGE
				//log.Println("完成阶段")
				reply.Reply = "quit"
				/*				go func() {
								c.Mutex.Lock()
								defer c.Mutex.Unlock()
								for i:=0;i<c.ReduceNum;i++{
									task:=Task{TaskNum: i,TaskType: REDUCE_TASK,FileName: c.ReduceFile[i]}
									c.ReduceTask<-task
								}
								c.CoordinatorType=REDUCE_STEP_STAGE
							}()*/
			}
		}

		return nil
	} else if c.CoordinatorType == COMPLETE_STAGE {
		reply.Reply = "quit"

		return nil
	} else if c.CoordinatorType == PREPARE_REDUCE_TASK_STAGE {
		reply.Reply = "wait"
	}
	return nil

}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	log.Println("测试example")
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {

	if c.CoordinatorType == COMPLETE_STAGE {
		return true
	}

	// Your code here.

	return false
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		CoordinatorType:    PREPARE_MAP_TASK_STAGE,
		MapTask:            make(chan Task, 100),
		ProcessMapTask:     make(map[int]int, 100),
		CompleteMapTask:    make(map[int]int, 100),
		MapTaskNum:         0,
		ReduceFile:         make([][]string, nReduce, nReduce),
		ReduceTask:         make(chan Task, 100),
		ProcessReduceTask:  make(map[int]int, 100),
		CompleteReduceTask: make(map[int]int, 100),
		ReduceTaskNum:      0,
		Mutex:              new(sync.Mutex),
		ReduceNum:          nReduce,
	}
	for i := range c.ReduceFile {
		c.ReduceFile[i] = make([]string, 0, 100)
	}

	// Your code here.
	c.Mutex.Lock()
	for index, file := range files {
		task := Task{TaskNum: index, TaskType: MAP_TASK, FileName: make([]string, 0, 1)}
		task.FileName = append(task.FileName, file)
		c.MapTask <- task
		c.MapTaskNum += 1
	}

	c.CoordinatorType = MAP_STEP_STAGE
	c.Mutex.Unlock()
	c.server()
	return &c
}
