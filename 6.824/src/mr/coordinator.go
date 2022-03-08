package mr

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	TaskIdle = iota
	TaskWorking
	TaskCommit
)

type Coordinator struct {
	// Your definitions here.
	files   []string
	nReduce int

	mapTasks    []int
	reduceTasks []int

	mapCount int

	workerCommit map[string]int
	allCommited  bool

	timeout time.Duration

	mu sync.RWMutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) ReplyHandler(args *WorkArgs, reply *WorkReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for k, v := range c.files {
		if c.mapTasks[k] != TaskIdle {
			continue
		}

		reply.Taskid = k
		reply.Filename = v
		reply.MapReduce = "map"
		reply.BucketNum = c.nReduce
		reply.Isfinished = false
		c.workerCommit[args.Workerid] = TaskWorking
		c.mapTasks[k] = TaskWorking

		ctx, _ := context.WithTimeout(context.Background(), c.timeout)
		go func() {
			select {
			case <-ctx.Done():
				{
					c.mu.Lock()
					defer c.mu.Unlock()
					if c.workerCommit[args.Workerid] != TaskCommit && c.mapTasks[k] != TaskCommit {
						c.mapTasks[k] = TaskIdle
						//log.Println("[Error]:", "worker:", args.Workerid, "map task:", k, "timeout")
					}
				}
			}
		}()
		return nil
	}

	for k, v := range c.reduceTasks {
		if c.mapCount != len(c.files) {
			return nil
		}
		if v != TaskIdle {
			continue
		}
		reply.Taskid = k
		reply.Filename = ""
		reply.MapReduce = "reduce"
		reply.BucketNum = len(c.files)
		reply.Isfinished = false
		c.workerCommit[args.Workerid] = TaskWorking
		c.reduceTasks[k] = TaskWorking
		ctx, _ := context.WithTimeout(context.Background(), c.timeout)
		go func() {
			select {
			case <-ctx.Done():
				{
					c.mu.Lock()
					defer c.mu.Unlock()
					if c.workerCommit[args.Workerid] != TaskCommit && c.reduceTasks[k] != TaskCommit {
						c.reduceTasks[k] = TaskIdle
						//log.Println("[Error]:", "worker:", args.Workerid, "reduce task:", k, "timeout")
					}
				}
			}
		}()
		//log.Println("a worker", args.Workerid, "apply a reduce task:", *reply)
		return nil
	}

	for _, v := range c.workerCommit {
		if v == TaskWorking {
			reply.Isfinished = false
			return nil
		}
	}
	reply.Isfinished = true
	//c.isfinished()
	return errors.New("worker apply but no tasks to dispatch")
}

func (c *Coordinator) CommitHandler(args *CommitArgs, reply *CommitReply) error {
	//log.Println("a worker", args.Workerid, "commit a "+args.MapReduce+" task:", args.Taskid)
	c.mu.Lock()
	//defer c.mu.Unlock()
	switch args.MapReduce {
	case "map":
		{
			c.mapTasks[args.Taskid] = TaskCommit
			c.workerCommit[args.Workerid] = TaskCommit
			c.mapCount++;
		}
	case "reduce":
		{
			c.reduceTasks[args.Taskid] = TaskCommit
			c.workerCommit[args.Workerid] = TaskCommit
		}
	}

	log.Println("current", c.mapTasks, c.reduceTasks)
	c.mu.Unlock()

	return c.isfinished()
}

func (c *Coordinator) isfinished() (error) {
	c.mu.Lock()
	for _, v := range c.mapTasks {
		if v != TaskCommit {
			c.mu.Unlock()
			return nil
		}
	}
	for _, v := range c.reduceTasks {
		if v != TaskCommit {
			c.mu.Unlock()
			return nil
		}
	}
	c.allCommited = true
	c.mu.Unlock()
	log.Println("all tasks completed")
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	//ret := false
	// Your code here.
	c.mu.RLock()
	ret := c.allCommited
	c.mu.RUnlock()
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:        files,
		nReduce:      nReduce,
		mapTasks:     make([]int, len(files)),
		reduceTasks:  make([]int, nReduce),
		workerCommit: make(map[string]int),
		allCommited:  false,
		timeout:      10 * time.Second,
	}
	// Your code here.
	log.Println("[init] with:", files, nReduce)
	c.server()
	return &c
}
