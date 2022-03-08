package mr

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func generateWorkId() (uuid string) {
	unix32bits := uint32(time.Now().UTC().Unix())
	buff := make([]byte, 12)
	num, err := rand.Read(buff);
	if num != len(buff) || err != nil {
		panic(err)
	}
	return fmt.Sprintf("%x-%x-%x-%x-%x-%x\n", unix32bits, buff[0:2], buff[2:4], buff[4:6], buff[6:8], buff[8:])
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	workId := generateWorkId()
	retry := 3
	for {
		args := WorkArgs{Workerid: workId}
		reply := WorkReply{}
		working := call("Coordinator.ReplyHandler", &args, &reply)
		if reply.Isfinished || !working {
			//log.Println("finished")
			//CommitCoodinator(workId, reply)
			return
		}
		//log.Println("task info:", reply)

		switch reply.MapReduce {
		case "map":
			Map(reply, mapf)
			retry = 3
		case "reduce":
			Reduce(reply, reducef)
			retry = 3
		default:
			log.Println("error reply: would retry times:", retry)
			if retry < 0 {
				return
			}
			retry--
		}

		CommitCoodinator(workId, reply)

		time.Sleep(500 * time.Millisecond)
	}

}

func CommitCoodinator(workId string, reply WorkReply) {
	commitArgs := CommitArgs{Workerid: workId, Taskid: reply.Taskid, MapReduce: reply.MapReduce}
	commitReply := CommitReply{}
	_ = call("Coordinator.CommitHandler", &commitArgs, &commitReply)
}

func Map(task WorkReply, mapf func(string, string) []KeyValue) {
	file, err := os.Open(task.Filename)
	if err != nil {
		log.Fatalf("cannot open %v", task.Filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.Filename)
	}
	file.Close()
	kva := mapf(task.Filename, string(content))
	//sort.Sort(ByKey(kva))

	tmpName := "mr-" + strconv.Itoa(task.Taskid)
	var fileBucket = make(map[int]*json.Encoder)
	for i := 0; i < task.BucketNum; i++ {
		ofile, _ := os.Create(tmpName + "-" + strconv.Itoa(i))
		fileBucket[i] = json.NewEncoder(ofile)
		defer ofile.Close()
	}

	for _, kv := range kva {
		key := kv.Key
		reduce_id := ihash(key) % task.BucketNum
		err := fileBucket[reduce_id].Encode(&kv)
		if err != nil {
			log.Fatal("Unable to write to file")
		}
	}
}

func Reduce(task WorkReply, reducef func(string, []string) string) error {
	intermediate := []KeyValue{}
	for mapTaskNum := 0; mapTaskNum < task.BucketNum; mapTaskNum++ {
		tmpName := "mr-" + strconv.Itoa(mapTaskNum) + "-" + strconv.Itoa(task.Taskid)
		f, err := os.Open(tmpName)
		if err != nil {
			log.Fatalf("cannot open %v", tmpName)
			return err
		}
		//defer f.Close()

		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		f.Close()
	}

	sort.Sort(ByKey(intermediate))
	oname := "mr-out-" + strconv.Itoa(task.Taskid+1)
	ofile, err := os.Create(oname)
	if err != nil {
		log.Fatal("Unable to create file: ", ofile)
		return err
	}
	defer ofile.Close()

	//log.Println("complete to ", task.Taskid, "start to write in to ", ofile)

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	return nil
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
