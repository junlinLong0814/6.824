package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "encoding/json"
import "strconv"
import "time"

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

		for {
			task_args := RequestTaskArgs{}
			task_reply := RequestTaskReply{}
	
			ok := call("Master.RequireTask",&task_args,&task_reply)
			if !ok {
				log.Fatal("call rpc error! work exist!")
			}
	
			if task_reply.Task_kind == Map_Task{
				filename := task_reply.Map_file_name
				file,err := os.Open(filename)
				if err != nil{
					log.Fatalf("can't open %v",filename)
				}
	
				content,err := ioutil.ReadAll(file)
				if err != nil{
					log.Fatalf("read %v content error!",filename)
				}
				
				file.Close()
				kva := mapf(filename,string(content))
				
				/*split total kva into reduce_num and then write into file*/
				reduceNum := task_reply.Reducer_num
				splitKva := make([][]KeyValue,reduceNum)
				for i := 0 ; i < reduceNum; i++{
					splitKva[i] = []KeyValue{}
				}
				//when the keys hash the same value,they shoule be dealt with the same reduce worker
				for _,perKv := range kva{
					hashIdx := ihash(perKv.Key) % reduceNum
					splitKva[hashIdx] = append(splitKva[hashIdx],perKv)
				} 
				//write into file
				for i:=0 ; i < reduceNum; i++{
					//eg mr-1-9 : the values of the first task/file (hash key = 9) should be counted by the 9th reduce worker
					intermediate_fileName := fmt.Sprintf("mr-%d-%d",task_reply.Task_idx,i)
					intermediate_file,err := ioutil.TempFile("",intermediate_fileName)
					if err != nil{
						log.Fatalf("TempFile %v error!",intermediate_fileName)
					}

					encode_file := json.NewEncoder(intermediate_file)
					for _,perKv := range splitKva[i]{
						err := encode_file.Encode(&perKv)
						if err != nil{
							log.Fatal("Json Encode error!")
						}
					}
					intermediate_file.Close()
					os.Rename(intermediate_file.Name(),intermediate_fileName)
				}
				
				go func(task_id int){
					args := DoneTaskArgs{}
					reply := DoneTaskReply{}
					args.Task_kind,args.Task_idx = Map_Task,task_id

					ok := call("Master.TaskDone",&args,&reply)
					if !ok{
						log.Fatal("call done func error!")
					}
				}(task_reply.Task_idx)
		
			} else if task_reply.Task_kind == Reduce_Task{
				total_kva := make(map[string][]string)
				for i:=0 ; i < task_reply.Map_num; i++{
					intermediate_fileName := fmt.Sprintf("mr-%d-%d",i,task_reply.Task_idx)
					intermediate_file, err := os.Open(intermediate_fileName)	
					if err != nil {
						log.Fatalf("Reduce Task open %v error!",intermediate_fileName)
					}

					encode_file := json.NewDecoder(intermediate_file)
					for {
						var perKv KeyValue
						err := encode_file.Decode(&perKv)
						if err != nil{
							break
						}
						total_kva[perKv.Key] = append(total_kva[perKv.Key],perKv.Value)
					}
					intermediate_file.Close()
				}

				reduce_out := []KeyValue{}
				for k,v := range total_kva {
					per_reduce_out := KeyValue{k,reducef(k,v)}
					reduce_out = append(reduce_out,per_reduce_out)
				}
				
				reduce_out_fileName := "mr-out-" + strconv.Itoa(task_reply.Task_idx)
				reduce_out_file , err := ioutil.TempFile("",reduce_out_fileName)
				if err != nil {
					log.Fatalf("Open %v error!",reduce_out_fileName)
				}

				for _,kv := range reduce_out{
					fmt.Fprintf(reduce_out_file,"%v %v\n",kv.Key,kv.Value)
				} 
				reduce_out_file.Close()
				os.Rename(reduce_out_file.Name(),reduce_out_fileName)
				
				go func(task_id int) {
					args := DoneTaskArgs{}
					reply := DoneTaskReply{}
					args.Task_kind,args.Task_idx = Reduce_Task,task_id

					ok := call("Master.TaskDone",&args,&reply)
					if !ok{
						log.Fatal("call done func error!")
					}
				}(task_reply.Task_idx)
			}else if task_reply.Task_kind == Idle_Task{
				time.Sleep(1 * time.Second)
			}
		}

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()

}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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
