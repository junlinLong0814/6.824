package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"

/*fault tolerance:
Failure worker:
-->Map_tasks: completed->idle; in-progress->idle; 
-->Reduce_tasks :completed->completed(if stored in gfs); in-progress->idle; 

Failure master:
-->kill process 
*/
type void struct{}
var void_val void

type Master struct {
	// Your definitions here.
	Files 							[]string
	ReduceNum						int

	Unallocated_MapTasks			map[int]void
	InProgress_MapTasks				map[int]time.Time

	Unallocated_ReduceTasks			map[int]void
	InProgress_ReduceTasks			map[int]time.Time

	mutex 							sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

func (m *Master) RequireTask(args *RequestTaskArgs, reply *RequestTaskReply) error{
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if len(m.Unallocated_MapTasks) > 0{
		for task_idx,_ := range m.Unallocated_MapTasks{
			reply.Task_kind = Map_Task
			reply.Map_file_name = m.Files[task_idx]
			reply.Reducer_num = m.ReduceNum
			reply.Task_idx = task_idx

			m.InProgress_MapTasks[task_idx] = time.Now()
			delete(m.Unallocated_MapTasks,task_idx)
			break
		}
	}else if len(m.InProgress_MapTasks) == len(m.Files) {
		//all map tasks are in-progress
		reply.Task_kind = Idle_Task
	}else if len(m.Unallocated_ReduceTasks) > 0{
		for task_idx,_ := range m.Unallocated_ReduceTasks{
			reply.Map_num = len(m.Files)
			reply.Task_kind = Reduce_Task
			reply.Task_idx = task_idx

			m.InProgress_ReduceTasks[task_idx] = time.Now()
			delete(m.Unallocated_ReduceTasks,task_idx)
			break
		}
	}else if len(m.InProgress_ReduceTasks) == m.ReduceNum{
		//all reduce tasks are in-progress
		reply.Task_kind = Idle_Task
	}else {
		//all reduce tasks are done
		reply.Task_kind = Idle_Task
	}

	/*check the tls tasks*/
	go func(){
		m.mutex.Lock()
		defer m.mutex.Unlock()

		t_now := time.Now()
		for idx,start := range m.InProgress_MapTasks{
			if t_now.Sub(start) >= 10{
				//out of time
				delete(m.InProgress_MapTasks,idx)
				m.Unallocated_MapTasks[idx] = void_val
			}
		}
	}()
	go func(){
		m.mutex.Lock()
		defer m.mutex.Unlock()

		t_now := time.Now()
		for idx,start := range m.InProgress_ReduceTasks{
			if t_now.Sub(start) >= 10{
				//out of time
				delete(m.InProgress_ReduceTasks,idx)
				m.Unallocated_ReduceTasks[idx] = void_val
			}
		}
	}()
	return nil	
}

func (m *Master) TaskDone(args *DoneTaskArgs, reply *DoneTaskReply) error{
	m.mutex.Lock()
	defer m.mutex.Unlock()

	task_idx := args.Task_idx
	if args.Task_kind == Map_Task{
		_,ok := m.InProgress_MapTasks[task_idx]
		if ok {
			delete(m.InProgress_MapTasks,task_idx)
		} else {
			_,ok := m.Unallocated_MapTasks[task_idx]
			if ok{
				//if the task was found in {unallocated},because it was timed out recently
				delete(m.Unallocated_MapTasks,task_idx)
			}
		}
		if len(m.InProgress_MapTasks) == 0 && len(m.Unallocated_MapTasks) == 0{
			// all map tasks done
			for i:= 0; i < m.ReduceNum; i++{
				m.Unallocated_ReduceTasks[i] = void_val
			}
		}
	}else if args.Task_kind == Reduce_Task{
		_,ok := m.InProgress_ReduceTasks[task_idx]
		if ok {
			delete(m.InProgress_ReduceTasks,task_idx)
		}else {
			_,ok := m.Unallocated_ReduceTasks[task_idx]
			if ok {
				delete(m.Unallocated_ReduceTasks,task_idx)
			}
		}
	}

	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
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

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if len(m.Unallocated_MapTasks) == 0 && len(m.InProgress_MapTasks) == 0 &&
		len(m.Unallocated_ReduceTasks) == 0 && len(m.InProgress_ReduceTasks) == 0{
			ret = true
	}

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	
	// Your code here.
	m.Files,m.ReduceNum = files,nReduce
	m.Unallocated_MapTasks = make(map[int]void)
	m.InProgress_MapTasks = make(map[int]time.Time)
	m.Unallocated_ReduceTasks = make(map[int]void)
	m.InProgress_ReduceTasks = make(map[int]time.Time)

	for i := 0 ; i < len(files); i++{
		m.Unallocated_MapTasks[i] = void_val
	}

	m.server()
	return &m
}
