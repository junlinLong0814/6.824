package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import "sync/atomic"
import "../labrpc"
import "time"
//import "fmt"


import "bytes"
import "../labgob"

type RaftState int

type LogEntry struct{
	Term  		int
	Index 		int
	Command		interface{}
}

const (
	ELECTION_CHECK_TIME = 5		//check whether the timeout occurs every 5ms
	APPEND_CHECK_TIME = 2
)

const (
	Follower 	RaftState = 1
	Candicate	RaftState = 2
	Leader		RaftState = 3	
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          		// Lock to protect shared access to this peer's state
	cv 		  *sync.Cond 				// the cv for sync producer and consumer
	peers     []*labrpc.ClientEnd 		// RPC end points of all peers
	persister *Persister          		// Object to hold this peer's persisted state
	me        int                 		// this peer's index into peers[]
	dead      int32               		// set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	myState   		RaftState			//Server state
	currentTerm		int					//my term (be persistent)
	voteFor	 		int					//vote for x (be persistent)
	logs	  		[]LogEntry			//log entries (be persistent)
	expiredElectionTime		time.Time	//if now() > expiredElectionTime,it should start a election

	poll			int					//received vote count

	commitIndex		int					//index of highest log entry known to be commited
	/*only for leader*/
	nextIndex		[]int				//for each server,index of the next log entry to send to that sever
										//initialized to leader last log index + 1		

	matchIndex		[]int				//for each server,index of highest log entry known to be replicated on server

	expiredAppendTimes	[]time.Time		//After appended the log entry to all server,
										//When a server does not reply within the expiredAppendTimes[i] time
										//and that server time out

	commitQueue		[]ApplyMsg			//consumer-procuder model,buff for applyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = (rf.myState == Leader)

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	LogInfo("[%d] persist, len(logs):[%d], currentTerm:[%d], voteFor:[%d]\n",rf.me,len(rf.logs),rf.currentTerm,rf.voteFor)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term	int
	var voteFor int
	var logs	[]LogEntry
	if d.Decode(&term) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&logs) != nil{
		DeBugPrintf("%d readPersist Decode error!\n",rf.me)	
	} else {
	  rf.currentTerm = term
	  rf.logs = logs
	  rf.voteFor = voteFor
	  LogInfo("[%d] readPersist, len(logs):[%d], currentTerm:[%d], voteFor:[%d]\n",rf.me,len(rf.logs),rf.currentTerm,rf.voteFor)
	}
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed(){
		return -1,-1,false
	}

	if rf.myState != Leader{
		return -1,-1,false
	}else{
		prevLogIdx,curTerm := rf.logs[len(rf.logs)-1].Index, rf.currentTerm
		rf.logs = append(rf.logs,LogEntry{
			Term:		curTerm,
			Index:		prevLogIdx + 1,
			Command:	command,
		})
		rf.persist()

		for i := 0; i < len(rf.peers); i++{
			if i != rf.me{
				rf.resetAppendTimer(i,true)
			}
		}

		return prevLogIdx+1, curTerm, true
	}
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) changeState(ideal_state RaftState,newTerm int, voteFor int){
	if ideal_state == Candicate{
		rf.currentTerm += 1
		rf.voteFor = rf.me
		rf.myState = Candicate
		rf.poll = 1
		rf.persist()
		LogInfo("[%s] %d -> Candicate Term:%d\n",TimeInfo(),rf.me,rf.currentTerm)
	}else if ideal_state == Leader{
		rf.myState = Leader
		for i := 0; i < len(rf.peers) ;i++{
			if i != rf.me{
				rf.nextIndex[i] = rf.logs[len(rf.logs)-1].Index + 1
			}
		}
		rf.poll = 0
		LogInfo("[%s] %d -> Leader Term:%d \n",TimeInfo(),rf.me,rf.currentTerm)
	}else if ideal_state == Follower{
		rf.myState = Follower
		rf.currentTerm = newTerm
		rf.voteFor = voteFor
		rf.poll = 0
		rf.persist()
		LogInfo("[%s] %d -> Follower Term:%d\n",TimeInfo(),rf.me,rf.currentTerm)
	}
	
}

func (rf *Raft) getTermByAbsoluteIndex(index int) int{
	
	return rf.logs[rf.getIndexByAbsoluteIndex(index)].Term
}

func (rf *Raft) getIndexByAbsoluteIndex(index int) int{
	//DeBugPrintf("Absoluate Index:%d Relaitve Index:%d\n",index,index - rf.logs[0].Index)
	return index - rf.logs[0].Index
}




//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	// Your initialization code here (2A, 2B, 2C).
	serversCount := len(peers)

	rf := &Raft{
		peers: 					peers,
		persister:				persister,
		me:						me,
		currentTerm: 			0,
		voteFor:				-1,
		logs:					make([]LogEntry,0),
		myState:				Follower,
		nextIndex:				make([]int,serversCount),
		expiredAppendTimes:		make([]time.Time,serversCount),
		matchIndex:				make([]int,serversCount),
		commitQueue:			make([]ApplyMsg,0),
		
	}
	rf.cv = sync.NewCond(&rf.mu)
	rf.logs = append(rf.logs,LogEntry{
		Index:		0,
		Term:		0,
		Command: 	nil,
	})

	rf.resetElectionTimer()
	for i := 0; i < serversCount; i++{
		rf.resetAppendTimer(i,false)
	}

	rf.commitIndex = rf.logs[0].Index
	DeBugPrintf("Have %d servers\n",serversCount)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	go rf.leaderElectionTicker()
	go rf.appendEntriesTricker()
	
	go rf.applyTicker(applyCh)

	return rf
}
