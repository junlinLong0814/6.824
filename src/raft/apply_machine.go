package raft

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid 	bool
	Command      	interface{}
	CommandIndex 	int
}

func (rf *Raft) applyTicker(applyCh chan ApplyMsg){
	for !rf.killed(){
		rf.mu.Lock()
		//consumer wait until buff not null
		for len(rf.commitQueue) == 0{
			rf.cv.Wait()
		}
		//buff not null,start consume
		tmpMsgs := rf.commitQueue
		rf.commitQueue = make([]ApplyMsg,0)
		rf.mu.Unlock()
		for _,perMsg := range tmpMsgs{
			//apply to state machine
			applyCh <- perMsg
		}
	}
}