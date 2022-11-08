package raft

import "time"

type appendArgs struct {
	Term			int				//leader's term
	LeaderId		int				//so follower can redirect clients
	PrevLogIndex	int 			//index of log entry immediately preceding new ones
	PrevLogTerm 	int				//term of prevLogIndex entry
	Entries			[]LogEntry		//log entries to store, empty for heartbeat
	LeaderCommit	int				//leader's commitIndex
}

type appendReply struct {
	Term			int				//currentTerm, for leader to update itself
	Success			bool 			//true if follower contained entry mathcing preLogIndex and prevLogTerm
}

func min(a,b int) int{
	if a < b {
		return a
	}
	return b
}

func max(a,b int) int{
	if a > b{
		return a
	}
	return b
}

func (rf *Raft) sendAppendEntries(server int, args *appendArgs, reply *appendReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) appendEntriesTricker(){
	for !rf.killed(){
		time.Sleep(APPEND_CHECK_TIME * time.Millisecond)

		rf.mu.Lock()
		//only leader can append entry 
		if rf.myState != Leader{
			rf.mu.Unlock()
			continue
		}

		//check the append time of every peer
		for i:=0 ; i < len(rf.peers); i++{
			if i == rf.me{
				continue 
			}

			if !time.Now().After(rf.expiredAppendTimes[i]){
				//still in time
				continue
			}
			tmpLog := make([]LogEntry,rf.logs[len(rf.logs)-1].Index - rf.nextIndex[i]+1)
			copy(tmpLog,rf.logs[rf.getIndexByAbsoluteIndex(rf.nextIndex[i]) : ])

			go rf.callForAppend(i,rf.currentTerm,rf.me,rf.nextIndex[i]-1,rf.getTermByAbsoluteIndex(rf.nextIndex[i]-1),rf.commitIndex,tmpLog)
			rf.resetAppendTimer(i,false)
		}
		rf.mu.Unlock()
	}
}


func (rf *Raft) callForAppend(peer,currentTerm,me,PrevLogIndex,PrevLogTerm,LeaderCommit int,logs []LogEntry){
	args,reply := appendArgs{},appendReply{}

	args.Term = currentTerm
	args.LeaderId = me
	args.PrevLogIndex = PrevLogIndex
	args.PrevLogTerm = PrevLogTerm
	args.LeaderCommit = LeaderCommit
	args.Entries = logs
	
	ok := rf.sendAppendEntries(peer,&args,&reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		VoteInfo("[%s] %d leader's term is %d and relpy's term is %d\n",TimeInfo(),rf.me,rf.currentTerm,reply.Term)
		if rf.currentTerm > reply.Term{
			return
		}

		if reply.Term > rf.currentTerm{
			//found other term bigger than mine
			//i should change follower
			rf.changeState(Follower,reply.Term,-1)
			rf.resetElectionTimer()
			return
		}

		if rf.myState != Leader{
			//my state is not leader
			return 
		}
		
		if args.Term != rf.currentTerm{
			//I was a leader in term (A) and send RPC to peers,
			//then sth happened , I become a leader in term (A + x),
			//so this packet was out of date(Term A),ignore it
			return ;
		}

		if !reply.Success{
			if rf.nextIndex[peer] > 0{
				rf.nextIndex[peer] -= 1
			}else{
				rf.nextIndex[peer] = 0
			}
			rf.resetAppendTimer(peer,true)
		}else{
			rf.nextIndex[peer] = rf.nextIndex[peer] + len(logs)
			rf.matchIndex[peer] = rf.nextIndex[peer] - 1
			//count all the peer's mathcIndex
			//check if there are more than half of the matchIndex,
			//if exist, update the commitIndex
			
			newCommitindex := 0
			count := make(map[int]int)
			for i:=0 ; i < len(rf.peers); i++{
				if i != rf.me{
					count[rf.matchIndex[i]]++
					if count[rf.matchIndex[i]] + 1 > len(rf.peers)/2{
						newCommitindex = max(newCommitindex,rf.matchIndex[i])
					} 
				}
			}
			if newCommitindex > rf.commitIndex && newCommitindex <= rf.logs[len(rf.logs)-1].Index && rf.getTermByAbsoluteIndex(newCommitindex) == rf.currentTerm{
				for i := rf.commitIndex+1; i <= newCommitindex; i++{
					rf.commitQueue = append(rf.commitQueue,ApplyMsg{
						CommandValid:	true,
						Command:		rf.logs[rf.getIndexByAbsoluteIndex(i)].Command,
						CommandIndex:	i,
					})
				}  
				rf.commitIndex = newCommitindex
				rf.cv.Broadcast()
			}

			rf.resetAppendTimer(peer,false)
		}

		
	}
}


func (rf *Raft) AppendEntries(args *appendArgs, reply *appendReply){
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm{
		//my term newer than leader
		reply.Success = false
		reply.Term = rf.currentTerm
		return 
	}

	//check the log version
	curLastLogIndex := rf.logs[len(rf.logs)-1].Index
	if curLastLogIndex < args.PrevLogIndex  {
		//my logs doesn't contain an entry at prevlogindex
		reply.Success = false
		reply.Term = args.Term
		//DeBugPrintf("%d's %d log's term is %d\n",rf.me,args.PrevLogIndex,rf.getTermByAbsoluteIndex(args.PrevLogIndex))
	}else if rf.getTermByAbsoluteIndex(args.PrevLogIndex) != args.PrevLogTerm{
		reply.Success = false
		reply.Term = args.Term
	}else{
		reply.Success = true
		reply.Term = args.Term
		//match prev index 
		//check if exist confict log
		confictIdx := -1
		for i := 0; i < len(args.Entries); i++{
			curIdx := args.PrevLogIndex + i + 1
			if curIdx > curLastLogIndex{
				confictIdx = curIdx
				break
			}
			if rf.getTermByAbsoluteIndex(curIdx) != args.Entries[i].Term {
				confictIdx = curIdx
				break
			}
		}

		if confictIdx != -1{
			confictAbsIdx := rf.getIndexByAbsoluteIndex(confictIdx)
			//partily match
			//[args.PrevLogIndex,confictIdx) match
			//[confictIdx,len(args.Entries)) not match
			rf.logs = rf.logs[0 : confictAbsIdx]
			rf.logs = append(rf.logs, args.Entries[confictIdx - args.PrevLogIndex - 1:]...)
		}


		LogInfo("leader's curLog:\n")
		for i:=0; i<len(args.Entries);i++{
			NoTimeLogInfo("{idx: %d,term: %d},",args.Entries[i].Index,args.Entries[i].Term)
		}
		NoTimeLogInfo("\nconfict idx: %d, abs: %d\n",confictIdx,rf.getIndexByAbsoluteIndex(confictIdx))

		LogInfo("%d 's curLog:\n",rf.me)
		for i:=0; i<len(rf.logs);i++{
			NoTimeLogInfo("{idx: %d,term: %d},",rf.logs[i].Index,rf.logs[i].Term)
		}
		NoTimeLogInfo("\n")

		//check leader's commit idx and update mine
		commitIdxBak := rf.commitIndex
		if args.LeaderCommit > commitIdxBak{
			//my commited idx smaller than leader's 
			//update my commited idx
			rf.commitIndex = min(args.LeaderCommit,rf.logs[len(rf.logs)-1].Index)
		}
		
		if rf.commitIndex > commitIdxBak{
			//apply logs in (commitIdxBak,rf.commitIndex] to state machine
			for i := commitIdxBak + 1; i <= rf.commitIndex; i++{
				//apply logs[i] to state machine
				rf.commitQueue = append(rf.commitQueue,ApplyMsg{
					CommandValid:	true,
					Command:		rf.logs[rf.getIndexByAbsoluteIndex(i)].Command,
					CommandIndex:	i,
				})
			}
			rf.cv.Broadcast()
		}
	}

	//if leader's term > mine , should change to be a follower
	//or if now i am not Follower and receive a entry ,should change to be a follower too.
	if args.Term > rf.currentTerm || rf.myState != Follower{
		rf.changeState(Follower,args.Term,-1)
	}

	//when receive a entry, reset the election time
	rf.resetElectionTimer()

}


func (rf *Raft) resetAppendTimer(peer int,now bool){
	//if now == true, retran immeidately
	t := time.Now()
	if !now{
		t = t.Add(100 * time.Millisecond)
	}
	rf.expiredAppendTimes[peer] = t
	
}