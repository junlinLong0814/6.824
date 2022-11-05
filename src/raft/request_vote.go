package raft

import "time"
import "math/rand"
//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	CandicatedTerm		int
	CandicateId			int
	LastLogIndex		int
	LastLogTerm			int 
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	UpdateTerm			int
	VoteGranted			bool
}

func (rf *Raft) leaderElectionTicker(){
	ticker := time.NewTicker(time.Millisecond * ELECTION_CHECK_TIME)

	for range ticker.C{
		rf.mu.Lock()
		if !rf.killed() {
			//haven't heard from leader, start the election
			if (time.Now().After(rf.expiredElectionTime)) &&
				(rf.myState == Follower || rf.myState == Candicate){
				//I become a candicate and reset timer to start a election
				rf.changeState(Candicate,-1,-1)
				rf.resetElectionTimer()
				//request for vote
				VoteInfo("[%s] %d start a election!\n",TimeInfo(),rf.me)
				for other,_ := range rf.peers{
					if other == rf.me{
						continue
					}
					go rf.callForVote(other, rf.currentTerm, rf.me,rf.logs[len(rf.logs)-1].Index,rf.logs[len(rf.logs)-1].Term)
				}
				if rf.myState != Leader{
					VoteInfo("[%s] %d got %d vote!\n",TimeInfo(),rf.me, rf.poll)
				}
			}
			rf.mu.Unlock()
		}else {
			rf.mu.Unlock()
		}
	}
	ticker.Stop()
}

func (rf *Raft) callForVote(other,currentTerm,me,index,term int){
	args, reply := RequestVoteArgs{},RequestVoteReply{}

	args.CandicatedTerm = currentTerm
	args.CandicateId = me
	args.LastLogIndex = index
	args.LastLogTerm = term

	ok := rf.sendRequestVote(other,&args,&reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		//check the reply info
		if reply.UpdateTerm > rf.currentTerm{
			//there are other newer candicate
			//update my term and change to be a follower
			rf.changeState(Follower,reply.UpdateTerm,-1)
			rf.resetElectionTimer()
			return 
		}

		//check my current state  
		if rf.myState != Candicate{
			//now i am a follower or leader
			//should ignore this reply; do nothing
			return 
		}

		if args.CandicatedTerm != reply.UpdateTerm{
			/*
			1.reply.UpdateTerm < args.CandicatedTerm : 
				this vote is a vote on previous election, so ignore it
			2.reply.UpdateTerm > args.CandicatedTerm && reply.UpdateTerm <= rf.currentTerm :
				impossible situation
			*/
			return 
		}

		if reply.VoteGranted{
			//got a vote from others 
			rf.poll += 1
			if rf.poll > len(rf.peers)/2{
				VoteInfo("[%s] %d got %d vote!\n",TimeInfo(),rf.me, rf.poll)
				rf.changeState(Leader,-1,-1)
			}
		}
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	//when i am follower,the election timer reset only after i have received a valid vote requestion
	rf.mu.Lock()
	defer rf.mu.Unlock()

	lastLogApplied,lastLogTerm := rf.logs[len(rf.logs)-1].Index, rf.logs[len(rf.logs)-1].Term

	if args.CandicatedTerm < rf.currentTerm{
		//candicate's term less than my term
		reply.UpdateTerm = rf.currentTerm
		reply.VoteGranted = false
	}else if args.CandicatedTerm == rf.currentTerm{
		reply.UpdateTerm = rf.currentTerm
		//candicate's term is equal to my term
		//situation : at the same time campaign

		//compare log's attribute and check votefor
		if (rf.voteFor == -1 || rf.voteFor == args.CandicateId) && 
			( (args.LastLogTerm > lastLogTerm) || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogApplied)){
				//candicate's log must newer than mine 
				reply.VoteGranted = true
				//reset the election timer
				rf.resetElectionTimer()
		}else {
				reply.VoteGranted = false
		}
	}else {
		//candicate's term newer than mine
		//I shoule update my term and change mystate to follower
		reply.UpdateTerm = args.CandicatedTerm

		//voting for candicate depends on the log's version
		if (args.LastLogTerm > lastLogTerm) || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogApplied){
			reply.VoteGranted = true
			rf.changeState(Follower, args.CandicatedTerm, args.CandicateId)
		}else {
			reply.VoteGranted = false
			rf.changeState(Follower,args.CandicatedTerm, -1)
		}
		//reset the election timer
		rf.resetElectionTimer()
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}



//got a random timeout range [200ms ~ 400ms]
func (rf *Raft) resetElectionTimer(){
	t := rand.Int31n(200)
	rf.expiredElectionTime = time.Now().Add(time.Duration(t+200) * time.Millisecond)
	VoteInfo("[%s] %d out time [%s]\n",TimeInfo(),rf.me,rf.expiredElectionTime.Format("2006-01-02 15:04:05.000"))
}


