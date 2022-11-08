package raft

import "log"
import "fmt"
import "time"

// Debugging
const Debug = 0
const Vote = 0
const MyDebug = 0
const Log = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func TimeInfo() string{
	return "["+time.Now().Format("2006-01-02 15:04:05.000")+"]"
}

func VoteInfo(format string, a ...interface{}) (n int , err error){
	if Vote > 0 {
		fmt.Printf(format,a...)
	}
	return 
}

func NoTimeLogInfo(format string, a ...interface{}) (n int , err error){
	if Log > 0 {
		fmt.Printf(format,a...)
	}
	return 
}

func LogInfo(format string, a ...interface{}) (n int , err error){
	if Log > 0 {
		format =TimeInfo() +format
		fmt.Printf(format,a...)
	}
	return 
}

func NoTimeDebug(format string, a ...interface{}) (n int , err error){
	if MyDebug > 0 {
		fmt.Printf(format,a...)
	}
	return 
}

func DeBugPrintf(format string, a ...interface{}) (n int , err error){
	if MyDebug > 0 {
		format = TimeInfo() +format
		fmt.Printf(format,a...)
	}
	return 
}
