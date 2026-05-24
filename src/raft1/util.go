package raft

import (
	"fmt"
	"log"
	_ "os"
	_ "strconv"
	"time"
)

var ToDebug bool = false

type logTopic string

const (
	dVote    logTopic = "VOTE"
	dLeader  logTopic = "LEAD"
	dTerm    logTopic = "TERM"
	dTimer   logTopic = "TIMR"
	dLog     logTopic = "LOG1"
	dLog2    logTopic = "LOG2"
	dCommit  logTopic = "CMIT"
	dPersist logTopic = "PERS"
	dSnap    logTopic = "SNAP"
	dDrop    logTopic = "DROP"
	dInfo    logTopic = "INFO"
	dError   logTopic = "ERRO"
	dClient  logTopic = "CLNT"
	dTest    logTopic = "TEST"
	dTrace   logTopic = "TRCE"
	dWarn    logTopic = "WARN"
)

func DPrintf(topic logTopic, format string, a ...interface{}) {
	//in case we arent on verbose mode
	if !ToDebug {
		return
	}

	relativeTime := time.Since(ProgramStart).Microseconds() / 100

	debugString := fmt.Sprintf("%v %s ", relativeTime, topic)
	fullDebug := debugString + format
	//have to spread interface slices
	log.Printf(fullDebug, a...)
}

func init() {
	// debug := os.Getenv("VERBOSE")
	debugVal := true
	//set printf log flags
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
	if debugVal {
		ToDebug = true
	}

	//set todebug to false for testing right now
	ToDebug = false
}
