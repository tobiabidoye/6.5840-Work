package rsm

import (
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/raft1"
	"6.5840/raftapi"
	"6.5840/tester1"
)

var ErrNotLeader = errors.New("not leader")

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Me int
	//id will be some n bit random identifier to avoid collisions
	Id  int
	Req any
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.
	db      map[int]MapValue
	curTerm int
}

// stores the channel for readers to signal to commanders and also the command for doop
type MapValue struct {
	ReaderSignal chan any
	Cmd          Op
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
	}
	if !tester.UseRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	rsm.db = make(map[int]MapValue)
	go rsm.Reader()
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	// your code here

	rsm.mu.Lock()
	curId := rand.Uint32()
	safeReq := Op{Me: rsm.me, Id: int(curId), Req: req}
	cmdChan := make(chan any, 1)
	commitIndex, startTerm, isLeader := rsm.rf.Start(safeReq)

	if !isLeader {
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}

	//now wait for a response from reader chan
	rsm.db[commitIndex] = MapValue{Cmd: safeReq, ReaderSignal: cmdChan}
	//received command after applying
	rsm.mu.Unlock()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	//submit calls start so command is replicated to log and then waits for result to return to client
	for {
		select {
		case tempCmd := <-cmdChan:
			if tempCmd == ErrNotLeader {
				return rpc.ErrWrongLeader, nil
			}
			return rpc.OK, tempCmd
		//continuously check if you are no longer leader every 100 ms
		case <-ticker.C:
			//lock the channel
			select {
			case tempCmd := <-cmdChan:
				if tempCmd == ErrNotLeader {
					return rpc.ErrWrongLeader, nil
				}
				return rpc.OK, tempCmd
			default:
			}
			rsm.mu.Lock()
			newTerm, isLeader := rsm.rf.GetState()
			if newTerm != startTerm || !isLeader {
				//clean up the index for that entry since no longer leader
				delete(rsm.db, commitIndex)
				rsm.mu.Unlock()
				return rpc.ErrWrongLeader, nil
			}
			rsm.mu.Unlock()
		}
	}
	//signal that leader stepped down
	//return received command to the client
}

func (rsm *RSM) Reader() {
	//gets applychannel to apply to state machine and calls doop
	for {

		applyMsg, ok := <-rsm.applyCh
		if !ok {
			return
		}
		//now from here
		rsm.mu.Lock()

		op, ok := applyMsg.Command.(Op)

		if !ok {
			fmt.Println("type assertion failed, type is:", reflect.TypeOf(applyMsg.Command))
			rsm.mu.Unlock()
			continue
		}

		//now compare

		toSend := rsm.sm.DoOp(op.Req)
		//then send

		curValue, ok := rsm.db[applyMsg.CommandIndex]
		if ok {
			delete(rsm.db, applyMsg.CommandIndex)

			var tempSend any

			if op.Id != curValue.Cmd.Id {
				//send -1 if leader steps down
				tempSend = ErrNotLeader
			} else {
				tempSend = toSend
			}

			rsm.mu.Unlock()
			curValue.ReaderSignal <- tempSend
			continue
		}

		rsm.mu.Unlock()
	}

}
