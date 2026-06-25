package kvraft

import (
	"errors"
	/* "fmt" */
	"sync"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/tester1"
)

type KVServer struct {
	me  int
	rsm *rsm.RSM

	// Your definitions here.
	dedupTracker map[FilterKey]VersionErr
	kvStore      map[string]ValueVersion
	mu           sync.Mutex
}

type FilterKey struct {
	key      string
	clientId int64
}
type VersionErr struct {
	versionNo int
	Err       rpc.Err
}

type ValueVersion struct {
	value     string
	versionNo int
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()
	switch r := req.(type) {
	case rpc.PutArgs:
		//check if it exists in the deduplication map
		if dedupCheck, ok := kv.dedupTracker[FilterKey{key: r.Key, clientId: r.ClerkId}]; ok {
			if r.Version == rpc.Tversion(dedupCheck.versionNo) {
				//equivalent versions return cached version
				return rpc.PutReply{Err: dedupCheck.Err}
			}
		}

		//check if it is in the store, works for zero values since the map will send zero value if key doesnt exist
		if int(r.Version) == kv.kvStore[r.Key].versionNo {
			//store new put
			//in dedup tracker store version the client sees
			kv.dedupTracker[FilterKey{key: r.Key, clientId: r.ClerkId}] = VersionErr{versionNo: int(r.Version), Err: rpc.OK}
			kv.kvStore[r.Key] = ValueVersion{versionNo: int(r.Version + 1), value: r.Value}
			return rpc.PutReply{Err: rpc.OK}
		}

		/* if r.Version < rpc.Tversion(kv.kvStore[r.Key].versionNo) {
			kv.dedupTracker[FilterKey{key: r.Key, clientId: r.ClerkId}] = VersionErr{versionNo: int(r.Version), Err: rpc.OK}
			return rpc.PutReply{Err: rpc.OK}
		} */
		//cant store non zero version that does not exist
		return rpc.PutReply{Err: rpc.ErrVersion}
	case rpc.GetArgs:
		if val, ok := kv.kvStore[r.Key]; ok {
			return rpc.GetReply{Value: val.value, Version: rpc.Tversion(val.versionNo), Err: rpc.OK}
		}
		//fake key in the map
		return rpc.GetReply{Err: rpc.ErrNoKey}
	default:
		return errors.New("Doop args not recognized unfortunately")
	}
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	return nil
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)
	err, tempResp := kv.rsm.Submit(*args)
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}

	resp, ok := tempResp.(rpc.GetReply)
	if !ok {
		//early return for failed assertion
		reply.Err = rpc.ErrWrongLeader
		return
	}

	reply.Err = resp.Err
	reply.Value = resp.Value
	reply.Version = resp.Version
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)

	err, tempResp := kv.rsm.Submit(*args)

	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}

	resp, ok := tempResp.(rpc.PutReply)
	if !ok {
		//early return for failed assertion
		reply.Err = rpc.ErrWrongLeader
		return
	}

	reply.Err = resp.Err
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{me: me}

	kv.dedupTracker = make(map[FilterKey]VersionErr)
	kv.kvStore = make(map[string]ValueVersion)
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartKVServer(ends, Gid, srv, persister, tester.MaxRaftState)
}
