package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu    sync.Mutex
	store map[string]VersionValue
	// Your definitions here.
}

type VersionValue struct {
	versionNo int
	value     string
}

func MakeKVServer() *KVServer {
	kv := &KVServer{store: make(map[string]VersionValue)}
	// Your code here.
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if val, ok := kv.store[args.Key]; ok {
		reply.Err = rpc.OK
		reply.Value = val.value
		reply.Version = rpc.Tversion(val.versionNo)
		return
	}

	reply.Err = rpc.ErrNoKey
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	//checking if the item is in the store first

	kv.mu.Lock()
	defer kv.mu.Unlock()
	if val, ok := kv.store[args.Key]; ok {
		//if wrong version number
		if val.versionNo != int(args.Version) {
			reply.Err = rpc.ErrVersion
			return
		}

		//increment version number
		//set key to new value
		cur := VersionValue{versionNo: int(args.Version) + 1, value: args.Value}
		kv.store[args.Key] = cur
	} else if args.Version == 0 {
		//new key situation

		cur := VersionValue{versionNo: int(args.Version) + 1, value: args.Value}
		kv.store[args.Key] = cur
	} else {
		reply.Err = rpc.ErrNoKey
		return
	}

	reply.Err = rpc.OK
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []any {
	kv := MakeKVServer()
	return []any{kv}
}
