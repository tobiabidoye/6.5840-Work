package lock

import (
	"log"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

const (
	acquired = "acquired"
	released = "released"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck      kvtest.IKVClerk
	curKey  string
	version int
	// You may add code here
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{ck: ck, curKey: lockname}
	// You may add code here
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here

	//get version number
	var value string
	var version rpc.Tversion
	var err rpc.Err

	for {
		value, version, err = lk.ck.Get(lk.curKey)
		//poll until released
		if err == rpc.ErrNoKey || value == released {
			err = lk.ck.Put(lk.curKey, acquired, version)
			if err == rpc.OK {
				//if not then continue
				lk.version = int(version) + 1
				break
			}
		}
	}

}

func (lk *Lock) Release() {
	// Your code here

	err := lk.ck.Put(lk.curKey, released, rpc.Tversion(lk.version))

	if err != rpc.OK {
		log.Print("false release of key not allowed")
		return
	}
}
