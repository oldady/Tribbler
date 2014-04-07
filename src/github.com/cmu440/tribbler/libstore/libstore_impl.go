package libstore

import (
	//"log"
	"errors"
	"net/rpc"
	"strings"
	"fmt"
	"time"
	"github.com/cmu440/tribbler/cache"
	"github.com/cmu440/tribbler/rpc/librpc"
	"github.com/cmu440/tribbler/rpc/storagerpc"
)

type libstore struct {
	// TODO: implement this!
	hostport string
	mode LeaseMode
	nodes []storagerpc.Node
	rpcClient []*rpc.Client
	leases *cache.Cache
}

// NewLibstore creates a new instance of a TribServer's libstore. masterServerHostPort
// is the master storage server's host:port. myHostPort is this Libstore's host:port
// (i.e. the callback address that the storage servers should use to send back
// notifications when leases are revoked).
//
// The mode argument is a debugging flag that determines how the Libstore should
// request/handle leases. If mode is Never, then the Libstore should never request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to false). If mode is Always, then the Libstore should always request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to true). If mode is Normal, then the Libstore should make its own
// decisions on whether or not a lease should be requested from the storage server,
// based on the requirements specified in the project PDF handout.  Note that the
// value of the mode flag may also determine whether or not the Libstore should
// register to receive RPCs from the storage servers.
//
// To register the Libstore to receive RPCs from the storage servers, the following
// line of code should suffice:
//
//     rpc.RegisterName("LeaseCallbacks", librpc.Wrap(libstore))
//
// Note that unlike in the NewTribServer and NewStorageServer functions, there is no
// need to create a brand new HTTP handler to serve the requests (the Libstore may
// simply reuse the TribServer's HTTP handler since the two run in the same process).
func NewLibstore(masterServerHostPort, myHostPort string, mode LeaseMode) (Libstore, error) {

	//fmt.Println("begin connection")
	cli, err := rpc.DialHTTP("tcp", masterServerHostPort)
	if err != nil {
		return nil, err
	}
	defer cli.Close()

	var args storagerpc.GetServersArgs
	var reply storagerpc.GetServersReply

	for i := 0; i < 5; i++ {
		err = cli.Call("StorageServer.GetServers", &args, &reply)
		if err != nil {
			fmt.Println("no such methods")
			return nil, err
		}
		if reply.Status == storagerpc.OK {
			//fmt.Println("connection succeed!")
			break
		}
		time.Sleep(1000 * time.Millisecond)
	}

	if reply.Status == storagerpc.NotReady || reply.Servers == nil {
		return nil, errors.New("storage server haven't ready")
	}

	lib := new(libstore)
	lib.hostport = myHostPort
	lib.mode = mode
	lib.nodes = reply.Servers
	lib.rpcClient = make([]*rpc.Client, len(lib.nodes))
	lib.leases = cache.NewCache()

	// DO WEEE NEED TO SORT???
	//fmt.Println("connection succeed!")
	rpc.RegisterName("LeaseCallbacks", librpc.Wrap(lib))

	go CleanCache(lib.leases)
	return lib, nil
}

func CleanCache(leases *cache.Cache) {
	d := time.Duration(storagerpc.LeaseGuardSeconds) * time.Second

	for {
		//leases.mu.Lock()
		leases.Clear()
		//leases.mu.Unlock()
		time.Sleep(d)
	}
}

func (ls *libstore) Get(key string) (string, error) {
	var args storagerpc.GetArgs
	var reply storagerpc.GetReply

	//set wantlease to be false
	args.Key = key
	args.WantLease = false
	args.HostPort = ls.hostport

	// check if it is in cache
	val, err := ls.leases.Get(key, &args)
	if err == nil {
		return val.(string), nil
	}

	//CHECK IF WE NEED LEASE
	if ls.mode == Always {
		args.WantLease = true
	} else if ls.mode == Never {
		args.WantLease = false
	}

	// pick the right server
	cli, err := ls.GetServer(key)
	if err != nil {
		return "", err
	}

	// call rpc
	err = cli.Call("StorageServer.Get", &args, &reply)
	if err != nil {
		return "", err
	}

	if reply.Status != storagerpc.OK {
		err = GetErrorType(reply.Status)
		return "", err
	}

	// insert into cache
	if reply.Lease.Granted {
		ls.leases.Insert(key, reply.Value, reply.Lease)
	} 

	return reply.Value, nil
}

func (ls *libstore) Put(key, value string) error {
	// Get the right server
	cli, err := ls.GetServer(key)
	if err != nil {
		return err
	}

	var args storagerpc.PutArgs
	var reply storagerpc.PutReply

	args.Key = key
	args.Value = value

	// call rpc put to storage server
	err = cli.Call("StorageServer.Put", &args, &reply)
	if err != nil {
		return err
	}

	if reply.Status != storagerpc.OK {
		err = GetErrorType(reply.Status)
		return err
	}

	return nil
}

func (ls *libstore) GetList(key string) ([]string, error) {
	var args storagerpc.GetArgs
	var reply storagerpc.GetListReply

	args.Key = key
	args.WantLease = false
	args.HostPort = ls.hostport

	// FIRST CHECK FROM LOCAL CACHE!!!
	val, err := ls.leases.Get(key, &args)
	if err == nil {
		return val.([]string), nil
	}

	// CHECK IF WE NEED LEASE
	if ls.mode == Always {
		args.WantLease = true
	} else if ls.mode == Never {
		args.WantLease = false
	}

	// pick the right server
	cli, err := ls.GetServer(key)
	if err != nil {
		return nil, err
	}

	// call rpc to storage server
	err = cli.Call("StorageServer.GetList", &args, &reply)
	if err != nil {
		return nil, err
	}

	// check reply status
	if reply.Status != storagerpc.OK {
		err = GetErrorType(reply.Status)
		return nil, err
	}

	// check if allow lease
	if reply.Lease.Granted {
		ls.leases.Insert(key, reply.Value, reply.Lease)
	}

	return reply.Value, nil
}

func (ls *libstore) RemoveFromList(key, removeItem string) error {
	var args storagerpc.PutArgs
	var reply storagerpc.PutReply

	args.Key = key
	args.Value = removeItem

	// pick the right server
	cli, err := ls.GetServer(key)
	if err != nil {
		return err
	}

	// call rpc to storage server
	err = cli.Call("StorageServer.RemoveFromList", &args, &reply)
	if err != nil {
		return err
	}

	// check the reply status
	if reply.Status != storagerpc.OK {
		err = GetErrorType(reply.Status)
		return err
	}

	return nil
}

func (ls *libstore) AppendToList(key, newItem string) error {
	var args storagerpc.PutArgs
	var reply storagerpc.PutReply

	args.Key = key
	args.Value = newItem

	// pick the right server
	cli, err := ls.GetServer(key)
	if err != nil {
		return err
	}

	// call rpc to storage server
	err = cli.Call("StorageServer.AppendToList", &args, &reply)
	if err != nil {
		return err
	}

	// check the reply status
	if reply.Status != storagerpc.OK {
		err = GetErrorType(reply.Status)
		return err
	}

	return nil
}

func (ls *libstore) RevokeLease(args *storagerpc.RevokeLeaseArgs, reply *storagerpc.RevokeLeaseReply) error {
	//fmt.Println("Revoke Happened!")
	ok := ls.leases.Revoke(args.Key)

	if ok == true {
		reply.Status = storagerpc.OK
	} else {
		reply.Status = storagerpc.KeyNotFound
	}
	return nil
}

func (ls *libstore) GetServer(key string) (*rpc.Client, error) {
	UserID := strings.Split(key, ":")[0]
	hashValue := StoreHash(UserID)

	var srvid int
	srvid = 0

	for i := 0; i < len(ls.nodes); i++ {
		if ls.nodes[i].NodeID >= hashValue {
			srvid = i
			break
		}
	}

	//fmt.Println("srvid is:", srvid)
	if ls.rpcClient[srvid] == nil {
		cli, err := rpc.DialHTTP("tcp", ls.nodes[srvid].HostPort)
		if err != nil {
			return nil, err
		}
		ls.rpcClient[srvid] = cli
	}
	return ls.rpcClient[srvid], nil
}

func GetErrorType(status storagerpc.Status) error {
	var error error
	switch status {
	case storagerpc.KeyNotFound:
		error = errors.New("KeyNotFound error")
	case storagerpc.ItemNotFound:
		error = errors.New("ItemNotFound error")
	case storagerpc.WrongServer:
		error = errors.New("WrongServer error")
	case storagerpc.ItemExists:
		error = errors.New("ItemExists error")
	case storagerpc.NotReady:
		error = errors.New("NotReady error")
	}

	return error
}
