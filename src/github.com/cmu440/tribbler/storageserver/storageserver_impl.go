package storageserver

import (
	//"errors"
	"fmt"
	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"container/list"
	"math"
	"sync"
	"time"
	"strings"
	"net/http"
	"net/rpc"
	"net"
)

type LeaseRecord struct {
	HostPort string
	GrantedTime time.Time
}

type storageServer struct {
	port   int
	nodeID uint32

	//var for starting up the storage servers
	numNodes    int
	nodeMap     map[uint32]storagerpc.Node
	serverReady bool

	//functional var of storage servers
	nodeList    []storagerpc.Node
	keyValueMap map[string]string
	keyListMap  map[string]*list.List //key->list<string>

	//storage server
	leaseMap map[string]*list.List //key->list<Lease>//map key to list<Libstore's Hostport String, vaid time>

	//a lock for each key
	lockMap map[string]*sync.Mutex //lock map for keys in keyValueMap and keyListMap

	mutex *sync.Mutex //lock when adding to lockMap

	//keep HTTP client
	clientMap map[string]*rpc.Client
}

// NewStorageServer creates and starts a new StorageServer. masterServerHostPort
// is the master storage server's host:port address. If empty, then this server
// is the master; otherwise, this server is a slave. numNodes is the total number of
// servers in the ring. port is the port number that this server should listen on.
// nodeID is a random, unsigned 32-bit ID identifying this server.


func NewStorageServer(masterServerHostPort string, numNodes, port int, nodeID uint32) (StorageServer, error) {
	portStr := fmt.Sprintf("%d", port)
	portStr = "localhost:" + portStr

	//initial storage server struct
	newServer := new(storageServer)
	newServer.port = port
	newServer.nodeID = nodeID
	newServer.numNodes = numNodes
	newServer.nodeMap = make(map[uint32]storagerpc.Node)
	newServer.serverReady = false
	newServer.nodeList = make([]storagerpc.Node, numNodes)
	newServer.keyValueMap = make(map[string]string)
	newServer.keyListMap = make(map[string]*list.List)
	newServer.leaseMap = make(map[string]*list.List)
	newServer.lockMap = make(map[string]*sync.Mutex)
	newServer.mutex = new(sync.Mutex)
	newServer.clientMap = make(map[string]*rpc.Client)
	
	if len(masterServerHostPort) == 0 {
		//add itself to nodeMap
		newServer.nodeMap[nodeID] = storagerpc.Node{portStr, nodeID}
		
		//if only one noede, registerServer will not be call by other slaves
		if numNodes == 1 {
			newServer.nodeList[0] =  storagerpc.Node{portStr, nodeID}
		}
		
		//register to receive RPC from the other servers in the system
		rpc.RegisterName("MasterServer", newServer)  //for slavves
		rpc.RegisterName("StorageServer", newServer) //for libServer
		rpc.HandleHTTP()                             //can only call for one time!!!

		//listen for incoming connections
		ln, e := net.Listen("tcp", portStr)
		if e != nil {
			fmt.Println("listen error:", e)
		}
		go http.Serve(ln, nil)

		//wait until all slave join the consitent hashing ring
		for len(newServer.nodeMap) < numNodes {
			time.Sleep(time.Second)
		}

	} else { /****This is a slave server****/

		//connect to server using masterServerHostPort
		client, err := rpc.DialHTTP("tcp", masterServerHostPort)
		if err != nil {
			fmt.Println("Err dialing to master node:", err)
		}

		nodeInfo := new(storagerpc.Node)
		//nodeInfo.HostPort = "localhost" + portStr //already added "localhost"
		nodeInfo.HostPort = portStr
		nodeInfo.NodeID = nodeID
		args := &storagerpc.RegisterArgs{*nodeInfo}
		var reply storagerpc.RegisterReply

		//register to master in the consisten hashing ring
		err = client.Call("MasterServer.RegisterServer", args, &reply)
		if err != nil {
			fmt.Println("arith error:", err)
		}
		fmt.Println("Result:", reply)

		//sleep for second and resend request until startup complete
		for reply.Status != storagerpc.OK {
			time.Sleep(time.Second)
			err := client.Call("MasterServer.RegisterServer", args, &reply)
			if err != nil {
				fmt.Println("arith error:", err)
			}
			fmt.Println("Result:", reply)
		}

		//update nodeList in struct
		newServer.nodeList = reply.Servers

		//register to RPC
		rpc.RegisterName("StorageServer", newServer) //for libServer
		rpc.HandleHTTP()                             //can only call for one time!!!

		//listen for incoming connections
		ln, e := net.Listen("tcp", portStr)
		if e != nil {
			fmt.Println("listen error:", e)
		}
		go http.Serve(ln, nil)
	}

	newServer.serverReady = true
	go newServer.timer()
	return newServer, nil
}

func (ss *storageServer) RegisterServer(args *storagerpc.RegisterArgs, reply *storagerpc.RegisterReply) error {
	//Add to node map if it's a new slave
	_, exist := ss.nodeMap[args.ServerInfo.NodeID]
	if !exist {
		ss.nodeMap[args.ServerInfo.NodeID] = args.ServerInfo
	}

	//RPC reply
	if len(ss.nodeMap) < ss.numNodes {
		reply.Status = storagerpc.NotReady
	} else {
		i := 0
		for _, node := range ss.nodeMap {
			ss.nodeList[i] = node
			i = i + 1
		}

		reply.Status = storagerpc.OK
		reply.Servers = ss.nodeList
	}

	return nil
}

func (ss *storageServer) GetServers(args *storagerpc.GetServersArgs, reply *storagerpc.GetServersReply) error {
	if ss.serverReady == false {
		reply.Status = storagerpc.NotReady
		reply.Servers = nil
	} else {
		reply.Status = storagerpc.OK
		reply.Servers = ss.nodeList
	}

	return nil
}

func (ss *storageServer) Get(args *storagerpc.GetArgs, reply *storagerpc.GetReply) error {
	if !ss.isInServerRange(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	//get the lock for current key
	ss.mutex.Lock()
	lock, exist := ss.lockMap[args.Key]
	if !exist {
		lock = new(sync.Mutex)
		ss.lockMap[args.Key] = lock
	}
	ss.mutex.Unlock()

	//Lock current key we are going to work on
	lock.Lock()

	//process lease request
	grantLease := false
	if args.WantLease {
		//add to lease map
		var libServerList *list.List
		libServerList, exist := ss.leaseMap[args.Key]
		if !exist {
			libServerList = new(list.List)
		}
		leaseRecord := LeaseRecord{args.HostPort, time.Now()}
		libServerList.PushBack(leaseRecord)
		ss.leaseMap[args.Key] = libServerList
		grantLease = true
	}
	reply.Lease = storagerpc.Lease{grantLease, storagerpc.LeaseSeconds}

	//retrieve value
	value, exist := ss.keyValueMap[args.Key]
	if !exist {
		reply.Status = storagerpc.KeyNotFound
	} else {
		reply.Status = storagerpc.OK
		reply.Value = value
	}

	lock.Unlock()
	return nil
}

func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	if !ss.isInServerRange(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	//get the lock for current key
	ss.mutex.Lock()
	lock, exist := ss.lockMap[args.Key]
	if !exist {
		lock = new(sync.Mutex)
		ss.lockMap[args.Key] = lock
	}
	ss.mutex.Unlock()

	//Lock current key we are going to work on
	lock.Lock()

	//process lease request
	grantLease := false
	if args.WantLease {
		//add to lease map
		var libServerList *list.List
		libServerList, exist := ss.leaseMap[args.Key]
		if !exist {
			libServerList = new(list.List)
		}
		leaseRecord := LeaseRecord{args.HostPort, time.Now()}
		libServerList.PushBack(leaseRecord)
		ss.leaseMap[args.Key] = libServerList
		grantLease = true
	}
	reply.Lease = storagerpc.Lease{grantLease, storagerpc.LeaseSeconds}

	//retrieve list
	list, exist := ss.keyListMap[args.Key]
	if !exist {
		reply.Status = storagerpc.KeyNotFound
	} else {
		//convert list format from "map" to "[]string"
		listSize := list.Len()
		replyList := make([]string, listSize)

		i := 0
		for e := list.Front(); e != nil; e = e.Next() {
			val := (e.Value).(string)
			replyList[i] = val
			i = i + 1
		}

		reply.Value = replyList
		reply.Status = storagerpc.OK
	}

	lock.Unlock()
	return nil
}

func (ss *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if !ss.isInServerRange(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	//get the lock for current key
	ss.mutex.Lock()
	lock, exist := ss.lockMap[args.Key]
	if !exist {
		lock = new(sync.Mutex)
		ss.lockMap[args.Key] = lock
	}
	ss.mutex.Unlock()

	//Lock current key we are going to work on
	lock.Lock()

	//If has granted lease for current key, then revoke all the lease
	_, exist = ss.leaseMap[args.Key]
	if exist {
		//revoke all the granted Lease
		finishRevokeChan := make(chan struct{})
		singleRevokeChan := make(chan struct{})
		leaseHolderList := ss.leaseMap[args.Key]
		//revokeNum := leaseHolderList.Len()

		go ss.revokeHandler(args.Key, singleRevokeChan, finishRevokeChan)
		for e := leaseHolderList.Front(); e != nil; e = e.Next() {
			leaseHolder := (e.Value).(LeaseRecord)
			go ss.revokeLease(leaseHolder.HostPort, args.Key, singleRevokeChan)
		}
		//wait until all lease holders reply or leases have expired
		<-finishRevokeChan

		//delete the lease list for the key
		delete(ss.leaseMap, args.Key)
	}

	//modify value
	ss.keyValueMap[args.Key] = args.Value

	//resume granting lease for that key
	lock.Unlock()

	reply.Status = storagerpc.OK
	return nil
}

func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if !ss.isInServerRange(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	//get the lock for current key
	ss.mutex.Lock()
	lock, exist := ss.lockMap[args.Key]
	if !exist {
		lock = new(sync.Mutex)
		ss.lockMap[args.Key] = lock
	}
	ss.mutex.Unlock()

	//Lock current key we are going to work on
	lock.Lock()

	//If has granted lease for current key, then revoke all the lease
	_, exist = ss.leaseMap[args.Key]
	if exist {
		//revoke all the granted Lease
		finishRevokeChan := make(chan struct{})
		singleRevokeChan := make(chan struct{})
		leaseHolderList := ss.leaseMap[args.Key]
		//revokeNum := leaseHolderList.Len()

		go ss.revokeHandler(args.Key, singleRevokeChan, finishRevokeChan)
		for e := leaseHolderList.Front(); e != nil; e = e.Next() {
			leaseHolder := (e.Value).(LeaseRecord)
			go ss.revokeLease(leaseHolder.HostPort, args.Key, singleRevokeChan)
		}
		//wait until all lease holders reply or leases have expired
		<-finishRevokeChan

		//delete the lease list for the key
		delete(ss.leaseMap, args.Key)
	}

	//modify value
	valList, exist := ss.keyListMap[args.Key]
	if !exist { //key does not exist in the keyListMap yet
		valList = new(list.List)
		valList.PushBack(args.Value)
		ss.keyListMap[args.Key] = valList
		reply.Status = storagerpc.OK
	} else { //key exists in the keyListMap, so check whether item exists
		
		//_, exist = valList[args.Value]
		hasVal := false
		for e := valList.Front(); e != nil; e = e.Next() {
			val := (e.Value).(string)
			if val == args.Value {
				hasVal = true
				break
			}
		}
		if hasVal { //item already exist
			reply.Status = storagerpc.ItemExists
		} else {
			valList.PushBack(args.Value)
			reply.Status = storagerpc.OK
		}
	}

	//resume granting lease for that key
	lock.Unlock()
	return nil
}

func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if !ss.isInServerRange(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	//get the lock for current key
	ss.mutex.Lock()
	lock, exist := ss.lockMap[args.Key]
	if !exist {
		lock = new(sync.Mutex)
		ss.lockMap[args.Key] = lock
	}
	ss.mutex.Unlock()

	//Lock current key we are going to work on
	lock.Lock()

	//If has granted lease for current key, then revoke all the lease
	_, exist = ss.leaseMap[args.Key]
	if exist {
		//revoke all the granted Lease
		finishRevokeChan := make(chan struct{})
		singleRevokeChan := make(chan struct{})
		leaseHolderList := ss.leaseMap[args.Key]
		//revokeNum := leaseHolderList.Len()

		go ss.revokeHandler(args.Key, singleRevokeChan, finishRevokeChan)
		for e := leaseHolderList.Front(); e != nil; e = e.Next() {
			leaseHolder := (e.Value).(LeaseRecord)
			go ss.revokeLease(leaseHolder.HostPort, args.Key, singleRevokeChan)
		}
		//wait until all lease holders reply or leases have expired
		<-finishRevokeChan

		//delete the lease list for the key
		delete(ss.leaseMap, args.Key)
	}

	//modify value
	valList, exist := ss.keyListMap[args.Key]
	if !exist { //key does not exist in the keyListMap yet
		reply.Status = storagerpc.ItemNotFound
	} else { //key exists in the keyListMap, so check whether item exists
		
		//_, exist = valList[args.Value]
		hasVal := false
		for e := valList.Front(); e != nil; e = e.Next() {
			val := (e.Value).(string)
			if val == args.Value { //item already exist
				valList.Remove(e)
				reply.Status = storagerpc.OK
				hasVal = true
				break
			}
		}
		if !hasVal { 
			reply.Status = storagerpc.ItemNotFound
		}
	}

	//resume granting lease for that key
	lock.Unlock()
	return nil
}

func (ss *storageServer) timer() {
	expireTime := time.Duration(storagerpc.LeaseSeconds + storagerpc.LeaseGuardSeconds) * time.Second
	for {
		select{
		case <-time.After(500 * time.Millisecond):
			//ss.mutex.Lock()
			//remove if lease expire
			for _, leaseList := range ss.leaseMap {
				//remove expired lease
				var hasDeleted bool
				hasDeleted = true
				for hasDeleted && leaseList.Len() > 0 {
					hasDeleted = false
					for e := leaseList.Front(); e != nil; e = e.Next() {
						leaseRecord := (e.Value).(LeaseRecord)
						
						//remove lease if exceed expireTime
						if time.Since(leaseRecord.GrantedTime) > expireTime {
							leaseList.Remove(e)
							hasDeleted = true
						}
					}
				}
			}
			//ss.mutex.Unlock()
		}
	}
}

func (ss *storageServer) revokeHandler(key string, singleRevokeChan chan struct{}, finishRevokeChan chan struct{}) {
	count := 0
	for {
		select {
		case <-singleRevokeChan:
			count = count + 1
			leaseList := ss.leaseMap[key]
			if count == leaseList.Len() {
				finishRevokeChan <- struct{}{}
				return
			}
		case <-time.After(500 * time.Millisecond):
			leaseList := ss.leaseMap[key] 
			if count == leaseList.Len() {
				finishRevokeChan <- struct{}{}
				return
			}
		}
	}
}

func (ss *storageServer) revokeLease(hostPort string , key string, singleRevokeChan chan struct{}) {
	client, exist := ss.clientMap[hostPort]
	if !exist {
		cli, err := rpc.DialHTTP("tcp", hostPort)
		if err != nil {
			fmt.Println("revoke lease err")
			return
		}
		ss.clientMap[hostPort] = cli
	}
	client = ss.clientMap[hostPort]

	args := &storagerpc.RevokeLeaseArgs{key}
	var reply storagerpc.RevokeLeaseReply
	err := client.Call("LeaseCallbacks.RevokeLease", args, &reply)
	if err != nil {
		fmt.Println("revoke lease call err", err)
		return
	}

	singleRevokeChan <- struct{}{}
}

func (ss *storageServer) isInServerRange(key string) bool {
	//only consider the part before colon
	hashKey := strings.Split(key, ":")[0]

	//two helper variable
	var minNodeID uint32
	var targetNodeID uint32
	minNodeID = math.MaxUint32
	targetNodeID = math.MaxUint32

	hashVal := libstore.StoreHash(hashKey)
	for i := 0; i < len(ss.nodeList); i++ {
		nodeID := ss.nodeList[i].NodeID
		if nodeID >= hashVal && nodeID < targetNodeID {
			targetNodeID = nodeID
		}
		
		//find the min value in all nodeIDs
		if nodeID < minNodeID {
			minNodeID = nodeID
		}
	}

	//case : hash value lager than the largest nodeID in consistent hashing ring
	if targetNodeID == math.MaxUint32 {
		targetNodeID = minNodeID
	}

	if targetNodeID == ss.nodeID {
		return true
	} else {
		return false
	}
}
