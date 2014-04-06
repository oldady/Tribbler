package tribserver

import (
	"errors"
	"fmt"
	"time"
	"strconv"
	"sort"
	"encoding/json"
	"github.com/cmu440/tribbler/rpc/tribrpc"
	"github.com/cmu440/tribbler/libstore"
)

type tribServer struct {
	// TODO: implement this!
	lib libstore.Libstore
	id int
}

// NewTribServer creates, starts and returns a new TribServer. masterServerHostPort
// is the master storage server's host:port and port is this port number on which
// the TribServer should listen. A non-nil error should be returned if the TribServer
// could not be started.
//
// For hints on how to properly setup RPC, see the rpc/tribrpc package.
func NewTribServer(masterServerHostPort, myHostPort string) (TribServer, error) {
	server := new(tribServer)
	lib, err := libstore.NewLibstore(masterServerHostPort, myHostPort, libstore.Normal)
	if err != nil {
		//fmt.Println("failed to connect!")
		//fmt.Println(err)
		return nil, err
	}

	server.lib = lib
	server.id = 0

	return server, nil
}

func (ts *tribServer) CreateUser(args *tribrpc.CreateUserArgs, reply *tribrpc.CreateUserReply) error {
	// generate user key
	UserKey := GenerateUserKey(args.UserID)

	// check if same user exists
	_, err := ts.lib.Get(UserKey)
	if err == nil {
		reply.Status = tribrpc.Exists
		return nil
	}

	// put username in the storage
	err = ts.lib.Put(UserKey, args.UserID)
	if err != nil {
		reply.Status = tribrpc.Exists
		return nil
	}



	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) AddSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	UserKey := GenerateUserKey(args.UserID)
	TargetUserKey := GenerateUserKey(args.TargetUserID)

	_, err := ts.lib.Get(UserKey)
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	_, err = ts.lib.Get(TargetUserKey)
	if err != nil {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}

	SubsKey := GenerateSubsKey(args.UserID)
	err = ts.lib.AppendToList(SubsKey, args.TargetUserID)
	if err != nil {
		reply.Status = tribrpc.Exists
		return nil
	}

	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) RemoveSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	UserKey := GenerateUserKey(args.UserID)
	TargetUserKey := GenerateUserKey(args.TargetUserID)

	_, err := ts.lib.Get(UserKey)
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	_, err = ts.lib.Get(TargetUserKey)
	if err != nil {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}

	SubsKey := GenerateSubsKey(args.UserID)
	err = ts.lib.RemoveFromList(SubsKey, args.TargetUserID)
	if err != nil {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}

	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) GetSubscriptions(args *tribrpc.GetSubscriptionsArgs, reply *tribrpc.GetSubscriptionsReply) error {
	UserKey := GenerateUserKey(args.UserID)

	_, err := ts.lib.Get(UserKey)
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		reply.UserIDs = nil
		return nil
	}

	SubsKey := GenerateSubsKey(args.UserID)
	SubsList, err := ts.lib.GetList(SubsKey)
	if err != nil {
		//return err
		return nil
	}

	reply.Status = tribrpc.OK
	reply.UserIDs = SubsList
	return nil
}

func (ts *tribServer) PostTribble(args *tribrpc.PostTribbleArgs, reply *tribrpc.PostTribbleReply) error {
	UserKey := GenerateUserKey(args.UserID)

	_, err := ts.lib.Get(UserKey)
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	TribListKey := GenerateTribListKey(args.UserID)

	err = ts.lib.AppendToList(TribListKey, strconv.Itoa(ts.id))
	if err != nil {
		reply.Status = tribrpc.Exists
		return nil
	}

	var trib tribrpc.Tribble
	trib.UserID = args.UserID
	trib.Posted = time.Now()
	trib.Contents = args.Contents

	TribIDKey := GenerateTribIDKey(args.UserID, strconv.Itoa(ts.id))
	val, _ := json.Marshal(trib)

	err = ts.lib.Put(TribIDKey, string(val))
	if err != nil {
		reply.Status = tribrpc.Exists
		return nil
	}

	reply.Status = tribrpc.OK
	ts.id++
	return nil
}

func (ts *tribServer) GetTribbles(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	UserKey := GenerateUserKey(args.UserID)

	_, err := ts.lib.Get(UserKey)
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		reply.Tribbles = nil
		return nil
	}

	TribListKey := GenerateTribListKey(args.UserID)
	TribIDs, err := ts.lib.GetList(TribListKey)
	if err != nil {
		//reply.Status = tribrpc.NoSuchUser
		//return nil
		reply.Status = tribrpc.OK
		return nil
	}

	var length int
	length = len(TribIDs)
	if length > 100 {
		length = 100
	}
	reply.Tribbles = make([]tribrpc.Tribble, length)

	for i := 0; i < length; i++ {
		TribIDKey := GenerateTribIDKey(args.UserID, TribIDs[len(TribIDs)-1-i])
		val, err := ts.lib.Get(TribIDKey)
		if err != nil {
			reply.Status = tribrpc.NoSuchUser
			return errors.New("get invalid tribble")
		}
		json.Unmarshal([]byte(val), &(reply.Tribbles[i]))
	}

	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) GetTribblesBySubscription(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	UserKey := GenerateUserKey(args.UserID)

	_, err := ts.lib.Get(UserKey)
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		reply.Tribbles = nil
		return nil
	}

	SubsKey := GenerateSubsKey(args.UserID)
	SubsList, err := ts.lib.GetList(SubsKey)
	if err != nil {
		//reply.Status = tribrpc.NoSuchUser
		reply.Status = tribrpc.OK
		reply.Tribbles = nil
		return nil
	}

	for i := 0; i < len(SubsList); i++ {
		var Subargs tribrpc.GetTribblesArgs
		var Subreply tribrpc.GetTribblesReply

		Subargs.UserID = SubsList[i]
		err = ts.GetTribbles(&Subargs, &Subreply)
		if err != nil {
			return err
		}

		reply.Tribbles = append(reply.Tribbles, Subreply.Tribbles...)
	}

	var tempTrib Tribs
	tempTrib = reply.Tribbles
	sort.Sort(tempTrib)
	if len(tempTrib) > 100 {
		reply.Status = tribrpc.OK
		reply.Tribbles = tempTrib[:100]
	} else {
		reply.Status = tribrpc.OK
		reply.Tribbles = tempTrib
	}

	return nil
}

type Tribs [](tribrpc.Tribble)

func (tribs Tribs) Len() int {
	return len(tribs)
}

func (tribs Tribs) Less(i,j int) bool {
	return tribs[i].Posted.After(tribs[j].Posted)
}

func (tribs Tribs) Swap(i,j int) {
	tribs[j], tribs[i] = tribs[i], tribs[j]
}

func GenerateUserKey(userID string) (userKey string) {
	userKey = fmt.Sprintf("%s:U", userID)
	return userKey
}

func GenerateSubsKey(UserID string) (SubsKey string) {
	SubsKey = fmt.Sprintf("%s:Subs", UserID)
	return SubsKey
}

func GenerateTribListKey(UserID string) (TribListKey string) {
	TribListKey = fmt.Sprintf("%s:Trib", UserID)
	return TribListKey
}

func GenerateTribIDKey(UserID string, id string) (TribIDKey string) {
	TribIDKey = fmt.Sprintf("%s:%s", UserID, id)
	return TribIDKey
}
