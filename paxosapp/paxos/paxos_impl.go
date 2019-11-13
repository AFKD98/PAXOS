package paxos

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"paxosapp/rpc/paxosrpc"
	"strconv"
	"time"
)

var PROPOSE_TIMEOUT = 15 * time.Second

type paxosNode struct {
	// TODO: implement this!
	myId          int
	myPort        int
	numNodes      int
	listener      net.Listener
	incommingChan chan net.Conn
	ring          map[int]nodeInternal
	database      map[int]paxosrpc.ProposeArgs
}

type nodeInternal struct {
	// internal struct for other nodes
	ID   int
	conn *rpc.Client
}

// Desc:
// NewPaxosNode creates a new PaxosNode. This function should return only when
// all nodes have joined the ring, and should return a non-nil error if this node
// could not be started in spite of dialing any other nodes numRetries times.
//
// Params:
// myHostPort: the hostport string of this new node. We use tcp in this project.
//			   	Note: Please listen to this port rather than hostMap[srvId]
// hostMap: a map from all node IDs to their hostports.
//				Note: Please connect to hostMap[srvId] rather than myHostPort
//				when this node try to make rpc call to itself.
// numNodes: the number of nodes in the ring
// numRetries: if we can't connect with some nodes in hostMap after numRetries attempts, an error should be returned
// replace: a flag which indicates whether this node is a replacement for a node which failed.
func NewPaxosNode(myHostPort string, hostMap map[int]string, numNodes, srvId, numRetries int, replace bool) (PaxosNode, error) {
	node := new(paxosNode)
	node.incommingChan = make(chan net.Conn)
	node.myId = srvId
	node.numNodes = numNodes
	// fmt.Println(hostMap)
	intPort, _ := strconv.Atoi(myHostPort)
	node.myPort = intPort
	// strPort := ":" + myHostPort
	fmt.Println("ServerID:", srvId, "Listening on:", myHostPort)
	ln, err := net.Listen("tcp", myHostPort)
	if err != nil {
		fmt.Println("Error in listening on port:", myHostPort)
	}

	// listen on port
	rpcServer := rpc.NewServer()
	rpcServer.Register(paxosrpc.Wrap(node))

	http.DefaultServeMux = http.NewServeMux()
	rpcServer.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)

	// listen for incoming connections
	go http.Serve(ln, nil)

	// start to dial other ports
	for i := 0; i < numNodes; i++ {
		success := false
		port := hostMap[i]
		fmt.Println(port)
		client, err := rpc.DialHTTP("tcp", port)

		if err != nil {
			fmt.Println(err)
			fmt.Println("ServerID:", srvId, "failed to dail port:", port)
			for j := 0; j < numRetries-1; j++ {
				time.Sleep(1000 * time.Millisecond)
				client, err := rpc.DialHTTP("tcp", port)
				if err == nil {
					success = true
					node.ring[i] = nodeInternal{i, client}
					fmt.Println("ServerID:", srvId, "connected to port", port)
					break // we have connected
				} else {
					fmt.Println(err)
				}
			}
			if success == false { // failed to connect to a node
				reply := "ServerID: " + string(srvId) + " num retries Failed with port:" + port
				return nil, errors.New(reply)
			}
		} else {
			// append to our ring
			node.ring[i] = nodeInternal{i, client}
			fmt.Println("ServerID:", srvId, "connected to port", port)
		}
	}

	fmt.Println("ServerID:", srvId, "Connected to all nodes")
	return node, err

	//return nil, errors.New("not implemented")
}

// Desc:
// GetNextProposalNumber generates a proposal number which will be passed to
// Propose. Proposal numbers should not repeat for a key, and for a particular
// <node, key> pair, they should be strictly increasing.
//
// Params:
// args: the key to propose
// reply: the next proposal number for the given key
func (pn *paxosNode) GetNextProposalNumber(args *paxosrpc.ProposalNumberArgs, reply *paxosrpc.ProposalNumberReply) error {
	theKey := args.Key
	fmt.Println("ServerID:", pn.myId, "GetNextProposalNumber called for key:", theKey)
	theProposalNum := -1
	i := 0
	found := false
	// find last proposed value
	for i = 0; i < len(pn.database); i++ {
		if pn.database[i].Key == theKey {
			found = true
			break
		}
	}
	if found == true {
		// increament last proposal number and send it
		fmt.Println("ServerID:", pn.myId, "GetNextProposalNumber found:", pn.database[i].N, "sending:", pn.database[i].N+1)
		pn.database[i] = paxosrpc.ProposeArgs{N: pn.database[i].N + 1, Key: pn.database[i].Key, V: pn.database[i].V}
		theProposalNum = pn.database[i].N
		toReturn := paxosrpc.ProposalNumberReply{N: theProposalNum}
		*reply = toReturn
	} else {
		// this is a new key, so make an entry
		theProposalNum = 101 // just to check
		fmt.Println("ServerID:", pn.myId, "GetNextProposalNumber not found, sending:", theProposalNum)
		newEntry := paxosrpc.ProposeArgs{N: theProposalNum, Key: theKey, V: nil}
		pn.database[len(pn.database)] = newEntry
		toReturn := paxosrpc.ProposalNumberReply{N: theProposalNum}
		*reply = toReturn
	}
	return nil
}

// Desc:
// Propose initializes proposing a value for a key, and replies with the
// value that was committed for that key. Propose should not return until
// a value has been committed, or PROPOSE_TIMEOUT seconds have passed.
//
// Params:
// args: the key, value pair to propose together with the proposal number returned by GetNextProposalNumber
// reply: value that was actually committed for the given key
func (pn *paxosNode) Propose(args *paxosrpc.ProposeArgs, reply *paxosrpc.ProposeReply) error {
	theKey := args.Key
	theN := args.N
	theValue := args.V

	fmt.Println("ServerID:", pn.myId, "Proposal key:", theKey, "(N)", theN, "value:", theValue)
	// PHASE 1, Broadcast

	// make the prepare request
	prepareReq := paxosrpc.PrepareArgs{Key: theKey, N: theN, RequesterId: pn.myId}
	prepareResponse := make(map[int]paxosrpc.PrepareReply) // map to get the replies

	// send rpc calls to all nodes
	for i := 0; i < pn.numNodes; i++ {
		reply := prepareResponse[i]
		pn.ring[i].conn.Call("RecvPrepare", prepareReq, &reply)
	}

	//check replies
	// ------------------------- NEED TO WORK ON THIS --------------------------------
	return errors.New("not implemented")
}

// Desc:
// GetValue looks up the value for a key, and replies with the value or with
// the Status KeyNotFound.
//
// Params:
// args: the key to check
// reply: the value and status for this lookup of the given key
func (pn *paxosNode) GetValue(args *paxosrpc.GetValueArgs, reply *paxosrpc.GetValueReply) error {
	// look for key in database
	theKey := args.Key
	fmt.Println("ServerID:", pn.myId, "GetValue called for key:", theKey)
	i := 0
	found := false
	for i = 0; i < len(pn.database); i++ {
		if pn.database[i].Key == theKey {
			found = true
			break
		}
	}
	if found == true {
		// return answer
		fmt.Println("ServerID:", pn.myId, "GetValue found:", pn.database[i].V, "for key:", theKey)
		toReturn := paxosrpc.GetValueReply{V: pn.database[i].V, Status: paxosrpc.KeyFound}
		*reply = toReturn
	} else {
		// not found
		fmt.Println("ServerID:", pn.myId, "GetValue not found for key:", theKey)
		toReturn := paxosrpc.GetValueReply{V: nil, Status: paxosrpc.KeyNotFound}
		*reply = toReturn

	}
	return nil
}

// Desc:
// Receive a Prepare message from another Paxos Node. The message contains
// the key whose value is being proposed by the node sending the prepare
// message. This function should respond with Status OK if the prepare is
// accepted and Reject otherwise.
//
// Params:
// args: the Prepare Message, you must include RequesterId when you call this API
// reply: the Prepare Reply Message
func (pn *paxosNode) RecvPrepare(args *paxosrpc.PrepareArgs, reply *paxosrpc.PrepareReply) error {
	return errors.New("not implemented")
}

// Desc:
// Receive an Accept message from another Paxos Node. The message contains
// the key whose value is being proposed by the node sending the accept
// message. This function should respond with Status OK if the prepare is
// accepted and Reject otherwise.
//
// Params:
// args: the Please Accept Message, you must include RequesterId when you call this API
// reply: the Accept Reply Message
func (pn *paxosNode) RecvAccept(args *paxosrpc.AcceptArgs, reply *paxosrpc.AcceptReply) error {
	return errors.New("not implemented")
}

// Desc:
// Receive a Commit message from another Paxos Node. The message contains
// the key whose value was proposed by the node sending the commit
// message.
//
// Params:
// args: the Commit Message, you must include RequesterId when you call this API
// reply: the Commit Reply Message
func (pn *paxosNode) RecvCommit(args *paxosrpc.CommitArgs, reply *paxosrpc.CommitReply) error {
	return errors.New("not implemented")
}

// Desc:
// Notify another node of a replacement server which has started up. The
// message contains the Server ID of the node being replaced, and the
// hostport of the replacement node
//
// Params:
// args: the id and the hostport of the server being replaced
// reply: no use
func (pn *paxosNode) RecvReplaceServer(args *paxosrpc.ReplaceServerArgs, reply *paxosrpc.ReplaceServerReply) error {
	return errors.New("not implemented")
}

// Desc:
// Request the value that was agreed upon for a particular round. A node
// receiving this message should reply with the data (as an array of bytes)
// needed to make the replacement server aware of the keys and values
// committed so far.
//
// Params:
// args: no use
// reply: a byte array containing necessary data used by replacement server to recover
func (pn *paxosNode) RecvReplaceCatchup(args *paxosrpc.ReplaceCatchupArgs, reply *paxosrpc.ReplaceCatchupReply) error {
	return errors.New("not implemented")
}
