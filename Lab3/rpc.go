package main

import (
	"errors"
	"fmt"
	"log"
	"net/rpc"
	"os"
)

// remember to capitalize all names.

type Args struct {
	ID string
	AddressDial string
	IDToFind string
	Found bool
	FoundNodeIP string
	NodeIPNotify NodeIP
	FileData []byte
	FileName string
}

type Reply struct {
	Found bool
	FoundNodeIP NodeIP
}

func (node *Node) call(rpcname string, args *Args, reply *Reply) bool { //not sure - interface{}
	client, err := rpc.DialHTTP("tcp", args.AddressDial)
	if err != nil {
		log.Fatal("Client connection error: ", err)
		return false // not sure if needed
	}
	defer client.Close()

	err = client.Call(rpcname, &args, &reply) // not sure
	if err == nil {
		return true
	}

	return false
}

func (node *Node) RPCFind(args *Args, reply *Reply) error{
	nextNode := NodeIP{ID: node.ID, Address: node.Address}
	found := false
	for i := 0; i < 160 && !found; i++ {
		// next line is weird
		if nextNode.Address == "" {
			continue
		}

		args.AddressDial =  nextNode.Address
		ok := node.call("Node.RPCFindSuccessor", args, reply)
		if !ok {
			// return errors.New("Error when calling RPCFindSuccessor!")
			return nil
		}
		found = reply.Found
		nextNode = reply.FoundNodeIP
	}
	if found {
		// reply.Found = true
		// reply.FoundNodeIP = nextNode
		return nil
	}
	// reply.Found = false
	// reply.FoundNodeIP = NodeIP{}
	// return errors.New("Successor not found!")
	return nil
}

func (node *Node) RPCFindSuccessor(args *Args, reply *Reply) error{
	// this function has no error returned now
	if node.ID == node.Successors[0].ID {
		reply.Found = true
		reply.FoundNodeIP = node.Successors[0]
		return nil // If we didn't return here, we would create an infinite loop
	}
	if args.IDToFind > node.ID && args.IDToFind <= node.Successors[0].ID {
		reply.Found = true
		reply.FoundNodeIP = node.Successors[0]
		return nil
	}
	reply.Found = false
	reply.FoundNodeIP = node.closestPrecedingNode(args.IDToFind)
	return nil
}

func (node *Node) RPCNotify(args *Args, reply *Reply) error {
	currentNode := args.NodeIPNotify
	if currentNode.ID == node.ID { // A node can't be it's own predecessor
		return nil
	}
	fmt.Println("predecessor before: " + node.Predecessor.ID)
	if node.Predecessor.ID == "" || (currentNode.ID > node.Predecessor.ID && currentNode.ID < node.ID) || (currentNode.ID < node.Predecessor.ID && currentNode.ID > node.ID) {
		node.Predecessor = currentNode
	}
	fmt.Println("predecessor after: " + node.Predecessor.ID)
	return nil
}

func (node *Node) RPCFindPredecessor(args *Args, reply *Reply) error {
	predecessor := node.Predecessor
	if predecessor.ID != "" {
		reply.Found = true
		reply.FoundNodeIP = predecessor
	}else{
		reply.Found = false
	}
	return nil
}

func (node *Node) RPCStoreFile(args *Args, reply *Reply) error {
	err := os.WriteFile(args.FileName, args.FileData, 0644)
	if err != nil {
		fmt.Println("Error when writing the file", err)
		return errors.New("Error when writing the file")
	}
	key := createIdentifier(args.FileName)
	node.Bucket[key] = FileName(args.FileName)
	return nil
}