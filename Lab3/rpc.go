package main

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/rpc"
	"os"
)

// remember to capitalize all names.

type Args struct {
	AddressDial  string
	IDToFind     string
	NodeIPNotify NodeIP
	FileData     []byte
	FileName     string
}

type Reply struct {
	Found        bool
	FoundNodeIPs []NodeIP
}

func (node *Node) call(rpcname string, args *Args, reply *Reply) bool {
	for attempts := 3; attempts > 0; attempts-- {
		client, err := rpc.DialHTTP("tcp", args.AddressDial)
		if err != nil {
			fmt.Println("Client connection error: ", err)
			continue
		}
		err = client.Call(rpcname, &args, &reply)
		if err == nil {
			return true
		}
		fmt.Println("Error when calling client:", err)
	}
	return false
}

func (node *Node) RPCFind(args *Args, reply *Reply) error {
	nextNode := NodeIP{ID: node.ID, Address: node.Address}
	found := false
	for i := 0; i < 160 && !found; i++ {
		// next line is weird
		if nextNode.Address == "" {
			continue
		}

		args.AddressDial = nextNode.Address
		ok := node.call("Node.RPCFindSuccessor", args, reply)
		if !ok {
			// return errors.New("Error when calling RPCFindSuccessor!")
			return nil
		}
		found = reply.Found
		nextNode = reply.FoundNodeIPs[0] // no need to first check if reply.FoundNodeIPs is empty, as there is always a non-empty returned
	}
	if found {
		reply.Found = true
		reply.FoundNodeIPs = []NodeIP{nextNode}
		return nil
	}

	// return errors.New("Successor not found!")
	return nil
}

func (node *Node) RPCFindSuccessor(args *Args, reply *Reply) error {
	// this function has no error returned now
	// three cases where we say successor is found
	if node.ID == node.Successors[0].ID {
		reply.Found = true
		reply.FoundNodeIPs = []NodeIP{node.Successors[0]}
		return nil // If we didn't return here, we would create an infinite loop
	}
	if node.ID < args.IDToFind { // todo: logic here is correct, but do we probably want to rewrite it as our purpose is check if the request node is in between the current node and its first successor on the ring
		if args.IDToFind <= node.Successors[0].ID || node.Successors[0].ID < node.ID {
			reply.Found = true
			reply.FoundNodeIPs = []NodeIP{node.Successors[0]}
			return nil
		}
	}
	if args.IDToFind < node.Successors[0].ID && node.Successors[0].ID < node.ID {
		reply.Found = true
		reply.FoundNodeIPs = []NodeIP{node.Successors[0]}
		return nil
	}

	reply.Found = false
	reply.FoundNodeIPs = []NodeIP{node.closestPrecedingNode(args.IDToFind)}
	return nil
}

func (node *Node) RPCNotify(args *Args, reply *Reply) error {
	// Node sends notification to its successor to update the successor's predecessor
	notifySender := args.NodeIPNotify // This is the node that sends the notification
	if notifySender.ID == node.ID {   // A node can't be it's own predecessor
		return nil
	}
	if node.Predecessor.ID == "" || (notifySender.ID > node.Predecessor.ID && notifySender.ID < node.ID) || (notifySender.ID < node.Predecessor.ID && notifySender.ID > node.ID) {
		node.Predecessor = notifySender
		currentPath, _ := os.Getwd()
		changeLocation := false
		for key, filename := range node.Bucket {
			if node.Predecessor.ID < node.ID {
				if key < node.Predecessor.ID || key > node.ID {
					changeLocation = true
				}
			} else {
				if key < node.Predecessor.ID && key > node.ID {
					changeLocation = true
				}
			}
			if changeLocation {
				fmt.Println("change file location")
				//fmt.Println(currentPath + "\\" + node.ID + "\\" + string(filename))
				//node.storeFile(currentPath + "\\" + node.ID + "\\" + string(filename))
				fileData, err := ioutil.ReadFile(currentPath + "\\" + node.ID + "\\" + string(filename))
				if err != nil {
					fmt.Println("Error when opening the file!")
					changeLocation = false
					return nil
				}

				args.AddressDial = node.Predecessor.Address
				args.FileData = fileData
				args.FileName = string(filename)
				ok := node.call("Node.RPCStoreFile", args, reply)
				if !ok {
					fmt.Println("Error when relocating file.")
					changeLocation = false
					return nil
				}

				delete(node.Bucket, key)
				err = os.Remove(node.ID + "\\" + string(filename))
				if err != nil {
					fmt.Println("Failed to delete the file.")
				}
				changeLocation = false
			}
		}
	}
	return nil
}

func (node *Node) RPCFindPredecessor(args *Args, reply *Reply) error {
	predecessor := node.Predecessor
	if predecessor.ID != "" {
		reply.Found = true
		reply.FoundNodeIPs = []NodeIP{predecessor}
	} else {
		reply.Found = false
	}
	return nil
}

func (node *Node) RPCFindAllSuccessors(args *Args, reply *Reply) error { // todo: maybe we want to name it as RPCFindAllSuccessors
	reply.Found = true
	reply.FoundNodeIPs = node.Successors
	return nil
}

func (node *Node) RPCStoreFile(args *Args, reply *Reply) error {
	//currentPath, _ := os.Getwd()
	err := os.WriteFile(node.ID+"\\"+args.FileName, args.FileData, 0644)
	if err != nil {
		fmt.Println("Error when writing the file", err)
		return errors.New("Error when writing the file")
	}
	key := createIdentifier(args.FileName)
	node.Bucket[key] = FileName(args.FileName)
	return nil
}
