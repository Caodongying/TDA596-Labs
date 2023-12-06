package main

import (
	"crypto/sha1"
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"strconv"
	"strings"
	"time"
)

type Key string

type NodeAddress string

type FileName string

type Node struct {
	ID string
	Address     NodeAddress
	FingerTable [40]NodeIP
	Predecessor NodeAddress
	Successors  []string

	Bucket map[Key]FileName
}

// Get the IP address via node id
type NodeIP struct {
	NodeID string
	Address NodeAddress
}

func main() {
	// parse the command
	ipAddressClient := flag.String("a", "127.0.0.1", "chord client's IP address")
	portClient := flag.Int("p", 8080, "chord client's port number")
	ipAddressChord := flag.String("ja", "", "IP address of machine running a chord node") // improve later
	portChord := flag.Int("jp", -1, "Port number")                                        // improve later
	ts := flag.Int("ts", 30000, "time in milliseconds between invocations of ‘stabilize’")
	tff := flag.Int("tff", 10000, "time in milliseconds between invocations of ‘fix fingers’")
	tcp := flag.Int("tcp", 30000, "time in milliseconds between invocations of ‘check predecessor’")
	r := flag.Int("r", 4, "number of successors maintained by the Chord client")
	id := flag.String("i", "", "customized chordnidentifier")

	flag.Parse()

	// TBA - validate the parameters
	// crash if only ipAddressChord or portChord is given in command line
	if (*ipAddressChord == "" && *portChord == -1) && (*ipAddressChord != "" && *portChord != -1) {
		fmt.Println("Please use either both -ja and -jp, or neither of them")
		return
	}

	// make sure that the given chord node is not the same as the client node
	// todo 

	// Instantiate the node
	node := Node{
		Address:    NodeAddress(*ipAddressClient + ":" + strconv.Itoa(*portClient)),
		Successors: make([]string, *r),
	}

	// Check if there's an customized id or not
	if *id != "" {
		node.ID = *id
	} else{
		node.ID = string(createIdentifier([]byte(*ipAddressClient)) + createIdentifier([]byte(strconv.Itoa(*portClient))))
	}

	// Check to join or to create a new chord ring
	// IMPROVE HERE
	if *ipAddressChord == "" && *portChord == -1 {
		// starts a new ring
		node.createRing()
	} else if *ipAddressChord != "" && *portChord != -1 {
		// joins an existing ring
		node.joinRing(*ipAddressChord, *portChord)
	}

	// Create a goroutin to start the three timers
	go node.setTimers(*ts, *tff, *tcp)

	// open a TCP socket
	listener, err := net.Listen("tcp", *ipAddressClient+":"+strconv.Itoa(*portClient))
	if err != nil {
		fmt.Println("Error when listening to ip:port", err)
		return
	}
	defer listener.Close()

	for {
		// Accept incoming connections
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error when accepting connections", err)
			continue // not sure if it's should be return; when should this loop terminate?
		}

		go handleConnection(conn, node)
	}

	// remove these later
	fmt.Println("ipAddressClient", *ipAddressClient)
	fmt.Println("portClient", *portClient)
	fmt.Println("ipAddressChord", *ipAddressChord)
	fmt.Println("portChord", *portChord)
	fmt.Println("ts", *ts)
	fmt.Println("tff", *tff)
	fmt.Println("tcp", *tcp)
	fmt.Println("r", *r)
	fmt.Println("id", *id)
}

func (node *Node) setTimers(ts int, tff int, tcp int) {
	for {
		timerStablize := time.NewTimer(time.Duration(ts) * time.Millisecond)
		<-timerStablize.C
		go node.stablize()

		timerFixFinger := time.NewTimer(time.Duration(tff) * time.Millisecond)
		<-timerFixFinger.C
		go node.fixFinger()

		timerCheckPredecessor := time.NewTimer(time.Duration(tcp) * time.Millisecond)
		<-timerCheckPredecessor.C
		go node.checkPredecessor()
	}
}

func (node *Node) stablize(){
	fmt.Println("stablize")
}

func (node *Node) fixFinger(){
	fmt.Println("fixFinger")
}

func (node *Node) checkPredecessor(){
	fmt.Println("checkPredecessor")
}


func (node *Node) createRing() {
	// initialize the successor list and finger table
	node.Successors[0] = node.ID

	for index := range node.FingerTable {
		node.FingerTable[index] = NodeIP{
			NodeID: node.ID,
			Address: node.Address,
		}
	}
}

func (node *Node) findSuccessor(id string) string {
	// if slices.Contains(node.Successors, address) {
	// 	return address
	// }
	
	// go through all intervals
	// check if the node id is within that range
	for i, successor := range node.Successors {
		if id == successor {
			return successor
		}

		if i == len(node.Successors) - 1 {
			nextNodeAddress := node.closestPrecedingNode(id)
			return requestToFindSuccessor(id, nextNodeAddress)
		}

		if successor < id && id < node.Successors[i+1]{
			return node.Successors[i+1]
		}
	}

	nextNodeAddress := node.closestPrecedingNode(id)
	return requestToFindSuccessor(id, nextNodeAddress)
}

func (node *Node) closestPrecedingNode(id string) NodeAddress {
	for i := 40 ; i >= 0 ; i-- {
		if (node.FingerTable[i].NodeID > node.ID && node.FingerTable[i].NodeID < id) {
			return node.FingerTable[i].Address
		}
	}
	return node.Address
}

func requestToFindSuccessor(nodeID string, ipAddressChord NodeAddress) string{
	// nodeID: the node that wants to join the ring
	// ipAddressChord: the node that is already on the ring. We want to communicate to this ring and join the ring via this node
	// This function returns the successor (node id)
	// find the node responsible for string
	conn, err := net.Dial("tcp", string(ipAddressChord))
	if err != nil {
		fmt.Println("Error when dialing the chord node", err)
		return ""
	}
	defer conn.Close()

	// write to the connection: findSuccessor
	_, writeErr := conn.Write([]byte("findSuccessor-" + nodeID))
	if writeErr != nil {
		fmt.Println("Error when sending request to the chord node", err)
		return ""
	}

	// receive the successor
	buf, readErr := ioutil.ReadAll(conn)
	if readErr != nil {
		fmt.Println("Error when receiving successor from the chord node", readErr)
		return ""
	}
	return string(buf)
}

func (node *Node) joinRing(ipChord string, portChord int) {
	successor := requestToFindSuccessor(node.ID, NodeAddress(ipChord+":"+strconv.Itoa(portChord)))
	if successor == "" {
		return
	}
	node.Successors[0] = successor
}

func handleConnection(conn net.Conn, node Node) {
	defer conn.Close()

	// Read the incoming request
	buf, readErr := ioutil.ReadAll(conn)
	if readErr != nil {
		fmt.Println("Error when reading request from other node", readErr)
		return
	}
	request := string(buf)

	requestSplit := strings.Split(request, "-")
	switch requestSplit[0] {
	case "findSuccessor":
		successor := node.findSuccessor(requestSplit[1])
		// send successor back to the node
		_, writeErr := conn.Write([]byte(successor))
		if writeErr != nil {
			fmt.Println("Error when sending request to the chord node", writeErr)
			return
		}
	}
}

func createIdentifier(name []byte) NodeAddress{
	// create a 20-character hash key for the name
	h := sha1.New()
	return NodeAddress(h.Sum(name))
}
