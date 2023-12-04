package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"slices"
	"strconv"
	"strings"
)

type Key string

type NodeAddress string

type FileName string

type Node struct {
	Address     NodeAddress
	FingerTable [40]NodeAddress
	Predecessor NodeAddress
	Successors  []NodeAddress

	Bucket map[Key]FileName
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
	id := flag.String("i", "default id of chord client", "customized chordnidentifier")

	flag.Parse()

	// TBA - validate the parameters
	// crash if only ipAddressChord or portChord is given in command line
	if (*ipAddressChord == "" && *portChord == -1) && (*ipAddressChord != "" && *portChord != -1) {
		fmt.Println("Please use either both -ja and -jp, or neither of them")
		return
	}

	// Instantiate the node
	node := Node{
		Address:    NodeAddress(*ipAddressClient + ":" + strconv.Itoa(*portClient)),
		Successors: make([]NodeAddress, *r),
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
			return
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

func (node *Node) createRing() {
	// initialize the successor list and finger table
	node.Successors[0] = node.Address

	for index := range node.FingerTable {
		node.FingerTable[index] = node.Address
	}
}

func (node *Node) findSuccessor(address NodeAddress) NodeAddress {
	if slices.Contains(node.Successors, address) {
		return address
	}
	return node.closestPrecedingNode(address)
}

func (node *Node) closestPrecedingNode(id NodeAddress) NodeAddress {

}

func (node *Node) joinRing(ipChord string, portChord int) {
	// find the node responsible for string
	conn, err := net.Dial("tcp", ipChord+":"+strconv.Itoa(portChord))
	if err != nil {
		fmt.Println("Error when dialing the chord node", err)
		return
	}
	defer conn.Close()

	// write to the connection: findSuccessor
	_, writeErr := conn.Write([]byte("findSuccessor-" + node.Address))
	if writeErr != nil {
		fmt.Println("Error when sending request to the chord node", err)
		return
	}

	// receive the successor
	buf, readErr := ioutil.ReadAll(conn)
	if readErr != nil {
		fmt.Println("Error when receiving successor from the chord node", readErr)
		return
	}
	node.Successors[0] = NodeAddress(buf)
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
		successor := node.findSuccessor(NodeAddress(requestSplit[1]))
		// send successor back to the node
		_, writeErr := conn.Write([]byte(NodeAddress(successor)))
		if writeErr != nil {
			fmt.Println("Error when sending request to the chord node", err)
			return
		}
	}
}
