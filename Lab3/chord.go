package main

import (
	"crypto/sha1"
	"flag"
	"fmt"
	"net"
	"strconv"
)

type Key string

type NodeAddress string

type Node struct {
    Address     NodeAddress
    FingerTable []NodeAddress
    Predecessor NodeAddress
    Successors  []NodeAddress

    Bucket map[Key]string
}

func main() {
	// parse the command
	ipAddressClient := flag.String("a", "127.0.0.1", "chord client's IP address")
	portClient := flag.Int("p", 8080, "chord client's port number")
	ipAddressChord := flag.String("ja", "", "IP address of machine running a chord node") // improve later
	portChord := flag.Int("jp", -1, "Port number") // improve later
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
	    Address: createIdentifier([]byte(*ipAddressClient + ":" + strconv.Itoa(*portClient))),
	}

	// Check to join or to create a new chord ring
	// IMPROVE HERE

	if *ipAddressChord == "" && *portChord == -1 {
		// starts a new ring
		createRing()
		
	} else if *ipAddressChord != "" && *portChord != -1 {
		// joins an existing ring
		joinRing()
	}
	// open a TCP socket
	listener, err := net.Listen("tcp", *ipAddressClient + ":" + strconv.Itoa(*portClient))
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

		go handleConnection(conn)
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

func createRing() {
	// initialize the successor list and finger table

}

func joinRing() {

}

func createIdentifier(name []byte) NodeAddress{
	// create a 40-character hash key for the name
	h := sha1.New()
	return NodeAddress(h.Sum(name))
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
}

