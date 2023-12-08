package main

import (
	"bufio"
	"crypto/sha1"
	"encoding/gob"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net"
	"os"
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
	FingerTable [160]NodeIP
	NextFinger int
	Predecessor NodeIP
	Successors  []NodeIP

	Bucket map[Key]FileName
}

// Get the IP address via node id
type NodeIP struct {
	ID string
	Address NodeAddress
}

type NodeFound struct {
	Found bool
	NodeIP NodeIP
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
	id := flag.String("i", "", "customized chord identifier")

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
		Successors: make([]NodeIP, *r),
	}

	// Check if there's an customized id or not
	if *id != "" {
		node.ID = *id
	} else{
		node.ID = createIdentifier(node.Address)
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
	go node.setStablizeTimer(*ts)
	go node.setFixFingerTimer(*tff)
	go node.setCheckPredecessorTimer(*tcp)

	// Create a goroutin to handle stdin command
	go node.handleThreeCommands()

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

func (node *Node) handleThreeCommands() {
	for {
		// read from stdin
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			splitScan := strings.Split(scanner.Text(), " ")
			switch splitScan[0] {
			case "Lookup":
				// add: some validation here
				node.lookUp(splitScan[1])
			case "StoreFile":
				node.storeFile(splitScan[1])
			case "PrintState":
				node.printState()
			}
				
		}
	}
}

func (node *Node) lookUp(fileName string){

}

func (node *Node) printState(){

}

func (node *Node) storeFile(filePath string){

}

func (node *Node) setStablizeTimer(ts int) {
	for {
		timerStablize := time.NewTimer(time.Duration(ts) * time.Millisecond)
		<-timerStablize.C
		go node.stablize()
	}
}

func (node *Node) setFixFingerTimer(tff int) {
	for {
		timerStablize := time.NewTimer(time.Duration(tff) * time.Millisecond)
		<-timerStablize.C
		go node.fixFinger()
	}
}

func (node *Node) setCheckPredecessorTimer(tcp int) {
	for {
		timerStablize := time.NewTimer(time.Duration(tcp) * time.Millisecond)
		<-timerStablize.C
		go node.checkPredecessor()
	}
}

func (node *Node) stablize(){
	// temp contains predecessor / empty NodeIP{}
	temp := makeRequest("findPredecessor", "", node.Successors[0].Address)
	if !temp.Found {
		// no predecessor found
		return
	}
	// predecessor exsits
	if temp.NodeIP.ID > node.ID && temp.NodeIP.ID < node.Successors[0].ID {
		node.Successors[0] = temp.NodeIP
	}
	// send notify to successor[0]
	makeNotifyRequest(NodeIP{ID: node.ID, Address: node.Address}, node.Successors[0].Address)
}

func (node *Node) notify(currentNode NodeIP) {
	if node.Predecessor.ID == "" || (currentNode.ID > node.Predecessor.ID && currentNode.ID < node.ID) {
		node.Predecessor = currentNode
	}
}

func (node *Node) fixFinger(){
	if node.NextFinger >= 160 {
		node.NextFinger = 0
	}

	nextNode, _ := strconv.Atoi("0x" + node.ID)
	nextNode += int(math.Pow(2, float64(node.NextFinger)))
	nextNode %= int(math.Pow(2, 160))
	temp := node.find(strconv.Itoa(nextNode))
	if temp.Found {
		node.FingerTable[node.NextFinger] = temp.NodeIP
	}else {
		fmt.Println("No suceesor found for node", nextNode)
	}
	node.NextFinger++
}

func (node *Node) checkPredecessor(){
	// check if predecessor is still running
	conn, err := net.Dial("tcp", string(node.Predecessor.Address))
	defer conn.Close()
	if err != nil {
		node.Predecessor = NodeIP{}
		fmt.Println("Predecessor has failed", err)
		return
	}
	return
}


func (node *Node) createRing() {
	// initialize the successor list and finger table
	node.Successors[0] = NodeIP{ID: node.ID, Address: node.Address}

	for index := range node.FingerTable {
		node.FingerTable[index] = NodeIP{
			ID: node.ID,
			Address: node.Address,
		}
	}
}

func (node *Node) findSuccessor(id string) NodeFound {
	if id > node.ID && id <= node.Successors[0].ID {
		return NodeFound{Found: true, NodeIP: node.Successors[0]}
	}
	return NodeFound{Found: false, NodeIP: node.closestPrecedingNode(id)}
}

func (node *Node) closestPrecedingNode(id string) NodeIP {
	for i := 160 ; i > 0 ; i-- {
		if (node.FingerTable[i].ID > node.ID && node.FingerTable[i].ID <= id) {
			return node.FingerTable[i]
		}
	}
	return node.Successors[0]
}

func (node *Node) find(id string) NodeFound {
	nextNode := NodeIP{ID: node.ID, Address: node.Address}
	found := false
	for i := 0 ; (i < 160 && !found) ; i++ {
		temp := makeRequest("findSuccessor", id, nextNode.Address) // execute findSuccessor
		found = temp.Found
		nextNode = temp.NodeIP
	}
	if found {
		return NodeFound{Found: true, NodeIP: nextNode}
	}
	fmt.Println("Successor not found!")
	return NodeFound{Found: false, NodeIP: NodeIP{}}
}

func makeNotifyRequest(nodeIP NodeIP, ipAddress NodeAddress) {
	conn, err := net.Dial("tcp", string(ipAddress))
	if err != nil {
		fmt.Println("Error when dialing the chord node", err)
		return
	}
	defer conn.Close()

	// parameter sent to the ipAddress: id and address
	// this is to avoid using encoder and creating another ugly structure
	_, writeErr := conn.Write([]byte("notify" + "-" + nodeIP.ID + "-" + string(nodeIP.Address)))
	if writeErr != nil {
		fmt.Println("Error when sending request to the chord node", writeErr)
		return
	}
}

func makeRequest(operation string, nodeID string, ipAddressChord NodeAddress) NodeFound {
	// nodeID: for finding successor, the node that wants to join the ring;
	//		   otherwise, for finding predecessor, nodeID will be empty
	// ipAddressChord: the node that is already on the ring. We want to communicate to this ring and join the ring via this node
	// This function returns found or not + the successor (NodeIP)
	conn, err := net.Dial("tcp", string(ipAddressChord))
	if err != nil {
		fmt.Println("Error when dialing the chord node", err)
		return NodeFound{Found: false, NodeIP: NodeIP{}}
	}
	defer conn.Close()

	// write to the connection
	// operations can be:
	// (1) find ---> find-nodeID
	// (2) findSuccessor ---> findSuccessor-nodeID
	// (3) findPredecessor ---> findPredecessor-
	_, writeErr := conn.Write([]byte(operation + "-" + nodeID))
	if writeErr != nil {
		fmt.Println("Error when sending request to the chord node", writeErr)
		return NodeFound{} // not sure
	}

	// receive the result: found, successor
	decoder := gob.NewDecoder(conn)
	receiveNode := NodeFound{}
	errDecode := decoder.Decode(receiveNode) // todo - not sure
	if errDecode != nil {
		fmt.Println("Error when receiving successor from the chord node", errDecode)
		return NodeFound{Found: false, NodeIP: NodeIP{}}
	}

	return receiveNode
}

func (node *Node) joinRing(ipChord string, portChord int) {
	// call find
	temp := makeRequest("find", node.ID, NodeAddress(ipChord+":"+strconv.Itoa(portChord)))
	if !temp.Found {
		return
	}
	node.Successors[0] = temp.NodeIP
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
	case "find":
		result := node.find(requestSplit[1])
		// send successor back to the node
		encoder := gob.NewEncoder(conn)
		errEncode := encoder.Encode(result)
		if errEncode != nil {
			fmt.Println("Error when sending request to the chord node", errEncode)
		    return
		}
	case "findSuccessor":
		result := node.findSuccessor(requestSplit[1])
		// send successor back to the node
		encoder := gob.NewEncoder(conn)
		errEncode := encoder.Encode(result)
		if errEncode != nil {
			fmt.Println("Error when sending request to the chord node", errEncode)
		    return
		}
	case "findPredecessor":
		predecessor := node.Predecessor
		result := NodeFound{}
		if predecessor.ID != "" {
			result.Found = true
			result.NodeIP = predecessor
		}
		// send predecessor back to the node
		encoder := gob.NewEncoder(conn)
		errEncode := encoder.Encode(result)
		if errEncode != nil {
			fmt.Println("Error when sending request to the chord node", errEncode)
		    return
		}
	case "notify":
		id := requestSplit[1]
		address := requestSplit[2]
		node.notify(NodeIP{ID: id, Address: NodeAddress(address)})
		return
	}


}

func createIdentifier(address NodeAddress) string{
	// address is ip:port
	// generate a 40-character hash key for the address
	h := sha1.New()
	io.WriteString(h, string(address))
	temp := string(h.Sum(nil))
	tempArr := strings.Split(temp, " ")
	result := ""
	for _, value := range tempArr {
		result += fmt.Sprintf("%s", value)
	}
	return result
}

