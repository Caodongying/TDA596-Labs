package main

import (
	"bufio"
	"crypto/sha1"
	"encoding/gob"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math/big"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

type NodeAddress string

type FileName string

type Node struct {
	ID          string
	Address     NodeAddress
	FingerTable [160]NodeIP
	NextFinger  int
	Predecessor NodeIP
	Successors  []NodeIP

	Bucket map[string]FileName
}

// Get the IP address via node id
type NodeIP struct {
	ID      string
	Address NodeAddress
}

type NodesFound struct {
	Found   bool
	NodesIP []NodeIP
}

func main() {
	// parse the command
	ipAddressClient := flag.String("a", "", "chord client's IP address")
	portClient := flag.Int("p", 0, "chord client's port number")
	ipAddressChord := flag.String("ja", "", "IP address of machine running a chord node") // improve later
	portChord := flag.Int("jp", -1, "Port number")                                        // improve later
	ts := flag.Int("ts", 0, "time in milliseconds between invocations of ‘stabilize’")
	tff := flag.Int("tff", 0, "time in milliseconds between invocations of ‘fix fingers’")
	tcp := flag.Int("tcp", 0, "time in milliseconds between invocations of ‘check predecessor’")
	r := flag.Int("r", 0, "number of successors maintained by the Chord client")
	id := flag.String("i", "", "customized chord identifier")

	flag.Parse()

	//validate the parameters
	if net.ParseIP(*ipAddressClient) == nil {
		fmt.Println(*ipAddressClient)
		fmt.Println("Please use a valid IP address for the client")
		return
	}

	if *portClient < 1024 || *portClient > 65535 {
		fmt.Println("Please use a number between 1024 and 65535 as a port number for the client")
		return
	}

	if net.ParseIP(*ipAddressChord) == nil && *ipAddressChord != "" {
		fmt.Println("Please use a valid IP address for the chord node")
		return
	}

	if (*portChord < 1024 || *portChord > 65535) && *portChord != -1 {
		fmt.Println("Please use a number between 1024 and 65535 as a port number for the chord node")
		return
	}

	if *ts < 1 || *ts > 60000 {
		fmt.Println("Please use a number between 1 and 60000 as a value for ts")
		return
	}

	if *tff < 1 || *tff > 60000 {
		fmt.Println("Please use a number between 1 and 60000 as a value for tff")
		return
	}

	if *tcp < 1 || *tcp > 60000 {
		fmt.Println("Please use a number between 1 and 60000 as a value for tcp")
		return
	}

	if *r < 1 || *r > 32 {
		fmt.Println("Please use a number between 1 and 32 as a value for r")
		return
	}

	if *id != "" {
		if len(*id) != 40 {
			fmt.Println("Please use an identifier with 40 characters")
			return
		}
		_, err := strconv.ParseUint(*id, 16, 64)
		if err != nil {
			fmt.Println("Please use an identifier consisting of hexcode characters")
			return
		}
	}

	// // crash if only ipAddressChord or portChord is given in command line
	if (*ipAddressChord == "" && *portChord == -1) && (*ipAddressChord != "" && *portChord != -1) {
		fmt.Println("Please use either both -ja and -jp, or neither of them")
		return
	}

	// // make sure that the given chord node is not the same as the client node
	if *ipAddressChord == *ipAddressClient && *portChord == *portClient {
		fmt.Println("Please make sure the new node has a different IP address and port number than the existing node")
		return
	}

	// Instantiate the node
	node := Node{
		Address:    NodeAddress(*ipAddressClient + ":" + strconv.Itoa(*portClient)),
		Successors: make([]NodeIP, *r),
	}

	// Check if there's an customized id or not
	if *id != "" {
		node.ID = *id
	} else {
		node.ID = createIdentifier(string(node.Address))
	}

	// Check to join or to create a new chord ring
	// IMPROVE HERE
	if *ipAddressChord == "" && *portChord == -1 {
		// starts a new ring
		node.createRing()
	} else if *ipAddressChord != "" && *portChord != -1 {
		// joins an existing ring
		fmt.Println("start to join the ring")
		success := node.joinRing(*ipAddressChord, *portChord)
		if !success {
			return
		}
	}

	// Create a goroutine to start the three timers
	go node.setStabilizeTimer(*ts)
	go node.setFixFingerTimer(*tff)
	go node.setCheckPredecessorTimer(*tcp)

	// Create a goroutine to handle stdin command
	go node.handleThreeCommands()

	// open a TCP socket
	fmt.Println("Listening")
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

		go handleConnection(conn, &node)
	}

}

func (node *Node) handleThreeCommands() {
	for {
		// read from stdin
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			splitScan := strings.Split(scanner.Text(), " ")
			switch splitScan[0] {
			case "Lookup":
				node.lookUp(splitScan[1])
			case "StoreFile":
				node.storeFile(splitScan[1])
			case "PrintState":
				node.printState()
			default:
				fmt.Println("Invalid command! Supported commands: Lookup, StoreFile, PrintState")
			}

		}
	}
}

func (node *Node) lookUp(fileName string) NodeIP {
	// 0 - "-" can't be in fileName
	if strings.ContainsAny(fileName, "-") {
		fmt.Println("Illegal file name, make sure there is no \"-\"")
		return NodeIP{}
	}
	// 1 - hash the filename
	key := createIdentifier(fileName)
	// 2 - find the successor of the file key
	temp := node.find(key)
	if !temp.Found {
		fmt.Println("File location is not found!")
		return NodeIP{}
	}
	// 3 - print out the node information
	//     id, ip, port
	fmt.Printf("Node Information: \n  %v  %v", temp.NodesIP[0].ID, temp.NodesIP[0].Address)
	return temp.NodesIP[0]
}

func (node *Node) printState() {
	fmt.Printf("Chord Client's node information:\n %v  %v\n", node.ID, node.Address)
	fmt.Println("Successor Nodes:")
	for _, successor := range node.Successors {
		fmt.Printf("successor %v  %v\n", successor.ID, successor.Address)
	}
	fmt.Println("Fingers:")
	for _, finger := range node.FingerTable {
		fmt.Printf("finger %v  %v\n", finger.ID, finger.Address)
	}
}

func (node *Node) storeFile(filePath string) {
	// 1 - see if the given filepath exists locally or not
	// 2 - parse the path and get the filename
	// 3 - call loopUp(filename)
	// 4 - make a request to the node that should store the file
	//     read the file, write it to the connection
	// 5 - In the destination node, store/upload the file
	//     (in handleConnection) read from the connection, store the file
	//     update node.Bucket
	_, err := os.Stat(filePath)
	if err != nil {
		fmt.Println("File does not exist!")
		return
	}

	filePathSplit := strings.Split(filePath, "\\") // split by \
	fileName := filePathSplit[len(filePathSplit)-1]
	destination := node.lookUp(fileName)
	if destination.ID == "" { // here we don't print error as it's already done in lookUp()
		return
	}

	conn, err := net.Dial("tcp", string(destination.Address))
	if err != nil {
		fmt.Println("Error when dialing the destination")
		return
	}

	defer conn.Close()

	fileData, err := ioutil.ReadFile(filePath)
	if err != nil {
		fmt.Println("Error when opening the file!")
		return
	}

	conn.Write(append([]byte("storeFile-"+fileName+"-"), fileData...))
}

func (node *Node) setStabilizeTimer(ts int) {
	for {
		timerStabilize := time.NewTimer(time.Duration(ts) * time.Millisecond)
		<-timerStabilize.C
		node.stabilize()
	}
}

func (node *Node) setFixFingerTimer(tff int) {
	for {
		timerFixFinger := time.NewTimer(time.Duration(tff) * time.Millisecond)
		<-timerFixFinger.C
		node.fixFinger()
	}
}

func (node *Node) setCheckPredecessorTimer(tcp int) {
	for {
		timerCheckPredecessor := time.NewTimer(time.Duration(tcp) * time.Millisecond)
		<-timerCheckPredecessor.C
		node.checkPredecessor()
	}
}

func (node *Node) stabilize() {
	fmt.Println("predecessor is: " + node.Predecessor.ID)
	if node.Successors[0].Address == "" {
		return
	}
	// temp contains predecessor / empty NodeIP{}
	temp := makeRequest("findPredecessor", "", node.Successors[0].Address)
	if temp.Found {
		// predecessor exists
		if (temp.NodesIP[0].ID > node.ID && temp.NodesIP[0].ID < node.Successors[0].ID) || node.ID == node.Successors[0].ID {
			fmt.Println("Predecessor exists. Update successors to ", temp.NodesIP)
			node.Successors = temp.NodesIP
		}
	}
	// update other successors
	temp = makeRequest("findOtherSuccessors", "", node.Successors[0].Address)
	if temp.Found {
		for i := 1; i < len(node.Successors); i++ {
			node.Successors[i] = temp.NodesIP[i-1]
		}
	}
	// send notify to successor[0]
	makeNotifyRequest(NodeIP{ID: node.ID, Address: node.Address}, node.Successors[0].Address)
}

func (node *Node) notify(currentNode NodeIP) { //chord has to be a ring!
	if node.Predecessor.ID == "" || (currentNode.ID > node.Predecessor.ID && currentNode.ID < node.ID) || (currentNode.ID < node.Predecessor.ID && currentNode.ID > node.ID) {
		node.Predecessor = currentNode
	}
}

func (node *Node) fixFinger() {
	if node.NextFinger >= 160 {
		node.NextFinger = 0
	}
	nextNode, _ := strconv.ParseInt(node.ID, 16, 64)
	nextNodeNumber := big.NewInt(nextNode)
	nextNodeNumber.Add(nextNodeNumber, new(big.Int).Exp(big.NewInt(2), big.NewInt(int64(node.NextFinger)), nil))
	nextNodeNumber.Mod(nextNodeNumber, new(big.Int).Exp(big.NewInt(2), big.NewInt(160), nil))

	nextNodeID := hex.EncodeToString([]byte(nextNodeNumber.String()))
	temp := node.find(nextNodeID)

	if temp.Found {
		node.FingerTable[node.NextFinger] = temp.NodesIP[0]
	}

	node.NextFinger++
}

func (node *Node) checkPredecessor() {
	// check if predecessor is still running
	if node.Predecessor.ID == "" {
		return
	}

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
	for i := range node.Successors {
		node.Successors[i] = NodeIP{ID: node.ID, Address: node.Address}
	}

	for index := range node.FingerTable {
		node.FingerTable[index] = NodeIP{
			ID:      node.ID,
			Address: node.Address,
		}
	}
}

func (node *Node) findSuccessor(id string) NodesFound {
	if node.ID == node.Successors[0].ID {
		return NodesFound{Found: true, NodesIP: []NodeIP{node.Successors[0]}} // If we didn't return here, we would create an infinite loop
	}
	if id > node.ID && id <= node.Successors[0].ID {
		return NodesFound{Found: true, NodesIP: []NodeIP{node.Successors[0]}}
	}
	return NodesFound{Found: false, NodesIP: []NodeIP{node.closestPrecedingNode(id)}}
}

func (node *Node) closestPrecedingNode(id string) NodeIP {
	for i := 159; i >= 0; i-- {
		if node.FingerTable[i].ID > node.ID && node.FingerTable[i].ID <= id {
			return node.FingerTable[i]
		}
	}
	fmt.Println(node.Successors[0].ID)
	return node.Successors[0]
}

// TODO: figure out if this should return 1 successor or all successors
// 		+: cleaner code, easy way to check length of successor list
//		-: we don't follow the paper anymore, 99% of the time too complicated

func (node *Node) find(id string) NodesFound {
	nextNode := NodeIP{ID: node.ID, Address: node.Address}
	found := false
	for i := 0; i < 160 && !found; i++ {
		if nextNode.Address == "" {
			continue
		}
		temp := makeRequest("findSuccessor", id, nextNode.Address) // execute findSuccessor
		found = temp.Found
		nextNode = temp.NodesIP[0]
	}
	if found {
		return NodesFound{Found: true, NodesIP: []NodeIP{nextNode}}
	}
	//fmt.Println("Successor not found!")
	return NodesFound{}
}

func makeNotifyRequest(nodeIP NodeIP, ipAddress NodeAddress) {
	conn, err := net.Dial("tcp", string(ipAddress))
	if err != nil {
		fmt.Println("Error when dialing the node", err)
		return
	}
	defer conn.Close()

	// parameter sent to the ipAddress: id and address
	// this is to avoid using encoder and creating another ugly structure
	_, writeErr := conn.Write([]byte("notify" + "-" + nodeIP.ID + "-" + string(nodeIP.Address)))
	if writeErr != nil {
		fmt.Println("Error when sending notify request to the node", writeErr)
		return
	}
}

func makeRequest(operation string, nodeID string, ipAddressChord NodeAddress) NodesFound {
	// nodeID: for finding successor, the node that wants to join the ring;
	//		   otherwise, for finding predecessor, nodeID will be empty
	// ipAddressChord: the node that is already on the ring. We want to communicate to this ring and join the ring via this node
	// This function returns found or not + the successor (NodeIP)
	conn, err := net.Dial("tcp", string(ipAddressChord))
	if err != nil {
		fmt.Printf("Error when dialing the node (%v), %v\n", operation, err.Error())
		return NodesFound{}
	}
	//fmt.Printf("Successfully dialing node (%v)\n", operation)
	defer conn.Close()

	// write to the connection
	// operations can be:
	// (1) find ---> find-nodeID
	// (2) findSuccessor ---> findSuccessor-nodeID
	// (3) findPredecessor ---> findPredecessor-
	// TODO: write other operations here!
	_, writeErr := conn.Write([]byte(operation + "-" + nodeID))
	if writeErr != nil {
		fmt.Println("Error when sending request to the node", writeErr)
		return NodesFound{}
	}
	//fmt.Printf("Successfully sending request to node (%v)\n", operation)

	if cw, ok := conn.(interface{ CloseWrite() error }); ok {
		cw.CloseWrite()
	} else {
		fmt.Errorf("Connection doesn't implement CloseWrite method")
		return NodesFound{}
	}

	// receive the result: found, successor
	decoder := gob.NewDecoder(conn)
	//receiveNode := NodesFound{}
	var receiveNode NodesFound
	errDecode := decoder.Decode(&receiveNode)
	if errDecode != nil {
		fmt.Println("Error when receiving successor from the chord node", errDecode)
		return NodesFound{}
	}
	//fmt.Printf("Successfully receiving successor from node (%v)\n", operation)

	return receiveNode
}

func (node *Node) joinRing(ipChord string, portChord int) bool {
	// call find
	temp := makeRequest("find", node.ID, NodeAddress(ipChord+":"+strconv.Itoa(portChord)))
	if !temp.Found {
		fmt.Println("Join ring fails! Cannot find the successor of the new node!")
		return false
	}
	// all successor lists need to have the same length, TAs confirmed this
	// TODO: find way to check this

	//if len(temp.NodesIP) != len(node.Successors) {
	//	fmt.Println("Join ring fails! Please try again with a value of " + strconv.Itoa(len(temp.NodesIP)) + " for r")
	//	return false
	//}

	node.Successors = temp.NodesIP
	fmt.Println("The new node's successor is ", node.Successors[0])
	return true
}

func handleConnection(conn net.Conn, node *Node) {
	defer conn.Close()

	// Read the incoming request
	buf, readErr := ioutil.ReadAll(conn)
	if readErr != nil {
		fmt.Println("Error when reading request from other node", readErr)
		return
	}
	request := string(buf[:])

	requestSplit := strings.Split(request, "-")

	if requestSplit[0] == "find" {
		fmt.Println("request split is ", requestSplit)
	}

	switch requestSplit[0] {
	case "find":
		result := node.find(requestSplit[1])
		fmt.Println(result.NodesIP[0].Address)
		// send successor back to the node
		encoder := gob.NewEncoder(conn)
		errEncode := encoder.Encode(result)
		if errEncode != nil {
			fmt.Println("Error when sending find request to the node", errEncode)
			return
		}
	case "findSuccessor":
		result := node.findSuccessor(requestSplit[1])
		// send successor back to the node
		encoder := gob.NewEncoder(conn)
		errEncode := encoder.Encode(result)
		if errEncode != nil {
			fmt.Println("Error when sending findSuccessor request to the node", errEncode)
			return
		}
	case "findPredecessor":
		predecessor := node.Predecessor
		result := NodesFound{}
		if predecessor.ID != "" {
			result.Found = true
			result.NodesIP[0] = predecessor
		}
		// send predecessor back to the node
		encoder := gob.NewEncoder(conn)
		errEncode := encoder.Encode(result)
		if errEncode != nil {
			fmt.Println("Error when sending findPredecessor request to the chord node", errEncode)
			return
		}
	case "findOtherSuccessors":
		result := NodesFound{Found: true, NodesIP: node.Successors}
		// send successors back to the node
		encoder := gob.NewEncoder(conn)
		errEncode := encoder.Encode(result)
		if errEncode != nil {
			fmt.Println("Error when sending findOtherSuccessors request to the chord node", errEncode)
			return
		}
	case "notify":
		id := requestSplit[1]
		address := requestSplit[2]
		if id != node.ID { // A node can't be it's own predecessor
			fmt.Println("predecessor before:" + node.Predecessor.ID)
			node.notify(NodeIP{ID: id, Address: NodeAddress(address)})
			fmt.Println("predecessor after:" + node.Predecessor.ID)
		}
		return
	case "storeFile":
		fileName := requestSplit[1]
		fileContent := requestSplit[2]
		for i := 3; i < len(requestSplit); i++ {
			fileContent += "-" + requestSplit[i]
		}
		err := os.WriteFile(fileName, []byte(fileContent), 0644)
		if err != nil {
			fmt.Println("Error when writing the file", err)
			return
		}
		key := createIdentifier(fileName)
		node.Bucket[key] = FileName(fileName)
	}
}

func createIdentifier(name string) string {
	// name is ip:port
	// generate a 40-character hash key for the name

	// h := sha1.New()
	// io.WriteString(h, string(name))
	// temp := string(h.Sum(nil))
	// tempArr := strings.Split(temp, " ")
	// result := ""
	// for _, value := range tempArr {
	// 	result += fmt.Sprintf("%s", value)
	// }
	// return result
	h := sha1.New()
	io.WriteString(h, name)
	identifier := hex.EncodeToString(h.Sum(nil))
	fmt.Println("Identifier is ", identifier)
	return identifier
}
