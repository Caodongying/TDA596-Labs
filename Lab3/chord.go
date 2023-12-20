package main

import (
	"bufio"
	"crypto/sha1"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math/big"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"time"
)

type NodeAddress string

type FileName string

type Node struct {
	ID          string
	Address     string
	FingerTable [160]NodeIP
	NextFinger  int
	Predecessor NodeIP
	Successors  []NodeIP

	Bucket map[string]FileName
}

// Get the IP address via node id
type NodeIP struct {
	ID      string
	Address string
}

type NodeFound struct {
	Found  bool
	NodeIP NodeIP
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
		Address:    *ipAddressClient + ":" + strconv.Itoa(*portClient),
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
		fmt.Println("start to create the ring")
		go node.createRing()
	} else if *ipAddressChord != "" && *portChord != -1 {
		// joins an existing ring
		fmt.Println("start to join the ring")
		ch := make(chan bool)
		go node.joinRing(*ipAddressChord, *portChord, ch)
		result := <-ch
		if !result {
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

	rpc.Register(&node) // not sure
	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", *ipAddressClient+":"+strconv.Itoa(*portClient))
	if err != nil {
		fmt.Println("Listener error: ", err)
		return
	}
	http.Serve(listener, nil) // todo: not sure if go is required
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
	args := Args{}
	reply := Reply{}
	args.AddressDial = node.Address
	args.IDToFind = key
	ok := node.call("Node.RPCFind", &args, &reply)
	if !ok {
		fmt.Println("Error happens when calling RPCFind in lookup!")
		return NodeIP{}
	}

	if !reply.Found {
		fmt.Println("File location is not found!")
		return NodeIP{}
	}
	// 3 - print out the node information
	//     id, ip, port
	fmt.Printf("Node Information: \n  %v  %v", reply.FoundNodeIPs[0].ID, reply.FoundNodeIPs[0].Address)
	return reply.FoundNodeIPs[0]
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

	fileData, err := ioutil.ReadFile(filePath)
	if err != nil {
		fmt.Println("Error when opening the file!")
		return
	}

	args := Args{}
	reply := Reply{}
	args.AddressDial = destination.Address
	args.FileData = fileData
	args.FileName = fileName

	ok := node.call("Node.RPCStoreFile", &args, &reply)
	if !ok {
		fmt.Println("Error when executing RPCStoreFile!")
		return
	}
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

	args := Args{}
	reply := Reply{}

	// temp contains predecessor / empty NodeIP{}
	args.AddressDial = node.Successors[0].Address
	ok := node.call("Node.RPCFindPredecessor", &args, &reply)
	if !ok {
		fmt.Println("Successor has failed. Updating successor.")
		for i := 1; i < len(node.Successors); i++ {
			node.Successors[i-1] = node.Successors[i]
		}
		return
	}
	if reply.Found {
		// predecessor exists
		if (reply.FoundNodeIPs[0].ID > node.ID && reply.FoundNodeIPs[0].ID < node.Successors[0].ID) || node.ID == node.Successors[0].ID {
			fmt.Println("Predecessor exists. Update successor to ", reply.FoundNodeIPs)
			node.Successors[0] = reply.FoundNodeIPs[0]
		}
	}
	// update other successors
	reply = Reply{}
	ok = node.call("Node.RPCFindAllSuccessors", &args, &reply)
	if reply.Found {
		for i := 1; i < len(node.Successors); i++ {
			node.Successors[i] = reply.FoundNodeIPs[i-1]
		}
	}
	// send notify to successor[0]
	args.AddressDial = node.Successors[0].Address
	args.NodeIPNotify = NodeIP{ID: node.ID, Address: node.Address}
	node.call("Node.RPCNotify", &args, &reply)
	return
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

	args := Args{}
	reply := Reply{}
	args.AddressDial = node.Address
	args.IDToFind = nextNodeID
	ok := node.call("Node.RPCFind", &args, &reply)
	if !ok {
		fmt.Println("Error happens when calling RPCFind in fixFinger!")
		return
	}

	if reply.Found {
		node.FingerTable[node.NextFinger] = reply.FoundNodeIPs[0]
	}

	node.NextFinger++
}

func (node *Node) checkPredecessor() {
	// check if predecessor is still running, meaning if predecessor quits or not
	if node.Predecessor.ID == "" {
		// fmt.Println("There is no predecessor now!")
		return
	}

	_, err := rpc.DialHTTP("tcp", node.Predecessor.Address)
	//defer client.Close()
	if err != nil {
		node.Predecessor = NodeIP{}
		fmt.Println("Predecessor has failed/quited!", err)
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

func (node *Node) closestPrecedingNode(id string) NodeIP {
	for i := 1; i < len(node.Successors); i++ {
		if node.Successors[i].ID > node.ID && node.Successors[i].ID <= id {
			return node.Successors[i]
		}
	}
	for i := 159; i >= 0; i-- {
		if node.FingerTable[i].ID > node.ID && node.FingerTable[i].ID <= id {
			return node.FingerTable[i]
		}
	}
	return node.Successors[0]
}

func (node *Node) joinRing(ipChord string, portChord int, ch chan bool) {
	// call find

	// all successor lists need to have the same length, TAs confirmed this
	// TODO: find way to check this

	args := Args{}
	reply := Reply{}
	args.IDToFind = node.ID
	args.AddressDial = ipChord + ":" + strconv.Itoa(portChord)
	ok := node.call("Node.RPCFind", &args, &reply)
	if !ok {
		ch <- false
		return
	}
	if !reply.Found {
		fmt.Println("Join ring fails! Cannot find the successor of the new node!")
		ch <- false
		return
	}
	node.Successors[0] = reply.FoundNodeIPs[0] //just give one, as the list length is always 1
	fmt.Println("The new node's successor is ", node.Successors[0])
	ch <- true
	return
}

func createIdentifier(name string) string {
	// name is ip:port
	// generate a 40-character hash key for the name
	h := sha1.New()
	io.WriteString(h, name)
	identifier := hex.EncodeToString(h.Sum(nil))
	fmt.Println("Identifier is ", identifier)
	return identifier
}
