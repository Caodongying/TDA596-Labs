package main

import (
	"net/http/httputil"
	"fmt"
	"net"
	"net/http"
	"bufio"
)

const (
	Network = "tcp"
	Host = "localhost"
	Port = "1234"
)

func main() {
	// listen on a specific port
	listener, err := net.Listen(Network, Host+":"+Port)
	if err != nil {
		// TBC - listen fails
		fmt.Println("Listening Error:", err.Error())
		return
	}
	defer listener.Close()

	fmt.Println("Group 6 server is listening on " + Host + ":" + Port)

	// keep accepting connection request
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Accepting Error", err.Error())
			continue
		}
		// TBC - goroutines maximum 10
		go handleConnection(conn)
	}	
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	// read request
	reader := bufio.NewReader(conn)
	request, err := http.ReadRequest(reader)
	if err != nil {
		fmt.Println("Reading http request Error:", err)
		return
	}
	printRequest(request)

	// get request type and route
	reqMethod := request.Method
	//reqUrl := request.URL

	if reqMethod == "GET" {
		// TBC - routing
		response := "Group 6: This is the response to GET"
		conn.Write([]byte("HTTP/1.1 200 OK\n"))
		conn.Write([]byte("Content-Type: not decided yet\n"))
		conn.Write([]byte("\n")) // does tcp require \r\n?
		conn.Write([]byte(response))
		return
	}else if reqMethod == "POST" {
		// routing
		fmt.Printf("This is a POST")
	}else{
		response := "Group 6: This request type is not implemented"
		conn.Write([]byte("HTTP/1.1 501 Not Implemented\n"))
		conn.Write([]byte("\n"))
		conn.Write([]byte(response))
	}
}

func printRequest(request *http.Request) {
	reqDump, err := httputil.DumpRequest(request, true)
	if err != nil {
		fmt.Println("Dumping request Error:", err)
		return
	}
	fmt.Printf("REQUEST:\n%s", string(reqDump))
}