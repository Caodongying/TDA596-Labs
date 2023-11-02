package main

import (
	"os"
	"net/http/httputil"
	"fmt"
	"net"
	"net/http"
	"bufio"
	"strings"
)

const (
	networkConn = "tcp"
	hostConn = "localhost"
	portConn = "1234"
	localDB = "/database"
)

func main() {
	// listen on a specific port
	listener, err := net.Listen(networkConn, hostConn + ":" + portConn)
	if err != nil {
		// TBC - listen fails
		fmt.Println("Listening Error:", err.Error())
		return
	}
	defer listener.Close()

	fmt.Println("Group 6 server is listening on " + hostConn + ":" + portConn)

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

	// parse request
	contentTypeMap := map[string]string{
		"html": "text/html",
		"txt": "text/plain",
		"gif": "image/gif",
		"jpeg": "image/jpeg",
		"jpg": "image/jpg",
		"css": "text/css",
	}
	reqMethod := request.Method
	fileUrl := request.URL.String()
	urlSplit := strings.Split(fileUrl, ".")
	fileExtension := urlSplit[len(urlSplit)-1]
	responseContentType := contentTypeMap[fileExtension] // empty result should respond with 400 "Bad Request"
	localFilePath := localDB + fileUrl

	// handle request
	if reqMethod == "GET" {
		if responseContentType == ""{
			// File extension not allowed
			// Respond with 400 "Bad Request" code
			conn.Write([]byte("HTTP/1.1 400 Bad Request\r\n"))
			conn.Write([]byte("\r\n"))
			conn.Write([]byte("400 Bad Request"))
			return
		}

		if fileExists(localFilePath){
			sendResource(conn, responseContentType)
			return
		}else{
			conn.Write([]byte("HTTP/1.1 404 Not Found\r\n"))
			conn.Write([]byte("\r\n"))
			conn.Write([]byte("404 Not Found"))
			return
		}
	}else if reqMethod == "POST" {
		// routing
		fmt.Printf("This is a POST")
		// process
		return
	}else{
		response := "Group 6: This request type is not implemented"
		conn.Write([]byte("HTTP/1.1 501 Not Implemented\n"))
		conn.Write([]byte("\r\n"))
		conn.Write([]byte(response))
		return // not sure
	}
}

func printRequest(request *http.Request) {
	reqDump, err := httputil.DumpRequest(request, true)
	if err != nil {
		fmt.Println("Dumping request Error:", err)
		return // not sure
	}
	fmt.Printf("REQUEST:\n%s", string(reqDump))
}

func fileExists(filePath string) bool {
	// Validates if the file exists or not
	_, err := os.Stat(filePath)
	if os.IsNotExist(err) {
		return false
	}
	return err == nil
}

func sendResource(conn net.Conn, responseContentType string) {
	// not done yet. Need to write html responseBody and show the resource
	responseBody := "Group 6: This is the response to GET"
	conn.Write([]byte("HTTP/1.1 200 OK\r\n"))
	conn.Write([]byte("Content-Type: " + responseContentType +"\r\n"))
	conn.Write([]byte("\r\n")) // does tcp require \r\n?
	conn.Write([]byte(responseBody))
	return
}