package main

import (
	"os"
	"net/http/httputil"
	"fmt"
	"net"
	"net/http"
	"bufio"
	"strings"
	"io/ioutil"
	"path/filepath"
	"flag"
	"strconv"
)

const (
	networkConn = "tcp"
	hostConn = "localhost"
	localDB = "/database"
)

func main() {
	portPtr := flag.Int("port", 8080, "A port that the server listens from")
	flag.Parse()
	portConn := *portPtr
	// listen on a specific port
	listener, err := net.Listen(networkConn, hostConn + ":" + strconv.Itoa(portConn))
	if err != nil {
		fmt.Println("Listening Error:", err.Error())
		return
	}
	defer listener.Close()

	fmt.Println("Group 6 server is listening on " + hostConn + ":" + strconv.Itoa(portConn))

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
	responseContentType := contentTypeMap[fileExtension]
	
	exePath, err := os.Executable()
	if err != nil{
		fmt.Printf("Getting executable path Error: " + err.Error())
		response := "500 Internal Server Error (Getting executable path Error)"
		conn.Write([]byte("HTTP/1.1 500 Internal Server Error\r\n"))
		conn.Write([]byte("\r\n"))
		conn.Write([]byte(response))
		return
	}

	lab1Directory := filepath.Dir(exePath)
	localFilePath := lab1Directory + localDB + fileUrl
	fmt.Println("localFilePath is: " + localFilePath)

	// handle request
	if reqMethod == "GET" {
		if responseContentType == ""{
			// File extension not allowed
			// Respond with 400 "Bad Request" code
			response := "400 Bad Request"
			conn.Write([]byte("HTTP/1.1 400 Bad Request\r\n"))
			conn.Write([]byte("\r\n"))
			conn.Write([]byte(response))
			return
		}

		if fileExists(localFilePath){
			sendResource(conn, responseContentType, localFilePath)
			return
		}else{
			response := "404 Not Found"
			conn.Write([]byte("HTTP/1.1 404 Not Found\r\n"))
			conn.Write([]byte("\r\n"))
			conn.Write([]byte(response))
			return
		}
	}else if reqMethod == "POST" {
		// routing
		fmt.Printf("This is a POST")
		// process
		return
	}else{
		response := "501 Not Implemented"
		conn.Write([]byte("HTTP/1.1 501 Not Implemented\r\n"))
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

func sendResource(conn net.Conn, responseContentType string, localFilePath string) {
	// open the file
	fileData, err := ioutil.ReadFile(localFilePath)
	if err != nil {
		fmt.Println("Opening file Error: " + err.Error())
		response := "500 Internal Server Error (Opening file Error)"
		conn.Write([]byte("HTTP/1.1 500 Internal Server Error\r\n"))
		conn.Write([]byte("\r\n"))
		conn.Write([]byte(response))
		return
	}

	responseBody := fileData
	conn.Write([]byte("HTTP/1.1 200 OK\r\n"))
	conn.Write([]byte("Content-Type: " + responseContentType +"\r\n"))
	conn.Write([]byte("\r\n")) // does tcp require \r\n?
	conn.Write([]byte(responseBody))
	return
}