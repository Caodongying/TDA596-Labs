package main

import (
	"bufio"
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httputil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	networkConn = "tcp"
	hostConn = "0.0.0.0"
	localDB = "/database"
)

func main() {
	// create a buffered channel for goroutine limitation
	channel := make(chan string, 10)

	// read the port number passed from terminal
	portPtr := flag.Int("port", 8080, "A port that the server listens from")
	flag.Parse()
	portConn := *portPtr

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
		channel <- "\n ************* A goroutine finished! *************" // this message will be shown before return handleConnection
		go handleConnection(conn, channel)
	}	
}

func handleConnection(conn net.Conn, channel chan string) {
	defer releaseBufferChannel(channel)
	defer conn.Close()

	// read request
	reader := bufio.NewReader(conn)
	request, err := http.ReadRequest(reader)
	if err != nil {
		fmt.Println("Reading http request Error:", err)
		sendErrorResponse(conn, 400, "Bad Request(Request cannot be read or parsed)")
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
		sendErrorResponse(conn, 500, "Internal Server Error (Getting executable path Error)")
		return
	}

	lab1Directory := filepath.Dir(exePath)
	localFilePath := lab1Directory + localDB + fileUrl
	fmt.Println("localFilePath is: " + localFilePath)

	if reqMethod == "GET" {
		if responseContentType == ""{
			sendErrorResponse(conn, 400, "Bad Request(Extension not supported)")
			return
		}

		if fileExists(localFilePath){
			sendResource(conn, responseContentType, localFilePath)
			return
		}else{
			sendErrorResponse(conn, 404, "Not Found")
			return
		}
	}else if reqMethod == "POST" {
		// routing
		fmt.Printf("This is a POST")
		// process
		return
	}else{
		sendErrorResponse(conn, 501, "Not Implemented")
		return
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

func fileExists(filePath string) bool {
	// Validates if the file exists or not
	_, err := os.Stat(filePath)
	if os.IsNotExist(err) {
		return false
	}
	return err == nil
}

func sendResource(conn net.Conn, responseContentType string, localFilePath string) {
	fileData, err := ioutil.ReadFile(localFilePath)
	if err != nil {
		fmt.Println("Opening file Error: " + err.Error())
		sendErrorResponse(conn, 500, "Internal Server Error (Opening file Error)")
		return
	}

	responseBody := fileData
	conn.Write([]byte("HTTP/1.1 200 OK\r\n"))
	conn.Write([]byte("Content-Type: " + responseContentType +"\r\n"))
	conn.Write([]byte("\r\n"))
	conn.Write([]byte(responseBody))
	return
}

func sendErrorResponse(conn net.Conn, statusCode int, errorMessage string) {
	responseHeader := fmt.Sprintf("HTTP/1.1 %d %s\r\n", statusCode, errorMessage)
	responseBody := fmt.Sprintf("%d %s", statusCode, errorMessage)
	conn.Write([]byte(responseHeader))
	conn.Write([]byte("\r\n"))
	conn.Write([]byte(responseBody))
}

func releaseBufferChannel(channel chan string) {
	temp := <- channel
	fmt.Println(temp)
}