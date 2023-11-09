package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"lab1/utility"
	"net"
	"net/http"
	"net/http/httputil"
	"strconv"
	"strings"
)

func main() {
	// Create a buffered channel for goroutine limitation
	channel := make(chan string, 10)

	// Read the port number passed from terminal
	proxyPortPointer := flag.Int("port", 8070, "A port that the server listens from")
	flag.Parse()
	proxyPort := *proxyPortPointer

	listener, err := net.Listen(utility.NetworkConn, utility.HostConn+":"+strconv.Itoa(proxyPort))
	if err != nil {
		fmt.Println("Listening Error:", err.Error())
		return
	}
	defer listener.Close()

	fmt.Println("Proxy server is listening on " + utility.HostConn + ":" + strconv.Itoa(proxyPort))

	// Keep accepting connection request
	for {
		connAsServer, err := listener.Accept()
		if err != nil {
			fmt.Println("Accepting Error", err.Error())
			continue
		}
		// Channel message will be shown only before the return of handleConnection
		channel <- "\n ************* A goroutine finished! *************"
		go handleProxyConnection(connAsServer, channel)
	}
}

func handleProxyConnection(connAsServer net.Conn, channel chan string) {
	defer utility.ReleaseBufferChannel(channel)
	defer connAsServer.Close()

	// Read request
	reader := bufio.NewReader(connAsServer)
	request, err := http.ReadRequest(reader)
	if err != nil {
		fmt.Println("Reading http request Error:", err)
		utility.SendResponse(connAsServer, 400, "Bad Request(Request cannot be read or parsed)")
		return
	}
	utility.PrintRequest(request)

	// Parse request
	reqMethod := request.Method
	serverPort := getPort(request.URL.String())

	if reqMethod == "GET" {
		connAsClient, err := net.Dial(utility.NetworkConn, utility.HostConn+":"+serverPort)
		if err != nil {
			fmt.Println("Dialing Error: ", err.Error())
			fmt.Println("ServerPort is ", serverPort)
			utility.SendResponse(connAsServer, 500, "Internal Server Error (Proxy cannot dial)")
			return
		}
		defer connAsClient.Close()

		// Forward the request to the server
		reqDump, err := httputil.DumpRequest(request, true)
		// Process the request from the client
		requestToServer := removeAddress(string(reqDump))

		if err != nil {
			fmt.Println("Dumping Request Error:", err.Error())
			utility.SendResponse(connAsServer, 500, "Internal Server Error (Cannot dump request)")
			return
		}
		if _, err := io.Copy(connAsClient, strings.NewReader(requestToServer)); err != nil {
			fmt.Println("Forwarding request Error:", err)
			utility.SendResponse(connAsServer, 500, "Internal Server Error (Forwarding request error)")
			return
		}

		// Read the data from the server
		readerAsClient := bufio.NewReader(connAsClient)
		data, err := io.ReadAll(readerAsClient)
		if err != nil {
			utility.SendResponse(connAsServer, 500, "Internal Server Error (Cannot read data from server)")
			return
		}

		// Send the data back to the client
		connAsServer.Write([]byte(data))

	} else {
		utility.SendResponse(connAsServer, 501, "Not Implemented")
		return
	}
}

func getPort(url string) string {
	urlSplit := strings.Split(url, ":")
	port := strings.Split(urlSplit[2], "/")[0]
	return port
}

func removeAddress(request string) string {
	requestSplit := strings.Split(request, " ")
	urlSplit := strings.Split(requestSplit[1], "/")
	fileName := urlSplit[len(urlSplit)-1]
	var result string
	for i, val := range requestSplit {
		if i == 0 {
			result += val + " /" + fileName
			continue
		}
		if i == 1 {
			continue
		}
		result += " " + val
	}
	return result
}
