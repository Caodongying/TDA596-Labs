package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"strconv"
	"strings"
	"utility"
)

var proxyPort int

func main() {
	// create a buffered channel for goroutine limitation
	channel := make(chan string, 10)

	// read the port number passed from terminal
	proxyPort = *(flag.Int("port", 8070, "A port that the server listens from"))
	flag.Parse()

	listener, err := net.Listen(utility.NetworkConn, utility.HostConn+":"+strconv.Itoa(proxyPort))
	if err != nil {
		fmt.Println("Listening Error:", err.Error())
		return
	}
	defer listener.Close()

	fmt.Println("Proxy server is listening on " + utility.HostConn + ":" + strconv.Itoa(proxyPort))

	// keep accepting connection request
	for {
		connAsServer, err := listener.Accept()
		if err != nil {
			fmt.Println("Accepting Error", err.Error())
			continue
		}
		channel <- "\n ************* A goroutine finished! *************" // this message will be shown before return handleConnection
		go handleProxyConnection(connAsServer, channel)
	}
}

func handleProxyConnection(connAsServer net.Conn, channel chan string) {
	defer utility.ReleaseBufferChannel(channel)
	defer connAsServer.Close()

	// read request
	reader := bufio.NewReader(connAsServer)
	request, err := http.ReadRequest(reader)
	if err != nil {
		fmt.Println("Reading http request Error:", err)
		utility.SendResponse(connAsServer, 400, "Bad Request(Request cannot be read or parsed)")
		return
	}
	utility.PrintRequest(request)

	// parse request
	reqMethod := request.Method

	if reqMethod == "GET" {
		connAsClient, err := net.Dial(utility.NetworkConn, utility.HostConn+":"+strconv.Itoa(utility.ServerPort))
		if err != nil {
			fmt.Println("Dialing Error", err.Error())
			utility.SendResponse(connAsServer, 500, "Internal Server Error (Proxy cannot dial)")
			return
		}
		defer connAsClient.Close()

		// Forward the request
		reqDump, err := httputil.DumpRequest(request, true)
		if err != nil {
			fmt.Println("Dumping Request Error:", err.Error())
			utility.SendResponse(connAsServer, 500, "Internal Server Error (Cannot dump request)")
			return
		}
		if _, err := io.Copy(connAsClient, strings.NewReader(string(reqDump))); err != nil {
			fmt.Println("Forwarding request Error:", err)
			utility.SendResponse(connAsServer, 500, "Internal Server Error (Forwarding request error)")
			return
		}

		// Read the echoed data back from the server
		// buf := make([]byte, len(data))
		// if _, err := io.ReadFull(conn, buf); err != nil {
		// 	fmt.Println("Error reading data:", err)
		// 	return
		// }
		/////

	} else {
		utility.SendResponse(connAsServer, 501, "Not Implemented")
		return
	}
}
