package utility

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
)

const (
	NetworkConn = "tcp"
	HostConn    = "0.0.0.0"
)

func PrintRequest(request *http.Request) {
	fmt.Println("We're now in PrintRequest !!!")
	reqDump, err := httputil.DumpRequest(request, true)
	if err != nil {
		fmt.Println("Dumping request Error:", err.Error())
		return
	}
	fmt.Printf("REQUEST:\n%s", string(reqDump))
}

func SendResponse(conn net.Conn, statusCode int, responseMessage string) {
	responseHeader := fmt.Sprintf("HTTP/1.1 %d %s\r\n", statusCode, responseMessage)
	responseBody := fmt.Sprintf("%d %s", statusCode, responseMessage)
	conn.Write([]byte(responseHeader))
	conn.Write([]byte("\r\n"))
	conn.Write([]byte(responseBody))
}

func ReleaseBufferChannel(channel chan string) {
	temp := <-channel
	fmt.Println(temp)
}
