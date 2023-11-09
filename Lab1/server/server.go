package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	localDB = "/database"
)

func main() {
	// create a buffered channel for goroutine limitation
	channel := make(chan string, 10)

	// read the port number passed from terminal
	utility.ServerPort = *(flag.Int("port", 8080, "A port that the server listens from"))
	flag.Parse()

	listener, err := net.Listen(utility.NetworkConn, utility.HostConn+":"+strconv.Itoa(utility.ServerPort))
	if err != nil {
		fmt.Println("Listening Error:", err.Error())
		return
	}
	defer listener.Close()

	fmt.Println("Group 6 server is listening on " + utility.HostConn + ":" + strconv.Itoa(utility.ServerPort))

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
	defer utility.ReleaseBufferChannel(channel)
	defer conn.Close()

	// read request
	reader := bufio.NewReader(conn)
	request, err := http.ReadRequest(reader)
	if err != nil {
		fmt.Println("Reading http request Error:", err)
		utility.SendResponse(conn, 400, "Bad Request(Request cannot be read or parsed)")
		return
	}
	utility.PrintRequest(request)

	// parse request
	reqMethod := request.Method

	exePath, err := os.Executable()
	if err != nil {
		fmt.Printf("Getting executable path Error: " + err.Error())
		utility.SendResponse(conn, 500, "Internal Server Error (Getting executable path Error)")
		return
	}

	lab1DatabaseDirectory := filepath.Dir(exePath) + localDB

	if reqMethod == "GET" {
		fileUrl := request.URL.String()
		responseContentType := checkExtension(fileUrl)
		localFilePath := lab1DatabaseDirectory + fileUrl
		fmt.Println("localFilePath is: " + localFilePath)

		if responseContentType == "" {
			utility.SendResponse(conn, 400, "Bad Request(Extension not supported or no extension specified)")
			return
		}

		if fileExists(localFilePath) {
			sendResource(conn, responseContentType, localFilePath)
			return
		} else {
			utility.SendResponse(conn, 404, "Not Found")
			return
		}
	} else if reqMethod == "POST" {
		// don't forget to send response before every return
		// not sure about the error code
		fmt.Println("Print the form")

		err := request.ParseMultipartForm(32 << 20)
		if err != nil {
			fmt.Println("Payload Too Large: " + err.Error())
			utility.SendResponse(conn, 413, "Payload Too Large")
			return
		}

		multiForm := request.MultipartForm

		if len(multiForm.File) == 0 {
			utility.SendResponse(conn, 400, "Bad Request(No file provided)")
			return
		}

		for key := range multiForm.File {
			file, fileHeader, err := request.FormFile(key)
			if err != nil {
				fmt.Println("Bad Request: " + err.Error())
				utility.SendResponse(conn, 400, "Bad Request")
				return
			}
			defer file.Close()

			fileName := fileHeader.Filename
			fmt.Println("Filename is: " + fileName)

			// extension check
			fileExtensionCheck := checkExtension(fileName)
			if fileExtensionCheck == "" {
				utility.SendResponse(conn, 400, "Bad Request(Extension not supported or no extension specified)")
				return
			}

			// Store the file
			localFilePath := lab1DatabaseDirectory + "/" + fileName
			out, err := os.Create(localFilePath)
			if err != nil {
				fmt.Println("Internal Server Error: " + err.Error())
				utility.SendResponse(conn, 500, "Internal Server Error")
				return
			}
			defer out.Close()
			_, err = io.Copy(out, file)
			if err != nil {
				fmt.Println("Internal Server Error: " + err.Error())
				utility.SendResponse(conn, 500, "Internal Server Error")
				return
			}
			fmt.Printf("File %s is stored successfully!\n", fileName)
			utility.SendResponse(conn, 200, "OK! The file is stored successfully")
		}

		return
	} else {
		utility.SendResponse(conn, 501, "Not Implemented")
		return
	}
}

// func PrintRequest(request *http.Request) {
// 	reqDump, err := httputil.DumpRequest(request, true)
// 	if err != nil {
// 		fmt.Println("Dumping request Error:", err.Error())
// 		return
// 	}
// 	fmt.Printf("REQUEST:\n%s", string(reqDump))
// }

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
		utility.SendResponse(conn, 500, "Internal Server Error (Opening file Error)")
		return
	}

	responseBody := fileData
	conn.Write([]byte("HTTP/1.1 200 OK\r\n"))
	conn.Write([]byte("Content-Type: " + responseContentType + "\r\n"))
	conn.Write([]byte("\r\n"))
	conn.Write([]byte(responseBody))
	return
}

// func SendResponse(conn net.Conn, statusCode int, responseMessage string) {
// 	responseHeader := fmt.Sprintf("HTTP/1.1 %d %s\r\n", statusCode, responseMessage)
// 	responseBody := fmt.Sprintf("%d %s", statusCode, responseMessage)
// 	conn.Write([]byte(responseHeader))
// 	conn.Write([]byte("\r\n"))
// 	conn.Write([]byte(responseBody))
// }

// func ReleaseBufferChannel(channel chan string) {
// 	temp := <-channel
// 	fmt.Println(temp)
// }

func checkExtension(fileName string) string {
	// Used to check the validation of extension
	// or to get the responseType
	contentTypeMap := map[string]string{
		"html": "text/html",
		"txt":  "text/plain",
		"gif":  "image/gif",
		"jpeg": "image/jpeg",
		"jpg":  "image/jpg",
		"css":  "text/css",
	}

	nameSplit := strings.Split(fileName, ".")
	fileExtension := nameSplit[len(nameSplit)-1]
	responseContentType := contentTypeMap[fileExtension]
	return responseContentType
}
