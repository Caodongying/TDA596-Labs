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

	"lab1/utility"
)

const (
	localDB = "/database"
)

func main() {
	// Create a buffered channel for goroutine limitation
	channel := make(chan string, 10)

	// Read the port number passed from terminal
	serverPortPointer := flag.Int("port", 8080, "A port that the server listens from")
	flag.Parse()
	serverPort := *serverPortPointer

	listener, err := net.Listen(utility.NetworkConn, utility.HostConn+":"+strconv.Itoa(serverPort))
	if err != nil {
		fmt.Println("Listening Error:", err.Error())
		return
	}
	defer listener.Close()

	fmt.Println("Group 6 server is listening on " + utility.HostConn + ":" + strconv.Itoa(serverPort))

	// Keep accepting connection request
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Accepting Error", err.Error())
			continue
		}
		// Channel message will be shown only before the return of handleConnection
		channel <- "\n ************* A goroutine finished! *************"
		go handleConnection(conn, channel)
	}
}

func handleConnection(conn net.Conn, channel chan string) {
	defer utility.ReleaseBufferChannel(channel)
	defer conn.Close()

	// Read request
	reader := bufio.NewReader(conn)

	request, err := http.ReadRequest(reader)
	if err != nil {
		fmt.Println("Reading http request Error:", err)
		utility.SendResponse(conn, 400, "Bad Request(Request cannot be read or parsed)")
		return
	}
	utility.PrintRequest(request)

	// Parse request
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
		handlePost(request, conn, lab1DatabaseDirectory)
		return
	} else {
		utility.SendResponse(conn, 501, "Not Implemented")
		return
	}
}

func handlePost(request *http.Request, conn net.Conn, lab1DatabaseDirectory string) {
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

		// Extension check
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
