# TDA596-Labs
## Concurrency Control
We use buffered channel to control the number of goroutines forked for the request handling. It blocks the sendings if the buffer is full and blocks the receives if the buffer is empty. In this case, manual control of concurrency is not a concern for us.

The buffered channel with size 10 is created before the server starts to listen to the port specified from command line. Everytime when a connection is established successfully, we send a string to the channel to indicate the creation of a goroutine. When the request handling is done, successfully or not, a string will be taken out of the channel and then the goroutine is terminated.

## Build and Run
The binary for server is produced by running `go build` under the parent directory of server.go.

The generated executable must be run from terminal as we parameterize the server:
1. cd Lab1
2. ./http_server -port=1234

Here 1234 can be replaced by any available port number.

