# TDA596-Labs
## Concurrency Control
We use buffered channel to control the number of goroutines forked for the request handling. It blocks the sendings if the buffer is full and blocks the receives if the buffer is empty. In this case, manual control of concurrency is not a concern for us.

The buffered channel with size 10 is created before the server starts to listen to the port specified from command line. Everytime when a connection is established successfully, we send a string to the channel to indicate the creation of a goroutine. When the request handling is done, successfully or not, a string will be taken out of the channel and then the goroutine is terminated.

## Build and Run
### Local build
The binary for server is produced by running `go build` under the parent directory of server.go.

The generated executable must be run from terminal as we parameterize the server:
1. cd Lab1
2. ./http_server -port=1234

Here 1234 can be replaced by any available port number.

A request example in Postman can be: `GET localhost:1234/group6.txt`

### Using Docker
As defined in Dockerfile, the container listens on network port 1234 and our go server inside the container listens to this port as well. Therefore, the host port that client sends requests to should be mapped to container port 1234.

The terminal commands below is a step-by-step guidance on how to build the docker image according to our Dockerfile and run the docker:

1. cd Lab1
2. docker build --tag tda596_lab1_http_server .
3. docker run -p 5678:1234 tda596_lab1_http_server

Please notice that `5678` can be replaced by any available port number on the host.

A request example in Postman can be: `GET localhost:5678/group6.txt`
