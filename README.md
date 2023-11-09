# Lab 1
## Concurrency Control
We use buffered channel to control the number of goroutines forked for the request handling. It blocks the sendings if the buffer is full and blocks the receives if the buffer is empty. In this case, manual control of concurrency is not a concern for us.

The buffered channel with size 10 is created before the server starts to listen to the port specified from command line. Everytime when a connection is established successfully, we send a string to the channel to indicate the creation of a goroutine. When the request handling is done, successfully or not, a string will be taken out of the channel and then the goroutine is terminated.

## Build and Run
### Local build
The excutable binary for the native server is produced by running `go build` under the parent directory of server.go. Same goes for proxy.server.

You can specify the binary name by running `go build -o your_binary_name`

The generated executable must be run from terminal as we parameterize the server. Take the native server for example if using the default binary name **http_server**:
1. `cd Lab1/http_server`
2. `./http_server -port=1234`

Here 1234 can be replaced by any available port number.

To build and run the proxy server:
1. `cd Lab1/http_proxy`
2. `./http_proxy -port=3333`

Here 3333 can be replaced by any available port number.

A request example in Postman can be: `GET localhost:1234/group6.txt`

### Using Docker
As defined in Dockerfile, the container exposes port 1234 and 3333. The native server inside the container listens to port 1234 and the proxy server listens to port 3333 as specified in `entrypoint.sh`. Therefore, the host port that client sends requests to should be mapped to container port 1234.

The terminal commands below is a step-by-step guidance on how to build the docker image according to our Dockerfile and run the container:

1. `cd Lab1`
2. `docker build --tag tda596_lab1 .`
3. `docker run -p 1234:1234 -p 3333:3333 tda596_lab1`

Please notice that the first **1234** and the first **3333** can be replaced by any available port number on the host, as these two host ports will be mapped to port 1234(for native server) and 3333(for proxy server) respectively inside the container.

A GET request example in Postman can be: `GET localhost:1234/group6.txt`. 

To configure proxy in Postman before sending the request, go to Setting -> Proxy -> Use custom proxy configuration.
