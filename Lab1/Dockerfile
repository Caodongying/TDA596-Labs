# syntax=docker/dockerfile:1

# Specifies a parent image
FROM golang:1.21.3-bookworm

# Creates a working directory inside the container to hold our server's source code
WORKDIR /group6_http_server

# Copies files from our root directory into /group6_http_server
COPY . ./

# Installs Go dependencies (not necessary for us now as we only have a self-defined dependency)
RUN go mod download

# Builds the binary
RUN go build -o http_server

# Tells Docker which network port your container listens on
EXPOSE 1234

# Specifies the executable command that runs when the container starts
ENTRYPOINT ["./http_server"]
CMD ["-port", "1234"]
