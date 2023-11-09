# syntax=docker/dockerfile:1

# Specifies a parent image
FROM golang:1.21.3-bookworm

# Creates a working directory inside the container to hold our server's source code
WORKDIR /group6_lab1

# Copies files from our root directory into /group6_http_server
COPY . ./

# Installs Go dependencies
# RUN go mod download

# Builds the binaries
WORKDIR /group6_lab1/server
RUN go build -o http_server

WORKDIR /group6_lab1/proxy
RUN go build -o http_proxy

# Tells Docker which network port your container listens on
# Native server listens on 1234
# Proxy server listens on 3333
EXPOSE 1234
EXPOSE 3333

# Specifies the executable command that runs when the container starts
# ENTRYPOINT ["./server/http_server"]
# CMD ["-port", "1234"]
WORKDIR /group6_lab1

# Add execute permissions to entrypoint.sh
RUN chmod +x entrypoint.sh
ENTRYPOINT ["./entrypoint.sh"]

