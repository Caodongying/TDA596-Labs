#!/bin/bash

# As we need to run two commands with parameters to start both proxy server and native server
# an extra bash file is required
# & is to concurrently run two commands
./server/http_server -port 1234 &
./proxy/http_proxy -port 3333