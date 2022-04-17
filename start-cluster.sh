#!/usr/bin/env bash

cd cmd && go build -o freya
trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM EXIT
./freya -node-id 1 -cluster-id 128 -members localhost:63001,localhost:63002,localhost:63003 -rest-port 8080 &
./freya -node-id 2 -cluster-id 128 -members localhost:63001,localhost:63002,localhost:63003 -rest-port 8081 &
./freya -node-id 3 -cluster-id 128 -members localhost:63001,localhost:63002,localhost:63003 -rest-port 8082