#!/bin/bash

IP_ADDR=$(ip -4 addr show dev wlp54s0 | grep -oP '(?<=inet\s)\d+(\.\d+){3}')

docker run --rm -d -e PGUSER=postgres -e PGPASSWORD=`cat ps_pass` -e PGHOST=${IP_ADDR} --name fill-pg history-tools:v1.0.1 /bin/sh -c "echo Waiting for Nodeos service start...; while ! nc -z ${IP_ADDR} 8080; do echo -n "."; sleep 1;done; fill-pg --fpg-create  --fill-connect-to=${IP_ADDR}:8080"