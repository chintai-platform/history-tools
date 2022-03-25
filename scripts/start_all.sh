#!/bin/bash

./status.sh | grep "Nodeos is running\|fill-pg is running" > /dev/null

if [ ${PIPESTATUS[1]} -eq 0  ]; then
    echo "Something is still running"
    ./status.sh
    exit 1
fi

docker ps | grep postgres > /dev/null
if [ ${PIPESTATUS[1]} -ne 0  ]; then
    echo "Starting PSQL"
    ./start_psql.sh
fi

echo "Starting Nodeos"
./start_nodeos.sh

echo "Starting Fill-pg"
./start_fpg.sh
