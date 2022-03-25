#!/bin/bash

./status.sh | grep "is running" > /dev/null

if [ ${PIPESTATUS[1]} -eq 0  ]; then
    echo "Something is still running"
    ./status.sh
    exit 1
fi

docker ps | grep postgres > /dev/null
if [ ${PIPESTATUS[1]} -ne 0  ]; then
    ./start_psql.sh
fi
./start_nodeos.sh
./start_fpg.sh