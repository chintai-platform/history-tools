#!/bin/bash

# ./status.sh | grep "Nodeos is running\|fill-pg is running" > /dev/null

./status.sh | grep "Nodeos is running" > /dev/null
NODEOS=${PIPESTATUS[1]}
./status.sh | grep "fill-pg is running" > /dev/null
FILLPG=${PIPESTATUS[1]}

if [ ${NODEOS} -eq 0  ]; then
    echo "Something is still running"
    ./status.sh
    exit 1
fi

CREATE=0
docker ps | grep postgres > /dev/null
if [ ${PIPESTATUS[1]} -ne 0  ]; then
    echo "Starting PSQL"
    ./start_psql.sh
    CREATE=1
fi

echo "Starting Nodeos"
./start_nodeos.sh

if [ ${FILLPG} -ne 0 ]; then
    echo "Starting Fill-pg"
    if [ $CREATE -eq 0 ]; then
        ./start_fpg.sh
    else
        ./start_fpg_create.sh
    fi
fi
