#!/bin/bash

./status.sh | grep "is running" > /dev/null

if [ ${PIPESTATUS[1]} -eq 0  ]; then
    echo "Something is still running"
    ./status.sh
    exit 1
fi

echo "Starting PSQL"
./start_psql.sh

echo "Starting Nodeos"
./start_nodeos.sh

# echo "Starting Fill-pg"
# ./start_fpg_create.sh
