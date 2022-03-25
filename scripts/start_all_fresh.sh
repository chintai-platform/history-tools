#!/bin/bash

./status.sh | grep "is running"

if [ ${PIPESTATUS[1]} -eq 0  ]; then
    echo "Something is still running"
    ./status.sh
    exit 1
fi

./start_psql.sh
./start_nodeos.sh
./start_fpg_create.sh