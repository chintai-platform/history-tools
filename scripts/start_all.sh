#!/bin/bash

docker ps | grep postgres
if [ ${PIPESTATUS[1]} -ne 0  ]; then
    ./start_psql.sh
fi
./start_nodeos.sh
./start_fpg.sh