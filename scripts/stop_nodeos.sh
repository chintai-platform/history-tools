#!/bin/bash

set -e

BASE_DIR=/home/nash/nodeos-ht

CONFIG_DIR="${BASE_DIR}/config"
DATA_DIR="${BASE_DIR}/data"
LOG_DIR="${BASE_DIR}/log"

if [ -f "${DATA_DIR}/eosd.pid" ]; then

    pid=`cat "${DATA_DIR}/eosd.pid"`
    echo $pid
    kill $pid
    rm -r "${DATA_DIR}/eosd.pid"

    echo -ne "Stoping Node"

    while true; do
        [ ! -d "/proc/$pid/fd" ] && break
        echo -ne "."
        sleep 1
    done

    echo -ne "\rNode Stopped. \n"

fi