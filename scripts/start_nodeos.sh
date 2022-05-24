#!/bin/bash

set -e

BASE_DIR=/home/${USER}/nodeos-ht

CONFIG_DIR="${BASE_DIR}/config"
DATA_DIR="${BASE_DIR}/data"
LOG_DIR="${BASE_DIR}/log"
SHPDATA_DIR="${BASE_DIR}/shpdata"

if [ -f "${DATA_DIR}/eosd.pid" ]; then
    echo "nodeos is already run"
else

    if [ ! -d "${CONFIG_DIR}" ]; then
        mkdir -p "${CONFIG_DIR}"
    fi

    if [ ! -d "${DATA_DIR}" ]; then
        mkdir -p "${DATA_DIR}"
    fi

    if [ ! -d "${LOG_DIR}" ]; then
        mkdir -p "${LOG_DIR}"
    fi

    if [ ! -d "${SHPDATA_DIR}" ]; then
        mkdir -p "${SHPDATA_DIR}"
    fi

    cp config.ini "${CONFIG_DIR}/config.ini"
    cp genesis.json "${CONFIG_DIR}/genesis.json"

    sed -i 's/__USER__/'"${USER}"'/g' "${CONFIG_DIR}/config.ini"

    nodeos \
        --config-dir "${CONFIG_DIR}" \
        --data-dir "${DATA_DIR}" \
        --disable-replay-opts \
        --delete-all-blocks \
        --genesis-json "${CONFIG_DIR}"/genesis.json \
        >> "${LOG_DIR}"/nodeos.log 2>&1 & \
        echo $! > "${DATA_DIR}/eosd.pid"
fi
