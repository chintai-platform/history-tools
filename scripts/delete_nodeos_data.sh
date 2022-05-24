#!/bin/bash

BASE_DIR=/home/${USER}/nodeos-ht

DATA_DIR="${BASE_DIR}/data"
LOG_DIR="${BASE_DIR}/log"
SHPDATA_DIR="${BASE_DIR}/shpdata"

./stop_nodeos.sh
rm -rf "${DATA_DIR}/*"
rm -rf "${LOG_DIR}/*"
rm -rf "${SHPDATA_DIR}/*"
