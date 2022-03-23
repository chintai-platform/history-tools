#!/bin/bash

set -e

BASE_DIR=/home/${USER}/nodeos-ht

DATA_DIR="${BASE_DIR}/data"

rm -r "${DATA_DIR}/eosd.pid"
