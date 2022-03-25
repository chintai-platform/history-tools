#!/bin/bash

echo "Stopping Nodeos"
./stop_nodeos.sh

echo "Stopping Fill-pg"
./stop_fpg.sh

echo "Stopping Postgres"
./stop_psql.sh
