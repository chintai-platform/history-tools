#!/bin/bash

docker ps -a | grep postgres > /dev/null
if [ ${PIPESTATUS[1]} -eq 0  ]; then
    echo "Postgres is running"
else
    echo "Postgres is not running"
fi
echo ""

ps ax | grep [n]odeos
if [ ${PIPESTATUS[1]} -eq 0  ]; then
    echo "Nodeos is running"
else
    echo "Nodeos is not running"
fi
echo ""

docker ps -a | grep fill-pg > /dev/null
if [ ${PIPESTATUS[1]} -eq 0  ]; then
    echo "fill-pg is running"
else
    echo "fill-pg is not running"
fi
echo ""
