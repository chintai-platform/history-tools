#!/bin/bash

echo -n $RANDOM | md5sum | head -c 20 > ps_pass
echo localhost:5432:postgres:postgres:$(cat ps_pass) > ~/.pgpass
chmod 0600 ~/.pgpass

docker run --rm -d -e POSTGRES_PASSWORD=`cat ps_pass` --rm --name postgres -p 0.0.0.0:5432:5432 postgres
