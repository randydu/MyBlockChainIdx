#!/bin/bash
#
# Build docker for tcoin-db
#

docker build -t docker.yummy.net/tcoin-db:v1.0 --file dockerfile.db .