#!/bin/bash

docker run -d --name bpx-test-mydb --rm  -v /home/randy/blks/bpx-test-mydb:/data -p 7777:27017 mongo 
