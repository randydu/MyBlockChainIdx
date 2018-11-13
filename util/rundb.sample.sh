#!/bin/bash

docker run -d --name mydb --rm  -v /home/randy/blks/mydb/db:/data/db -v /home/randy/blks/mydb/configdb:/data/configdb -p 8888:27017 mongo 
