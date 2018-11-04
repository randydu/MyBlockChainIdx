#!/bin/bash

docker run -d --name mydb --rm  -v /home/randy/blks/mydb:/data -p 8888:27017 mongo 
