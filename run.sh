#!/bin/sh

docker run --name bus_believability -d --mount src=$HOME/bus-believability/db,target=/usr/app/db,type=bind --restart=always bus_believability

