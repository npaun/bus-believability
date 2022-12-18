#!/bin/sh

curl -sSL https://bct.tmix.se/Tmix.Cap.TdExport.WebApi/gtfs/?operatorIds=20 -o gtfs.zip
unzip -o -d gtfs gtfs.zip 

