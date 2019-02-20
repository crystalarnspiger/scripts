#!/bin/bash

for f in *.tif
do
	raster2pgsql -s 4326 -a -F $f.tif ams_topo.ams_raster > ams_topo_huehuetenango.sql
done