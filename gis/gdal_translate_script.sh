#!/bin/bash
for f in *.tif
do
	gdal_translate -co compression=lzw $f $f_c.tif
done