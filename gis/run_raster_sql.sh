#!/bin/bash
START=8
END=40
for i in {$START..$END}
do
	psql -d gis -f $i.sql
done