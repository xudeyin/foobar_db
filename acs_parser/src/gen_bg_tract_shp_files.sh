#!/bin/bash
pgsql2shp -f ma_tract -h 10.25.25.182 -u mapster -P mapster mapster_db "select a.geoid, b.geom from tacs_geo_p1 a, tl_2011_25_tract b where a.geoid=b.geoid"
pgsql2shp -f ma_blockgroup -h 10.25.25.182 -u mapster -P mapster mapster_db "select a.geoid, b.geom from tacs_geo_p1 a, tl_2011_25_bg b where a.geoid=b.geoid"


tar -cvf ma_tract.tar ma_tract.*
tar -cvf ma_blockgroup.tar ma_blockgroup.*

gzip ma_tract.tar 
gzip ma_blockgroup.tar 
