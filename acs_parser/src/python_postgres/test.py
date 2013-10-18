#!/usr/bin/python

# this script calculate the distance between 2 bank branches using
# 1. spherical assumption
# 2. spheroid assumption

import psycopg2


conn = psycopg2.connect(database='mapster_db', user='mapster')

cur1 = conn.cursor()
cur2 = conn.cursor()

p1=None

#row = cur1.execute("SELECT ST_AsGeoJSON(geom) FROM bank_branch")
row = cur1.execute("SELECT geom FROM bank_branch")

#this is the spheroid id string for SRID=4269 (the one currently used in the db)
spheroid_str='SPHEROID["GRS 1980",6378137,298.257222101]'

for row in cur1.fetchall():
    if p1 :
        #ret=cur2.execute("select id, c001 from bank_branch where id=%s and c015=%s", (i, f))
        ret=cur2.execute('select ST_Distance_Sphere(%s, %s)', (p1, row[0]))
        dist=cur2.fetchone()
        
        ret=cur2.execute('select ST_Distance_Spheroid(%s, %s, %s)', (p1, row[0], spheroid_str))
        dist1 = cur2.fetchone()

        delta = 0;
        if dist[0] != 0 :
            delta = (dist[0]-dist1[0])/dist[0] * 100

        print dist[0], dist1[0], "delta=" + str(delta)
    p1=row[0]

cur1.close();
cur2.close();
