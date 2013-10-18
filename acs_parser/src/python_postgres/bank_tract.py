#!/usr/bin/python

# this script calculates the tract area each bank branch belongs to.

import psycopg2

conn = psycopg2.connect(database='mapster_db', user='mapster')

cur1 = conn.cursor()
cur2 = conn.cursor()

row = cur1.execute("SELECT geom,id,c014,c004,c005,c006,c007,c008,c009 FROM bank_branch order by id")

#this is the spheroid id string for SRID=4269 (the one currently used in the db)
spheroid_str='SPHEROID["GRS 1980",6378137,298.257222101]'

tract_query_str = 'select geoid, name from tl_2011_25_tract where ST_Within(%s, geom)=True'

for row in cur1.fetchall():
        print "bank: " + str(row)
        ret=cur2.execute(tract_query_str, (row[0],))
        cnt = 0
        for t in cur2.fetchall() :
           print "count"+str(cnt) + ":tract=" + str(t)
           cnt += 1

cur1.close();
cur2.close();
