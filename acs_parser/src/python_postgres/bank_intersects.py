#!/usr/bin/python

# this script calculates the tract areas 
import psycopg2
import time

# SRID 26986 is MA mainland
# method 1: calculate the intersections using ST_DWithin() function.  Need to
# transform the geom into SRID 26986 (or any other SCS that uses meter as the
# unit.
#
# This method is reasonably fast
def getIntersections(cur, point, radius) :
    qstr='select geoid, name from tl_2011_25_tract where ST_DWithin(%s, ST_Transform(geom, 26986), %s)=True order by geoid'

    cur.execute(qstr, (point, radius))
    cnt = 0
    for t in cur.fetchall() :
        print "    count"+str(cnt) + ":tract=" + str(t)
        cnt += 1

# spheroid_str='SPHEROID["GRS 1980",6378137,298.257222101]'
# this is the spheroid for SRID 4269.  (the one used in the database)
# method 2: calculate the intersections using ST_Distance_Spheroid() and the
# above spheroid 
# (ST_Distance_Sphere() can also be used, with less accuracy.
#
# this method is EXTREMELY slow!!!
def getIntersections2(cur, point, radius) :
    spheroid_str='SPHEROID["GRS 1980",6378137,298.257222101]'

    qstr='select geoid, name from tl_2011_25_tract where ST_Distance_Spheroid(%s,geom,%s) < %s order by geoid'
    cur.execute(qstr, (point, spheroid_str, radius))
    cnt = 0
    for t in cur.fetchall() :
        print "    count"+str(cnt) + ":tract=" + str(t)
        cnt += 1


def main() :

    current_time_milli= lambda: int(round(time.time() * 1000))

    conn = psycopg2.connect(database='mapster_db', user='mapster')
    
    cur1 = conn.cursor()
    cur2 = conn.cursor()
    cur3 = conn.cursor()
    
    row = cur1.execute("SELECT ST_Transform(geom, 26986), geom, id FROM bank_branch order by id")
    
    #this is the spheroid id string for SRID=4269 (the one currently used in the db)
    
    tract_query_str = 'select geoid, name from tl_2011_25_tract where ST_Within(%s, geom)=True'
#    qstr='select geoid, name from tl_2011_25_tract where ST_DWithin(ST_Transform(%s, 26986), ST_Transform(geom, 26986), %s)=True'
    
    for row in cur1.fetchall():
        print "bank_id: " + str(row)
        startT=current_time_milli()
        getIntersections(cur2, row[0], 1000)
        print "    timer1: " + str(current_time_milli()-startT)
        print "    ========================"
        startT=current_time_milli()
        getIntersections2(cur3, row[1], 1000)
        print "    timer2 " + str(current_time_milli()-startT)
        print ""


    
    cur1.close();
    cur2.close();
    cur3.close();

if __name__ =='__main__':main()

