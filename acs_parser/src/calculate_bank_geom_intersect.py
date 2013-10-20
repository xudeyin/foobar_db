#!/usr/bin/python

# this script calculates the tract areas 
import psycopg2
import time
import csv

# SRID 26986 is MA mainland
# method 1: calculate the intersections using ST_DWithin() function.  Need to
# transform the geom into SRID 26986 (or any other SCS that uses meter as the
# unit.
#
# This method is reasonably fast
def getIntersections(cur, point, radius) :
    qstr='select geoid, name from tl_2011_25_bg where ST_DWithin(%s, ST_Transform(geom, 26986), %s)=True order by geoid'

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

    qstr='select geoid, name from tl_2011_25_bg where ST_Distance_Spheroid(%s,geom,%s) < %s order by geoid'
    cur.execute(qstr, (point, spheroid_str, radius))
    cnt = 0
    for t in cur.fetchall() :
        print "    count"+str(cnt) + ":tract=" + str(t)
        cnt += 1


def getTractArea(cur) :
    tractDict={}

    cur.execute("select geoid, ST_Area(ST_Transform(geom, 26986)) from tl_2011_25_bg")

    for row in cur.fetchall() :
        tractDict[row[0]]=row[1];

    return tractDict

def getIntersection3(cur, writer, bank_id, circle, c_area, srid, radius, tractDict) :
    qstr=''' select  
                t.geoid, 
                ST_Area(ST_Intersection(%s, t.tgeom)) as a_intersect 
            from 
                (select geoid, ST_Transform(geom, %s) as tgeom from tl_2011_25_bg) as t 
            where 
                ST_Intersects(%s, t.tgeom) '''

    cur.execute(qstr, (circle, srid, circle))
   
    for t in cur.fetchall() :
        geoid=t[0]
        tractArea=tractDict[geoid]
        writer.writerow([bank_id, radius, geoid, c_area, tractArea, t[1], t[1]/tractArea*100])

def calculateOneRadius(conn, srid, radius, tractDict) :
    cur1=conn.cursor()
    cur2=conn.cursor()
   
    cur1.execute('''SELECT 
                           id, 
                           ST_Buffer(ST_Transform(geom, %s),%s),
                           ST_Area(ST_Buffer(ST_Transform(geom, %s),%s))
                    FROM
                           bank_branch order by id''', (srid, radius, srid, radius))

    with open('intersect_bg_' + str(radius) + '.csv', 'wb') as csvfile :
        mywriter=csv.writer(csvfile) 

        for row in cur1.fetchall():
            getIntersection3(cur2, mywriter, row[0], row[1], row[2], srid, radius, tractDict)

    cur1.close()
    cur2.close()
   
        
def main() :

    current_time_milli= lambda: int(round(time.time() * 1000))

    conn = psycopg2.connect(database='mapster_db', user='mapster')
    
    cur1 = conn.cursor()

    tractDict = getTractArea(cur1)

    cur1.close()

    for radius in xrange(100, 2001, 100) :
        startT=current_time_milli()
        print "Processing radius " + str(radius) +"m. Start Time: " + str(startT)
        calculateOneRadius(conn, 26986, radius, tractDict)
        endT=current_time_milli()
        print "End Time: " + str(endT)
        print "Duration: " + str(endT-startT)
        print ""

if __name__ =='__main__':main()

