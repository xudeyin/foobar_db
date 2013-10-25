#!/usr/bin/python

# this script calculates the intersctions of circular buffers with various radius for each bank location and 
# the tract or block groups defined in the TIGER file.
# The calculation is based on SRID 26986 (for Massachusetts mainland)
import psycopg2
import time
import csv
import argparse

def getCurrentTime() :
    return int(round(time.time() * 1000))

def getTigerFileType(sType) :
    if sType == 'tract' :
	return 1
    elif sType == 'bg' :
        return 2
    return -1
        
def getTractArea(cur, t) :
    tractDict={}

    cur.execute("select geoid, ST_Area(geom26986) from tl_2011_25_26986 where type=%s", (getTigerFileType(t),))

    for row in cur.fetchall() :
        tractDict[row[0]]=row[1];

    return tractDict

def getIntersection3(cur, writer, bank_id, circle, c_area, srid, radius, t, tractDict) :
    qstr=''' select  
                t.geoid, 
                ST_Area(ST_Intersection(%s, t.geom26986)) as a_intersect 
            from 
                tl_2011_25_26986 t
            where 
                ST_Intersects(%s, t.geom26986)  and t.type = %s'''

    cur.execute(qstr, (circle, circle, getTigerFileType(t)))
   
    for t in cur.fetchall() :
        geoid=t[0]
        tractArea=tractDict[geoid]
        writer.writerow([bank_id, radius, geoid, c_area, tractArea, t[1], t[1]/tractArea*100])

def calculateOneRadius(conn, srid, radius, sType, tractDict) :
    cur1=conn.cursor()
    cur2=conn.cursor()
  
    t1 = getCurrentTime(); 
    cur1.execute('''SELECT 
                           id, 
                           ST_Buffer(ST_Transform(geom, %s),%s),
                           ST_Area(ST_Buffer(ST_Transform(geom, %s),%s))
                    FROM
                           bank_branch order by id''', (srid, radius, srid, radius))

##    print "    T1=" + str(getCurrentTime()-t1)
    with open('intersect_' + sType + '_' + str(radius) + '.csv', 'wb') as csvfile :
        mywriter=csv.writer(csvfile) 

        for row in cur1.fetchall():
            t1 = getCurrentTime(); 
            getIntersection3(cur2, mywriter, row[0], row[1], row[2], srid, radius, sType, tractDict)
##            print "        T2=" + str(getCurrentTime()-t1)

    cur1.close()
    cur2.close()
   
        
def main() :

    parser = argparse.ArgumentParser(description='Bank location buffer and tract/bg intersection calculation based on SRID 26986')
    parser.add_argument('start_radius',  type=int,  help='Starting radius')
    parser.add_argument('end_radius', type=int,  help='End radius')
    parser.add_argument('step', type=int, help='Step')
    parser.add_argument('type', choices=['tract', 'bg'],  help='process tract or block group')
    args = vars(parser.parse_args())

    print args

    current_time_milli= lambda: int(round(time.time() * 1000))

    conn = psycopg2.connect(database='mapster_db', user='mapster')
    
    cur1 = conn.cursor()

    tractDict = getTractArea(cur1, args['type'])

    cur1.close()

    for radius in xrange(args['start_radius'], args['end_radius'] + 1, args['step']) :
        startT=current_time_milli()
        print "Processing radius " + str(radius) +"m. Start Time: " + str(startT)
        calculateOneRadius(conn, 26986, radius,  args['type'], tractDict)
        endT=current_time_milli()
        print "End Time: " + str(endT)
        print "Duration: " + str(endT-startT)
        print ""

if __name__ =='__main__':main()

