#!/usr/bin/python

# this script calculates the intersctions of circular buffers of each bank location and 
# the tract or block groups defined in the TIGER file. 
# The calculation is based on SRID 26986 (for Massachusetts mainland)
import os
import psycopg2
import time
import csv
import argparse
import ConfigParser
import StringIO
import csv
#from UnicodeUtil import UnicodeWriter

def getCurrentTime() :
    return int(round(time.time() * 1000))

def getTigerFileType(sType) :
    if sType == 'tract' :
        return 140
    elif sType == 'bg' :
        return 150
    return -1
        
def getTractArea(cur, t) :
    tractDict={}

    cur.execute("select geoid, ST_Area(geom26986) from tl_2011_transform where sumlev=%s", (getTigerFileType(t),))

    for row in cur.fetchall() :
        tractDict[row[0]]=row[1];

    return tractDict

def getIntersection(conn, csvWriter, cur1, cur2, intersectionTableName, bank_id, circle, c_area, srid, radius, t, tractDict) :
    qstr1=''' select  
                t.geoid, 
                t.logrecno,
                ST_Area(ST_Intersection(%s, t.geom26986)),
                ST_Multi(ST_Intersection(%s, t.geom26986))
            from 
                tl_2011_transform t
            where 
                ST_Intersects(%s, t.geom26986)  and t.sumlev = %s'''

    cur1.execute(qstr1, (circle, circle, circle, getTigerFileType(t)))

    for t in cur1.fetchall() :
        geoid=t[0]
        tractArea=tractDict[geoid]
        csvWriter.writerow((bank_id, radius, geoid, t[1], c_area, tractArea, t[2], t[2]/tractArea*100, srid, t[3]))

def calculateOneRadius(conn, csvWriter, intersectTableName, srid, radius, sType, tractDict) :
    cur1=conn.cursor()
    cur2=conn.cursor()
  
    t1 = getCurrentTime(); 
    cur1.execute('''SELECT 
                           id, 
                           ST_Buffer(ST_Transform(geom, %s),%s),
                           ST_Area(ST_Buffer(ST_Transform(geom, %s),%s))
                    FROM
                           bank_branch order by id''', (srid, radius, srid, radius))

    for row in cur1.fetchall():
        getIntersection(conn, csvWriter, cur1, cur2, intersectTableName, row[0], row[1], row[2], srid, radius, sType, tractDict)

    cur1.close()
    cur2.close()

def queryACSTables(conn, tDict, intersectionTableName, resultTableName) :
 
    ## construct query statement for the _detail table.
    qStr1 = 'insert into ' + resultTableName + '_detail \nselect t0.bank_id\n, t0.radius\n, t0.geoid\n, t0.logrecno\n'

    i = 1
    for tName in sorted(tDict) :
        if tName=='bank_branch' :
            continue
        for col in tDict[tName] :
            qStr1 = qStr1 + ', t' + str(i) + '.' + col[0] + '*t0.ratio/100\n'
        i += 1
    qStr1 = qStr1 + 'from ' + intersectionTableName + ' t0\n'

    i = 1
    for tName in sorted(tDict) :
        if tName=='bank_branch' :
            continue
        qStr1 = qStr1 + 'left join ' + tName + ' t' + str(i) + ' on t0.logrecno=t' + str(i) + '.region_id\n'
        i += 1

    print qStr1

    cur = conn.cursor()
    cur.execute(qStr1)
    conn.commit()

    ## now populate the result table.

    qStr2='( select bank_id, radius'

    for tName in sorted(tDict) :
        if tName=='bank_branch' :
            continue
        for col in tDict[tName] :
            qStr2 = qStr2 + ', sum(' +  col[1] + ') as ' + col[1]

    qStr2 = qStr2 + '\nfrom ' + resultTableName + '_detail group by bank_id, radius ) t '
  
    qStr1='insert into ' + resultTableName + '\nselect t.bank_id, t.radius'
    if 'bank_branch' in tDict : 
        for c in tDict['bank_branch'] :
            qStr1 = qStr1 +  ', b.' + c[0] 

    for tName in sorted(tDict) :
        if tName=='bank_branch' :
            continue
        for col in tDict[tName] :
            qStr1 = qStr1 + ', t.' +  col[1]

    qStr1 = qStr1 + '\nfrom ' + qStr2

    if 'bank_branch' in tDict :
        qStr1 = qStr1 + ', bank_branch b where t.bank_id = b.id'

    print qStr1
    cur.execute(qStr1)
    conn.commit()

    
def createResultTable(conn, tDict, tablename) :

    qStr0 = 'drop table if exists ' + tablename + '_detail cascade'
    qStr00 = 'drop table if exists ' + tablename + ' cascade'
 
    qStr = 'create table ' + tablename + '''_detail (
bank_id                int4            not null,
radius                 float           not null,
geoid                  varchar(64)     not null,
logrecno               int4            not null,
'''
    
    for tName in sorted(tDict) :
        if tName=='bank_branch' :
            continue
        for col in tDict[tName] :
            qStr = qStr + col[1] + '            float,\n'

    qStr = qStr + 'primary key(bank_id, radius, geoid)\n )'

    print qStr

    qStr1 = 'create table ' + tablename + ''' (
bank_id                int4            not null,
radius                 float           not null,
'''
    if 'bank_branch' in tDict : 
        for c in tDict['bank_branch'] :
            qStr1 = qStr1 +  c[1] + '             varchar(128),\n'

    for tName in sorted(tDict) :
        if tName=='bank_branch' :
            continue
        for col in tDict[tName] :
            qStr1 = qStr1 + col[1] + '            float,\n'
    qStr1 = qStr1 + 'primary key(bank_id, radius)\n )'
   
    print qStr1

    cur = conn.cursor()
    cur.execute(qStr0)
    cur.execute(qStr00)
    cur.execute(qStr)           ## tablename_detail
    cur.execute(qStr1)          ## tablename
    conn.commit()

    print 'Table ' + tablename + '_detail created.'
    print 'Table ' + tablename + ' created.'

          
def createIntersectionTable(conn, tName, srid) :
    qstr0 = 'drop table if exists ' + tName + ' cascade'

    qstr= 'create table ' + tName +  ''' (
            bank_id                int4         not null,
            radius                 float        not null,
            geoid                  varchar(64)  not null,
            logrecno               int4         not null,
            area1                  float,
            area2                  float,
            intersect_area         float,
            ratio                  float,
            srid                   int4,
            primary key(bank_id, radius, geoid)
        )'''


    qstr1 = "SELECT AddGeometryColumn(%s, 'geom', %s, 'MULTIPOLYGON',2)"
    qstr2 = 'create index idx_' + tName + ' on ' + tName + ' (logrecno)'

    cur=conn.cursor()
    cur.execute(qstr0)

    cur.execute(qstr)
    cur.execute(qstr1, (tName, srid))
    cur.execute(qstr2)

    conn.commit()
    cur.close()
    print "==>Intersection table created" 

def parseOutputColumnName(arr) :
    if not arr[2] :
        arr[2] = arr[0] + '_c' + arr[1][-3:]

def parseTableSelectionStr(tDict, columnStr) :
    arr = columnStr.split(',')
    for a in arr :
        b = a.strip('[').strip(']').strip(' ')
        cArr=b.split(':')
        parseOutputColumnName(cArr)
        tKey=cArr[0]
        if tKey in tDict :
            tDict[tKey].append(cArr[1:])
        else :
            tDict[tKey] = [cArr[1:]]

    print tDict
 

def doIntersectionCal(conn, intersectTableName, gType, start, end, step, srid, shortCommitFlag) :

    cur1 = conn.cursor()

    tractDict = getTractArea(cur1, gType)


    output = StringIO.StringIO()
    writer = csv.writer(output)

    for radius in xrange(start, end + 1, step) :
        startT=getCurrentTime()
        print "==>Processing radius " + str(radius) +"m. Start Time: " + str(startT)
        calculateOneRadius(conn, writer, intersectTableName, srid, radius,  gType, tractDict)
        if shortCommitFlag :
            output.seek(0) 
            cur1.copy_from(output, intersectTableName, sep=',')
            conn.commit()
            output = StringIO.StringIO()
            writer = csv.writer(output)

        endT=getCurrentTime()
        print "==>End Time: " + str(endT)
        print "==>Duration: " + str(endT-startT)
        print ""

    output.seek(0) 
    cur1.copy_from(output, intersectTableName, sep=',')
    conn.commit()
    cur1.close()

def createOutputFile(conn, outputTableName, outputFileName) :
    print '==>Copy to ' + outputFileName
    qStr = 'copy ' + outputTableName + " to '" + \
            os.path.abspath(outputFileName) + \
            "' WITH CSV HEADER"

    print qStr
    cur = conn.cursor()
    ##fout=open(outputFileName, 'w')
    ##cur.copy_expert(qStr, fout)
    cur.execute(qStr)

    cur.close()
    print '==>Output file generated'

#    qStr = 'copy ' + outputTableName + " to '" + outputFileName + "' with CSV HEADER"
#    cursor.execute(qStr)
 

def main() :

    parser = argparse.ArgumentParser(description='Bank location and ACS data analysis based on SRID 26986 (Massachusetts mainland)')
    parser.add_argument('config_file', type=str,  help='Path to the configuration file')
    parser.add_argument('-r', action='store_true', help='Re-calculate the intersection table. By default the program uses the existing table defined in the configuration file.')
    parser.add_argument('-tiger', choices=['tract','bg', 'all'], default='all', help='Which TIGER file(s) to use in the spatial calculation. Default = ALL')
    parser.add_argument('-shortcommit', action='store_true', help='Invoke database commit for each radius calculation. Use this option on small server with limited resources')

    args = vars(parser.parse_args())

    print args

    config=ConfigParser.RawConfigParser()
    config.read(args['config_file'])

    gType=args['tiger']

    intersectionTableName=config.get('database', 'intersection.table')
    minRadius=config.getint('bank', 'min.radius')
    maxRadius=config.getint('bank', 'max.radius')
    step=config.getint('bank', 'radius.step')
    srid = config.getint('GIS', 'srid.MA')

    isOutputToFile = config.getboolean('output', 'output.wirte.to.file')
    outputFileName = config.get('output', 'output.file.name')

    columnStr=config.get('output', 'output.column.list')
    outputTableName = config.get('output', 'output.db.table.name')

    ## use this dictionary to store the selected table and columns.
    ## the key is table name
    ## the value is a list of [table.column_name, output_column_name]
    tableSelectionDict={}
    parseTableSelectionStr(tableSelectionDict, columnStr)

    print intersectionTableName

    conn = psycopg2.connect(database='mapster_db', user='mapster')

    shortCommit=args['shortcommit']

    ## calculate spatial intersections
    if args['r'] :
        createIntersectionTable(conn, intersectionTableName, srid)
        if gType=='tract' or gType=='all' :
            doIntersectionCal(conn, intersectionTableName, 'tract', minRadius, maxRadius, step, srid, shortCommit)
        if gType=='bg' or gType=='all' :
            doIntersectionCal(conn, intersectionTableName, 'bg', minRadius, maxRadius, step, srid, shortCommit)


    ## calculate acs data based on spatial analysis
    createResultTable(conn, tableSelectionDict, outputTableName)
    queryACSTables(conn, tableSelectionDict, intersectionTableName, outputTableName)

    ## output to CSV file
    if isOutputToFile :
        createOutputFile(conn, outputTableName, outputFileName)


    print '==> Done <=='
#### end of main ####

if __name__ =='__main__':main()
