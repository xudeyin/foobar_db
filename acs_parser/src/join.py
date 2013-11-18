#!/usr/bin/python


import sys
import re
from collections import OrderedDict

def main() : 

    fin = open('output/wanted_tableList.txt', 'r')
  
    rowCnt= 50
    if len(sys.argv) > 1 and sys.argv[1] :
        rowCnt = int(sys.argv[1])

    print "Join " + str(rowCnt) + " tables."
    tableDict = {} 
    columns = []
    tableName=None 
    for line in fin:
        if line.startswith('B') : ## new line
            if tableName :
                tableDict[tableName] = columns

            columns = []
            tableName = line.split(' ')[0]
        else :  ## columns
            #cols = line.split(' ')
            cols = re.split(' +|\t|\*|\s', line.strip())
            if int(cols[1]) != 1 :
                tableName = None
            else :
                columns.append(cols[0])

             
    ## process the last record 
    if tableName : 
        tableDict[tableName] = columns
        
    
    fout = open("output/join.sql", "w")
    fout_view = open("output/acs_view.sql", "w")
    fout_tract = open("output/gen_tract.sh", "w")
    fout_bg = open("output/gen_bg.sh", "w")
    
    fout_view.write("create view acs_view_p1 as select x.state, x.id as region_id") 
    fout.write  ("copy ( select x.id as region_id")

    fout_tract.write("#!/bin/bash\n") 
    fout_tract.write('pgsql2shp -f acs_p1_tract -h localhost -u mapster -P mapster mapster_db "select c.geoid, c.geom')

    fout_bg.write("#!/bin/bash\n") 
    fout_bg.write('pgsql2shp -f acs_p1_blockgroup -h localhost -u mapster -P mapster mapster_db "select c.geoid, c.geom')
    
##    for i in range (0, len(tableDict)) :
##        if i != 0 :
##            fout.write (", \n    t" + str(i) + ".region_id")
##        else :
##            fout.write ("    t" + str(i) + ".region_id")
##    
    i = 0
    for k, v in OrderedDict(sorted(tableDict.items())).iteritems() :
        print k + " size  = " + str(len(v))
        for c in v :
            c1 = c[6:]
            fout_view.write(", \n    t" + str(i) + "." + c + " as "+ k + "_" + c1)        
            fout.write(", \n    t" + str(i) + "." + c + " as "+ k + "_" + c1)        
            fout_tract.write(",    \na." + k + "_" + c1)        
            fout_bg.write(",     \na." + k + "_" + c1)        
            
        i += 1
        
    fout.write("\nfrom record_id x\n")        
    fout_view.write("\nfrom record_id x\n")        

    fout_tract.write("\nfrom acs_view_p1 a, vg20115 b, tl_2011_25_tract c ")
    fout_bg.write("\nfrom acs_view_p1 a, vg20115 b, tl_2011_25_bg c ")
            
    i = 0
    for k, v in OrderedDict(sorted(tableDict.items())).iteritems() :
        fout.write ("    full join " + k + " t"  + str(i) + "\n")
        fout.write ("        on x.state=t" + str(i) + ".state and x.id=t" + str(i) + ".region_id\n")
        fout_view.write ("    full join " + k + " t"  + str(i) + "\n")
        fout_view.write ("        on x.state=t" + str(i) + ".state and x.id=t" + str(i) + ".region_id\n")
        i += 1

    fout.write(") to '/tmp/join.csv' with DELIMITER ',' QUOTE '\"' csv header;")    
    fout_view.write(";\n\n")


    fout_view.write("create view vacs_geo_p1 as select substr(b.geocode, 8) as geoid, a.* \n")
    fout_view.write("from acs_view_p1 a, vg20115 b where a.state=b.state and a.region_id=b.region_id;")

    fout_tract.write('\nwhere a.state=b.state and a.region_id=b.region_id and substr(b.geocode,8)=c.geoid"')
    fout_bg.write('\nwhere a.state=b.state and a.region_id=b.region_id and substr(b.geocode,8)=c.geoid"')

    fin.close()

    fout.close()
    fout_view.close()
    fout_tract.close()
    fout_bg.close()

    print "Done!!" 
      
if  __name__ =='__main__':main()
