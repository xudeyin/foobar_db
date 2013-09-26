#!/usr/bin/python


#import csv
#import os
import sys
import re

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
    
    fout.write  ("copy ( select x.id as region_id")
    
##    for i in range (0, len(tableDict)) :
##        if i != 0 :
##            fout.write (", \n    t" + str(i) + ".region_id")
##        else :
##            fout.write ("    t" + str(i) + ".region_id")
##    
    i = 0
    for k, v in tableDict.iteritems() :
        for c in v :
            fout.write(", \n    t" + str(i) + "." + c + " as "+ k + "_" + c)        
        i += 1
        
    fout.write("\nfrom record_id x,\n")        
            
    #now print
    i = 0
    for k, v in tableDict.iteritems() :
        fout.write ("    full join " + k + " t"  + str(i) + "\n")
        fout.write ("        on x.state=t" + str(i) + ".state and x.id=t" + str(i) + ".region_id\n")
        i += 1

    fout.write(") to '/tmp/join.csv' with DELIMITER ',' QUOTE '\"' csv header;")    
    fin.close()
    fout.close()
    print "Done!!" 
      
if  __name__ =='__main__':main()
