#!/usr/bin/python

import csv
import string
from collections import OrderedDict

def cellHasIntValue(s):
    try :
        int(s)
        return True
    except ValueError:
        return False

def processOneTable(columnList, columnCnt ) :
    i  = 0
    wantedColumns = []
    for c in columnList :
        ##sometimes c[3] has a value of 0.5. Dont know how to handle it. just skip it.
        if c[3] and cellHasIntValue(c[3]) :
            if c[4] :
                print "ERROR FORMAT!!"
                return;
            i += 1 
            if len(c) > 11 :
                if not cellHasIntValue(c[11]) :
                    continue
                   
                wanted = int(c[11])
                if wanted == 1 or wanted == 2 or wanted == 3:
                    print "        column" + str(i).zfill(3) + "    " + c[11] 
                    wantedColumns.append(["column" + str(i).zfill(3), c[11]])
            if c[3] == columnCnt :
                break
    return wantedColumns
            

########################## MAIN ########################
## Generate data columns we are interested in
## column L (index=11) is the "wanted" column
## column B (index=1) is the table name column
########################## MAIN ########################

def main() : 
    allTableDict = {}
    
    with open('resources/Sequence_Number_and_Table_Number_Lookup_Wanted.csv') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=',', quotechar='"')
        rowN = 0;   ##row count in the whole file
        rowCnt = 0;  ##number of columns
        foundTable = 0;
        tableList = [];
        tableName = "";
        for row in spamreader :
            rowN += 1;
            if row[4] and row[5] :
                ##found a new table. dump the old one out first
                ##if tableName.startswith("074 [36,59] PRESENCE OF OWN CHILDREN UNDER 18 YEARS BY FAMILY TYPE BY EMPLOYMENT STATUS") :
                ##    print "break!!!!"
                wantedColumns = processOneTable(tableList, rowCnt)
                
                if len(wantedColumns) > 0 :
                    allTableDict [tableName] = wantedColumns
    
                ##start a new table
                tableList = [ ]
                rowCnt=row[5].split(' ', 1)[0];
                tableName = row[1] + " " + string.zfill(row[2], 3) + " [" + row[4] + "," + rowCnt + "] " + row[7] + "|" + row[8]
                print tableName
                foundTable = 1
            else :
                if foundTable :
                    tableList.append(row)
    
        ##handle the last record
        if len(tableList) != 0 :
            wantedColumns = processOneTable(tableList, rowCnt)
        if len(wantedColumns) > 0 :
            allTableDict [tableName] = wantedColumns
    
    ## write out the dictionary
    fo = open("output/wanted_tableList.txt", "w")
    for tname, columns in OrderedDict(sorted(allTableDict.items())).iteritems() :
        fo.write(tname + "\n")
        for c in columns :
            fo.write("    " + c[0] + "    " + c[1] + "\n")
    fo.close()
    
    print "======Done!!======"

if  __name__ =='__main__':main()
