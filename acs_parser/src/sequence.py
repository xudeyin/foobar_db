#!/usr/bin/python

import csv
import string
import re
from collections import Counter

def recursiveProcessColumns (columnList, counter, indent, level) :
    retStr= ""
    localCounter = Counter()
    colList = []
    for c in columnList :
        if counter[c] <= 1 :
            ##process the bufferred colLIst first
            if len(colList) != 0 :
                ret = recursiveProcessColumns (colList, localCounter, indent + "         ", level + 1)
                retStr = retStr + ret;
                colList = []
                localCounter.clear()
            ##now add the new column
            retStr = retStr + indent + c + "\n"
        else :
            colList.append(c)
            if level >= 2 :
                localCounter[c] = 1
            else :
                localCounter.update([c])

    ## process the last record
    if len(colList) != 0 :
        ret = recursiveProcessColumns(colList, localCounter, indent + "        ", level+1)
        retStr = retStr + ret
    return retStr

def processOneTable(columnList, columnCnt ) :
    ##colStr=""
    colC = Counter()
    colList=[]
    for c in columnList :
        if c[3] :
            if c[4] :
                print "ERROR FORMAT!!"
                return;
            ## collapsing mutliple white spaces
            rex = re.compile(r'\W+');
            s = rex.sub(' ', c[7]).upper()
            ## to upper case, remove "' : - ,"
            s = s.strip(' ').replace("'", "").replace(":", "").replace(",", "").replace("-", "").replace(" ", "_")
            colC.update([s])
            colList.append(s)
            s =  "        " + s;
            print s
            if c[3] == columnCnt :
                break

    colStr = recursiveProcessColumns(colList, colC, "        ", 0);
    return colStr

########################## MAIN ########################
##1. read column C to get all lines with the same number.
##
##2. for each line with the same seq number:
##
##3. read E for start position in e file, F for # of column H for table name
##
##4. keep going to next line until D is not empty.  D should start with 1 and go all the way to "# of columns" found in step 3
##
##5. on the same line read H: that's the column name.
##
##6. go to next line, repeat steps 4 and 5 until "# of columns" is reached.
##
##7. one table is done!
##
##8. repeat 2 - 7
##
##9. repeat from 1
########################## MAIN ########################

def main() : 
    allTableDict = {}
    
    with open('resources/Sequence_Number_and_Table_Number_Lookup.csv') as csvfile:
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
                ret = processOneTable(tableList, rowCnt)
                v = allTableDict.get(ret)
                if not v :
                    v = []
                    allTableDict[ret] = v
                v.append(tableName)
    
                ##start a new table
                tableList = [ ]
                rowCnt=row[5].split(' ', 1)[0];
                tableName = string.zfill(row[2], 3) + " [" + row[4] + "," + rowCnt + "] " + row[7]
                print tableName
                foundTable = 1
            else :
                if foundTable :
                    tableList.append(row)
    
        ##handle the last record
        if len(tableList) != 0 :
            ret = processOneTable(tableList, rowCnt)
            v = allTableDict.get(ret)
            if not v :
                v = []
                allTableDict[ret] = v
            v.append(tableName)
    
    ## write out the dictionary
    fo = open("output/tableList.txt", "w")
    fo1 = open("output/tables.txt", "w")
    ##fo.write("##Total tables needed: "+ str(len(allTableDict)) + "\n\n")
    
    tabs= allTableDict.items()
    
    for tItem in tabs :
        tt = tItem[1];   ## value of the key-value pair is a list of table names that share the same schema (key)
        ##first print out all tables that share the same schema
        for tn in tt :
            fo.write(tn+"\n")
            fo1.write(tn+"\n")
        ##now write out column list
        fo.write(str(tItem[0]));
        ##print tItem[0];
        fo.write("\n")
    fo.close()
    fo1.close()

if  __name__ =='__main__':main()