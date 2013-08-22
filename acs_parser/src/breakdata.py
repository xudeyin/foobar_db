#!/usr/bin/python

import csv
import os
import re
    
def createTableBlockDataFile(srcDir, fname, start, length, columns=None) :
    theT = []
    fullpath=os.path.join(srcDir, fname + ".txt")
    with open(fullpath) as csvfile:
        lreader = csv.reader(csvfile, delimiter=',', quotechar='"')
        for srcLine in lreader :
            newLine = [srcLine[5]] + srcLine[start:start + length]
            theT.append(newLine)
   
    fullpath = os.path.join(srcDir, fname + "_" + str(start+1) + "_" + str(length) + ".csv")
    with open(fullpath, 'w')  as csvfile:
        tWriter = csv.writer(csvfile, delimiter=',')
        for newLine in theT:
            tWriter.writerow(newLine)
    return
           
def main() : 
    ##SRC_DIR = "/Users/dxu/Mapster/Massachusetts_Tracts_Block_Groups_Only"
    SRC_DIR = "/home/dxu/mac_work"
    fin = open('output/tables.txt', 'r')

    for line in fin:
        ##process data block for this table
        if not line :
            continue;
        pos = re.findall(r'\d+', line);
        if len (pos) == 0 :
            continue
        ## file name: e20115ma0002000, m20115ma0113000
        fname = str(pos[0]).zfill(4)
        fname = "e20115ma" + fname +"000"
        ##the source file count field index starting from 1
        createTableBlockDataFile(SRC_DIR, fname, int(pos[1])-1, int(pos[2]))
           
def main1() : 
    ##SRC_DIR = "/Users/dxu/Mapster/Massachusetts_Tracts_Block_Groups_Only"
    SRC_DIR = "/home/dxu/mac_work"
    fin = open('output/tableList.txt', 'r')
    allTabDict = {}
    tableNames = []
    columns = []
    
    for line in fin:
        ##process data block for this table
        if not line :
            continue;
        
        if line.startwith('    ') :
            columns.append(line.strip(' '))
        else :
            ##new table name found
            ##1. check old tableNames and columns first
            if len(tableNames) != 0 and len(columns) != 0 :
                for tn in tableNames :
                    allTabDict[tn] = columns
                ##2. reset the tableNames and columns  
                tableNames = []
                columns = []
           
            ##add new name to tableNames[]
            tableNames.append(line)
   
    ## process the last table 
    if len(tableNames) != 0 and len(columns) != 0 : 
        for tn in tableNames : 
            allTabDict[tn] = columns
            
    tabs= allTabDict.items()
    
    for tItem in tabs :
        tt = tItem[0];   ##key=tablename, value = column []
        pos = re.findall(r'\d+', tt);
        if len (pos) == 0 :
            continue
        ## file name: e20115ma0002000, m20115ma0113000
        fname = str(pos[0]).zfill(4)
        fname = "e20115ma" + fname +"000"
        ##the source file count field index starting from 1
        createTableBlockDataFile(SRC_DIR, fname, int(pos[1])-1, int(pos[2]), tItem[1])
        
        
if  __name__ =='__main__':main()