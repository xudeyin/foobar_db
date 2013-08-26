#!/usr/bin/python


import csv
import os
import re
from collections import Counter

##
## fschema: schema file
## fdataset = acs_dataset data file
## fcolumns = acs_dataset_columns
## columnTypeList: [INT4, FOLOAT, ...] Note that it starts with the 2nd column!
## first column is region_id which is always INT4
##
def createTableSchemaFile(tabIdx, fname, columnTypeList, tItem, fschema, fload, wdataset, wcolumns) : 
    cCounter = Counter()
    wdataset.writerow([tabIdx, fname] + tItem[0].strip('\n').split('|')) 
    columns = tItem[1]
    
    fschema.write ("CREATE TABLE " + fname + " (\n")
    
    fschema.write("    " + 'state'.ljust(32) + " CHAR(2) NOT NULL,\n") ## state
    fschema.write("    " + columns[0].ljust(32) + " INT4 NOT NULL,\n") ## region id
    for idx, c in enumerate(columns[1:]) :
        if c[0].isdigit() :
            cName = 'N' + c[:29]
        else :
            cName = c[:30]
        
        cCounter.update([cName])
        appendStr=str(cCounter[cName]-1).zfill(2);
        
        try :
            fschema.write("    " + (cName + appendStr).ljust(32) + " " + columnTypeList[idx] + " DEFAULT 0,\n")
        except IndexError:
            print "    ERROR FORMAT IN TABLE " + fname
            
        wcolumns.writerow([tabIdx, cName + appendStr, c])
        
    fschema.write("    PRIMARY KEY (state, region_id)\n")
    fschema.write(");\n\n")
    
    fload.write("\\COPY " + fname + " FROM \'data/" + fname + ".csv\' DELIMITER \',\' CSV;\n")
    

def isListEmpty(row):
    for r in row :
        if r :
            return 0
    return 1

def cellHasIntValue(s):
    try :
        int(s)
        return True
    except ValueError:
        return False

def removeInvalidChar(row):
    for cnt, c in enumerate(row) :
        if str(c) == "." :
            row[cnt] = None
            
def processColumnTypes(columnTypes, row):
    for cnt, c in enumerate(row) :
        if c and not cellHasIntValue(c) :
                columnTypes[cnt] = 'FLOAT'
            
    
##this function returns a list of column types [INT4,FLOAT,INT4,FLOAT....]
##based on the cell values. if all cell has integer value then the data type for this column is INT4. 
##otherwise it is FLOAT
## note the column type starts with 2nd column. the first column is region_id which is always integer.
def createTableBlockDataFile(srcDir, fname, start, length, columns=None) :
    noHeader = 1
    theT = [ ]
    colDataType=['INT4']*length
    
    fullpath=os.path.join(srcDir, fname + ".txt")
    with open(fullpath) as csvfile:
        lreader = csv.reader(csvfile, delimiter=',', quotechar='"')
        for srcLine in lreader :
            if isListEmpty(srcLine[start: start + length]) == 1 :
                ##skip empty list
                continue
            ## regionID is the first column
            newLine = [srcLine[5]] + srcLine[start:start + length]
            removeInvalidChar(newLine)
            theT.append(newLine)
            
            processColumnTypes(colDataType, newLine[1:])
   
    fullpath = os.path.join(srcDir, fname + "_" + str(start+1) + "_" + str(length) + ".csv")
    with open(fullpath, 'w')  as csvfile:
        tWriter = csv.writer(csvfile, delimiter=',')
        if noHeader == 0 and columns :
            tWriter.writerow(['state'] + columns)
        for newLine in theT:
            tWriter.writerow(['MA'] + newLine)
    return colDataType
           
def main() : 
    SRC_DIR = "/Users/dxu/Mapster/Massachusetts_Tracts_Block_Groups_Only"
    ##SRC_DIR = "/home/dxu/mac_work"
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
    SRC_DIR = "/Users/dxu/Mapster/Massachusetts_Tracts_Block_Groups_Only"
    ##SRC_DIR = "/home/dxu/mac_work"
    fin = open('output/tableList.txt', 'r')
    allTabDict = {}
    tableNames = []
    columns = ['region_id']
    
    for line in fin:
        ##process data block for this table
        if not line :
            continue;
        
        if line.startswith('    ') :
            columns.append(line.strip(' ').strip('\n'))
        else :
            ##new table name found
            ##1. check old tableNames and columns first
            ## Note the columns[] already has at least 1 element : region_id
            if len(tableNames) != 0 and len(columns) >1 :
                for tn in tableNames :
                    allTabDict[tn] = columns
                ##2. reset the tableNames and columns  
                tableNames = []
                columns = ['region_id']
           
            ##add new name to tableNames[]
            tableNames.append(line)
   
    ## process the last table 
    if len(tableNames) != 0 and len(columns) != 0 : 
        for tn in tableNames : 
            allTabDict[tn] = columns

    ##open schema file
    fschema = open("sql/create_acs_tables.sql", "w")
    
    ##open load script file
    fload = open("sql/load_tables.sql", "w");
    
    ##open data file for acs_dataset
    f_acs_dataset = open("sql/acs_dataset.csv", "w")
    writer1 = csv.writer(f_acs_dataset, delimiter=',')
    #open data file for acs_dataset_columns
    f_acs_dataset_columns = open("sql/acs_dataset_columns.csv", "w")
    writer2 = csv.writer(f_acs_dataset_columns, delimiter=',')
    
    tableIdx = 0        
    tabs= allTabDict.items()
    
    fload.write("\\COPY acs_dataset FROM \'data/acs_dataset.csv\' DELIMITER \',\' CSV;\n")
    fload.write("\\COPY acs_dataset_columns FROM \'data/acs_dataset_columns.csv\' DELIMITER \',\' CSV;\n")
   
    ##init a dictionary that stores the column type list
    ##key = table name ( e20115ma0034000_43_5 )
    ##value = [INT4, INT4, FLOAT , ....]
    dbColumnTypeDict = {}
    for tItem in tabs :
        tt = tItem[0];   ##key=tablename, value = column []
        pos = re.findall(r'\d+', tt);
        if len (pos) == 0 :
            continue
        ## file name: e20115ma0002000, m20115ma0113000
        fname = str(pos[0]).zfill(4)
        fname = "e20115ma" + fname +"000"
        print "process file " + fname
        
        ##the source file count field index starting from 1
        colTypeList = createTableBlockDataFile(SRC_DIR, fname, int(pos[1])-1, int(pos[2]), tItem[1])
        
        dbTableName = fname+"_"+pos[1]+"_"+pos[2]
       
        dbColumnTypeDict[dbTableName] = colTypeList
        
        ## generate schema file and assistent table data files
        createTableSchemaFile(tableIdx, dbTableName, colTypeList, tItem, fschema, fload, writer1, writer2) 
        
        tableIdx += 1
        
    fschema.close()
    f_acs_dataset.close()
    f_acs_dataset_columns.close()
    fload.close()
    
    print "Done!"
        
if  __name__ =='__main__':main1()
