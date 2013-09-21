#!/usr/bin/python


import csv
import os
import re
from acs_table import ACSFile, ACSTable
from collections import Counter
from collections import OrderedDict

def isListEmpty(row):
    for r in row :
        if r :
            return 0
    return 1

def addOneValue(row, val) : 
    if not val or val == '.':
        row.append('')
    else :
        row.append(val)

def cellHasIntValue(s):
    try :
        int(s)
        return True
    except ValueError:
        return False

def processColumnTypes(columnTypes, row):
    for cnt, c in enumerate(row) :
        if c and not cellHasIntValue(c) :
            columnTypes[cnt] = 'FLOAT'
            
            
def removeInvalidChar(row):
    for cnt, c in enumerate(row) :
        if str(c) == "." :
            row[cnt] = None
            
def createTableBlockData(dataDict, srcDir, fname, start, length) :
    colDataType=['INT4'] * length

    fullpath=os.path.join(srcDir, fname + ".txt")
    with open(fullpath) as csvfile:
        lreader = csv.reader(csvfile, delimiter=',', quotechar='"')
        for srcLine in lreader :
            if isListEmpty(srcLine[start: start + length]) == 1 :
                continue
            newLine = srcLine[start:start + length]
            removeInvalidChar(newLine)
            key = srcLine[5]
            if key in dataDict :
                v = dataDict[key]
                dataDict[key] = v + newLine
            else :
                dataDict[key] = newLine
 
            processColumnTypes(colDataType, newLine)

    return colDataType


def writeTableBlockData(statePrefix, dataDict, srcDir, tname, columns=None) :
    noHeader = 1
    fullpath = os.path.join(srcDir + "/output/", tname + ".csv")
    with open(fullpath, 'w')  as csvfile:
        tWriter = csv.writer(csvfile, delimiter=',')
        if noHeader == 0 and columns :
            tWriter.writerow(columns)
        for k, v in dataDict.iteritems() :
            tWriter.writerow([statePrefix, k] + v)
    
 
                        
def createTableBlockDataFile_1(statePrefix, srcDir, tname, fname, start, length, columns=None) :
    noHeader = 1
    theT = []
    colDataType=['INT4'] * length

    fullpath=os.path.join(srcDir, fname + ".txt")
    with open(fullpath) as csvfile:
        lreader = csv.reader(csvfile, delimiter=',', quotechar='"')
        for srcLine in lreader :
            if isListEmpty(srcLine[start: start + length]) == 1 :
                continue
            #newLine = [statePrefix] + [srcLine[5]] + srcLine[start:start + length]
            newLine = [statePrefix] + [srcLine[5]]
            for v in srcLine[start:start + length] :
                addOneValue(newLine, v)
            theT.append(newLine)
 
            processColumnTypes(colDataType, newLine[2:])
             
    ##fullpath = os.path.join(srcDir, fname + "_" + str(start+1) + "_" + str(length) + ".csv")
    fullpath = os.path.join(srcDir + "/output/", tname + ".csv")
    with open(fullpath, 'a')  as csvfile:
        tWriter = csv.writer(csvfile, delimiter=',')
        if noHeader == 0 and columns :
            tWriter.writerow(columns)
        for newLine in theT:
            tWriter.writerow(newLine)
            
    return colDataType;
           
def main() : 
    ##SRC_DIR = "/Users/dxu/Mapster/Massachusetts_Tracts_Block_Groups_Only"
    SRC_DIR = "/home/dxu/mac_work"
    fin = open('output/tableList.txt', 'r')
    allTabDict = {}   ## key= table name (Bxxxxx) val = ACSTable object
    
    tableNames = []
    columns = []
    combinedTabDict = {} ## key= table name (Bxxxxx) val = ACSTable object
     
    for line in fin:
        ##process data block for this table
        if not line :
            continue;
       
        line = line.rstrip() 
        
        if len(line) == 0 :
            continue;
        
        if line.startswith('    ') :
            columns.append(line.lstrip())
        else :
            ##this is a table line.
            ##if tablename list is NOT empty and column list already has elements
            ##  then we found a new table name block.  process the old block first
            if len(tableNames) != 0 and len(columns) > 0 :
                for tn in tableNames :
                    allTabDict[tn] = columns
                ##2. reset the tableNames and columns  
                tableNames = []
                columns = []
           
            ##add new name to tableNames[]
            tableNames.append(line)
   
    ## process the last table block
    if len(tableNames) != 0 and len(columns) != 0 : 
        for tn in tableNames : 
            allTabDict[tn] = columns

    combineTables(allTabDict, combinedTabDict)
    
    ##open schema file
    fschema = open("sql/create_acs_tables.sql", "w")
    
    ##open load script file
    fload = open("sql/load_tables.sql", "w");
    
    fdrop = open("sql/drop_acs_data_tables.sql", "w")
    
    ##open data file for acs_dataset
    f_acs_dataset = open("sql/acs_dataset.csv", "w")
    writer1 = csv.writer(f_acs_dataset, delimiter=',')
    
    f_acs_dataset_columns = open("sql/acs_dataset_columns.csv", "w")
    writer2 = csv.writer(f_acs_dataset_columns, delimiter=',')
    #open data file for acs_dataset_columns
    createACSTableData(combinedTabDict, writer1,writer2)
    f_acs_dataset.close()
    f_acs_dataset_columns.close()

    ##process data files 
  
    colTypeDict = {}
     
    for tname, tObj in OrderedDict(sorted(combinedTabDict.items())).iteritems() :
        print "Process table " + tname + "..."
        tableColTypes = []
        dataDict = {}
        for fObj in tObj.fileList :
            ##tmp=createTableBlockDataFile(prefix, SRC_DIR, tname, fObj.name, fObj.startPos -1, fObj.length)
            tmp=createTableBlockData(dataDict, SRC_DIR, fObj.name, fObj.startPos -1, fObj.length)
            tableColTypes += tmp
        
        colTypeDict[tname] = tableColTypes
        writeTableBlockData('MA', dataDict, SRC_DIR, tname)

    createACSTableSchema(combinedTabDict, colTypeDict, fschema, fload, fdrop) 
    fschema.close()
    fload.close()
    fdrop.close()
    
    print "Done." 


def createACSTableSchema(outDict, colTypeDict, fschema, fload, fdrop) :
    fschema.write ("create table acs_dataset (\n")
    fschema.write ("    state                     char(2) not null,\n")
    fschema.write ("    table_name                varchar(32) not null,\n")
    fschema.write ("    file_name                 varchar(32) not null,\n")
    fschema.write ("    start_pos                 int4 not null,\n")
    fschema.write ("    length                    int4 not null,\n")
    fschema.write ("    description               varchar(256) not null,\n")
    fschema.write ("    subject_area              varchar(128)\n")
    fschema.write (");\n\n")
    
    fschema.write ("create table acs_dataset_columns (\n")
    fschema.write ("    state                     char(2) not null,\n")
    fschema.write ("    table_name                varchar(32) not null,\n")
    fschema.write ("    column_name               varchar(32) not null,\n")
    fschema.write ("    description               varchar(256) not null,\n")
    fschema.write ("    primary key (state, table_name, column_name)\n")
    fschema.write (");\n\n")
    
    fload.write("\\COPY acs_dataset FROM \'data/acs_dataset.csv\' DELIMITER \',\' CSV;\n")
    fload.write("\\COPY acs_dataset_columns FROM \'data/acs_dataset_columns.csv\' DELIMITER \',\' CSV;\n")
    
    for tname, tObj in OrderedDict(sorted(outDict.items())).iteritems() :
        fschema.write("create table " + tname + " (\n")
        fschema.write("    state            char(2) not null,\n")
        fschema.write("    region_id        int4 not null,\n")
        colTypes = colTypeDict[tname]

        i = 0 
        for c in tObj.columns :
            i += 1
            fschema.write("    column" + str(i).zfill(3) + "        " + colTypes[i-1] + ",\n")
        fschema.write("    primary key (state, region_id)\n")
        fschema.write(");\n\n")
        fload.write("\\COPY " + tname + " FROM \'data/" + tname + ".csv\' DELIMITER \',\' CSV;\n")
        fdrop.write("drop table " + tname + ";\n")
        
    
def createACSTableData(outDict, csvWriter, csvColumnWriter ):
    for tname, tObj in OrderedDict(sorted(outDict.items())).iteritems() :
        for fObj in tObj.fileList :
            csvWriter.writerow(['MA', tname, fObj.name, fObj.startPos, fObj.length, tObj.desc, tObj.subject_area])
            
        i = 0    
        for c in tObj.columns :
            i += 1
            csvColumnWriter.writerow(['MA', tname, 'column' + str(i).zfill(3), c])
## inDict : key = table line in tableList.txt, value = columns[]
## outDict: key=  tableName (Bxxxxxx), value = ACSTable object
def combineTables(inDict, outDict) : 
    for line, columns in OrderedDict(sorted(inDict.items())).iteritems() :
        fields = line.split();
        pos = re.findall(r'\d+', line);
        
        if len (pos) == 0 :
            continue
        
        if not fields or len(fields) < 2:
            continue
        
        fname = fields[1].zfill(4)
        fname = "e20115ma" + fname +"000"
        ##print "process file " + fname
        
        f = ACSFile(fname)
        f.startPos = int(pos[2])
        f.length = int(pos[3])
        
        if fields[0] in outDict :
            ## process exsisting table
            t = outDict[fields[0]]
            t.fileList.append(f)
            t.columns += columns
            print "    " + t.name + ": column cnt = " + str(len(t.columns))
        else :  ## table already exist
            t = ACSTable(fields[0]);
            outDict[t.name] = t
            t.fileList= [f]
            t.columns = []
            t.columns += columns
            idx = str.find(line, '] ') 
            t.desc = line[idx+2 :].strip(' ').strip('\n');
            ss = t.desc.split('|')
            t.desc = ss[0]
            t.subject_area=ss[1]
        
if  __name__ =='__main__':main()
