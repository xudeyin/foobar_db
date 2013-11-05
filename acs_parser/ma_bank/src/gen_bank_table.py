#!/usr/bin/python
import csv

columns= []
date_column=[]
bool_column=[]
count_column=[]
money_column=[]

data = []

with open('MA_Data_09-09-13_v2.csv') as csvfile:
    lreader = csv.reader(csvfile,delimiter=',', quotechar='"')
    lncnt=0
    for eachline in lreader :
        if lncnt == 0 or lncnt == 2 or lncnt == 3 or lncnt == 4 :
            lncnt += 1
            continue
        
        if lncnt > 4 :
            data.append([lncnt-4])    
        colcnt=0
        for c in eachline:
            colcnt += 1 
            if lncnt == 1:
                if "? Yes/No" in c :
                    bool_column.append(colcnt)
                elif "(mm/dd/yyyy)" in c:
                    date_column.append(colcnt)
                elif "(actual)" in c:
                    count_column.append(colcnt)
                elif "($000)" in c:
                    money_column.append(colcnt)
            else :
                if colcnt in bool_column :
                    if c :
                        if c.lower() == "yes" :
                            c="true"
                        else :
                            c="false"
                elif colcnt in date_column :
                    if c == "" :
                        c = 'NULL'
                elif colcnt in count_column  or colcnt in money_column :
                    if c :
                        c = c.replace(',', '')
                if c.upper() == 'NA' or c=="" : 
                    c = "NULL"    
                data[lncnt-5].append(c[0:127])

        lncnt += 1

csvfile.close()

#print columns

with open ('bank_branch.csv', 'w') as csvfile:
    lwrt=csv.writer(csvfile, delimiter=',', quotechar='"')
    for row in data :
        lwrt.writerow(row)

csvfile.close()
