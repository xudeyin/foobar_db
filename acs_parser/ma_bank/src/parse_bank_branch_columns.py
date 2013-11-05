#!/usr/bin/python
import csv

columns= []

with open('column_head.csv') as csvfile:
    lreader = csv.reader(csvfile,delimiter=',', quotechar='"')
    lncnt=0
    for eachline in lreader :
        colcnt=0
        for c in eachline:
            #print "    " + c.upper().replace("(FILTERED)", "").replace("(ACTUAL)","").replace("(MM/DD/YYYY)", "").replace("($000)", "").replace('&', '').strip(' ').replace("'", "").replace(":", "").replace(",", "").replace("-", "").replace(" ", "_")
            #print c.rstrip()
            colcnt += 1 
            if lncnt == 0:
                columns.append(['c' + str(colcnt).zfill(3), c, 0, 0])
            else :
                if c :
                    columns[colcnt-1][lncnt+1] = int(c)
        lncnt += 1

csvfile.close()

#print columns

fout = open('create_bank_branch_table.sql', 'w')

with open ('bank_branch_column_map.csv', 'w') as csvfile:
    lwrt=csv.writer(csvfile, delimiter=',', quotechar='"')
    for row in columns :
        lwrt.writerow(row)
        fout.write(row[0] + "        varchar(128)\n")

csvfile.close()
fout.close()





