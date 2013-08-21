#!/usr/bin/python

import csv
import string


fin = open('output/tables.txt', 'r')

for line in fin:
    ##process data block for this table
    if len(line.strip([' '])) == 0:
        continue;
    pos = [int(s) for s in line.split() if s.isdigit()]
    
    