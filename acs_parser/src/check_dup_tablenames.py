#!/usr/bin/python


def main() : 
    tables = {}
    
    fin = open('output/tables.txt', 'r')

    for line in fin:
        ##process data block for this table
        if not line :
            continue;
        words=line.split()
        if words and len(words) > 1 :
            if words[0] not in tables :
                tables[words[0]] = 1
            else :
                i = tables[words[0]];
                i += 1
                print "table name " + words[0] + " appeared " + str(i) + " times."
                tables[words[0]] = i;
                
    fin.close();
  
    print "=========================================================" 
    for k, v in tables.iteritems() :
        if( v > 1) :
            print "table name " + k + " appeared " + str(v) + " times."
        

if  __name__ =='__main__':main()