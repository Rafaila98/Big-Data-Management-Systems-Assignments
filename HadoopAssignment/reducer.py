#!/usr/bin/env python

import sys
import re

oldcluster = None
sumx = 0
sumy = 0
count = 0

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    #print(line)
    # parse the input we got from mapper.py
    x, y, clusternum = re.split('\s', line)
    # convert count (currently a string) to int
    try:
        x = float(x)
        y = float(y)
    except ValueError:
        # count was not a number, so silently
        # ignore/discard this line
        continue

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: word) before it is passed to the reducer
    if oldcluster == clusternum:
        count += 1
        sumx += x
        sumy += y
    else:
        if count != 0:
            # write result to STDOUT
            print(str(sumx/count) +"," + str(sumy/count))
        oldcluster = clusternum
        sumx = x
        sumy = y
        count =1

# do not forget to output the last word if needed!
if oldcluster == clusternum and count!=0:
    print(str(sumx/count) +"," + str(sumy/count))
