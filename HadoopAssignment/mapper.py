#!/usr/bin/env python

import sys
from math import sqrt
import pprint


centerfile = 'centers.txt'

def read_centers(file):
    centers=[]
    with open(file) as cf:
        line = cf.readline()
        while line:
            if line :
                try:
                    line = line.strip()
                    #print(line)
                    coords = line.split(',')
                    #print(coords[1])
                    centers.append([float(coords[0]),float(coords[1])])
                except:
                    break
            else:
                break
            line = cf.readline()
    cf.close()
    del cf
    return centers

def euclidiandistance(x, y, cx, cy):
	#Calculate euclidian distance between two coordinates
    distance = sqrt(pow(x - cx,2) + pow(y - cy,2))
    return distance
#print(read_centers(centerfile))
centers = read_centers(centerfile)
for line in sys.stdin:
    line = line.strip()
    coords = line.split(',')
    cluster=-1
    mindist=1000000000
    for center in centers:
        try:
            x = float(coords[0])
            y = float(coords[1])
        except ValueError:
            continue

        dist = euclidiandistance(x,y,center[0],center[1])
        if dist <= mindist:
            mindist = dist
            cluster = centers.index(center)
    #result = "%.6f%.6f%d" %()
    print(x, y, cluster)
