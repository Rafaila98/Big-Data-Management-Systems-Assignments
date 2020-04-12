#!/bin/bash

hadoop-2.6.0-cdh5.5.1/bin/hadoop jar /home/hduser/hadoop-2.6.0-cdh5.5.1/share/hadoop/tools/lib/hadoop-streaming-2.6.0-cdh5.5.1.jar -file dataset/centers.txt -file ./mapper.py -mapper ./mapper.py -file ./reducer.py -reducer ./reducer.py -input /assignment/ -output /assignment/output


