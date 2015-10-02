#from __future__ import print_function
import numpy as np
import matplotlib.pyplot as plt 
import matplotlib.cm as cm
import findspark
import os
findspark.init('/home/serena/spark-1.5.0') # you need that before import pyspark.
import pyspark

sc = pyspark.SparkContext(appName="Spark1")

input_marvel_data = sc.textFile("./source.csv")
#print input_marvel_data.take(10)

input_marvel_data = input_marvel_data.map(lambda row: (row.split(',"')[0],row.split(',"')[1]))

reverse_marvel_data = input_marvel_data.map(lambda (K, V): (V,[K]))

grouped_by_episode = reverse_marvel_data.reduceByKey(lambda x,y: [] + x + y )
print grouped_by_episode.take(10)

#all_characters = grouped_by_episode.flatMap(lambda (key,val): (val[1],val[2:]))
#print all_characters.take(1)
#print "\n"
#print all_characters.take(2)

#print input_marvel_data.collect()