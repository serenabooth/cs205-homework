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

reverse_marvel_data = input_marvel_data.map(lambda row: (row[1], [row[0]]))
grouped_by_episode = reverse_marvel_data.reduceByKey(lambda x,y: x + y)

all_characters = grouped_by_episode.map(lambda row: tuple(row[1]))
print all_characters.take(10)

#print input_marvel_data.collect()