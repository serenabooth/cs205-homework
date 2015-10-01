#from __future__ import print_function
import numpy as np
import matplotlib.pyplot as plt 
import matplotlib.cm as cm
import findspark
import os
findspark.init('/home/serena/spark-1.5.0') # you need that before import pyspark.
import pyspark

def sequence(word):
    letter_array = []
    for i in range(0,len(word)):
        letter_array += word[i:i+1]

    return str(sorted(letter_array))

sc = pyspark.SparkContext(appName="Spark1")

all_data = sc.textFile("./LF_delim/")

#words.map(lambda x: (x, 1)).reduceByKey(lambda x,y:x+y).map(lambda x:(x[1],x[0])).sortByKey(False)
all_data = all_data.map(lambda row: (sequence(row), [row]),100)

all_data = all_data.reduceByKey(lambda x,y: x + y)
all_data = all_data.map(lambda row: (row[0], len(row[1]), row[1]))
print all_data.takeOrdered(10, key = lambda row: -row[1])