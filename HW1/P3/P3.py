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

    return sorted(letter_array)

sc = pyspark.SparkContext(appName="Spark1")

all_data = sc.textFile("./LF_delim/")
all_data = all_data.repartition(100)

all_data = all_data.map(lambda row: (sequence(row), row))
print all_data.take(10)