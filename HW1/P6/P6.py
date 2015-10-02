import numpy as np
import matplotlib.pyplot as plt 
import matplotlib.cm as cm
import findspark
import os
findspark.init('/home/serena/spark-1.5.0') # you need that before import pyspark.
import pyspark

sc = pyspark.SparkContext(appName="Spark1")

all_shakespeare_words = sc.textFile("./shakes.txt").map(lambda row: row.split(" "))
all_words = all_shakespeare_words.flatMap(lambda p: p)

all_words = all_words.zipWithIndex()
all_words = all_words.filter(lambda row: not (row[0] == row[0].upper() or row[0].isdigit()) )
print all_words.take(1000)
#print all_shakespeare_words.take(10)