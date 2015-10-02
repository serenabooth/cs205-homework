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

def p (i):
    print i 

input_marvel_data = input_marvel_data.map(lambda row: (row.split(',"')[0],row.split(',"')[1]))
reverse_marvel_data = input_marvel_data.map(lambda (K, V): (V,[K]))

grouped_by_episode = reverse_marvel_data.reduceByKey(lambda x,y: [] + x + y )

adj_list_representation = reverse_marvel_data.join(grouped_by_episode).values()
print adj_list_representation.count()

#search_character_set = [u'"JONES, CHARLOTTE"']


#for i in range(0,2):
#    for character in search_character_set:
#        filtered = grouped_by_episode.filter(lambda (row, indx): character in row[1]).map(lambda (row, indx): (row, min(i,i+1)))
#        character_set = filtered.map(lambda (row, indx): row[1]).toArray()

#    print filtered.take(100)

#print grouped_by_episode.take(10)


#all_characters = grouped_by_episode.flatMap(lambda (key,val): (str(val[0:1]), val[1:]))
#all_characters = input_marvel_data.map(lambda row: row[0]).cartesian(all_characters)
#all_characters = all_characters.filter(lambda (K,V): K in V).reduceByKey(lambda x,y: y, 100)
#print all_characters.take(4)
