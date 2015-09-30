from __future__ import print_function 

from P2 import *
import findspark
import os
findspark.init('/home/serena/spark-1.5.0') # you need that before import pyspark.
import pyspark

# Your code here
sc = pyspark.SparkContext(appName="Spark1")

imData = sc.parallelize(([[i,j],mandelbrot(j/500.0 - 2, i/500.0 -2)] for i in range(2000) for j in range(2000)),100)
#imData.foreach(lambda t : mandlebrot(t[0][1]/500.0 - 2, t[0][0]/500.0 - 2))
draw_image(imData)
#cite: http://stackoverflow.com/questions/25295277/view-rdd-contents-in-python-spark
#sum_values_for_partitions(imData).foreach(print)

#while(True):
#    draw_image(imData)
#while(True):
#    print "hi"