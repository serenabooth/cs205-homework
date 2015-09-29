from P2 import *
import findspark
import os
findspark.init('/home/serena/spark-1.5.0') # you need that before import pyspark.
import pyspark 

# Your code here
sc = pyspark.SparkContext(appName="Spark1")
#imRep = np.array([2000,2000])
imData = sc.parallelize([[i,j],0] for i in range(2000) for j in range(2000))
draw_image(imData)
#while(True):
#    draw_image(imData)
#while(True):
#    print "hi"