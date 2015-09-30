from P2 import *
import findspark
import os
findspark.init('/home/serena/spark-1.5.0') # you need that before import pyspark.
import pyspark

sc = pyspark.SparkContext(appName="Spark1")

imData = sc.parallelize(([[i,j],mandelbrot(j/500.0 - 2, i/500.0 -2)]
             for i in range(2000) for j in range(2000)), 100)
draw_image(imData)

plt.figure()
plt.title("100 Default Partitions")
plt.hist(sum_values_for_partitions(imData).collect())
plt.show()

#while(True):
#    draw_image(imData)
#while(True):
#    print "hi"