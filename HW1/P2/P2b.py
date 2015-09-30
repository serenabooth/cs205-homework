from P2 import *
import findspark
import os
findspark.init('/home/serena/spark-1.5.0') # you need that before import pyspark.
import pyspark

def mandel(x, y):
    z = c = complex(x, y)
    iteration = 0
    max_iteration = 511  # arbitrary cutoff
    while abs(z) < 2 and iteration < max_iteration:
        z = z * z + c
        iteration += 1
    return iteration

sc = pyspark.SparkContext(appName="Spark1")

imData = sc.parallelize(([i,j] for i in range(2000) for j in range(2000)),100)
imDataRepartitioned = imData.repartition(100)

imDataWithMandlebrot = imDataRepartitioned.map(lambda row: [row, mandel(row[1]/500.0 - 2, row[0]/500.0 -2)])

draw_image((imDataWithMandlebrot))

plt.figure()
plt.title("100 Random Partitions")
plt.hist(sum_values_for_partitions((imDataWithMandlebrot)).collect())
plt.show()