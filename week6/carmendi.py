from pyspark import SparkContext, SparkConf
import os
import sys

dataset = "/wrk/users/carmendi/data-1.txt"

conf = (SparkConf()
        .setAppName("carmendi") # change app name to your username
        .setMaster("spark://128.214.48.205:7077") # change according to the ip from ifconfig
        .set("spark.cores.max", "10")
        .set("spark.rdd.compress", "true")
        .set("spark.broadcast.compress", "true"))
sc = SparkContext(conf=conf)

data = sc.textFile(dataset)
data = data.map(lambda s: float(s))

count = data.count()
sum = data.sum()
mean = sum/count
max = data.max()
min = data.min()

print "Count = %.8f" % count
print "Sum = %.8f" % sum
print "Mean = %.8f" % mean
print "Min = %.8f" % min
print "Max = %.8f" % max
