from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

sparkConf = SparkConf().setAppName("Cookies").setMaster("local")
sc = SparkContext(conf=sparkConf)

#add this if you are dealing with files...
sqc = SQLContext(sc)

rdd = sqc.read.csv("customercookies.csv", header=False).rdd
print(rdd.take(5))

rawesome = rdd.map(lambda row: (row[1], row[3])).\
            filter(lambda row: row[1] == "'awesome‘").\
            map(lambda row: (row[0],float(1))).\
            reduceByKey(lambda first, second: first+second)
			

print(rawesome.take(10))

rawesome2 = rdd.map(lambda row: (row[1], row[2], row[3])).\
                filter(lambda row: row[2] == "'awesome‘").\
                map(lambda row: (row[0], float(row[1]))).\
                reduceByKey(lambda first, second: first+second)

				
print(rawesome2.take(10))

rawesome3 = rawesome.join(rawesome2).map(lambda row: (row[0], (row[1])[1]/(row[1])[0] ))

print(rawesome3.take(10))