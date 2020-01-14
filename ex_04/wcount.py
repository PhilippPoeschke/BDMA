from pyspark import SparkConf, SparkContext

# Configure the Spark environment
sparkConf = (SparkConf()
             .setAppName("WordCount")
             .setMaster("local"))
sc = SparkContext(conf=sparkConf)

words2 = sc.parallelize("How much ground would a groundhog hog if a groundhog could hog ground A groundhog would hog all the ground he could hog if a groundhog could hog ground".lower().split(" ")) #makes an RDD from a given list 
wordsMapped2 = words2.map(lambda word: (word,1)) #assign key to each word 
wordCount2 = wordsMapped2.reduceByKey(lambda c1, c2: c1 + c2) #return RDD object 
print('Number of occurences per word: ', wordCount2.collect())