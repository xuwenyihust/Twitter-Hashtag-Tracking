from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import desc
import socket
import time
from collections import namedtuple

def main(sc):
	print('>'*30+'SPARK START'+'>'*30)
	# Initialize sparksql context
	# Will be used to query the trends from the result.
	sqlContext = SQLContext(sc)
	# Initialize spark streaming context with a batch interval of 10 sec, 
	# The messages would accumulate for 10 seconds and then get processed.
	ssc = StreamingContext(sc, 10)

	# Receive the tweets	
	host = socket.gethostbyname(socket.gethostname())
	# Create a DStream that represents streaming data from TCP source
	lines = ssc.socketTextStream(host, 5555)
	# Create a window
	# The RDD will be created for every 10 seconds, but the data in RDD will be for the last 20 seconds
	#lines = socket_stream.window( 20 )

	# Process the stream
	'''
	lines.flatMap( lambda text: text.split( " " ) ) \
		.filter( lambda word: word.lower().startswith("#") ) \
		.map( lambda word: ( word.lower(), 1 ) ) \
		.reduceByKey( lambda a, b: a + b ) \
		.map( lambda rec: Tweet( rec[0], rec[1] ) ) \
		.foreachRDD( lambda rdd: rdd.toDF().sort( desc("count") ) \
		.limit(10).register	Tweet = namedtuple( 'Tweet', fields ))
	'''
	
	words = lines.flatMap(lambda line: line.split(" "))
	pairs = words.map(lambda word: (word, 1))
	wordCounts = pairs.reduceByKey(lambda x, y: x + y)
	wordCounts.pprint()
	print(wordCounts)

	ssc.start()
	time.sleep(50)
	#ssc.awaitTermination()
	ssc.stop()
	print('>'*30+'SPARK STOP'+'>'*30)


if __name__=="__main__":
	# Define Spark configuration
	conf = SparkConf()
	conf.setMaster("local[4]")
	conf.setAppName("Twitter-Hashtag-Tracking")
	# Initialize a SparkContext
	sc = SparkContext(conf=conf)
	# Execute main function
	main(sc)

