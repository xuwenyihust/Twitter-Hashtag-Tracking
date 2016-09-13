from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import desc
import socket
import time

def main(sc):
	print('>'*30+'SPARK START'+'>'*30)
	# Initialize sparksql context
	# Will be used to query the trends from the result.
	sqlContext = SQLContext(sc)
	# Initialize spark streaming context with a batch interval of 10 sec, 
	# The messages would accumulate for 10 seconds and then get processed.
	ssc = StreamingContext(sc, 10)

	# Receive the tweets
	#host = socket.gethostname()
	host = socket.gethostbyname(socket.gethostname())
	socket_stream = ssc.socketTextStream(host, 5555)
	# Create a window
	lines = socket_stream.window( 20 )

	# Create a namedtuple
	from collections import namedtuple
	fields = ("tag", "count" )
	Tweet = namedtuple( 'Tweet', fields )

	# Process the stream
	( lines.flatMap( lambda text: text.split( " " ) )
		.filter( lambda word: word.lower().startswith("#") )
		.map( lambda word: ( word.lower(), 1 ) )
		.reduceByKey( lambda a, b: a + b )
		.map( lambda rec: Tweet( rec[0], rec[1] ) )
		.foreachRDD( lambda rdd: rdd.toDF().sort( desc("count") )
		.limit(10).registerTempTable("tweets") ) )

	ssc.start()  
	time.sleep(10)
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

