from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import desc
import socket
import time
from collections import namedtuple
import urllib
import json

def main(sc):

	print('>'*30+'SPARK START'+'>'*30)

	batch_interval = 30
	process_times = 1

	# Initialize sparksql context
	# Will be used to query the trends from the result.
	sqlContext = SQLContext(sc)
	# Initialize spark streaming context with a batch interval of 10 sec, 
	# The messages would accumulate for 10 seconds and then get processed.
	ssc = StreamingContext(sc, batch_interval)

	# Receive the tweets	
	host = socket.gethostbyname(socket.gethostname())
	# Create a DStream that represents streaming data from TCP source
	lines = ssc.socketTextStream(host, 5555)

	# Process the stream	
	'''words = lines.flatMap(lambda line: line.split(" "))
	pairs = words.map(lambda word: (word, 1))
	wordCounts = pairs.reduceByKey(lambda x, y: x + y)
	wordCounts.pprint()'''
	# 1) Count the number of tweets
	tweet_count = lines.count()
	tweet_count.pprint()
	# 2) Find the related keywords
	# Get the stop words
	stop_words_url = 'https://raw.githubusercontent.com/6/stopwords-json/master/dist/en.json'
	stop_words_json = urllib.request.urlopen(stop_words_url).read()
	stop_words_decoded = stop_words_json.decode('utf8')
	# Load the stop words into a list
	stop_words = json.loads(stop_words_decoded)	
	# Create a nametuple
	fields = ("keyword", "count" )
	Tweet = namedtuple( 'Tweet', fields )

	words = lines\
				.map(lambda line: line.replace(',', '')) \
				.map(lambda line: line.replace('.', '')) \
				.map(lambda line: line.replace('!', '')) \
				.map(lambda line: line.replace('?', '')) \
				.map(lambda line: line.replace(':', '')) \
				.map(lambda line: line.replace(';', '')) \
				.map(lambda line: line.replace('"', '')) \
				.map(lambda line: line.replace('@', '')) \
				.map(lambda line: line.replace('&', '')) \
				.map(lambda line: line.replace('(', '')) \
				.map(lambda line: line.replace(')', '')) \
				.map(lambda line: line.replace('#', '')) \
				.map(lambda line: line.replace('.', '')) \
				.map(lambda line: line.replace('\\', '')) \
				.map(lambda line: line.replace('/', '')) \
				.flatMap(lambda line: line.split()) \
				.map(lambda word: word if word in stop_words else 'None') \
				.map(lambda word: (word.lower(), 1)) \
				.reduceByKey(lambda x, y: x+y)
				#.map(lambda word: Tweet( word[0], word[1] ))

	words.pprint()
	#words.saveAsTextFiles('Test')
	words.foreachRDD(lambda x: x.toDF(['Keyword', 'Count']).limit(10).registerTempTable("tweets"))

	ssc.start()
	time.sleep((batch_interval+1)*process_times)
	#ssc.awaitTermination()
	#ssc.stop()

	#time.sleep((batch_interval+1)*process_times + 10)
	top_10_words = sqlContext.sql( 'Select Keyword, Count from tweets' )
	top_10_df = top_10_words.toPandas()
	print(top_10_df.head())

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

