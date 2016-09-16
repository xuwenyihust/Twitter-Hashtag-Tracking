from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import desc
import socket
import time
from collections import namedtuple
import urllib.request
import json

def main(sc):

	print('>'*30+'SPARK START'+'>'*30)

	batch_interval = 10
	process_times = 1
	window_time = 20

	# Initialize sparksql context
	# Will be used to query the trends from the result.
	sqlContext = SQLContext(sc)
	# Initialize spark streaming context with a batch interval of 10 sec, 
	# The messages would accumulate for 10 seconds and then get processed.
	ssc = StreamingContext(sc, batch_interval)

	# Receive the tweets	
	host = socket.gethostbyname(socket.gethostname())
	# Create a DStream that represents streaming data from TCP source
	socket_stream = ssc.socketTextStream(host, 5555)
	lines = socket_stream.window(window_time)

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

	keywords = lines\
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
				.map(lambda line: line.replace('-', '')) \
				.map(lambda line: line.replace('\\', '')) \
				.map(lambda line: line.replace('/', '')) \
				.flatMap(lambda line: line.split()) \
				.map(lambda word: word.lower()) \
				.map(lambda word: word if word not in stop_words else 'none') \
				.map(lambda word: (word, 1)) \
				.reduceByKey(lambda x, y: x+y)
				
	keywords.foreachRDD(lambda x: x.toDF(['Keyword', 'Count']).sort(desc('Count')).limit(100).registerTempTable("related_keywords"))

	# 3) Find the related hashtags
	hashtags = lines.flatMap(lambda line: line.split()) \
					.map(lambda line: line.replace(',', '')) \
					.map(lambda line: line.replace('.', '')) \
					.map(lambda line: line.replace('!', '')) \
					.map(lambda line: line.replace('?', '')) \
					.filter(lambda word: word.startswith('#')) \
					.map(lambda word: (word.lower(), 1)) \
					.reduceByKey(lambda x, y: x+y)
	
	hashtags.foreachRDD(lambda x: x.toDF(['Hashtag', 'Count']).sort(desc('Count')).limit(100).registerTempTable("related_hashtags"))

	# Start the streaming process
	ssc.start()
	time.sleep(window_time*2)
	#ssc.awaitTermination()
	#ssc.stop()

	top_words = sqlContext.sql( 'Select Keyword, Count from related_keywords' )	
	top_words_df = top_words.toPandas()
	top_words_df = top_words_df[top_words_df['Keyword'] != 'none']
	print(top_words_df.head(10))

	top_hashtags = sqlContext.sql( 'Select Hashtag, Count from related_hashtags' )
	top_hashtags_df = top_hashtags.toPandas()	
	print(top_hashtags_df.head(10))

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

