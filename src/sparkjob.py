from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import desc
import socket
import time
import datetime
from collections import namedtuple
import urllib.request
import json

def main(sc):

	print('>'*30+'SPARK START'+'>'*30)

	batch_interval = 10
	window_time = 10

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
	'''tweet_cnt = tweet_count.map(lambda x: (datetime.time, x)) \
					.foreachRDD(lambda x: x.toDF(['Time', 'Count']).registerTempTable("tweet_count"))'''
	tweet_cnt = lines.map(lambda line: 1) \
					.reduce(lambda x,y: x+y) 
	
	tweet_cnt_li = []				
	tweet_cnt.foreachRDD(lambda x: tweet_cnt_li.append(x.collect()))

	tweet_cnt.pprint()




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

	process_cnt = 0
	#tweet_cnt_li = []
	while process_cnt < 2:
		time.sleep(window_time+5)
		print('Count:')
		print(tweet_cnt_li)
		print('Tables:')
		print(sqlContext.tables().collect())
		# The counts of tweets at different times
		if len(sqlContext.tables().filter("tableName LIKE '%tweet_count%'").collect()) == 1:

			t_count = sqlContext.sql( 'Select Time, Count from tweet_count' )
			t_count_df = t_count.toPandas()
			print(t_count_df.head())
		else:
			print('Currently no table tweet_count.')
		# Find the top related keywords
		if len(sqlContext.tables().filter("tableName LIKE '%related_keywords%'").collect()) == 1:
			top_words = sqlContext.sql( 'Select Keyword, Count from related_keywords' )	
			top_words_df = top_words.toPandas()
			top_words_df = top_words_df[top_words_df['Keyword'] != 'none']
			print(top_words_df.head(10))
		else:
			print('Currently no table related_keywords.')

		# Find the top related hashtags
		if len(sqlContext.tables().filter("tableName LIKE '%related_hashtags%'").collect()) == 1:
			top_hashtags = sqlContext.sql( 'Select Hashtag, Count from related_hashtags' )
			top_hashtags_df = top_hashtags.toPandas()	
			print(top_hashtags_df.head(10))
		else:
			print('Currently no table related_hashtags.')

		process_cnt += 1



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

