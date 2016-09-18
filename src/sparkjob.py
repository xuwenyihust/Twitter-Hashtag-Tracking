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


def tweet_count(lines):
	tweet_cnt = lines.map(lambda line: 1) \
                    .reduce(lambda x,y: x+y)
	tweet_cnt.foreachRDD(lambda x: tweet_cnt_li.append(x.collect()))


def related_keywords(lines):
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


def related_hashtags(lines):
	hashtags = lines.flatMap(lambda line: line.split()) \
                    .map(lambda line: line.replace(',', '')) \
                    .map(lambda line: line.replace('.', '')) \
                    .map(lambda line: line.replace('!', '')) \
                    .map(lambda line: line.replace('?', '')) \
                    .filter(lambda word: word.startswith('#')) \
                    .map(lambda word: (word.lower(), 1)) \
                    .reduceByKey(lambda x, y: x+y)

	hashtags.foreachRDD(lambda x: x.toDF(['Hashtag', 'Count']).sort(desc('Count')).limit(100).registerTempTable("related_hashtags"))
    


def main(sc):

	print('>'*30+'SPARK START'+'>'*30)

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
	tweet_count(lines)

	# 2) Find the related keywords
	related_keywords(lines)

	# 3) Find the related hashtags
	related_hashtags(lines)
	
	###########################################################################
	# Start the streaming process
	ssc.start()

	process_cnt = 0

	while process_cnt < process_times:
		time.sleep(window_time)
		print('Count:')
		print(tweet_cnt_li)
		#print('Tables:')
		#print(sqlContext.tables().collect())
	
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
	###########################################################################

	print('>'*30+'SPARK STOP'+'>'*30)


if __name__=="__main__":
	# Define Spark configuration
	conf = SparkConf()
	conf.setMaster("local[4]")
	conf.setAppName("Twitter-Hashtag-Tracking")
	# Initialize a SparkContext
	sc = SparkContext(conf=conf)
	# Initialize sparksql context
    # Will be used to query the trends from the result.
	sqlContext = SQLContext(sc)
	# Initialize the tweet_cnt_li
	tweet_cnt_li = []
	# Load parameters
	with open('parameters.json') as f:
		p = json.load(f)
		batch_interval = int(p['DStream']['batch_interval'])
		window_time = int(p['DStream']['window_time'])
		process_times = int(p['DStream']['process_times'])
	# Execute main function
	main(sc)

