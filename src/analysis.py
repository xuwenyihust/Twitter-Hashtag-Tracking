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
import pandas as pd
from pymongo import MongoClient


def tweet_count(lines):
	tweet_cnt = lines.map(lambda line: 1) \
                    .reduce(lambda x,y: x+y)
	tweet_cnt.foreachRDD(lambda x: tweet_cnt_li.extend(x.collect()))


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
	
	keywords.foreachRDD(lambda x: x.toDF(['Keyword', 'Count']).sort(desc('Count')).limit(100).registerTempTable("related_keywords_tmp"))



def related_hashtags(lines):
	hashtags = lines.flatMap(lambda line: line.split()) \
                    .map(lambda line: line.replace(',', '')) \
                    .map(lambda line: line.replace('.', '')) \
                    .map(lambda line: line.replace('!', '')) \
                    .map(lambda line: line.replace('?', '')) \
                    .filter(lambda word: word.startswith('#')) \
                    .map(lambda word: (word.lower(), 1)) \
                    .reduceByKey(lambda x, y: x+y)

	hashtags.foreachRDD(lambda x: x.toDF(['Hashtag', 'Count']).sort(desc('Count')).limit(100).registerTempTable("related_hashtags_tmp"))
   

def data_to_db(db, start_time, counts, keywords, hashtags):
	# Store counts
	counts = json.dumps(counts)
	print(start_time)
	print(counts)
	# Store keywords
	collection = db['keywords']
	db['keywords'].insert(keywords)
	# Store hashtags
	collection = db['hashtags']
	db['hashtags'].insert(hashtags)
	'''cursor = db['keywords'].find()
	for document in cursor:
		print(document)
	'''


def main(sc, db):

	#print('>'*30+'SPARK START'+'>'*30)

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

	# Construct tables
	tmp = [('none', 0)]
	related_keywords_df = sqlContext.createDataFrame(tmp, ['Keyword', 'Count'])
	related_hashtags_df = sqlContext.createDataFrame(tmp, ['Hashtag', 'Count'])

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
		start_time = datetime.datetime.now()	
		print('Count:')
		print(tweet_cnt_li)		
	
		# Find the top related keywords
		if len(sqlContext.tables().filter("tableName LIKE 'related_keywords_tmp'").collect()) == 1:
			top_words = sqlContext.sql( 'Select Keyword, Count from related_keywords_tmp' )		
			related_keywords_df = related_keywords_df.unionAll(top_words)

		# Find the top related hashtags
		if len(sqlContext.tables().filter("tableName LIKE 'related_hashtags_tmp'").collect()) == 1:
			top_hashtags = sqlContext.sql( 'Select Hashtag, Count from related_hashtags_tmp' )	
			related_hashtags_df = related_hashtags_df.unionAll(top_hashtags)


		process_cnt += 1
		
	# Final tables	
	related_keywords_df = related_keywords_df.filter(related_keywords_df['Keyword'] != 'none')	
	# Spark SQL to Pandas Dataframe
	related_keywords_pd = related_keywords_df.toPandas()
	related_keywords_pd = related_keywords_pd.groupby(related_keywords_pd['Keyword']).sum()
	related_keywords_pd = pd.DataFrame(related_keywords_pd)
	related_keywords_pd = related_keywords_pd.sort("Count", ascending=0).iloc[0:9]

	# Spark SQL to Pandas Dataframe
	related_hashtags_pd = related_hashtags_df.toPandas() 
	related_hashtags_pd = related_hashtags_pd.groupby(related_hashtags_pd['Hashtag']).sum()
	related_hashtags_pd = pd.DataFrame(related_hashtags_pd)
	related_hashtags_pd = related_hashtags_pd.sort("Count", ascending=0).iloc[0:9]	

	ssc.stop()
	###########################################################################


	print(related_keywords_pd.head(10))
	print(related_hashtags_pd.head(10))
	related_keywords_js = json.loads(related_keywords_pd.reset_index().to_json(orient='records'))
	#print(related_keywords_js)
	related_hashtags_js = json.loads(related_hashtags_pd.reset_index().to_json(orient='records'))
	#print(related_hashtags_js)

	# Store the data to MongoDB
	data_to_db(db, start_time, tweet_cnt_li, related_keywords_js, related_hashtags_js)

	#print('>'*30+'SPARK STOP'+'>'*30)


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
	with open('conf/parameters.json') as f:
		p = json.load(f)
		batch_interval = int(p['DStream']['batch_interval'])
		window_time = int(p['DStream']['window_time'])
		process_times = int(p['DStream']['process_times'])
	# Connect to the running mongod instance
	conn = MongoClient()
	# Switch to the database
	db = conn['twitter']
	db['counts'].drop()
	db['keywords'].drop()
	db['hashtags'].drop()
	# Execute main function
	main(sc, db)

