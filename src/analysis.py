from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import desc
from pyspark.mllib.classification import NaiveBayes, NaiveBayesModel
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.feature import IDF

import socket
import time
import datetime
from collections import namedtuple
import urllib.request
import json
import pandas as pd
from pymongo import MongoClient


def tweet_count(lines):
	tweet_cnt = lines.flatMap(lambda line: line.split('---')) \
					.map(lambda line: 1) \
                    .reduce(lambda x,y: x+y)
	tweet_cnt.foreachRDD(lambda x: tweet_cnt_li.extend(x.collect()))

def user_count(lines):
	user_cnt = lines.flatMap(lambda line: line.split('---')) \
				 .map(lambda line: line[:line.find('+++')]) 

	#user_cnt.pprint()	
	user_cnt.foreachRDD(lambda x: user_cnt_li.extend(x.distinct().collect()))	


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
				.map(lambda line: line.replace('+', '')) \
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
   

def classify_tweet(tf):
	return IDF().fit(tf).transform(tf)

def sentiment_analysis(lines, model, hashingTF, iDF):
	analysis = lines.map(lambda line: line.split()) \
					.map(lambda x: hashingTF.transform(x)) \
					.transform(classify_tweet) \
					.map(lambda x: LabeledPoint(1, x)) \
					.map(lambda x: model.predict(x.features)) \
					.reduce(lambda x,y: x+y)

	analysis.foreachRDD(lambda x: pos_cnt_li.extend(x.collect()))	


def data_to_db(db, start_time, counts, user_cnt_len_li, keywords, hashtags, pos, tracking_word, related_keywords_tb):
	# 1) Store counts
	# Complement the counts list
	#if len(counts) < len(start_time):
	#	counts.extend([0]*(len(start_time)-len(counts)))
	counts_t = []
	for i in range(min(len(counts), len(start_time))):
		print(str(start_time[i]))
		time = str(start_time[i]).split(" ")[1]
		time = time.split(".")[0]
		counts_t.append((time, counts[i]))
	counts_df = pd.DataFrame(counts_t, columns=['Time', 'Count'])
	counts_js = json.loads(counts_df.reset_index().to_json(orient='records'))
	#print(counts_t)	
	collection = db['counts']
	db['counts'].insert(counts_js)
	# 2) Store users
	users_df = pd.DataFrame([user_cnt_len_li], columns=['Users'])
	users_js = json.loads(users_df.reset_index().to_json(orient='records'))
	collection = db['users']
	db['users'].insert(users_js)
	# 3) Store time
	time_df = pd.DataFrame([total_time], columns=['Time'])
	time_js = json.loads(time_df.reset_index().to_json(orient='records'))
	collection = db['time']
	db['time'].insert(time_js)
	# 4) Store keywords
	collection = db['keywords']
	if related_keywords_tb:
		db['keywords'].insert(keywords)
	# 5) Store hashtags
	collection = db['hashtags']
	db['hashtags'].insert(hashtags)
	# 6) Store ratio
	whole = sum(counts[:len(pos)])
	pos_whole = sum(pos)
	# Prevent divide 0:
	if whole:
		pos = 1.0*pos_whole/whole	
	else:
		pos = 1
	neg = 1-pos
	ratio_df = pd.DataFrame([(pos, 'P'), (neg, 'N')], columns=['Ratio', 'PN'])
	ratio_js = json.loads(ratio_df.reset_index().to_json(orient='records'))
	collection = db['ratio']
	db['ratio'].insert(ratio_js)
	# 7) Store tracking_word
	tracking_word_df = pd.DataFrame([tracking_word], columns=['Tracking_word'])
	tracking_word_js = json.loads(tracking_word_df.reset_index().to_json(orient='records'))
	collection = db['tracking_word']
	db['tracking_word'].insert(tracking_word_js)

def parseLine(line):
	parts = line.split('\t')
	label = int(parts[0])
	tweet = parts[1]
	# Tokenize
	# Split a document into a collection of words
	words = tweet.strip().split(" ")
	return (label, words)


def main(sc, db, tracking_word):

	print('>'*30+'SPARK START'+'>'*30)

	# MLlib construction
	training_examples = sc.textFile('data/train.txt').map(parseLine)
	# Train/Test split
	training, test = training_examples.randomSplit([0.7, 0.3], seed = 0)

	# TF-IDF
	
	# TF
	# Features will be hashed to indexes
	# And the feature(term) frequencies will be calculated
	hashingTF = HashingTF()
	iDF = IDF()
	# For each training example
	tf_training = training.map(lambda tup: hashingTF.transform(tup[1]))
	# IDF
	# Compute the IDF vector
	idf_training = IDF().fit(tf_training)
	# Scale the TF by IDF
	tfidf_training = idf_training.transform(tf_training)

	# (SparseVector(1048576, {110670: 1.5533, ...), 0)
	tfidf_idx = tfidf_training.zipWithIndex()
	# (['The', 'Da', 'Vinci', 'Code', 'book', 'is', 'just', 'awesome.'], 0)
	training_idx = training.zipWithIndex()

	# Reverse the index and the SparseVector
	idx_training = training_idx.map(lambda line: (line[1], line[0]))
	idx_tfidf = tfidf_idx.map(lambda l: (l[1], l[0]))
	#print(idx_training.take(10))

	# rdd.join: (K,V).join(K,W) -> (K, (V,W)) 
	# idx_tfidf has no info about lables(0/1)
	# but idx_training has
	joined_tfidf_training = idx_training.join(idx_tfidf)
	training_labeled = joined_tfidf_training.map(lambda tup: tup[1])
	training_labeled = training_labeled.map(lambda x: LabeledPoint(x[0][0], x[1]))
	#print(training_labeled.take(10))
	# Train a naive Bayes model
	model = NaiveBayes.train(training_labeled, 1.0)

	# Test the model
	tf_test = test.map(lambda tup: hashingTF.transform(tup[1]))
	idf_test = IDF().fit(tf_test)

	tfidf_test = idf_test.transform(tf_test)
	tfidf_idx = tfidf_test.zipWithIndex()
	test_idx = test.zipWithIndex()
	idx_test = test_idx.map(lambda line: (line[1], line[0]))
	idx_tfidf = tfidf_idx.map(lambda l: (l[1], l[0]))
	joined_tfidf_test = idx_test.join(idx_tfidf)

	test_labeled = joined_tfidf_test.map(lambda tup: tup[1])
	labeled_test_data = test_labeled.map(lambda k: LabeledPoint(k[0][0], k[1]))
	# Apply the trained model on Test data
	predictionAndLabel = labeled_test_data.map(lambda p : (model.predict(p.features), p.label))
	#print(predictionAndLabel.take(10))

	# Calculate the accuracy
	accuracy = 1.0 * predictionAndLabel.filter(lambda x: x[0] == x[1]).count() / labeled_test_data.count()

	#print('>>> Model accuracy:')
	#print(accuracy)

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

	# 2) Count the number of users
	user_count(lines)

	# 3) Find the related keywords
	related_keywords(lines)

	# 4) Find the related hashtags
	related_hashtags(lines)

	# 5) Sentiment analysis
	sentiment_analysis(lines, model, hashingTF, iDF)
	
	###########################################################################
	# Start the streaming process
	ssc.start()

	process_cnt = 0
	start_time = [datetime.datetime.now()]

	while process_cnt < process_times:
		time.sleep(window_time)			
		start_time.append(datetime.datetime.now())	
		# Find the top related keywords
		if len(sqlContext.tables().filter("tableName LIKE 'related_keywords_tmp'").collect()) == 1:
			top_words = sqlContext.sql( 'Select Keyword, Count from related_keywords_tmp' )		
			related_keywords_df = related_keywords_df.unionAll(top_words)
			related_keywords_tb = True
		else:
			related_keywords_tb = False

		# Find the top related hashtags
		if len(sqlContext.tables().filter("tableName LIKE 'related_hashtags_tmp'").collect()) == 1:
			top_hashtags = sqlContext.sql( 'Select Hashtag, Count from related_hashtags_tmp' )	
			related_hashtags_df = related_hashtags_df.unionAll(top_hashtags)


		process_cnt += 1
		
	# Final tables	
	if related_keywords_tb:
		related_keywords_df = related_keywords_df.filter(related_keywords_df['Keyword'] != 'none')	
		# Spark SQL to Pandas Dataframe
		related_keywords_pd = related_keywords_df.toPandas()
		related_keywords_pd = related_keywords_pd[related_keywords_pd['Keyword'] != tracking_word]
		related_keywords_pd = related_keywords_pd.groupby(related_keywords_pd['Keyword']).sum()
		related_keywords_pd = pd.DataFrame(related_keywords_pd)
		related_keywords_pd = related_keywords_pd.sort("Count", ascending=0).iloc[0:min(9, related_keywords_pd.shape[0])]	

	# Spark SQL to Pandas Dataframe
	related_hashtags_pd = related_hashtags_df.toPandas()
	related_hashtags_pd = related_hashtags_pd[related_hashtags_pd['Hashtag'] != '#'+tracking_word] 
	related_hashtags_pd = related_hashtags_pd.groupby(related_hashtags_pd['Hashtag']).sum()
	related_hashtags_pd = pd.DataFrame(related_hashtags_pd)
	related_hashtags_pd = related_hashtags_pd.sort("Count", ascending=0).iloc[0:min(9, related_hashtags_pd.shape[0])]

	ssc.stop()
	###########################################################################

	print(">>>tweet_cnt_li:")
	print(tweet_cnt_li)
	print(">>>user_cnt_li:")
	user_cnt_len_li = len(user_cnt_li)
	print(user_cnt_len_li)
	print(">>>start_time:")
	print(start_time)
	print(">>>pos_cnt_li")
	print(pos_cnt_li)
	print(">>>related_keywords_tb")
	print(related_keywords_tb)
	#print(related_keywords_pd.head(10))
	#print(related_hashtags_pd.head(10))
	if related_keywords_tb:
		related_keywords_js = json.loads(related_keywords_pd.reset_index().to_json(orient='records'))
	else:
		related_keywords_js = None
	#print(related_keywords_js)
	related_hashtags_js = json.loads(related_hashtags_pd.reset_index().to_json(orient='records'))
	#print(related_hashtags_js)

	# Store the data to MongoDB
	data_to_db(db, start_time, tweet_cnt_li, user_cnt_len_li, related_keywords_js, related_hashtags_js, pos_cnt_li, tracking_word, related_keywords_tb)

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
	user_cnt_li = []
	pos_cnt_li = []
	# Load parameters
	with open('conf/parameters.json') as f:
		p = json.load(f)
		tracking_word = p['hashtag'][1:]
		batch_interval = int(p['DStream']['batch_interval'])
		window_time = int(p['DStream']['window_time'])
		process_times = int(p['DStream']['process_times'])
	# Compute the whole time for displaying
	total_time = batch_interval * process_times
	# Connect to the running mongod instance
	conn = MongoClient()
	# Switch to the database
	db = conn['twitter']
	db['counts'].drop()
	db['users'].drop()
	db['time'].drop()
	db['keywords'].drop()
	db['hashtags'].drop()
	db['ratio'].drop()
	db['tracking_word'].drop()
	# Execute main function
	main(sc, db, tracking_word)

